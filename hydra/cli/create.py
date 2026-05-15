from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import typer
from rich.console import Console

from hydra import executor, http, planner
from hydra import journal as journal_mod
from hydra import providers as providers_mod
from hydra.cli import _common, app
from hydra.cli._render import _render_api_error, _render_retry_footer
from hydra.config import Config, HostSpec
from hydra.errors import HydraAPIError
from hydra.hostspec_utils import match_fork
from hydra.providers.base import MirrorSource, PrimaryMirror, RepoRef
from hydra.wizard import CreateOptions, WizardCancelled, run_create_wizard


@app.command()
def create(
    name: Optional[str] = typer.Argument(
        None, help="Repository name. Omit to launch the interactive wizard."
    ),
    description: str = typer.Option("", "--description", "-d"),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Group path on the primary host"
    ),
    public: bool = typer.Option(False, "--public", help="Create as public (default: private)"),
    host_option: List[str] = typer.Option(
        [],
        "--host-option",
        help="Override per-host option: `host_id.key=value` (repeatable). "
        "Value is YAML-parsed (booleans/null/ints supported). "
        "Example: --host-option github.org=acme",
    ),
    no_mirror: bool = typer.Option(False, "--no-mirror", help="Skip push-mirror setup"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print planned actions without making API calls"
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Skip the confirmation before applying the plan."
    ),
    skip_preflight: bool = typer.Option(
        False,
        "--skip-preflight",
        help="Skip the pre-mutation token-scope probe. Faster but a "
        "wrong-scope token may orphan groups/repos before failing.",
    ),
    adopt_existing: bool = typer.Option(
        False,
        "--adopt-existing",
        help="If the repo already exists on the primary but the journal "
        "has no record, adopt it without prompting.",
    ),
    no_probe: bool = typer.Option(
        False,
        "--no-probe",
        help="Skip the pre-create existence probe. May lead to 409 errors "
        "on re-runs; only use for very large fan-outs where the extra GETs hurt.",
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Create a repo on the primary host and mirror to all forks."""
    console = Console()
    cfg = _common._load_or_die(config_path, console)
    cfg = _common._apply_overrides(cfg, _common._parse_host_options(host_option))

    if name is None:
        try:
            opts = run_create_wizard(cfg=cfg, console=console)
        except WizardCancelled as e:
            console.print(f"\n[yellow]Cancelled:[/yellow] {e or 'aborted'}")
            raise typer.Exit(code=1) from None
        except KeyboardInterrupt:
            console.print("\n[yellow]Cancelled:[/yellow] aborted")
            raise typer.Exit(code=1) from None
        if dry_run:
            opts.dry_run = True
    else:
        opts = CreateOptions(
            name=name,
            description=description,
            group=group if group is not None else cfg.defaults.group,
            is_private=False if public else cfg.defaults.private,
            mirror=not no_mirror,
            dry_run=dry_run,
        )

    plan = planner.plan_create(cfg, opts)

    # Token resolution + preflight are Phase 7 concerns; the existence
    # probe is Phase 6. Keep the two switches (--skip-preflight, --no-probe)
    # independent so disabling one doesn't silently disable the other.
    tokens = _common._resolve_tokens_or_die(cfg, console=console)
    if not skip_preflight:
        _common._preflight_or_die(cfg=cfg, tokens=tokens, console=console)

    if not no_probe:
        existing_repos, existing_mirrors = _probe_existing_state(
            cfg=cfg, opts=opts, tokens=tokens, console=console
        )
        _handle_existing_state(
            cfg=cfg,
            opts=opts,
            existing_repos=existing_repos,
            adopt_existing=adopt_existing,
            dry_run=opts.dry_run,
            console=console,
        )
        plan = planner.plan_create_with_existing(
            plan, existing_repos=existing_repos, existing_mirrors=existing_mirrors
        )

    planner.render_plan(plan, console, dry_run=opts.dry_run, title=f"Create '{opts.name}'")

    if opts.dry_run:
        return

    if not yes and not typer.confirm(f"Apply {len(plan.actions)} action(s)?", default=False):
        console.print("[dim]No changes made.[/dim]")
        return

    # CLI already resolved tokens and ran preflight above; pass both forward
    # so _execute_create doesn't duplicate either.
    _execute_create(
        cfg=cfg,
        opts=opts,
        verbose=verbose,
        console=console,
        skip_preflight=True,
        plan_override=plan,
        tokens_override=tokens,
    )


def _execute_create(
    *,
    cfg: Config,
    opts: CreateOptions,
    verbose: bool,
    console: Console,
    skip_preflight: bool = False,
    plan_override: Optional[planner.Plan] = None,
    tokens_override: Optional[Dict[str, str]] = None,
) -> None:
    """Build a create plan and apply it. Token resolution + error rendering
    happen here so the CLI command stays focused on plan/render/confirm.

    Kept as a public-by-convention helper so tests can drive the apply
    without going through the CLI prompts. The ``create`` command passes
    ``plan_override`` (the adoption-aware transformed plan) and
    ``tokens_override`` (already resolved up-front) along with
    ``skip_preflight=True`` because it already ran preflight; direct test
    callers pass neither and get token resolution + preflight here.
    """
    tokens = (
        tokens_override
        if tokens_override is not None
        else _common._resolve_tokens_or_die(cfg, console=console)
    )

    if not skip_preflight:
        _common._preflight_or_die(cfg=cfg, tokens=tokens, console=console)

    http.reset_retry_stats()
    plan = plan_override if plan_override is not None else planner.plan_create(cfg, opts)
    try:
        result = executor.apply_plan(plan, cfg=cfg, tokens=tokens, console=console, verbose=verbose)
        if not result.ok:
            err = result.error
            if isinstance(err, HydraAPIError):
                _render_api_error(console, err, result.created)
            else:
                console.print(f"[red]{err}[/red]")
            raise typer.Exit(code=1) from None
    finally:
        _render_retry_footer(console)


def _probe_existing_state(
    *,
    cfg: Config,
    opts: CreateOptions,
    tokens: Dict[str, str],
    console: Console,
) -> Tuple[Dict[str, RepoRef], Dict[str, PrimaryMirror]]:
    """Concurrently ask every configured host whether ``opts.name`` already exists.

    Returns ``(existing_repos, existing_mirrors)``:
    - ``existing_repos`` maps host_id → RepoRef for hosts that already have
      the repo.
    - ``existing_mirrors`` maps fork host_id → PrimaryMirror for forks whose
      configured URL is already wired up as a push-mirror on the primary.
      Only populated when the primary itself is in ``existing_repos``.
    """
    primary_spec = cfg.primary_host()
    fork_specs = cfg.fork_hosts()
    all_specs = [primary_spec, *fork_specs]
    providers: Dict[str, Any] = {h.id: providers_mod.get(h.kind)(h) for h in all_specs}

    existing_repos: Dict[str, RepoRef] = {}

    def probe(spec: HostSpec) -> Tuple[str, Optional[RepoRef]]:
        try:
            ref = providers[spec.id].find_repo(
                token=tokens[spec.id], name=opts.name, namespace=opts.group
            )
        except HydraAPIError as e:
            # A probe failure shouldn't kill the whole flow — surface and
            # treat as "doesn't exist" so create proceeds normally.
            console.print(f"[yellow]⚠ {spec.id} existence probe failed: {e.message}[/yellow]")
            return spec.id, None
        return spec.id, ref

    workers = max(1, min(len(all_specs), 8))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(probe, spec) for spec in all_specs]
        for fut in as_completed(futures):
            host_id, ref = fut.result()
            if ref is not None:
                existing_repos[host_id] = ref

    # If the primary is adopted, also fetch its existing mirrors so we can
    # skip add_outbound_mirror for forks that are already wired up.
    existing_mirrors: Dict[str, PrimaryMirror] = {}
    primary_ref = existing_repos.get(primary_spec.id)
    primary_prov = providers[primary_spec.id]
    if primary_ref is not None and isinstance(primary_prov, MirrorSource) and opts.mirror:
        try:
            mirrors = primary_prov.list_mirrors(
                token=tokens[primary_spec.id], primary_repo=primary_ref
            )
        except HydraAPIError as e:
            console.print(
                f"[yellow]⚠ couldn't fetch existing mirrors on {primary_spec.id}: "
                f"{e.message}[/yellow]"
            )
            mirrors = []
        for m in mirrors:
            fork = match_fork(m.url, fork_specs)
            if fork is not None:
                existing_mirrors[fork.id] = PrimaryMirror(id=m.id, url=m.url)
    return existing_repos, existing_mirrors


def _handle_existing_state(
    *,
    cfg: Config,
    opts: CreateOptions,
    existing_repos: Dict[str, RepoRef],
    adopt_existing: bool,
    dry_run: bool,
    console: Console,
) -> None:
    """Apply the three resolution branches from Phase 6 of the plan.

    - (a) repo exists everywhere AND the journal already records the primary
      → print "already managed" and exit 0.
    - (b) primary has it but journal is empty → prompt for adoption
      (skip prompt under ``--adopt-existing`` or ``--dry-run`` — dry-run
      assumes adoption so the user can preview the transformed plan).
      On interactive decline → exit 1.
    - (c) anything else → return; caller transforms the plan with whatever
      is in ``existing_repos``.

    Opens the journal once and reuses the result for both branches.
    """
    primary_id = cfg.primary
    if primary_id not in existing_repos:
        return  # primary doesn't have it — clean create path

    primary_ref = existing_repos[primary_id]
    journal_has = _journal_records_primary(primary_host_id=primary_id, primary_repo=primary_ref)

    # Case (a): all hosts have it AND journal records it.
    all_hosts = {h.id for h in cfg.hosts}
    if existing_repos.keys() >= all_hosts and journal_has:
        console.print(
            f"[green]✓[/green] '{opts.name}' already exists on every configured host "
            f"and is recorded in the journal. Nothing to do."
        )
        console.print(f"  [dim]hydra status {opts.name}[/dim]")
        raise typer.Exit(code=0) from None

    # Case (b): primary exists, journal is empty.
    if not journal_has:
        console.print()
        console.print(
            f"[yellow]⚠ '{opts.name}' already exists on {primary_id}[/yellow] "
            f"([dim]{primary_ref.http_url}[/dim])"
        )
        console.print("  The hydra journal has no record of it.")
        if dry_run:
            console.print(
                "  [dim](dry-run: assuming adoption to render the transformed plan; "
                "pass --adopt-existing when actually applying.)[/dim]"
            )
            return
        if not adopt_existing and not typer.confirm("  Adopt it?", default=False):
            console.print(
                "[dim]Adoption declined; no changes made. "
                "Pass --adopt-existing to skip this prompt.[/dim]"
            )
            raise typer.Exit(code=1) from None


def _journal_records_primary(*, primary_host_id: str, primary_repo: RepoRef) -> bool:
    """True if the journal already has an entry for this repo + primary host.

    Only swallows :class:`sqlite3.Error` (file-locked, schema mismatch, etc.)
    where falling through to the adoption branch is the safer default —
    other exceptions propagate so a real bug doesn't masquerade as
    "journal empty".
    """
    import sqlite3

    if primary_repo.project_id is None:
        return False
    try:
        with journal_mod.journal() as j:
            for repo in j.list_repos():
                if (
                    repo.primary_host_id == primary_host_id
                    and repo.primary_repo_id == primary_repo.project_id
                ):
                    return True
    except sqlite3.Error:
        return False
    return False

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

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
from hydra.mirrors import scrub_credentials
from hydra.providers.base import MirrorSource, PrimaryProject


@app.command("scan")
def scan_command(
    namespace: Optional[str] = typer.Option(
        None,
        "--namespace",
        help="Group/namespace path to scan on the primary "
        "(default: primary's managed_group_prefix or defaults.group).",
    ),
    scan_all: bool = typer.Option(
        False,
        "--all",
        help="Enumerate every project visible to the token via "
        "/projects?membership=true. Slow on large self-hosted instances — "
        "prefer --namespace when possible.",
    ),
    apply: bool = typer.Option(
        False,
        "--apply",
        help="Bulk-adopt unknown repos into the journal and resync drifted "
        "push-mirror ids. Use --interactive for per-repo prompts.",
    ),
    interactive: bool = typer.Option(
        False,
        "--interactive",
        "-i",
        help="Prompt y/N for each unknown repo before adopting. Drifted ids "
        "are still auto-resynced (an id change isn't a semantic change).",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip the final confirmation before applying the plan.",
    ),
    max_workers: int = typer.Option(
        8,
        "--max-workers",
        envvar="HYDRA_SCAN_WORKERS",
        min=1,
        max=32,
        help="Concurrent HTTP workers for scan (default 8).",
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Diff the journal against the primary host. With --apply or --interactive,
    also adopts unknown repos and resyncs drifted push-mirror ids.
    """
    console = Console()
    cfg = _common._load_or_die(config_path, console)
    primary_spec = cfg.primary_host()
    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    if not isinstance(primary, MirrorSource):
        console.print("[red]Primary provider cannot enumerate projects.[/red]")
        raise typer.Exit(code=1)

    scope = namespace or _default_scan_namespace(primary_spec, cfg)
    if scope is None and not scan_all:
        console.print(
            "[yellow]No namespace given and no managed_group_prefix / defaults.group "
            "configured.[/yellow] Pass --namespace <group> to scope the scan, or "
            "--all to enumerate every visible project (slow)."
        )
        raise typer.Exit(code=1)
    if scan_all and scope is None:
        console.print(
            "[yellow]⚠ --all enumerates every project the token can see. This may "
            "be slow on large self-hosted instances.[/yellow]"
        )

    token = _common._resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)
    http.reset_retry_stats()

    try:
        try:
            with journal_mod.journal() as j:
                journal_repos = j.list_repos()
        except Exception as e:  # noqa: BLE001
            console.print(f"[red]Could not open journal: {e}[/red]")
            raise typer.Exit(code=1) from None

        try:
            snapshot = primary.list_projects_with_mirrors(
                token=token, namespace=scope, max_workers=max_workers
            )
        except HydraAPIError as e:
            _render_api_error(console, e, created=[])
            raise typer.Exit(code=1) from None

        diff = journal_mod.scan_diff(
            journal_repos, _to_primary_snapshots(snapshot), primary_host_id=primary_spec.id
        )
        by_repo_id = {p.project_id: p for p in snapshot}
        fork_specs = cfg.fork_hosts()

        _print_scan_diff(console, diff, by_repo_id=by_repo_id, fork_specs=fork_specs)

        mutated = False
        if (apply or interactive) and (diff.unknown or diff.drift):
            mutated = _apply_scan_diff(
                console=console,
                cfg=cfg,
                diff=diff,
                by_repo_id=by_repo_id,
                fork_specs=fork_specs,
                interactive=interactive,
                yes=yes,
            )

        # Re-eval state for exit code: if user adopted everything in scope, exit 0.
        if mutated:
            try:
                with journal_mod.journal() as j:
                    final_repos = j.list_repos()
            except Exception:  # noqa: BLE001 — already reported above
                final_repos = journal_repos
            final_diff = journal_mod.scan_diff(
                final_repos,
                _to_primary_snapshots(snapshot),
                primary_host_id=primary_spec.id,
            )
            raise typer.Exit(code=0 if final_diff.is_clean else 1)

        raise typer.Exit(code=0 if diff.is_clean else 1)
    finally:
        _render_retry_footer(console)


def _default_scan_namespace(primary_spec: HostSpec, cfg: Config) -> Optional[str]:
    prefix = primary_spec.options.get("managed_group_prefix")
    if prefix:
        return str(prefix)
    if cfg.defaults.group:
        return cfg.defaults.group
    return None


def _to_primary_snapshots(
    projects: List[PrimaryProject],
) -> List[journal_mod.PrimaryRepoSnapshot]:
    return [
        journal_mod.PrimaryRepoSnapshot(
            repo_id=p.project_id,
            repo_url=p.web_url,
            name=p.name or p.full_path,
            mirror_push_ids=list(p.mirror_push_ids),
        )
        for p in projects
    ]


def _render_mirror_line(mirror_url: str, push_id: int, fork_specs: List[HostSpec]) -> str:
    """One-line description of a mirror: matched fork id + scrubbed URL + push id."""
    fork = match_fork(mirror_url, fork_specs)
    label = fork.id if fork else "[dim]unknown target[/dim]"
    return f"{label}: {scrub_credentials(mirror_url)} (id={push_id})"


def _mirror_summary(mirrors: List[Any], fork_specs: List[HostSpec]) -> str:
    """One-line count + matched-fork breakdown — `2 mirrors → gitlab, github`,
    or `3 mirrors → gitlab, ⚠ 2 unknown` when some targets don't match a fork."""
    matched: List[str] = []
    unknown = 0
    for m in mirrors:
        fork = match_fork(m.url, fork_specs)
        if fork is None:
            unknown += 1
        else:
            matched.append(fork.id)
    n = len(mirrors)
    parts = matched.copy()
    if unknown:
        parts.append(f"[yellow]⚠ {unknown} unknown[/yellow]")
    targets = ", ".join(parts) if parts else "[dim](none)[/dim]"
    return f"[dim]{n} mirror{'s' if n != 1 else ''} →[/dim] {targets}"


def _print_scan_diff(
    console: Console,
    diff: journal_mod.ScanDiff,
    *,
    by_repo_id: Dict[int, PrimaryProject],
    fork_specs: List[HostSpec],
) -> None:
    if diff.is_clean:
        console.print("[green]Journal matches primary.[/green]")
        return

    if diff.unknown:
        console.print()
        console.print(
            f"[yellow]Found {len(diff.unknown)} repo(s) on primary not in journal:[/yellow]"
        )
        for snap in diff.unknown:
            proj = by_repo_id.get(snap.repo_id)
            n = len(proj.mirrors) if proj else 0
            console.print(
                f"  • [bold]{snap.name or snap.repo_url}[/bold] "
                f"(id={snap.repo_id}, {n} mirror{'s' if n != 1 else ''})"
            )
            if proj:
                for m in proj.mirrors:
                    console.print(f"      → {_render_mirror_line(m.url, m.id, fork_specs)}")

    if diff.missing:
        console.print()
        console.print(
            f"[red]Found {len(diff.missing)} active journal repo(s) no longer on primary:[/red]"
        )
        for jrepo in diff.missing:
            console.print(f"  • [bold]{jrepo.name}[/bold] ({jrepo.primary_repo_url})")

    if diff.drift:
        console.print()
        console.print(f"[yellow]Mirror drift on {len(diff.drift)} repo(s):[/yellow]")
        for jrepo, snap in diff.drift:
            proj = by_repo_id.get(snap.repo_id)
            n_primary = len(proj.mirrors) if proj else 0
            n_journal = len(jrepo.mirrors)
            console.print(
                f"  • [bold]{jrepo.name}[/bold] ({n_primary} on primary / {n_journal} in journal)"
            )
            j_by_host = {m.target_host_id: m for m in jrepo.mirrors}
            primary_mirrors = proj.mirrors if proj else []
            seen_hosts: set = set()
            for m in primary_mirrors:
                fork = match_fork(m.url, fork_specs)
                host_id = fork.id if fork else None
                if host_id and host_id in j_by_host:
                    seen_hosts.add(host_id)
                    jm = j_by_host[host_id]
                    if jm.push_mirror_id == m.id:
                        marker = "[green]match[/green]"
                    else:
                        marker = (
                            f"[yellow]drift[/yellow] (journal={jm.push_mirror_id} → primary={m.id})"
                        )
                else:
                    marker = "[yellow]new on primary[/yellow]"
                console.print(
                    f"      primary: {_render_mirror_line(m.url, m.id, fork_specs)} — {marker}"
                )
            for host_id, jm in j_by_host.items():
                if host_id in seen_hosts:
                    continue
                console.print(
                    f"      [red]journal-only:[/red] {host_id}: "
                    f"{scrub_credentials(jm.target_repo_url)} (id={jm.push_mirror_id})"
                )

    console.print()
    if diff.unknown or diff.drift:
        console.print(
            "[dim]Pass [bold]--apply[/bold] to adopt unknowns + resync drift, "
            "or [bold]--interactive[/bold] to choose per repo.[/dim]"
        )
    else:
        console.print("[dim]Re-run [bold]hydra scan[/bold] after fixing drift.[/dim]")


def _apply_scan_diff(
    *,
    console: Console,
    cfg: Config,
    diff: journal_mod.ScanDiff,
    by_repo_id: Dict[int, PrimaryProject],
    fork_specs: List[HostSpec],
    interactive: bool,
    yes: bool = False,
) -> bool:
    """Adopt unknown repos + resync drifted push_mirror_ids via the planner.

    With ``--interactive``, prompt per unknown repo to filter the plan first.
    Drift actions are always included — an id change is not semantic. After
    filtering, render the final plan, prompt once (unless ``--yes``), then
    apply through the executor.

    Returns True if the journal was written to at least once.
    """
    accepted_ids: Optional[List[int]] = None
    if interactive and diff.unknown:
        accepted_ids = []
        for snap in diff.unknown:
            proj = by_repo_id.get(snap.repo_id)
            if proj is None:
                continue
            label = proj.name or proj.full_path or proj.web_url
            console.print()
            console.print(f"  [bold]{label}[/bold]  [dim]{proj.web_url}[/dim]")
            console.print(f"    {_mirror_summary(proj.mirrors, fork_specs)}")
            for m in proj.mirrors:
                console.print(f"      → {_render_mirror_line(m.url, m.id, fork_specs)}")
            if typer.confirm(f"  Adopt '{label}'?", default=True):
                accepted_ids.append(snap.repo_id)
            else:
                console.print(f"  [dim]skipped {label}[/dim]")

    plan = planner.plan_scan_apply(
        diff, cfg, by_repo_id=by_repo_id, accept_unknown_ids=accepted_ids
    )
    if plan.is_empty:
        console.print("[dim]Nothing to apply.[/dim]")
        return False

    console.print()
    planner.render_plan(plan, console, title="Scan apply plan")
    if not yes and not typer.confirm(f"Apply {len(plan.actions)} action(s)?", default=False):
        console.print("[dim]No changes made.[/dim]")
        return False

    # Tokens not needed for journal-only actions, but the executor expects a
    # bag — supply primary's so a future provider-touching action wouldn't
    # silently misroute.
    primary_token = _common._resolve_token_or_die(cfg.primary, allow_prompt=True, console=console)
    tokens = {cfg.primary: primary_token}

    result = executor.apply_plan(plan, cfg=cfg, tokens=tokens, console=console)
    if not result.ok:
        console.print(f"[red]Journal write failed during --apply: {result.error}[/red]")
        return result.applied > 0
    return result.applied > 0

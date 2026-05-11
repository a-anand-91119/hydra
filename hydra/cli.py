from __future__ import annotations

import copy
import fnmatch
import json as json_mod
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import typer
import yaml
from rich.console import Console
from rich.table import Table

from hydra import __version__, executor, planner
from hydra import doctor as doctor_mod
from hydra import journal as journal_mod
from hydra import paths as paths_mod
from hydra import providers as providers_mod
from hydra import secrets as secrets_mod
from hydra.config import Config, ConfigError, HostSpec, load_config, resolve_config_path
from hydra.errors import HydraAPIError, MirrorReplaceError
from hydra.mirrors import scrub_credentials
from hydra.providers.base import MirrorSource, PrimaryProject, RepoRef
from hydra.wizard import (
    CreateOptions,
    WizardCancelled,
    apply_token_storage,
    run_create_wizard,
    run_wizard,
)

# Register built-in providers exactly once at CLI entry.
providers_mod.bootstrap()

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Hydra — provision a repo across one primary and N forks.",
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"hydra {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(False, "--version", callback=_version_callback, is_eager=True),
) -> None:
    pass


def _load_or_die(config_path: Optional[Path], console: Console) -> Config:
    try:
        return load_config(config_path)
    except ConfigError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1) from None


def _resolve_token_or_die(host_id: str, *, allow_prompt: bool, console: Console) -> str:
    try:
        return secrets_mod.get_token(host_id, allow_prompt=allow_prompt)
    except secrets_mod.SecretError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1) from None


def _parse_host_options(values: List[str]) -> Dict[str, Dict[str, Any]]:
    """Parse repeated --host-option `id.key=value` pairs.

    Values are YAML-parsed so booleans, ints, and null work naturally:
        --host-option github.org=acme       → "acme"
        --host-option gl.add_timestamp=true → True
        --host-option gl.retries=3          → 3

    Only the FIRST `=` splits key from value, so values may contain `=`.
    Only the FIRST `.` splits id from key, so keys may not (use a dotted
    YAML structure in the config file for nested options).
    """
    out: Dict[str, Dict[str, Any]] = {}
    for raw in values:
        if "=" not in raw:
            raise typer.BadParameter(f"--host-option must be `id.key=value`, got {raw!r}")
        spec, value = raw.split("=", 1)
        if "." not in spec:
            raise typer.BadParameter(f"--host-option spec must be `id.key`, got {spec!r}")
        host_id, key = spec.split(".", 1)
        host_id = host_id.strip()
        key = key.strip()
        if not host_id:
            raise typer.BadParameter(f"--host-option missing host id: {raw!r}")
        if not key:
            raise typer.BadParameter(f"--host-option missing key: {raw!r}")
        try:
            parsed: Any = yaml.safe_load(value)
        except yaml.YAMLError:
            parsed = value
        out.setdefault(host_id, {})[key] = parsed
    return out


def _apply_overrides(cfg: Config, overrides: Dict[str, Dict[str, Any]]) -> Config:
    if not overrides:
        return cfg
    cfg = copy.deepcopy(cfg)
    for host_id, kvs in overrides.items():
        try:
            host = cfg.host(host_id)
        except KeyError:
            raise typer.BadParameter(f"--host-option references unknown host {host_id!r}") from None
        host.options.update(kvs)
    return cfg


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
    config_path: Optional[Path] = typer.Option(None, "--config"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Create a repo on the primary host and mirror to all forks."""
    console = Console()
    cfg = _load_or_die(config_path, console)
    cfg = _apply_overrides(cfg, _parse_host_options(host_option))

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
    planner.render_plan(plan, console, dry_run=opts.dry_run, title=f"Create '{opts.name}'")

    if opts.dry_run:
        return

    if not yes and not typer.confirm(f"Apply {len(plan.actions)} action(s)?", default=False):
        console.print("[dim]No changes made.[/dim]")
        return

    _execute_create(cfg=cfg, opts=opts, verbose=verbose, console=console)


def _execute_create(*, cfg: Config, opts: CreateOptions, verbose: bool, console: Console) -> None:
    """Build a create plan and apply it. Token resolution + error rendering
    happen here so the CLI command stays focused on plan/render/confirm.

    Kept as a public-by-convention helper so existing tests (and the wizard
    callback) can drive the apply without re-prompting.
    """
    primary_spec = cfg.primary_host()
    fork_specs = cfg.fork_hosts()
    tokens: Dict[str, str] = {
        primary_spec.id: _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)
    }
    for spec in fork_specs:
        if spec.id not in tokens:
            tokens[spec.id] = _resolve_token_or_die(spec.id, allow_prompt=True, console=console)

    plan = planner.plan_create(cfg, opts)
    result = executor.apply_plan(plan, cfg=cfg, tokens=tokens, console=console, verbose=verbose)
    if not result.ok:
        err = result.error
        if isinstance(err, HydraAPIError):
            _render_api_error(console, err, result.created)
        else:
            console.print(f"[red]{err}[/red]")
        raise typer.Exit(code=1) from None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _render_api_error(console: Console, err: HydraAPIError, created: List[Tuple[str, str]]) -> None:
    """Pretty-print a HydraAPIError with hint and partial-progress info."""
    console.print()
    console.print(f"[bold red]✗[/bold red] [bold]{err.message}[/bold]")

    if err.hint:
        console.print()
        for line in err.hint.split("\n"):
            console.print(f"  [dim]{line}[/dim]")

    if created:
        console.print()
        console.print("[yellow]⚠ Partial progress before the failure:[/yellow]")
        for label, url in created:
            console.print(f"  • [bold]{label}[/bold]: {url}")
        console.print()
        console.print(
            "  [dim]These resources exist now. Delete them manually before retrying, "
            "or use a different repo name.[/dim]"
        )
    console.print()


@app.command()
def configure(
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Guided wizard for hosts, defaults, and API tokens."""
    console = Console()

    try:
        result = run_wizard(config_path=config_path, console=console)
    except WizardCancelled as e:
        console.print(f"\n[yellow]Configuration not saved:[/yellow] {e or 'aborted'}")
        raise typer.Exit(code=1) from None
    except KeyboardInterrupt:
        console.print("\n[yellow]Configuration not saved:[/yellow] aborted")
        raise typer.Exit(code=1) from None

    console.print(f"\n[green]✓[/green] Config saved to [bold]{result.config_path}[/bold]")
    apply_token_storage(result, console=console)
    console.print(
        "\n[dim]Next:[/dim] try [bold]hydra create my-repo --dry-run[/bold] to verify the setup."
    )


@app.command(no_args_is_help=True)
def status(
    name: str = typer.Argument(..., help="Repo name (with optional group/path)"),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Group path on the primary host"
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Show mirror status for a repo on the primary host."""
    console = Console()
    cfg = _load_or_die(config_path, console)
    primary_spec = cfg.primary_host()
    primary_caps = providers_mod.capabilities_for(primary_spec.kind)
    if not primary_caps.supports_status_lookup:
        console.print(
            f"[red]Status lookup is not supported for primary kind {primary_spec.kind!r}.[/red]"
        )
        raise typer.Exit(code=1)

    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    primary_token = _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)

    effective_group = group if group is not None else cfg.defaults.group
    repo_path = f"{effective_group}/{name}" if effective_group else name

    try:
        repo = primary.find_project(token=primary_token, repo_path=repo_path)
        if repo is None:
            console.print(f"[red]Project not found:[/red] {repo_path}")
            raise typer.Exit(code=1)

        mirror_list = primary.list_mirrors(token=primary_token, primary_repo=repo)
    except HydraAPIError as e:
        _render_api_error(console, e, created=[])
        raise typer.Exit(code=1) from None

    if not mirror_list:
        console.print(f"No mirrors configured for {repo_path}.")
        return

    console.print(f"Mirrors for [bold]{repo_path}[/bold] (project {repo.project_id}):")
    fork_specs = cfg.fork_hosts()
    for m in mirror_list:
        match = _match_fork(m.url, fork_specs)
        label = f"[bold]{match.id}[/bold]" if match else "[dim](unconfigured)[/dim]"
        flag = "[green]enabled [/green]" if m.enabled else "[yellow]disabled[/yellow]"
        # Always strip credentials before printing — GitLab echoes them back.
        safe_url = scrub_credentials(m.url)
        line = f"  {label} [{flag}] {safe_url}"
        if m.last_update_status:
            line += f" — {m.last_update_status}"
        if m.last_update_at:
            line += f" @ {m.last_update_at}"
        console.print(line)
        if m.last_error:
            console.print(f"    [red]error: {m.last_error}[/red]")


def _spec_mirror_hostname(spec: HostSpec) -> Optional[str]:
    """The hostname mirror URLs use for this host (NOT the API base).

    GitHub's API lives at api.github.com but git push URLs use github.com.
    GitLab uses the same hostname for both. Self-hosted GitHub Enterprise
    typically also uses the same hostname for API and git, so the special
    case is limited to the public api.github.com.
    """
    try:
        host = (urlparse(spec.url).hostname or "").lower()
    except ValueError:
        return None
    if not host:
        return None
    if spec.kind == "github" and host == "api.github.com":
        return "github.com"
    return host


def _match_fork(mirror_url: str, forks: List[HostSpec]) -> Optional[HostSpec]:
    """Match a mirror URL to a configured fork by exact hostname (case-insensitive).

    Substring matching would be unsafe (e.g. `gitlab.com` would match
    `evilgitlab.com.attacker.example`).
    """
    try:
        mirror_host = (urlparse(mirror_url).hostname or "").lower()
    except ValueError:
        return None
    if not mirror_host:
        return None
    for spec in forks:
        spec_host = _spec_mirror_hostname(spec)
        if spec_host and spec_host == mirror_host:
            return spec
    return None


@app.command()
def doctor(
    fix: bool = typer.Option(
        False, "--fix", help="Apply safe fixes (run pending migrations, etc.)"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show full details for each finding"
    ),
    check_keyring: bool = typer.Option(
        False,
        "--check-keyring",
        help="Probe the OS keyring for stored tokens. May prompt for "
        "Keychain access on macOS — disabled by default.",
    ),
    check_tokens: bool = typer.Option(
        False,
        "--check-tokens",
        help="Make one network call per host to validate each token and "
        "report its scopes/expiry. Disabled by default to keep doctor offline.",
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Diagnose configuration, tokens, and topology. Use --fix to apply
    pending migrations and other safe automatic fixes.
    """
    console = Console()
    result = doctor_mod.run_doctor(
        config_path=config_path,
        fix=fix,
        verbose=verbose,
        check_keyring=check_keyring,
        check_tokens=check_tokens,
        console=console,
    )
    if result.exit_code != 0:
        raise typer.Exit(code=result.exit_code)


@app.command("config-path")
def show_config_path(
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Print the resolved config file path."""
    typer.echo(str(resolve_config_path(config_path)))


@app.command("journal-path")
def show_journal_path() -> None:
    """Print the resolved journal database path."""
    typer.echo(str(paths_mod.journal_path()))


# ──────────────────────────── list / scan / rotate-token ────────────────


_STATUS_STYLES = {
    "success": "green",
    "ok": "green",
    "failed": "red",
    "error": "red",
    "started": "yellow",
    "running": "yellow",
}


def _render_status(value: Optional[str]) -> str:
    if not value:
        return "[yellow]stale[/yellow]"
    style = _STATUS_STYLES.get(value.lower(), "white")
    return f"[{style}]{value}[/{style}]"


@app.command("list")
def list_repos(
    refresh: bool = typer.Option(
        False, "--refresh", help="Re-query the primary host and update cached status."
    ),
    host: Optional[str] = typer.Option(None, "--host", help="Filter by target host id."),
    name_filter: Optional[str] = typer.Option(
        None, "--filter", help="Filter by repo name (glob: 'foo-*')."
    ),
    output_json: bool = typer.Option(False, "--json", help="Emit JSON instead of a table."),
    max_workers: int = typer.Option(
        8,
        "--max-workers",
        envvar="HYDRA_SCAN_WORKERS",
        min=1,
        max=32,
        help="Concurrent HTTP workers for --refresh (default 8).",
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """List hydra-tracked repos and their last-known mirror status."""
    console = Console()
    cfg = _load_or_die(config_path, console)

    try:
        with journal_mod.journal() as j:
            if refresh:
                _refresh_status(cfg=cfg, journal=j, console=console, max_workers=max_workers)
            repos = j.list_repos()
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Could not open journal: {e}[/red]")
        raise typer.Exit(code=1) from None

    repos = _filter_repos(repos, host=host, name_pattern=name_filter)

    if output_json:
        typer.echo(json_mod.dumps(_repos_to_json(repos), indent=2))
        return

    if not repos:
        console.print(
            "[yellow]No tracked repos.[/yellow] Run [bold]hydra create[/bold] or "
            "[bold]hydra scan[/bold] to populate the journal."
        )
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("Name")
    table.add_column("Primary")
    table.add_column("Mirrors")
    table.add_column("Last scan")
    for r in repos:
        mirror_cell = (
            "\n".join(f"{m.target_host_id}: {_render_status(m.last_status)}" for m in r.mirrors)
            or "[dim](none)[/dim]"
        )
        table.add_row(
            r.name,
            f"{r.primary_host_id}",
            mirror_cell,
            r.last_scanned_at or "[dim]never[/dim]",
        )
    console.print(table)


def _filter_repos(
    repos: List[journal_mod.JournalRepo],
    *,
    host: Optional[str],
    name_pattern: Optional[str],
) -> List[journal_mod.JournalRepo]:
    out = repos
    if host:
        out = [r for r in out if any(m.target_host_id == host for m in r.mirrors)]
    if name_pattern:
        out = [r for r in out if fnmatch.fnmatchcase(r.name, name_pattern)]
    return out


def _repos_to_json(repos: List[journal_mod.JournalRepo]) -> List[Dict[str, Any]]:
    return [
        {
            "name": r.name,
            "primary_host_id": r.primary_host_id,
            "primary_repo_id": r.primary_repo_id,
            "primary_repo_url": r.primary_repo_url,
            "created_at": r.created_at,
            "last_scanned_at": r.last_scanned_at,
            "state": r.state,
            "mirrors": [
                {
                    "target_host_id": m.target_host_id,
                    "target_repo_url": m.target_repo_url,
                    "push_mirror_id": m.push_mirror_id,
                    "last_status": m.last_status,
                    "last_error": m.last_error,
                    "last_update_at": m.last_update_at,
                }
                for m in r.mirrors
            ],
        }
        for r in repos
    ]


def _refresh_status(
    *,
    cfg: Config,
    journal: journal_mod.Journal,
    console: Console,
    max_workers: int = 8,
) -> None:
    """For each journaled repo on the configured primary, fetch mirror status
    from the primary and update cached fields.

    Mirror fetches run concurrently across ``max_workers`` threads; journal
    writes are funnelled back to the main thread to keep SQLite single-writer.
    """
    primary_spec = cfg.primary_host()
    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    if not isinstance(primary, MirrorSource):
        console.print("[red]Primary provider does not expose mirror status.[/red]")
        return
    token = _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)

    repos = [r for r in journal.list_repos() if r.primary_host_id == primary_spec.id]
    if not repos:
        return

    def fetch(repo: journal_mod.JournalRepo):
        return primary.list_mirrors(
            token=token,
            primary_repo=RepoRef(http_url="", project_id=repo.primary_repo_id),
        )

    workers = max(1, min(max_workers, len(repos)))
    if workers <= 1:
        results = [(r, _safe_fetch(fetch, r)) for r in repos]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_safe_fetch, fetch, r): r for r in repos}
            for fut in as_completed(futures):
                results.append((futures[fut], fut.result()))

    for repo, outcome in results:
        if isinstance(outcome, HydraAPIError):
            console.print(f"[yellow]⚠ {repo.name}:[/yellow] {outcome.message}")
            continue
        by_push_id = {m.id: m for m in outcome}
        for jm in repo.mirrors:
            live = by_push_id.get(jm.push_mirror_id)
            if live is None:
                journal.update_mirror_status(
                    mirror_db_id=jm.id,
                    last_status="missing",
                    last_error="push mirror no longer present on primary",
                    last_update_at=None,
                )
                continue
            journal.update_mirror_status(
                mirror_db_id=jm.id,
                last_status=live.last_update_status,
                last_error=live.last_error,
                last_update_at=live.last_update_at,
            )
        journal.touch_repo_scanned(repo_db_id=repo.id)


def _safe_fetch(fn, repo):
    """Run ``fn(repo)`` and return its result, or the HydraAPIError it raised.

    Used to ferry per-repo failures back to the main thread without aborting
    the whole pool.
    """
    try:
        return fn(repo)
    except HydraAPIError as e:
        return e


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
    cfg = _load_or_die(config_path, console)
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

    token = _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)

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
        except Exception:  # noqa: BLE001 — already reported above if anything went sideways
            final_repos = journal_repos
        final_diff = journal_mod.scan_diff(
            final_repos,
            _to_primary_snapshots(snapshot),
            primary_host_id=primary_spec.id,
        )
        raise typer.Exit(code=0 if final_diff.is_clean else 1)

    raise typer.Exit(code=0 if diff.is_clean else 1)


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
    fork = _match_fork(mirror_url, fork_specs)
    label = fork.id if fork else "[dim]unknown target[/dim]"
    return f"{label}: {scrub_credentials(mirror_url)} (id={push_id})"


def _mirror_summary(mirrors: List[Any], fork_specs: List[HostSpec]) -> str:
    """One-line count + matched-fork breakdown — `2 mirrors → gitlab, github`,
    or `3 mirrors → gitlab, ⚠ 2 unknown` when some targets don't match a fork."""
    matched: List[str] = []
    unknown = 0
    for m in mirrors:
        fork = _match_fork(m.url, fork_specs)
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
                fork = _match_fork(m.url, fork_specs)
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
    primary_token = _resolve_token_or_die(cfg.primary, allow_prompt=True, console=console)
    tokens = {cfg.primary: primary_token}

    result = executor.apply_plan(plan, cfg=cfg, tokens=tokens, console=console)
    if not result.ok:
        console.print(f"[red]Journal write failed during --apply: {result.error}[/red]")
        return result.applied > 0
    return result.applied > 0


@app.command("rotate-token", no_args_is_help=True)
def rotate_token(
    host_id: str = typer.Argument(..., help="Host id whose PAT to rotate."),
    new_token: Optional[str] = typer.Option(
        None,
        "--token",
        help="New PAT (skips prompt; useful for CI). NOTE: passing a secret on the "
        "command line makes it visible in shell history and process listings — "
        "prefer the interactive prompt or env-var resolution.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show which mirrors would be updated, without changes."
    ),
    skip_verify: bool = typer.Option(
        False, "--skip-verify", help="Don't probe the host to validate the token before storing."
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Rotate a host's PAT — updates keyring and rewrites every push-mirror
    on the primary that embeds the old token for this host.
    """
    console = Console()
    cfg = _load_or_die(config_path, console)
    try:
        target_spec = cfg.host(host_id)
    except KeyError:
        console.print(f"[red]Unknown host id:[/red] {host_id!r}")
        raise typer.Exit(code=1) from None

    if new_token is None:
        if not sys.stdin.isatty():
            console.print("[red]No --token provided and no TTY for an interactive prompt.[/red]")
            raise typer.Exit(code=1)
        new_token = typer.prompt(f"New {host_id} token", hide_input=True).strip()
    if not new_token:
        console.print("[red]Empty token.[/red]")
        raise typer.Exit(code=1)

    if not skip_verify:
        try:
            _verify_token(target_spec, new_token, console=console)
        except HydraAPIError as e:
            _render_api_error(console, e, created=[])
            raise typer.Exit(code=1) from None

    if dry_run:
        console.print(
            f"[dim](dry-run)[/dim] would store new token for [bold]{host_id}[/bold] in keyring."
        )
    else:
        try:
            secrets_mod.set_token(host_id, new_token)
        except secrets_mod.SecretError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(code=1) from None
        console.print(f"[green]✓[/green] stored new {host_id} token in keyring.")

    if host_id == cfg.primary:
        console.print(
            "[dim]Primary token rotated. No outbound push-mirrors carry this token, "
            "so no mirror updates are needed.[/dim]"
        )
        return

    primary_spec = cfg.primary_host()
    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    if not isinstance(primary, MirrorSource):
        console.print("[red]Primary provider cannot update mirrors.[/red]")
        raise typer.Exit(code=1)

    try:
        with journal_mod.journal() as j:
            pairs = j.mirrors_for_target_host(host_id)
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Could not open journal: {e}[/red]")
        raise typer.Exit(code=1) from None

    if not pairs:
        console.print(f"[dim]No journaled mirrors targeting {host_id}. Nothing to update.[/dim]")
        return

    target_caps = providers_mod.capabilities_for(target_spec.kind)
    username = target_caps.inbound_mirror_username

    if dry_run:
        console.print()
        console.print(f"[bold]Would update {len(pairs)} mirror(s):[/bold]")
        for repo, m in pairs:
            console.print(f"  • {repo.name} → {scrub_credentials(m.target_repo_url)}")
        return

    primary_token = _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)

    updated = 0
    failed = 0
    destroyed = 0
    try:
        with journal_mod.journal() as j:
            for repo, m in pairs:
                try:
                    payload = primary.replace_outbound_mirror(
                        token=primary_token,
                        primary_repo=RepoRef(http_url="", project_id=repo.primary_repo_id),
                        old_push_mirror_id=m.push_mirror_id,
                        target_url=m.target_repo_url,
                        target_token=new_token,
                        target_username=username,
                        target_label=host_id,
                    )
                except MirrorReplaceError as e:
                    destroyed += 1
                    failed += 1
                    j.update_mirror_status(
                        mirror_db_id=m.id,
                        last_status="broken",
                        last_error=e.message,
                        last_update_at=None,
                    )
                    console.print(f"[bold red]✗[/bold red] {repo.name}: {e.message}")
                    if e.hint:
                        for line in e.hint.split("\n"):
                            console.print(f"    [dim]{line}[/dim]")
                    continue
                except HydraAPIError as e:
                    failed += 1
                    console.print(f"[red]✗[/red] {repo.name}: {e.message}")
                    continue
                new_push_id = _safe_int(payload.get("id") if payload else None)
                if new_push_id is not None:
                    j.update_mirror_push_id(mirror_db_id=m.id, new_push_mirror_id=new_push_id)
                updated += 1
                console.print(f"[green]✓[/green] {repo.name}")
    except Exception as e:  # noqa: BLE001 — journal layer; we already report API failures above
        console.print(f"[red]Journal write failed mid-rotation: {e}[/red]")
        raise typer.Exit(code=1) from None

    console.print()
    summary = f"{updated} updated"
    if failed:
        summary += f", [red]{failed} failed[/red]"
    if destroyed:
        summary += f" ([bold red]{destroyed} mirror(s) DELETED with no replacement[/bold red])"
    console.print(summary + ".")
    if failed:
        raise typer.Exit(code=1)


def _verify_token(spec: HostSpec, token: str, *, console: Optional[Console] = None) -> None:
    """Probe the host using a token. Raises HydraAPIError on failure.

    For unknown provider kinds, no probe runs and a warning is printed so the
    user knows verification was skipped.
    """
    if spec.kind == "gitlab":
        from hydra import gitlab as gitlab_api

        gitlab_api.verify_token(host=spec.id, base_url=spec.url, token=token)
    elif spec.kind == "github":
        from hydra import github as github_api

        github_api.verify_token(base_url=spec.url, token=token)
    elif console is not None:
        console.print(
            f"[yellow]⚠ no token-verification probe for provider kind "
            f"{spec.kind!r} — skipping pre-flight check.[/yellow]"
        )


if __name__ == "__main__":
    app()

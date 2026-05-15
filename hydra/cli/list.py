from __future__ import annotations

import fnmatch
import json as json_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console
from rich.table import Table

from hydra import journal as journal_mod
from hydra import providers as providers_mod
from hydra.cli import _common, app
from hydra.config import Config
from hydra.errors import HydraAPIError
from hydra.providers.base import MirrorSource, RepoRef

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
    cfg = _common._load_or_die(config_path, console)

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
    token = _common._resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)

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

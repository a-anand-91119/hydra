from __future__ import annotations

import fnmatch
import json as json_mod
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table

from hydra import journal as journal_mod
from hydra.cli import _common, app
from hydra.cli._common import _repos_to_json
from hydra.cli._render import _render_status


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
                _common._refresh_status(
                    cfg=cfg, journal=j, console=console, max_workers=max_workers
                )
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



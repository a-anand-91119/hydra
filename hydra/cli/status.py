from __future__ import annotations

import json as json_mod
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from hydra import journal as journal_mod
from hydra.cli import _common, app
from hydra.cli._common import UNHEALTHY_STATUSES
from hydra.cli._render import _render_status


@app.command(no_args_is_help=True)
def status(
    name: str = typer.Argument(..., help="Repo name (as tracked in the journal)."),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Re-query the primary host and update the journal before showing.",
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
    """Show one repo's mirror health from the journal (offline by default).

    Per-mirror last status and last error are shown inline. Pass --refresh to
    re-query the primary host and update the journal first. Exits non-zero if
    any mirror is unhealthy, so it doubles as a CI health gate.
    """
    console = Console()
    cfg = _common._load_or_die(config_path, console)

    try:
        with journal_mod.journal() as j:
            if refresh:
                _common._refresh_status(
                    cfg=cfg,
                    journal=j,
                    console=console,
                    max_workers=max_workers,
                    only_repo=name,
                )
            matches = [r for r in j.list_repos() if r.name == name]
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Could not open journal: {e}[/red]")
        raise typer.Exit(code=1) from None

    if not matches:
        console.print(
            f"[yellow]Not tracked:[/yellow] {name}. "
            f"Run [bold]hydra scan[/bold] to adopt it, or [bold]hydra create[/bold]."
        )
        raise typer.Exit(code=1)

    if len(matches) > 1:
        console.print(
            f"[yellow]Ambiguous:[/yellow] {len(matches)} repos named {name!r} in the journal:"
        )
        for r in matches:
            console.print(f"  • {r.primary_host_id}: {r.primary_repo_url}")
        console.print("[dim]Disambiguation by group is not yet supported.[/dim]")
        raise typer.Exit(code=1)

    repo = matches[0]

    if output_json:
        typer.echo(json_mod.dumps(_common._repos_to_json([repo])[0], indent=2))
        # JSON consumers inspect last_status themselves; still set the exit code.
        raise typer.Exit(code=_exit_code(repo))

    console.print(f"[bold]{repo.name}[/bold]  [dim](primary: {repo.primary_host_id})[/dim]")
    if not repo.mirrors:
        console.print("  [dim](no mirrors tracked)[/dim]")
        raise typer.Exit(code=0)

    for m in repo.mirrors:
        when = m.last_update_at or "[dim]never[/dim]"
        console.print(
            f"  {m.target_host_id}: {_render_status(m.last_status)}  [dim]{when}[/dim]"
        )
        if m.last_error:
            console.print(f"      [red]error: {m.last_error}[/red]")

    if not refresh:
        console.print(
            "[dim](journal cache · --refresh to re-query the primary)[/dim]"
        )

    raise typer.Exit(code=_exit_code(repo))


def _exit_code(repo: journal_mod.JournalRepo) -> int:
    """0 if every mirror is healthy, 1 if any is in an unhealthy state."""
    unhealthy = any(
        (m.last_status or "").lower() in UNHEALTHY_STATUSES for m in repo.mirrors
    )
    return 1 if unhealthy else 0

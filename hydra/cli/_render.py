"""Cross-command rendering helpers. Per-command rendering (status table,
scan diff, rotation outcomes) lives in the relevant command module.
"""

from __future__ import annotations

from typing import List, Tuple

from rich.console import Console

from hydra import http
from hydra.errors import HydraAPIError


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


def _render_retry_footer(console: Console) -> None:
    """Print "Retried N transient errors" if any retries fired this command."""
    stats = http.pop_retry_stats()
    total = sum(stats.values())
    if total <= 0:
        return
    hosts = ", ".join(sorted(stats))
    suffix = "s" if total != 1 else ""
    console.print(f"[dim]Retried {total} transient error{suffix} ({hosts}).[/dim]")

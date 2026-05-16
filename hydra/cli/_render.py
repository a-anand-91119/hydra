"""Cross-command rendering helpers shared by multiple CLI commands.

Per-command-only rendering (e.g. the scan diff) stays in its command module;
anything two or more commands need lives here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple

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


# ── Mirror status rendering (shared by `list` and `status`) ──────────────

_STATUS_STYLES = {
    "success": "green",
    "ok": "green",
    "failed": "red",
    "error": "red",
    "broken": "red",
    "missing": "red",
    "started": "yellow",
    "running": "yellow",
}


def _render_status(value: Optional[str]) -> str:
    if not value:
        return "[yellow]stale[/yellow]"
    style = _STATUS_STYLES.get(value.lower(), "white")
    return f"[{style}]{value}[/{style}]"


# ── Per-mirror operation outcomes (shared by `rotate-token` and `repair`) ─

MirrorOpState = Literal[
    "ok", "api_failed", "destroyed", "journal_failed", "not_attempted", "skipped"
]


@dataclass
class MirrorOpOutcome:
    """One mirror's fate during a bulk provider operation (rotate / repair)."""

    repo_name: str
    state: MirrorOpState
    message: str = ""
    hint: Optional[str] = None


def render_mirror_outcomes(
    console: Console, outcomes: List[MirrorOpOutcome], *, ok_verb: str
) -> int:
    """Render a final per-mirror summary table.

    ``ok_verb`` is the past-tense word for a successful op ("updated" for
    rotate-token, "repaired" for repair). Returns the count of non-success
    outcomes so the caller can choose the exit code.
    """
    n_ok = sum(1 for o in outcomes if o.state == "ok")
    api_failed = sum(1 for o in outcomes if o.state == "api_failed")
    destroyed = sum(1 for o in outcomes if o.state == "destroyed")
    journal_failed = sum(1 for o in outcomes if o.state == "journal_failed")
    not_attempted = sum(1 for o in outcomes if o.state == "not_attempted")
    skipped = sum(1 for o in outcomes if o.state == "skipped")
    failed = api_failed + destroyed + journal_failed + not_attempted

    # Render the per-mirror table only when there is information the inline
    # progress lines didn't already convey (journal_failed is set silently
    # before re-raise; not_attempted entries were never visited).
    if journal_failed or not_attempted or skipped:
        console.print()
        console.print("[bold]Outcomes:[/bold]")
        for o in outcomes:
            if o.state == "ok":
                console.print(f"  [green]✓[/green] {o.repo_name}")
            elif o.state == "api_failed":
                console.print(f"  [red]✗[/red] {o.repo_name}: {o.message}")
            elif o.state == "destroyed":
                console.print(
                    f"  [bold red]✗[/bold red] {o.repo_name}: DELETED — {o.message}"
                )
            elif o.state == "journal_failed":
                console.print(
                    f"  [yellow]![/yellow] {o.repo_name}: "
                    f"mirror updated on host but journal write failed — {o.message}"
                )
            elif o.state == "not_attempted":
                console.print(f"  [dim]?[/dim] {o.repo_name}: not attempted")
            elif o.state == "skipped":
                console.print(f"  [dim]–[/dim] {o.repo_name}: skipped ({o.message})")

    console.print()
    summary = f"{n_ok} {ok_verb}"
    non_summary_failed = api_failed + destroyed + journal_failed
    if non_summary_failed:
        summary += f", [red]{non_summary_failed} failed[/red]"
    if destroyed:
        summary += (
            f" ([bold red]{destroyed} mirror(s) DELETED with no replacement[/bold red])"
        )
    if not_attempted:
        summary += f", [yellow]{not_attempted} not attempted[/yellow]"
    if skipped:
        summary += f", [dim]{skipped} skipped[/dim]"
    console.print(summary + ".")
    return failed

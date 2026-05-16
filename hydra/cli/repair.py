from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import typer
from rich.console import Console
from rich.table import Table

from hydra import journal as journal_mod
from hydra import providers as providers_mod
from hydra.cli import _common, app
from hydra.cli._common import UNHEALTHY_STATUSES
from hydra.cli._render import MirrorOpOutcome, render_mirror_outcomes
from hydra.errors import HydraAPIError, MirrorReplaceError
from hydra.providers.base import MirrorSource, RepoRef
from hydra.utils import safe_int

# (repo, mirror, action) where action is "add" (mirror gone on primary) or
# "replace" (mirror still present but unhealthy).
_Candidate = Tuple[journal_mod.JournalRepo, journal_mod.JournalMirror, str]


@app.command()
def repair(
    name: Optional[str] = typer.Argument(
        None, help="Only repair this repo (default: every repo with an unhealthy mirror)."
    ),
    host: Optional[str] = typer.Option(
        None, "--host", help="Only repair mirrors targeting this host id."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show the repair plan; make no changes."
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Skip the confirmation prompt."
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Re-establish push-mirrors the journal marks unhealthy.

    Acts on mirrors whose last_status is broken / missing / failed / error.
    For each it checks the primary's live mirror list: if the mirror is gone
    it is re-added; if it still exists it is replaced (delete + recreate with
    the currently-stored target token).

    --dry-run renders the plan (including any probe failures and their
    reasons) and always exits 0 — it makes no changes and is not a health
    gate; use `hydra status` for that.
    """
    console = Console()
    cfg = _common._load_or_die(config_path, console)
    primary_spec = cfg.primary_host()
    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    if not isinstance(primary, MirrorSource):
        console.print("[red]Primary provider cannot manage mirrors.[/red]")
        raise typer.Exit(code=1)

    try:
        with journal_mod.journal() as j:
            repos = j.list_repos()
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Could not open journal: {e}[/red]")
        raise typer.Exit(code=1) from None

    unhealthy: List[Tuple[journal_mod.JournalRepo, journal_mod.JournalMirror]] = []
    for repo in repos:
        if name is not None and repo.name != name:
            continue
        for m in repo.mirrors:
            if (m.last_status or "").lower() not in UNHEALTHY_STATUSES:
                continue
            if host is not None and m.target_host_id != host:
                continue
            unhealthy.append((repo, m))

    if not unhealthy:
        console.print("[dim]Nothing to repair.[/dim]")
        raise typer.Exit(code=0)

    primary_token = _common._resolve_token_or_die(
        primary_spec.id, allow_prompt=True, console=console
    )

    # Decide add-vs-replace from the primary's live state (one GET per repo).
    # A read-only probe — fine even under --dry-run (same as `scan`'s preview).
    # ``None`` marks a project whose probe FAILED — distinct from a project
    # that simply has no live mirrors (empty set). Every unhealthy mirror on
    # a failed-probe project must go to ``probe_failed``, not be guessed.
    live_ids_by_pid: Dict[int, Optional[set]] = {}
    probe_error_by_pid: Dict[int, str] = {}
    candidates: List[_Candidate] = []
    probe_failed: List[Tuple[journal_mod.JournalRepo, journal_mod.JournalMirror, str]] = []
    for repo, m in unhealthy:
        pid = repo.primary_repo_id
        if pid not in live_ids_by_pid:
            try:
                mirrors = primary.list_mirrors(
                    token=primary_token,
                    primary_repo=RepoRef(http_url="", project_id=pid),
                )
                live_ids_by_pid[pid] = {mi.id for mi in mirrors}
            except HydraAPIError as e:
                live_ids_by_pid[pid] = None
                probe_error_by_pid[pid] = e.message
        live = live_ids_by_pid[pid]
        if live is None:
            probe_failed.append((repo, m, probe_error_by_pid[pid]))
            continue
        action = "replace" if m.push_mirror_id in live else "add"
        candidates.append((repo, m, action))

    _render_plan(console, candidates, probe_failed)

    if dry_run:
        raise typer.Exit(code=0)

    # Probe failures are non-success outcomes that must be summarised even
    # when nothing is actionable.
    probe_outcomes = [
        MirrorOpOutcome(
            repo_name=f"{r.name} → {m.target_host_id}", state="api_failed", message=msg
        )
        for r, m, msg in probe_failed
    ]

    if not candidates:
        # ``unhealthy`` was non-empty (checked earlier) so everything here
        # failed its probe — render the reasons rather than exiting silently.
        render_mirror_outcomes(console, probe_outcomes, ok_verb="repaired")
        raise typer.Exit(code=1)

    if not yes and not typer.confirm(
        f"Repair {len(candidates)} mirror(s)?", default=False
    ):
        console.print("[dim]No changes made.[/dim]")
        raise typer.Exit(code=0)

    # Resolve every distinct target token up front so a missing one fails
    # before any mutation.
    target_tokens: Dict[str, str] = {}
    target_users: Dict[str, str] = {}
    for _repo, m, _ in candidates:
        hid = m.target_host_id
        if hid not in target_tokens:
            target_tokens[hid] = _common._resolve_token_or_die(
                hid, allow_prompt=True, console=console
            )
            target_users[hid] = providers_mod.capabilities_for(
                cfg.host(hid).kind
            ).inbound_mirror_username

    outcomes: List[MirrorOpOutcome] = [
        MirrorOpOutcome(repo_name=f"{r.name} → {m.target_host_id}", state="not_attempted")
        for r, m, _ in candidates
    ]
    outcomes.extend(probe_outcomes)  # probe failures are reportable too

    try:
        with journal_mod.journal() as j:
            for idx, (repo, m, action) in enumerate(candidates):
                primary_repo = RepoRef(http_url="", project_id=repo.primary_repo_id)
                kw = dict(
                    token=primary_token,
                    primary_repo=primary_repo,
                    target_url=m.target_repo_url,
                    target_token=target_tokens[m.target_host_id],
                    target_username=target_users[m.target_host_id],
                    target_label=m.target_host_id,
                )
                label = f"{repo.name} → {m.target_host_id}"
                try:
                    if action == "add":
                        payload = primary.add_outbound_mirror(**kw)
                    else:
                        payload = primary.replace_outbound_mirror(
                            old_push_mirror_id=m.push_mirror_id, **kw
                        )
                except MirrorReplaceError as e:
                    outcomes[idx] = MirrorOpOutcome(
                        repo_name=label, state="destroyed", message=e.message, hint=e.hint
                    )
                    try:
                        j.update_mirror_status(
                            mirror_db_id=m.id,
                            last_status="broken",
                            last_error=e.message,
                            last_update_at=None,
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    console.print(f"[bold red]✗[/bold red] {label}: {e.message}")
                    if e.hint:
                        for line in e.hint.split("\n"):
                            console.print(f"    [dim]{line}[/dim]")
                    continue
                except HydraAPIError as e:
                    outcomes[idx] = MirrorOpOutcome(
                        repo_name=label, state="api_failed", message=e.message
                    )
                    console.print(f"[red]✗[/red] {label}: {e.message}")
                    continue

                new_push_id = safe_int(payload.get("id") if payload else None)
                try:
                    if new_push_id is not None:
                        j.update_mirror_push_id(
                            mirror_db_id=m.id, new_push_mirror_id=new_push_id
                        )
                    # Clear cached status: freshly (re)provisioned, real state
                    # comes from the next `status --refresh` / `scan`.
                    j.update_mirror_status(
                        mirror_db_id=m.id,
                        last_status=None,
                        last_error=None,
                        last_update_at=None,
                    )
                except Exception as e:  # noqa: BLE001
                    outcomes[idx] = MirrorOpOutcome(
                        repo_name=label, state="journal_failed", message=str(e)
                    )
                    raise
                outcomes[idx] = MirrorOpOutcome(repo_name=label, state="ok")
                console.print(f"[green]✓[/green] {label} ({action})")
    except Exception as e:  # noqa: BLE001 — catastrophic mid-repair failure
        render_mirror_outcomes(console, outcomes, ok_verb="repaired")
        console.print()
        console.print(f"[red]Journal write failed mid-repair: {e}[/red]")
        raise typer.Exit(code=1) from None

    failed = render_mirror_outcomes(console, outcomes, ok_verb="repaired")
    if failed:
        raise typer.Exit(code=1)


def _render_plan(
    console: Console,
    candidates: List[_Candidate],
    probe_failed: List[Tuple[journal_mod.JournalRepo, journal_mod.JournalMirror, str]],
) -> None:
    table = Table(show_header=True, header_style="bold", title="Repair plan")
    table.add_column("Repo")
    table.add_column("Target")
    table.add_column("Action")
    for repo, m, action in candidates:
        table.add_row(repo.name, m.target_host_id, action)
    for repo, m, msg in probe_failed:
        table.add_row(repo.name, m.target_host_id, f"[red]probe failed[/red] — {msg}")
    console.print(table)

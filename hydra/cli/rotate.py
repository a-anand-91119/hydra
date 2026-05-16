from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console

from hydra import journal as journal_mod
from hydra import providers as providers_mod
from hydra import secrets as secrets_mod
from hydra.cli import _common, app
from hydra.cli._render import MirrorOpOutcome, _render_api_error, render_mirror_outcomes
from hydra.errors import HydraAPIError, MirrorReplaceError
from hydra.mirrors import scrub_credentials
from hydra.providers.base import MirrorSource, RepoRef
from hydra.utils import safe_int


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
    cfg = _common._load_or_die(config_path, console)
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
            _common._verify_token(target_spec, new_token, console=console)
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

    primary_token = _common._resolve_token_or_die(
        primary_spec.id, allow_prompt=True, console=console
    )

    outcomes: List[MirrorOpOutcome] = [
        MirrorOpOutcome(repo_name=repo.name, state="not_attempted") for repo, _ in pairs
    ]

    try:
        with journal_mod.journal() as j:
            for idx, (repo, m) in enumerate(pairs):
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
                    outcomes[idx] = MirrorOpOutcome(
                        repo_name=repo.name, state="destroyed", message=e.message, hint=e.hint
                    )
                    # Best-effort journal write: mirror is already gone on the host,
                    # so a journal failure here doesn't change the user-visible truth.
                    try:
                        j.update_mirror_status(
                            mirror_db_id=m.id,
                            last_status="broken",
                            last_error=e.message,
                            last_update_at=None,
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    console.print(f"[bold red]✗[/bold red] {repo.name}: {e.message}")
                    if e.hint:
                        for line in e.hint.split("\n"):
                            console.print(f"    [dim]{line}[/dim]")
                    continue
                except HydraAPIError as e:
                    outcomes[idx] = MirrorOpOutcome(
                        repo_name=repo.name, state="api_failed", message=e.message
                    )
                    console.print(f"[red]✗[/red] {repo.name}: {e.message}")
                    continue

                # API succeeded. Persist the new push id; if that write fails,
                # mark the outcome and re-raise so the outer except renders the
                # full per-mirror summary before exiting.
                new_push_id = safe_int(payload.get("id") if payload else None)
                try:
                    if new_push_id is not None:
                        j.update_mirror_push_id(mirror_db_id=m.id, new_push_mirror_id=new_push_id)
                except Exception as e:  # noqa: BLE001
                    outcomes[idx] = MirrorOpOutcome(
                        repo_name=repo.name, state="journal_failed", message=str(e)
                    )
                    raise
                outcomes[idx] = MirrorOpOutcome(repo_name=repo.name, state="ok")
                console.print(f"[green]✓[/green] {repo.name}")
    except Exception as e:  # noqa: BLE001 — catastrophic mid-rotation failure
        render_mirror_outcomes(console, outcomes, ok_verb="updated")
        console.print()
        console.print(f"[red]Journal write failed mid-rotation: {e}[/red]")
        raise typer.Exit(code=1) from None

    failed = render_mirror_outcomes(console, outcomes, ok_verb="updated")
    if failed:
        raise typer.Exit(code=1)

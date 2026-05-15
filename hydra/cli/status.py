from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from hydra import providers as providers_mod
from hydra.cli import _common, app
from hydra.cli._render import _render_api_error
from hydra.errors import HydraAPIError
from hydra.hostspec_utils import match_fork
from hydra.mirrors import scrub_credentials


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
    cfg = _common._load_or_die(config_path, console)
    primary_spec = cfg.primary_host()
    primary_caps = providers_mod.capabilities_for(primary_spec.kind)
    if not primary_caps.supports_status_lookup:
        console.print(
            f"[red]Status lookup is not supported for primary kind {primary_spec.kind!r}.[/red]"
        )
        raise typer.Exit(code=1)

    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    primary_token = _common._resolve_token_or_die(
        primary_spec.id, allow_prompt=True, console=console
    )

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
        match = match_fork(m.url, fork_specs)
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

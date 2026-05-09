from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from hydra import __version__
from hydra import gitlab as gitlab_api
from hydra import github as github_api
from hydra import mirrors as mirrors_api
from hydra import secrets as secrets_mod
from hydra.config import Config, ConfigError, load_config, resolve_config_path
from hydra.errors import HydraAPIError
from hydra.wizard import (
    CreateOptions,
    WizardCancelled,
    apply_token_storage,
    run_create_wizard,
    run_wizard,
)

app = typer.Typer(
    add_completion=False,
    help="Hydra — provision a repo across self-hosted GitLab, GitLab.com, and GitHub.",
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"hydra {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(
        False, "--version", callback=_version_callback, is_eager=True
    ),
) -> None:
    pass


def _load_or_die(config_path: Optional[Path], console: Console) -> Config:
    try:
        return load_config(config_path)
    except ConfigError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)


def _resolve_token_or_die(
    service: str, *, allow_prompt: bool, console: Console
) -> str:
    try:
        return secrets_mod.get_token(service, allow_prompt=allow_prompt)
    except secrets_mod.SecretError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def create(
    name: Optional[str] = typer.Argument(
        None, help="Repository name. Omit to launch the interactive wizard."
    ),
    description: str = typer.Option("", "--description", "-d"),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Group path on self-hosted GitLab"
    ),
    public: bool = typer.Option(
        False, "--public", help="Create as public (default: private)"
    ),
    github_org: Optional[str] = typer.Option(
        None, "--github-org", help="Create under this GitHub org instead of user"
    ),
    no_mirror: bool = typer.Option(
        False, "--no-mirror", help="Skip push-mirror setup"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print planned actions without making API calls"
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Create a repo on self-hosted GitLab, GitLab.com, and GitHub."""
    console = Console()
    cfg = _load_or_die(config_path, console)

    if name is None:
        try:
            opts = run_create_wizard(cfg=cfg, console=console)
        except WizardCancelled as e:
            console.print(f"\n[yellow]Cancelled:[/yellow] {e or 'aborted'}")
            raise typer.Exit(code=1)
        except KeyboardInterrupt:
            console.print("\n[yellow]Cancelled:[/yellow] aborted")
            raise typer.Exit(code=1)
        if dry_run:
            opts.dry_run = True
    else:
        opts = CreateOptions(
            name=name,
            description=description,
            group=group if group is not None else cfg.defaults.group,
            is_private=False if public else cfg.defaults.private,
            github_org=github_org if github_org is not None else cfg.github.org,
            mirror=not no_mirror,
            dry_run=dry_run,
        )

    if opts.dry_run:
        _print_dry_run(cfg, opts)
        return

    _execute_create(cfg=cfg, opts=opts, verbose=verbose, console=console)


def _print_dry_run(cfg: Config, opts: CreateOptions) -> None:
    typer.echo(f"[dry-run] would create '{opts.name}' on:")
    typer.echo(
        f"  - self-hosted: {cfg.self_hosted_gitlab.url} "
        f"(group={opts.group or 'none'})"
    )
    typer.echo(
        f"  - gitlab.com:  {cfg.gitlab.url} "
        f"(group={cfg.gitlab.managed_group_prefix}/{opts.group or ''})"
    )
    org_label = opts.github_org or "<user>"
    typer.echo(f"  - github:      {cfg.github.url} (owner={org_label})")
    typer.echo(f"  visibility: {'private' if opts.is_private else 'public'}")
    typer.echo(f"  mirror: {'yes' if opts.mirror else 'no'}")


def _execute_create(
    *, cfg: Config, opts: CreateOptions, verbose: bool, console: Console
) -> None:
    sh_token = _resolve_token_or_die(
        "self_hosted_gitlab", allow_prompt=True, console=console
    )
    gl_token = _resolve_token_or_die("gitlab", allow_prompt=True, console=console)
    gh_token = _resolve_token_or_die("github", allow_prompt=True, console=console)

    created: list[tuple[str, str]] = []

    try:
        sh_groups = gitlab_api.get_or_create_group_path(
            host="self_hosted_gitlab",
            base_url=cfg.self_hosted_gitlab.url,
            token=sh_token,
            group_path=opts.group or None,
            add_timestamp=False,
        )
        for path in sh_groups.created_paths:
            created.append(
                ("self-hosted GitLab group", f"{cfg.self_hosted_gitlab.url}/{path}")
            )

        gl_group_path = (
            f"{cfg.gitlab.managed_group_prefix}/{opts.group}"
            if opts.group
            else cfg.gitlab.managed_group_prefix
        )
        gl_groups = gitlab_api.get_or_create_group_path(
            host="gitlab",
            base_url=cfg.gitlab.url,
            token=gl_token,
            group_path=gl_group_path,
            add_timestamp=True,
        )
        for path in gl_groups.created_paths:
            created.append(("gitlab.com group", f"{cfg.gitlab.url}/{path}"))

        if verbose:
            console.print(
                f"[dim]self-hosted group id: {sh_groups.group_id}, "
                f"gitlab.com group id: {gl_groups.group_id}[/dim]"
            )

        sh_repo = gitlab_api.create_repo(
            host="self_hosted_gitlab",
            base_url=cfg.self_hosted_gitlab.url,
            token=sh_token,
            name=opts.name,
            description=opts.description,
            namespace_id=sh_groups.group_id,
            is_private=opts.is_private,
        )
        created.append(("self-hosted GitLab repo", sh_repo.http_url))
        console.print(f"[green]✓[/green] self-hosted: {sh_repo.http_url}")

        gl_repo = gitlab_api.create_repo(
            host="gitlab",
            base_url=cfg.gitlab.url,
            token=gl_token,
            name=opts.name,
            description=opts.description,
            namespace_id=gl_groups.group_id,
            is_private=opts.is_private,
        )
        created.append(("gitlab.com repo", gl_repo.http_url))
        console.print(f"[green]✓[/green] gitlab.com:  {gl_repo.http_url}")

        gh_url = github_api.create_repo(
            base_url=cfg.github.url,
            token=gh_token,
            name=opts.name,
            description=opts.description,
            org=opts.github_org,
            is_private=opts.is_private,
        )
        created.append(("github repo", gh_url))
        console.print(f"[green]✓[/green] github:      {gh_url}")

        if opts.mirror:
            mirrors_api.setup_mirrors(
                base_url=cfg.self_hosted_gitlab.url,
                self_hosted_token=sh_token,
                project_id=sh_repo.project_id,
                github_repo_url=gh_url,
                github_token=gh_token,
                gitlab_repo_url=gl_repo.http_url,
                gitlab_token=gl_token,
            )
            console.print("[green]✓[/green] mirrors configured")

    except HydraAPIError as e:
        _render_api_error(console, e, created)
        raise typer.Exit(code=1)


def _render_api_error(
    console: Console, err: HydraAPIError, created: list[tuple[str, str]]
) -> None:
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
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Configuration not saved:[/yellow] aborted")
        raise typer.Exit(code=1)

    console.print(f"\n[green]✓[/green] Config saved to [bold]{result.config_path}[/bold]")
    apply_token_storage(result, console=console)
    console.print(
        "\n[dim]Next:[/dim] try [bold]hydra create my-repo --dry-run[/bold] "
        "to verify the setup."
    )


@app.command()
def status(
    name: str = typer.Argument(..., help="Repo name (with optional group/path)"),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Group path on self-hosted GitLab"
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Show mirror status for a repo on the self-hosted GitLab."""
    console = Console()
    cfg = _load_or_die(config_path, console)
    sh_token = _resolve_token_or_die(
        "self_hosted_gitlab", allow_prompt=True, console=console
    )

    effective_group = group if group is not None else cfg.defaults.group
    repo_path = f"{effective_group}/{name}" if effective_group else name

    try:
        project_id = mirrors_api.find_project_id(
            base_url=cfg.self_hosted_gitlab.url, token=sh_token, repo_path=repo_path
        )
        if project_id is None:
            console.print(f"[red]Project not found:[/red] {repo_path}")
            raise typer.Exit(code=1)

        mirror_list = mirrors_api.list_mirrors(
            base_url=cfg.self_hosted_gitlab.url,
            token=sh_token,
            project_id=project_id,
        )
    except HydraAPIError as e:
        _render_api_error(console, e, created=[])
        raise typer.Exit(code=1)

    if not mirror_list:
        console.print(f"No mirrors configured for {repo_path}.")
        return

    console.print(f"Mirrors for [bold]{repo_path}[/bold] (project {project_id}):")
    for m in mirror_list:
        flag = "[green]enabled [/green]" if m.enabled else "[yellow]disabled[/yellow]"
        line = f"  [{flag}] {m.url}"
        if m.last_update_status:
            line += f" — {m.last_update_status}"
        if m.last_update_at:
            line += f" @ {m.last_update_at}"
        console.print(line)
        if m.last_error:
            console.print(f"    [red]error: {m.last_error}[/red]")


@app.command("config-path")
def show_config_path(
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Print the resolved config file path."""
    typer.echo(str(resolve_config_path(config_path)))


if __name__ == "__main__":
    app()


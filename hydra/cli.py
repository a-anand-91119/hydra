from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from hydra import __version__
from hydra.config import (
    Config,
    ConfigError,
    Defaults,
    GitHubConfig,
    GitLabCloudConfig,
    HostConfig,
    load_config,
    load_config_or_default,
    resolve_config_path,
    save_config,
)
from hydra import gitlab as gitlab_api
from hydra import github as github_api
from hydra import mirrors as mirrors_api
from hydra import secrets as secrets_mod

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


def _load_or_die(config_path: Optional[Path]) -> Config:
    try:
        return load_config(config_path)
    except ConfigError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


def _resolve_token_or_die(service: str, *, allow_prompt: bool) -> str:
    try:
        return secrets_mod.get_token(service, allow_prompt=allow_prompt)
    except secrets_mod.SecretError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@app.command()
def create(
    name: str = typer.Argument(..., help="Repository name"),
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
    cfg = _load_or_die(config_path)
    is_private = False if public else cfg.defaults.private
    effective_group = group if group is not None else cfg.defaults.group
    effective_github_org = github_org if github_org is not None else cfg.github.org

    if dry_run:
        typer.echo(f"[dry-run] would create '{name}' on:")
        typer.echo(f"  - self-hosted: {cfg.self_hosted_gitlab.url} (group={effective_group or 'none'})")
        typer.echo(f"  - gitlab.com:  {cfg.gitlab.url} (group={cfg.gitlab.managed_group_prefix}/{effective_group or ''})")
        org_label = effective_github_org or "<user>"
        typer.echo(f"  - github:      {cfg.github.url} (owner={org_label})")
        typer.echo(f"  visibility: {'private' if is_private else 'public'}")
        typer.echo(f"  mirror: {'no' if no_mirror else 'yes'}")
        return

    sh_token = _resolve_token_or_die("self_hosted_gitlab", allow_prompt=True)
    gl_token = _resolve_token_or_die("gitlab", allow_prompt=True)
    gh_token = _resolve_token_or_die("github", allow_prompt=True)

    try:
        sh_group_id = gitlab_api.get_or_create_group_path(
            base_url=cfg.self_hosted_gitlab.url,
            token=sh_token,
            group_path=effective_group,
            add_timestamp=False,
        )
        gl_group_path = (
            f"{cfg.gitlab.managed_group_prefix}/{effective_group}"
            if effective_group
            else cfg.gitlab.managed_group_prefix
        )
        gl_group_id = gitlab_api.get_or_create_group_path(
            base_url=cfg.gitlab.url,
            token=gl_token,
            group_path=gl_group_path,
            add_timestamp=True,
        )

        if verbose:
            typer.echo(f"self-hosted group id: {sh_group_id}, gitlab.com group id: {gl_group_id}")

        sh_repo = gitlab_api.create_repo(
            base_url=cfg.self_hosted_gitlab.url,
            token=sh_token,
            name=name,
            description=description,
            namespace_id=sh_group_id,
            is_private=is_private,
        )
        typer.secho(f"✓ self-hosted: {sh_repo.http_url}", fg=typer.colors.GREEN)

        gl_repo = gitlab_api.create_repo(
            base_url=cfg.gitlab.url,
            token=gl_token,
            name=name,
            description=description,
            namespace_id=gl_group_id,
            is_private=is_private,
        )
        typer.secho(f"✓ gitlab.com:  {gl_repo.http_url}", fg=typer.colors.GREEN)

        gh_url = github_api.create_repo(
            base_url=cfg.github.url,
            token=gh_token,
            name=name,
            description=description,
            org=effective_github_org,
            is_private=is_private,
        )
        typer.secho(f"✓ github:      {gh_url}", fg=typer.colors.GREEN)

        if not no_mirror:
            mirrors_api.setup_mirrors(
                base_url=cfg.self_hosted_gitlab.url,
                self_hosted_token=sh_token,
                project_id=sh_repo.project_id,
                github_repo_url=gh_url,
                github_token=gh_token,
                gitlab_repo_url=gl_repo.http_url,
                gitlab_token=gl_token,
            )
            typer.secho("✓ mirrors configured", fg=typer.colors.GREEN)

    except (gitlab_api.GitLabError, github_api.GitHubError, mirrors_api.MirrorError) as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@app.command()
def configure(
    config_path: Optional[Path] = typer.Option(None, "--config"),
    store: str = typer.Option(
        "keyring",
        "--store",
        help="Where to store tokens: 'keyring' or 'env' (prints export lines)",
    ),
) -> None:
    """Interactively set up config and tokens."""
    if store not in ("keyring", "env"):
        typer.secho("--store must be 'keyring' or 'env'", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    existing = load_config_or_default(config_path)

    typer.echo("Configuring hydra. Press Enter to keep existing values.\n")

    sh_url = typer.prompt(
        "Self-hosted GitLab URL",
        default=existing.self_hosted_gitlab.url or "https://gitlab.example.com",
    )
    gl_url = typer.prompt("GitLab.com URL", default=existing.gitlab.url)
    gl_prefix = typer.prompt(
        "GitLab.com managed group prefix", default=existing.gitlab.managed_group_prefix
    )
    gh_url = typer.prompt("GitHub API URL", default=existing.github.url)
    gh_org = typer.prompt(
        "GitHub org (blank for user account)", default=existing.github.org or "", show_default=False
    )
    default_group = typer.prompt(
        "Default group path (blank for none)", default=existing.defaults.group, show_default=False
    )
    default_private = typer.confirm(
        "Default to private repos?", default=existing.defaults.private
    )

    cfg = Config(
        self_hosted_gitlab=HostConfig(url=sh_url),
        gitlab=GitLabCloudConfig(url=gl_url, managed_group_prefix=gl_prefix),
        github=GitHubConfig(url=gh_url, org=gh_org or None),
        defaults=Defaults(private=default_private, group=default_group),
    )
    saved_path = save_config(cfg, config_path)
    typer.secho(f"✓ Wrote config to {saved_path}", fg=typer.colors.GREEN)

    typer.echo("\nNow let's collect API tokens (input is hidden).")
    tokens = {}
    for service in secrets_mod.SERVICES:
        token = typer.prompt(f"  {service} token (blank to skip)", default="", hide_input=True, show_default=False)
        if token:
            tokens[service] = token

    if not tokens:
        typer.echo("No tokens entered.")
        return

    if store == "keyring":
        for service, token in tokens.items():
            secrets_mod.set_token(service, token)
            typer.secho(f"✓ stored {service} in keyring", fg=typer.colors.GREEN)
    else:
        typer.echo("\nAdd these to your shell or .env:")
        typer.echo(secrets_mod.export_lines(tokens))


@app.command()
def status(
    name: str = typer.Argument(..., help="Repo name (with optional group/path)"),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Group path on self-hosted GitLab"
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Show mirror status for a repo on the self-hosted GitLab."""
    cfg = _load_or_die(config_path)
    sh_token = _resolve_token_or_die("self_hosted_gitlab", allow_prompt=True)

    effective_group = group if group is not None else cfg.defaults.group
    repo_path = f"{effective_group}/{name}" if effective_group else name

    try:
        project_id = mirrors_api.find_project_id(
            base_url=cfg.self_hosted_gitlab.url, token=sh_token, repo_path=repo_path
        )
        if project_id is None:
            typer.secho(f"Project not found: {repo_path}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)

        mirror_list = mirrors_api.list_mirrors(
            base_url=cfg.self_hosted_gitlab.url, token=sh_token, project_id=project_id
        )
    except mirrors_api.MirrorError as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    if not mirror_list:
        typer.echo(f"No mirrors configured for {repo_path}.")
        return

    typer.echo(f"Mirrors for {repo_path} (project {project_id}):")
    for m in mirror_list:
        flag = "enabled " if m.enabled else "disabled"
        line = f"  [{flag}] {m.url}"
        if m.last_update_status:
            line += f" — {m.last_update_status}"
        if m.last_update_at:
            line += f" @ {m.last_update_at}"
        typer.echo(line)
        if m.last_error:
            typer.secho(f"    error: {m.last_error}", fg=typer.colors.RED)


@app.command("config-path")
def show_config_path(
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Print the resolved config file path."""
    typer.echo(str(resolve_config_path(config_path)))


if __name__ == "__main__":
    app()

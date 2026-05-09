from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import typer
import yaml
from rich.console import Console

from hydra import __version__
from hydra import doctor as doctor_mod
from hydra import providers as providers_mod
from hydra import secrets as secrets_mod
from hydra.config import Config, ConfigError, HostSpec, load_config, resolve_config_path
from hydra.errors import HydraAPIError
from hydra.mirrors import scrub_credentials
from hydra.providers.base import MirrorSource, RepoRef
from hydra.wizard import (
    CreateOptions,
    WizardCancelled,
    apply_token_storage,
    run_create_wizard,
    run_wizard,
)

# Register built-in providers exactly once at CLI entry.
providers_mod.bootstrap()

app = typer.Typer(
    add_completion=False,
    help="Hydra — provision a repo across one primary and N forks.",
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"hydra {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(False, "--version", callback=_version_callback, is_eager=True),
) -> None:
    pass


def _load_or_die(config_path: Optional[Path], console: Console) -> Config:
    try:
        return load_config(config_path)
    except ConfigError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1) from None


def _resolve_token_or_die(host_id: str, *, allow_prompt: bool, console: Console) -> str:
    try:
        return secrets_mod.get_token(host_id, allow_prompt=allow_prompt)
    except secrets_mod.SecretError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1) from None


def _parse_host_options(values: List[str]) -> Dict[str, Dict[str, Any]]:
    """Parse repeated --host-option `id.key=value` pairs.

    Values are YAML-parsed so booleans, ints, and null work naturally:
        --host-option github.org=acme       → "acme"
        --host-option gl.add_timestamp=true → True
        --host-option gl.retries=3          → 3

    Only the FIRST `=` splits key from value, so values may contain `=`.
    Only the FIRST `.` splits id from key, so keys may not (use a dotted
    YAML structure in the config file for nested options).
    """
    out: Dict[str, Dict[str, Any]] = {}
    for raw in values:
        if "=" not in raw:
            raise typer.BadParameter(
                f"--host-option must be `id.key=value`, got {raw!r}"
            )
        spec, value = raw.split("=", 1)
        if "." not in spec:
            raise typer.BadParameter(
                f"--host-option spec must be `id.key`, got {spec!r}"
            )
        host_id, key = spec.split(".", 1)
        host_id = host_id.strip()
        key = key.strip()
        if not host_id:
            raise typer.BadParameter(f"--host-option missing host id: {raw!r}")
        if not key:
            raise typer.BadParameter(f"--host-option missing key: {raw!r}")
        try:
            parsed: Any = yaml.safe_load(value)
        except yaml.YAMLError:
            parsed = value
        out.setdefault(host_id, {})[key] = parsed
    return out


def _apply_overrides(cfg: Config, overrides: Dict[str, Dict[str, Any]]) -> Config:
    if not overrides:
        return cfg
    cfg = copy.deepcopy(cfg)
    for host_id, kvs in overrides.items():
        try:
            host = cfg.host(host_id)
        except KeyError:
            raise typer.BadParameter(
                f"--host-option references unknown host {host_id!r}"
            ) from None
        host.options.update(kvs)
    return cfg


@app.command()
def create(
    name: Optional[str] = typer.Argument(
        None, help="Repository name. Omit to launch the interactive wizard."
    ),
    description: str = typer.Option("", "--description", "-d"),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Group path on the primary host"
    ),
    public: bool = typer.Option(False, "--public", help="Create as public (default: private)"),
    host_option: List[str] = typer.Option(
        [],
        "--host-option",
        help="Override per-host option: `host_id.key=value` (repeatable). "
        "Value is YAML-parsed (booleans/null/ints supported). "
        "Example: --host-option github.org=acme",
    ),
    no_mirror: bool = typer.Option(False, "--no-mirror", help="Skip push-mirror setup"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print planned actions without making API calls"
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Create a repo on the primary host and mirror to all forks."""
    console = Console()
    cfg = _load_or_die(config_path, console)
    cfg = _apply_overrides(cfg, _parse_host_options(host_option))

    if name is None:
        try:
            opts = run_create_wizard(cfg=cfg, console=console)
        except WizardCancelled as e:
            console.print(f"\n[yellow]Cancelled:[/yellow] {e or 'aborted'}")
            raise typer.Exit(code=1) from None
        except KeyboardInterrupt:
            console.print("\n[yellow]Cancelled:[/yellow] aborted")
            raise typer.Exit(code=1) from None
        if dry_run:
            opts.dry_run = True
    else:
        opts = CreateOptions(
            name=name,
            description=description,
            group=group if group is not None else cfg.defaults.group,
            is_private=False if public else cfg.defaults.private,
            mirror=not no_mirror,
            dry_run=dry_run,
        )

    if opts.dry_run:
        _print_dry_run(cfg, opts)
        return

    _execute_create(cfg=cfg, opts=opts, verbose=verbose, console=console)


def _print_dry_run(cfg: Config, opts: CreateOptions) -> None:
    primary = cfg.primary_host()
    typer.echo(f"[dry-run] would create '{opts.name}' on:")
    typer.echo(f"  primary  · {primary.id} ({primary.url}) (group={opts.group or 'none'})")
    for fork in cfg.fork_hosts():
        typer.echo(f"  fork     · {fork.id} ({fork.url})")
    typer.echo(f"  visibility: {'private' if opts.is_private else 'public'}")
    typer.echo(f"  mirror: {'yes' if opts.mirror else 'no'}")


def _execute_create(*, cfg: Config, opts: CreateOptions, verbose: bool, console: Console) -> None:
    primary_spec = cfg.primary_host()
    fork_specs = cfg.fork_hosts()

    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    forks = [(spec, providers_mod.get(spec.kind)(spec)) for spec in fork_specs]

    primary_token = _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)
    fork_tokens: Dict[str, str] = {
        spec.id: _resolve_token_or_die(spec.id, allow_prompt=True, console=console)
        for spec in fork_specs
    }

    created: List[Tuple[str, str]] = []
    primary_repo: Optional[RepoRef] = None

    try:
        ns = primary.ensure_namespace(group_path=opts.group or None, token=primary_token)
        for path in ns.created_paths:
            created.append((f"{primary_spec.id} group", f"{primary_spec.url}/{path}"))

        primary_repo = primary.create_repo(
            token=primary_token,
            name=opts.name,
            description=opts.description,
            namespace=ns,
            is_private=opts.is_private,
        )
        created.append((f"{primary_spec.id} repo", primary_repo.http_url))
        console.print(f"[green]✓[/green] {primary_spec.id}: {primary_repo.http_url}")

        if verbose and ns.namespace_id is not None:
            console.print(f"[dim]{primary_spec.id} group id: {ns.namespace_id}[/dim]")

        fork_repos: List[Tuple[HostSpec, Any, RepoRef]] = []
        for spec, prov in forks:
            tok = fork_tokens[spec.id]
            f_ns = prov.ensure_namespace(group_path=opts.group or None, token=tok)
            for path in f_ns.created_paths:
                created.append((f"{spec.id} group", f"{spec.url}/{path}"))
            f_repo = prov.create_repo(
                token=tok,
                name=opts.name,
                description=opts.description,
                namespace=f_ns,
                is_private=opts.is_private,
            )
            created.append((f"{spec.id} repo", f_repo.http_url))
            console.print(f"[green]✓[/green] {spec.id}: {f_repo.http_url}")
            fork_repos.append((spec, prov, f_repo))

        if opts.mirror:
            # Config validation already enforces this, but assert to satisfy
            # the type narrowing.
            assert isinstance(primary, MirrorSource)
            mirrored: List[str] = []
            try:
                for spec, _prov, f_repo in fork_repos:
                    fork_caps = providers_mod.capabilities_for(spec.kind)
                    primary.add_outbound_mirror(
                        token=primary_token,
                        primary_repo=primary_repo,
                        target_url=f_repo.http_url,
                        target_token=fork_tokens[spec.id],
                        target_username=fork_caps.inbound_mirror_username,
                        target_label=spec.id,
                    )
                    mirrored.append(spec.id)
                console.print(
                    f"[green]✓[/green] mirrors configured: {', '.join(mirrored) or '(none)'}"
                )
            except HydraAPIError:
                if mirrored:
                    console.print(
                        f"[yellow]⚠[/yellow] mirrors configured for: {', '.join(mirrored)}"
                    )
                raise

    except HydraAPIError as e:
        _render_api_error(console, e, created)
        raise typer.Exit(code=1) from None


def _render_api_error(
    console: Console, err: HydraAPIError, created: List[Tuple[str, str]]
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
        raise typer.Exit(code=1) from None
    except KeyboardInterrupt:
        console.print("\n[yellow]Configuration not saved:[/yellow] aborted")
        raise typer.Exit(code=1) from None

    console.print(f"\n[green]✓[/green] Config saved to [bold]{result.config_path}[/bold]")
    apply_token_storage(result, console=console)
    console.print(
        "\n[dim]Next:[/dim] try [bold]hydra create my-repo --dry-run[/bold] to verify the setup."
    )


@app.command()
def status(
    name: str = typer.Argument(..., help="Repo name (with optional group/path)"),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Group path on the primary host"
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Show mirror status for a repo on the primary host."""
    console = Console()
    cfg = _load_or_die(config_path, console)
    primary_spec = cfg.primary_host()
    primary_caps = providers_mod.capabilities_for(primary_spec.kind)
    if not primary_caps.supports_status_lookup:
        console.print(
            f"[red]Status lookup is not supported for primary kind {primary_spec.kind!r}.[/red]"
        )
        raise typer.Exit(code=1)

    primary = providers_mod.get(primary_spec.kind)(primary_spec)
    primary_token = _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)

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
        match = _match_fork(m.url, fork_specs)
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


def _match_fork(mirror_url: str, forks: List[HostSpec]) -> Optional[HostSpec]:
    """Match a mirror URL to a configured fork by exact hostname (case-insensitive).

    Substring matching would be unsafe (e.g. `gitlab.com` would match
    `evilgitlab.com.attacker.example`).
    """
    try:
        mirror_host = (urlparse(mirror_url).hostname or "").lower()
    except ValueError:
        return None
    if not mirror_host:
        return None
    for spec in forks:
        try:
            spec_host = (urlparse(spec.url).hostname or "").lower()
        except ValueError:
            continue
        if spec_host and spec_host == mirror_host:
            return spec
    return None


@app.command()
def doctor(
    fix: bool = typer.Option(
        False, "--fix", help="Apply safe fixes (run pending migrations, etc.)"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show full details for each finding"
    ),
    check_keyring: bool = typer.Option(
        False,
        "--check-keyring",
        help="Probe the OS keyring for stored tokens. May prompt for "
        "Keychain access on macOS — disabled by default.",
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Diagnose configuration, tokens, and topology. Use --fix to apply
    pending migrations and other safe automatic fixes.
    """
    console = Console()
    result = doctor_mod.run_doctor(
        config_path=config_path,
        fix=fix,
        verbose=verbose,
        check_keyring=check_keyring,
        console=console,
    )
    if result.exit_code != 0:
        raise typer.Exit(code=result.exit_code)


@app.command("config-path")
def show_config_path(
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Print the resolved config file path."""
    typer.echo(str(resolve_config_path(config_path)))


if __name__ == "__main__":
    app()

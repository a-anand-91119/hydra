"""Shared CLI helpers: config loading, token resolution, host-option parsing,
preflight, and per-host token verification.

These are lifted out of the per-command modules because each is used by 2+
commands. Tests patch ``preflight_mod.check_tokens`` and
``secrets_mod.get_token`` via the ``hydra.cli`` namespace; ``__init__.py``
re-exports both modules so existing patch paths keep resolving.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
import yaml
from rich.console import Console

from hydra import preflight as preflight_mod
from hydra import secrets as secrets_mod
from hydra.config import Config, ConfigError, HostSpec, load_config
from hydra.errors import HydraAPIError


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


def _resolve_tokens_or_die(cfg: Config, *, console: Console) -> Dict[str, str]:
    """Resolve tokens for every configured host. Lifted out so the CLI can
    pre-resolve once and hand them to both preflight + executor.
    """
    primary_spec = cfg.primary_host()
    fork_specs = cfg.fork_hosts()
    tokens: Dict[str, str] = {
        primary_spec.id: _resolve_token_or_die(primary_spec.id, allow_prompt=True, console=console)
    }
    for spec in fork_specs:
        if spec.id not in tokens:
            tokens[spec.id] = _resolve_token_or_die(spec.id, allow_prompt=True, console=console)
    return tokens


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
            raise typer.BadParameter(f"--host-option must be `id.key=value`, got {raw!r}")
        spec, value = raw.split("=", 1)
        if "." not in spec:
            raise typer.BadParameter(f"--host-option spec must be `id.key`, got {spec!r}")
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
            raise typer.BadParameter(f"--host-option references unknown host {host_id!r}") from None
        host.options.update(kvs)
    return cfg


def _preflight_or_die(*, cfg: Config, tokens: Dict[str, str], console: Console) -> None:
    """Probe every token before mutating. Exit 1 on any error finding;
    print warnings inline and continue.
    """
    report = preflight_mod.check_tokens(cfg.hosts, tokens)
    for w in report.warnings:
        console.print(f"[yellow]⚠ {w.message}[/yellow]")
    if not report.errors:
        return
    console.print()
    console.print("[bold red]✗ Token preflight failed:[/bold red]")
    for err in report.errors:
        console.print(f"  [red]•[/red] {err.message}")
        if err.hint:
            for line in err.hint.split("\n"):
                console.print(f"    [dim]{line}[/dim]")
    console.print()
    console.print(
        "[dim]Pass [bold]--skip-preflight[/bold] to bypass this check "
        "(may orphan resources on failure).[/dim]"
    )
    raise typer.Exit(code=1) from None


def _verify_token(spec: HostSpec, token: str, *, console: Optional[Console] = None) -> None:
    """Probe the host using a token. Raises HydraAPIError on failure.

    Also checks that the token carries the scopes hydra needs (via the
    shared preflight). For unknown provider kinds, no probe runs and a
    warning is printed so the user knows verification was skipped.
    """
    if spec.kind == "gitlab":
        from hydra import gitlab as gitlab_api

        gitlab_api.verify_token(host=spec.id, base_url=spec.url, token=token)
    elif spec.kind == "github":
        from hydra import github as github_api

        github_api.verify_token(base_url=spec.url, token=token)
    elif console is not None:
        console.print(
            f"[yellow]⚠ no token-verification probe for provider kind "
            f"{spec.kind!r} — skipping pre-flight check.[/yellow]"
        )
        return

    report = preflight_mod.check_tokens([spec], {spec.id: token})
    if report.errors:
        err = report.errors[0]
        raise HydraAPIError(message=err.message, host=spec.id, hint=err.hint)
    if console is not None:
        for w in report.warnings:
            console.print(f"[yellow]⚠ {w.message}[/yellow]")

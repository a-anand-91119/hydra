"""Token storage keyed by host id.

Resolution order: env var → .env → keyring → interactive prompt.
Env-first ordering means an explicit `HYDRA_TOKEN_<ID>` always wins, which is
what users expect for CI overrides and for invalidating a stale keyring entry.
Use `hydra configure` to populate the keyring for desktop convenience.
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import keyring
from dotenv import load_dotenv

KEYRING_SERVICE = "hydra"


@dataclass
class TokenScopes:
    """What a network probe learned about a token.

    `scopes_known` is False when the host doesn't expose scopes (e.g., GitHub
    fine-grained PATs return an empty X-OAuth-Scopes header even though they
    have permissions). In that case `scopes` is empty but the token IS valid.
    """

    scopes: List[str]
    expires_at: Optional[str] = None
    scopes_known: bool = True


# Backward-compat env vars only honored when the host id matches one of the
# legacy three. Any new id gets the modern HYDRA_TOKEN_<UPPER_ID> scheme.
_LEGACY_ENV_VARS = {
    "github": "HYDRA_GITHUB_TOKEN",
    "gitlab": "HYDRA_GITLAB_TOKEN",
    "self_hosted_gitlab": "HYDRA_SELF_HOSTED_GITLAB_TOKEN",
}


class SecretError(Exception):
    pass


_dotenv_loaded = False


def _ensure_dotenv_loaded() -> None:
    global _dotenv_loaded
    if _dotenv_loaded:
        return
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)
    _dotenv_loaded = True


def _validate_id(host_id: str) -> None:
    if not host_id or not host_id.strip():
        raise SecretError("host id must be non-empty")


def env_var_for(host_id: str) -> str:
    """Canonical env var name for a host id."""
    safe = re.sub(r"[^A-Za-z0-9_]", "_", host_id).upper()
    return f"HYDRA_TOKEN_{safe}"


def _candidate_env_vars(host_id: str) -> List[str]:
    names = [env_var_for(host_id)]
    legacy = _LEGACY_ENV_VARS.get(host_id)
    if legacy and legacy not in names:
        names.append(legacy)
    return names


def get_token(host_id: str, *, allow_prompt: bool = True) -> str:
    """Resolve a token. Order: env → dotenv → keyring → prompt.

    Env-first lets users override stale keyring entries without ceremony.
    """
    _validate_id(host_id)
    candidates = _candidate_env_vars(host_id)

    for name in candidates:
        token = os.environ.get(name)
        if token:
            return token

    _ensure_dotenv_loaded()
    for name in candidates:
        token = os.environ.get(name)
        if token:
            return token

    try:
        token = keyring.get_password(KEYRING_SERVICE, host_id)
        if token:
            return token
    except keyring.errors.KeyringError:
        pass

    if allow_prompt and sys.stdin.isatty():
        import typer

        return typer.prompt(f"{host_id} token", hide_input=True)

    raise SecretError(
        f"No token found for {host_id}. Set {candidates[0]}, store via "
        f"`hydra configure`, or add it to a .env file."
    )


def set_token(host_id: str, token: str) -> None:
    _validate_id(host_id)
    try:
        keyring.set_password(KEYRING_SERVICE, host_id, token)
    except keyring.errors.KeyringError as e:
        raise SecretError(
            f"Could not store token for {host_id} in the OS keyring: {e}. "
            f"Set {env_var_for(host_id)} in your shell or a .env file instead."
        ) from None


def delete_token(host_id: str) -> None:
    _validate_id(host_id)
    try:
        keyring.delete_password(KEYRING_SERVICE, host_id)
    except keyring.errors.KeyringError:
        pass


def export_lines(tokens: Dict[str, str]) -> str:
    """Render shell-export lines for a {host_id: token} mapping."""
    lines = []
    for host_id, token in tokens.items():
        if not host_id:
            continue
        lines.append(f"export {env_var_for(host_id)}={token}")
    return "\n".join(lines)

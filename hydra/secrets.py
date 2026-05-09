from __future__ import annotations

import os
import sys
from pathlib import Path

import keyring
from dotenv import load_dotenv

KEYRING_SERVICE = "hydra"

SERVICES = ("github", "gitlab", "self_hosted_gitlab")

ENV_VAR_BY_SERVICE = {
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


def get_token(service: str, *, allow_prompt: bool = True) -> str:
    if service not in SERVICES:
        raise SecretError(f"Unknown secret service: {service}")

    try:
        token = keyring.get_password(KEYRING_SERVICE, service)
        if token:
            return token
    except keyring.errors.KeyringError:
        pass

    env_var = ENV_VAR_BY_SERVICE[service]
    token = os.environ.get(env_var)
    if token:
        return token

    _ensure_dotenv_loaded()
    token = os.environ.get(env_var)
    if token:
        return token

    if allow_prompt and sys.stdin.isatty():
        import typer

        return typer.prompt(f"{service} token", hide_input=True)

    raise SecretError(
        f"No token found for {service}. Set {env_var}, store via `hydra configure`, "
        f"or add it to a .env file."
    )


def set_token(service: str, token: str) -> None:
    if service not in SERVICES:
        raise SecretError(f"Unknown secret service: {service}")
    keyring.set_password(KEYRING_SERVICE, service, token)


def delete_token(service: str) -> None:
    if service not in SERVICES:
        raise SecretError(f"Unknown secret service: {service}")
    try:
        keyring.delete_password(KEYRING_SERVICE, service)
    except keyring.errors.PasswordDeleteError:
        pass


def export_lines(tokens: dict[str, str]) -> str:
    lines = []
    for service, token in tokens.items():
        if service not in SERVICES:
            continue
        lines.append(f"export {ENV_VAR_BY_SERVICE[service]}={token}")
    return "\n".join(lines)

from __future__ import annotations

import json

from hydra import http
from hydra.errors import raise_for_response


def create_repo(
    *,
    base_url: str,
    token: str,
    name: str,
    description: str,
    org: str | None = None,
    is_private: bool = True,
) -> str:
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }
    body = json.dumps({"name": name, "description": description, "private": is_private})

    if org:
        url = f"{base_url}/orgs/{org}/repos"
        action = f"creating repo '{name}' under org '{org}'"
    else:
        url = f"{base_url}/user/repos"
        action = f"creating repo '{name}' under user account"

    response = http.post(url, headers=headers, data=body)
    raise_for_response(response, host="github", action=action, host_url=base_url)
    return response.json()["clone_url"]


def verify_token(*, base_url: str, token: str) -> None:
    """Probe GitHub with the new token. Raises HydraAPIError on failure."""
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }
    response = http.get(f"{base_url}/user", headers=headers)
    raise_for_response(response, host="github", action="verifying token", host_url=base_url)


def inspect_token(*, base_url: str, token: str):
    """Return scopes for a GitHub token.

    Classic PATs surface scopes via the ``X-OAuth-Scopes`` response header.
    Fine-grained PATs return an empty header — we report ``scopes_known=False``
    in that case (the token is still valid; permissions just aren't introspectable).
    """
    from hydra.secrets import TokenScopes

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }
    response = http.get(f"{base_url}/user", headers=headers)
    raise_for_response(response, host="github", action="inspecting token", host_url=base_url)
    raw = response.headers.get("X-OAuth-Scopes", "")
    scopes = [s.strip() for s in raw.split(",") if s.strip()]
    return TokenScopes(scopes=scopes, expires_at=None, scopes_known=bool(scopes))

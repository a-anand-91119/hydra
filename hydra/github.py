from __future__ import annotations

import json

import requests

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

    response = requests.post(url, headers=headers, data=body)
    raise_for_response(response, host="github", action=action, host_url=base_url)
    return response.json()["clone_url"]

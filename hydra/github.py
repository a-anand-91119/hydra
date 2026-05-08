from __future__ import annotations

import json
from typing import Optional

import requests


class GitHubError(Exception):
    pass


def create_repo(
    *,
    base_url: str,
    token: str,
    name: str,
    description: str,
    org: Optional[str] = None,
    is_private: bool = True,
) -> str:
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }
    body = json.dumps({"name": name, "description": description, "private": is_private})

    if org:
        url = f"{base_url}/orgs/{org}/repos"
    else:
        url = f"{base_url}/user/repos"

    response = requests.post(url, headers=headers, data=body)
    if response.status_code != 201:
        raise GitHubError(
            f"Failed to create repo {name} on GitHub: "
            f"{response.status_code} {response.text}"
        )
    return response.json()["clone_url"]

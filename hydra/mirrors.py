from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse, urlunparse

import requests


class MirrorError(Exception):
    pass


@dataclass
class Mirror:
    url: str
    enabled: bool
    last_update_status: Optional[str]
    last_update_at: Optional[str]
    last_error: Optional[str]


def _inject_credentials(repo_url: str, username: str, token: str) -> str:
    parsed = urlparse(repo_url)
    netloc = f"{username}:{token}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


def _add_mirror(
    *,
    base_url: str,
    token: str,
    project_id: int,
    mirror_url: str,
) -> dict:
    headers = {"PRIVATE-TOKEN": token}
    data = {"url": mirror_url, "enabled": True, "only_protected_branches": False}
    response = requests.post(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors",
        headers=headers,
        data=data,
    )
    if response.status_code not in (200, 201):
        raise MirrorError(
            f"Failed to add mirror: {response.status_code} {response.text}"
        )
    return response.json()


def setup_mirrors(
    *,
    base_url: str,
    self_hosted_token: str,
    project_id: int,
    github_repo_url: str,
    github_token: str,
    gitlab_repo_url: str,
    gitlab_token: str,
) -> list[dict]:
    github_mirror_url = _inject_credentials(github_repo_url, "oauth2", github_token)
    gitlab_mirror_url = _inject_credentials(gitlab_repo_url, "oauth2", gitlab_token)

    return [
        _add_mirror(
            base_url=base_url,
            token=self_hosted_token,
            project_id=project_id,
            mirror_url=github_mirror_url,
        ),
        _add_mirror(
            base_url=base_url,
            token=self_hosted_token,
            project_id=project_id,
            mirror_url=gitlab_mirror_url,
        ),
    ]


def list_mirrors(
    *, base_url: str, token: str, project_id: int
) -> list[Mirror]:
    headers = {"PRIVATE-TOKEN": token}
    response = requests.get(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors", headers=headers
    )
    if response.status_code != 200:
        raise MirrorError(
            f"Failed to list mirrors: {response.status_code} {response.text}"
        )
    return [
        Mirror(
            url=m.get("url", ""),
            enabled=bool(m.get("enabled")),
            last_update_status=m.get("last_update_status"),
            last_update_at=m.get("last_update_at"),
            last_error=m.get("last_error"),
        )
        for m in response.json()
    ]


def find_project_id(
    *, base_url: str, token: str, repo_path: str
) -> Optional[int]:
    headers = {"PRIVATE-TOKEN": token}
    from urllib.parse import quote

    encoded = quote(repo_path, safe="")
    response = requests.get(
        f"{base_url}/api/v4/projects/{encoded}", headers=headers
    )
    if response.status_code == 200:
        return response.json().get("id")
    if response.status_code == 404:
        return None
    raise MirrorError(
        f"Failed to look up project {repo_path}: "
        f"{response.status_code} {response.text}"
    )

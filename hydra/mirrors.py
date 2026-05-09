from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote, urlparse, urlunparse

import requests

from hydra.errors import raise_for_response


@dataclass
class Mirror:
    url: str
    enabled: bool
    last_update_status: str | None
    last_update_at: str | None
    last_error: str | None


def _inject_credentials(repo_url: str, username: str, token: str) -> str:
    parsed = urlparse(repo_url)
    if not parsed.hostname:
        raise ValueError(f"Cannot inject credentials into URL without host: {repo_url!r}")
    encoded_user = quote(username, safe="")
    encoded_token = quote(token, safe="")
    netloc = f"{encoded_user}:{encoded_token}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


def _add_mirror(
    *,
    base_url: str,
    token: str,
    project_id: int,
    mirror_url: str,
    target_label: str,
) -> dict:
    headers = {"PRIVATE-TOKEN": token}
    data = {"url": mirror_url, "enabled": True, "only_protected_branches": False}
    response = requests.post(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors",
        headers=headers,
        data=data,
    )
    raise_for_response(
        response,
        host="self_hosted_gitlab",
        action=f"adding {target_label} mirror to project {project_id}",
        host_url=base_url,
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
            target_label="GitHub",
        ),
        _add_mirror(
            base_url=base_url,
            token=self_hosted_token,
            project_id=project_id,
            mirror_url=gitlab_mirror_url,
            target_label="GitLab.com",
        ),
    ]


def list_mirrors(*, base_url: str, token: str, project_id: int) -> list[Mirror]:
    headers = {"PRIVATE-TOKEN": token}
    response = requests.get(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors", headers=headers
    )
    raise_for_response(
        response,
        host="self_hosted_gitlab",
        action=f"listing mirrors for project {project_id}",
        host_url=base_url,
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


def find_project_id(*, base_url: str, token: str, repo_path: str) -> int | None:
    headers = {"PRIVATE-TOKEN": token}
    encoded = quote(repo_path, safe="")
    response = requests.get(f"{base_url}/api/v4/projects/{encoded}", headers=headers)
    if response.status_code == 200:
        return response.json().get("id")
    if response.status_code == 404:
        return None
    raise_for_response(
        response,
        host="self_hosted_gitlab",
        action=f"looking up project '{repo_path}'",
        host_url=base_url,
    )
    return None  # unreachable; raise_for_response always raises on non-2xx

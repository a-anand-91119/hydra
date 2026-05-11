from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlparse, urlunparse

from hydra.errors import raise_for_response
from hydra.gitlab import _session


@dataclass
class Mirror:
    id: int
    url: str
    enabled: bool
    last_update_status: Optional[str]
    last_update_at: Optional[str]
    last_error: Optional[str]


def inject_credentials(repo_url: str, username: str, token: str) -> str:
    """Inject `username:token` userinfo into a repo URL for push-mirror auth."""
    parsed = urlparse(repo_url)
    if not parsed.hostname:
        raise ValueError(f"Cannot inject credentials into URL without host: {repo_url!r}")
    encoded_user = quote(username, safe="")
    encoded_token = quote(token, safe="")
    netloc = f"{encoded_user}:{encoded_token}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


def scrub_credentials(url: str) -> str:
    """Strip userinfo (`user:pass@`) from a URL for safe display/logging.

    Returns the input unchanged if it doesn't parse as a URL.
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return url
    if not parsed.hostname:
        return url
    netloc = parsed.hostname
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


# Backward-compat private alias (kept for any direct test imports).
_inject_credentials = inject_credentials


def add_mirror(
    *,
    host_id: str,
    base_url: str,
    token: str,
    project_id: int,
    mirror_url: str,
    target_label: str,
) -> Dict[str, Any]:
    headers = {"PRIVATE-TOKEN": token}
    data = {"url": mirror_url, "enabled": True, "only_protected_branches": False}
    response = _session().post(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors",
        headers=headers,
        data=data,
    )
    raise_for_response(
        response,
        host=host_id,
        action=f"adding {target_label} mirror to project {project_id}",
        host_url=base_url,
    )
    return response.json()


def list_mirrors(*, host_id: str, base_url: str, token: str, project_id: int) -> List[Mirror]:
    headers = {"PRIVATE-TOKEN": token}
    response = _session().get(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors", headers=headers
    )
    raise_for_response(
        response,
        host=host_id,
        action=f"listing mirrors for project {project_id}",
        host_url=base_url,
    )
    return [
        Mirror(
            id=int(m.get("id", 0)),
            url=m.get("url", ""),
            enabled=bool(m.get("enabled")),
            last_update_status=m.get("last_update_status"),
            last_update_at=m.get("last_update_at"),
            last_error=m.get("last_error"),
        )
        for m in response.json()
    ]


def delete_mirror(
    *,
    host_id: str,
    base_url: str,
    token: str,
    project_id: int,
    mirror_id: int,
) -> None:
    """Remove a push-mirror by id. GitLab returns 204 No Content on success."""
    headers = {"PRIVATE-TOKEN": token}
    response = _session().delete(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors/{mirror_id}",
        headers=headers,
    )
    if response.status_code in (200, 202, 204):
        return
    raise_for_response(
        response,
        host=host_id,
        action=f"deleting mirror {mirror_id} on project {project_id}",
        host_url=base_url,
    )


def find_project_id(*, host_id: str, base_url: str, token: str, repo_path: str) -> Optional[int]:
    headers = {"PRIVATE-TOKEN": token}
    encoded = quote(repo_path, safe="")
    response = _session().get(f"{base_url}/api/v4/projects/{encoded}", headers=headers)
    if response.status_code == 200:
        return response.json().get("id")
    if response.status_code == 404:
        return None
    raise_for_response(
        response,
        host=host_id,
        action=f"looking up project '{repo_path}'",
        host_url=base_url,
    )
    # Unreachable — raise_for_response always raises on non-2xx.
    raise AssertionError("unreachable")

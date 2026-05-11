from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import quote

import requests

from hydra.errors import raise_for_response
from hydra.utils import create_slug


@dataclass
class GitLabMirrorSummary:
    """One push-mirror entry from GitLab's /remote_mirrors response."""

    id: int
    url: str


@dataclass
class GitLabProjectSummary:
    """Raw GitLab project record + its push-mirrors. Layer-pure: no provider types."""

    project_id: int
    web_url: str
    name: str
    full_path: str
    mirrors: List[GitLabMirrorSummary]


@dataclass
class CreatedRepo:
    http_url: str
    project_id: int


@dataclass
class GroupResolution:
    """Result of resolving / creating a nested group path."""

    group_id: int | None
    created_paths: list[str] = field(default_factory=list)


def create_repo(
    *,
    host: str,
    base_url: str,
    token: str,
    name: str,
    description: str,
    namespace_id: int | None = None,
    is_private: bool = True,
) -> CreatedRepo:
    headers = {"PRIVATE-TOKEN": token}
    data = {
        "name": name,
        "description": description,
        "visibility": "private" if is_private else "public",
    }
    if namespace_id is not None:
        data["namespace_id"] = namespace_id

    response = requests.post(f"{base_url}/api/v4/projects", headers=headers, data=data)
    raise_for_response(response, host=host, action=f"creating repo '{name}'", host_url=base_url)
    payload = response.json()
    return CreatedRepo(http_url=payload["http_url_to_repo"], project_id=payload["id"])


def get_or_create_group_path(
    *,
    host: str,
    base_url: str,
    token: str,
    group_path: str | None,
    add_timestamp: bool = False,
) -> GroupResolution:
    """Walk a slash-separated group path, creating any segments that don't exist.

    Returns a GroupResolution with the leaf group id and a list of full paths
    that were created (so the caller can report orphans on later failure).
    """
    if not group_path:
        return GroupResolution(group_id=None)

    headers = {"PRIVATE-TOKEN": token}
    parent_id: int | None = None
    full_path_parts: list[str] = []
    created_paths: list[str] = []

    for component in group_path.split("/"):
        if not component:
            continue

        slug = create_slug(component, add_timestamp)

        search_resp = requests.get(
            f"{base_url}/api/v4/groups",
            headers=headers,
            params={"search": component, "per_page": 100},
        )
        raise_for_response(
            search_resp,
            host=host,
            action=f"searching for group '{component}'",
            host_url=base_url,
        )

        existing_id = _find_group(search_resp.json(), component, parent_id)
        if existing_id is not None:
            parent_id = existing_id
            full_path_parts.append(component)
            continue

        data = {"name": component, "path": slug}
        if parent_id is not None:
            data["parent_id"] = parent_id

        create_resp = requests.post(f"{base_url}/api/v4/groups", headers=headers, data=data)
        raise_for_response(
            create_resp,
            host=host,
            action=f"creating group '{slug}'",
            host_url=base_url,
        )
        parent_id = create_resp.json()["id"]
        full_path_parts.append(slug)
        created_paths.append("/".join(full_path_parts))

    return GroupResolution(group_id=parent_id, created_paths=created_paths)


def _find_group(groups: list[dict], name: str, parent_id: int | None) -> int | None:
    for group in groups:
        if group.get("name") == name and group.get("parent_id") == parent_id:
            return group["id"]
    return None


def verify_token(*, host: str, base_url: str, token: str) -> None:
    """Probe the host with the new token to confirm it works. Raises HydraAPIError on failure."""
    headers = {"PRIVATE-TOKEN": token}
    response = requests.get(f"{base_url}/api/v4/user", headers=headers)
    raise_for_response(response, host=host, action="verifying token", host_url=base_url)


def inspect_token(*, host: str, base_url: str, token: str):
    """Return scopes + expiry for the calling token.

    Uses ``GET /personal_access_tokens/self`` (GitLab 15.5+); falls back to
    ``GET /user`` on 404 with ``scopes_known=False`` so older instances still
    confirm the token is valid.
    """
    from hydra.secrets import TokenScopes

    headers = {"PRIVATE-TOKEN": token}
    resp = requests.get(f"{base_url}/api/v4/personal_access_tokens/self", headers=headers)
    if resp.status_code == 404:
        # Older GitLab: fall back to /user just to confirm validity.
        fb = requests.get(f"{base_url}/api/v4/user", headers=headers)
        raise_for_response(fb, host=host, action="inspecting token (fallback)", host_url=base_url)
        return TokenScopes(scopes=[], expires_at=None, scopes_known=False)
    raise_for_response(resp, host=host, action="inspecting token", host_url=base_url)
    payload = resp.json()
    return TokenScopes(
        scopes=list(payload.get("scopes") or []),
        expires_at=payload.get("expires_at"),
        scopes_known=True,
    )


def list_projects_with_mirrors(
    *,
    host: str,
    base_url: str,
    token: str,
    namespace: Optional[str],
) -> List[GitLabProjectSummary]:
    """Enumerate projects on the primary that have at least one push-mirror.

    If ``namespace`` is set, scope the listing to that group (recursive into
    subgroups). Without a namespace, fall back to the token's accessible
    projects via ``/projects?membership=true`` — which can be expensive on
    large self-hosted instances, so prefer scoping by namespace when possible.
    """
    headers = {"PRIVATE-TOKEN": token}
    projects = _list_projects(host=host, base_url=base_url, headers=headers, namespace=namespace)

    out: List[GitLabProjectSummary] = []
    for proj in projects:
        pid = proj.get("id")
        if pid is None:
            continue
        mirrors_resp = requests.get(
            f"{base_url}/api/v4/projects/{pid}/remote_mirrors",
            headers=headers,
        )
        if mirrors_resp.status_code == 403:
            # Not the owner — skip rather than fail the whole scan.
            continue
        raise_for_response(
            mirrors_resp,
            host=host,
            action=f"listing mirrors for project {pid}",
            host_url=base_url,
        )
        summaries = [
            GitLabMirrorSummary(id=int(m["id"]), url=m.get("url", ""))
            for m in mirrors_resp.json()
            if "id" in m
        ]
        if not summaries:
            continue
        out.append(
            GitLabProjectSummary(
                project_id=int(pid),
                web_url=proj.get("web_url", ""),
                name=proj.get("name", ""),
                full_path=proj.get("path_with_namespace", ""),
                mirrors=summaries,
            )
        )
    return out


def _list_projects(
    *,
    host: str,
    base_url: str,
    headers: dict,
    namespace: Optional[str],
) -> List[dict]:
    """Return all visible projects in a namespace (recursive), or membership-scoped if none."""
    if namespace:
        encoded = quote(namespace, safe="")
        endpoint = f"{base_url}/api/v4/groups/{encoded}/projects"
        params = {"include_subgroups": "true", "per_page": 100, "archived": "false"}
    else:
        endpoint = f"{base_url}/api/v4/projects"
        params = {"membership": "true", "per_page": 100, "archived": "false"}
    return _paginate(host=host, endpoint=endpoint, headers=headers, params=params)


def _paginate(*, host: str, endpoint: str, headers: dict, params: dict) -> List[dict]:
    out: List[dict] = []
    page = 1
    while True:
        page_params = dict(params)
        page_params["page"] = page
        resp = requests.get(endpoint, headers=headers, params=page_params)
        raise_for_response(resp, host=host, action="listing projects", host_url=endpoint)
        items = resp.json()
        if not items:
            break
        out.extend(items)
        # GitLab returns X-Next-Page; empty when on the last page.
        next_page = resp.headers.get("X-Next-Page", "").strip()
        if not next_page:
            break
        page = int(next_page)
    return out

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import quote

from hydra import http
from hydra.errors import raise_for_response
from hydra.utils import create_slug

DEFAULT_MAX_WORKERS = 8


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

    response = http.post(f"{base_url}/api/v4/projects", headers=headers, data=data)
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

        search_resp = http.get(
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

        create_resp = http.post(f"{base_url}/api/v4/groups", headers=headers, data=data)
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
    response = http.get(f"{base_url}/api/v4/user", headers=headers)
    raise_for_response(response, host=host, action="verifying token", host_url=base_url)


def inspect_token(*, host: str, base_url: str, token: str):
    """Return scopes + expiry for the calling token.

    Uses ``GET /personal_access_tokens/self`` (GitLab 15.5+); falls back to
    ``GET /user`` on 404 with ``scopes_known=False`` so older instances still
    confirm the token is valid.
    """
    from hydra.secrets import TokenScopes

    headers = {"PRIVATE-TOKEN": token}
    resp = http.get(f"{base_url}/api/v4/personal_access_tokens/self", headers=headers)
    if resp.status_code == 404:
        # Older GitLab: fall back to /user just to confirm validity.
        fb = http.get(f"{base_url}/api/v4/user", headers=headers)
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
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> List[GitLabProjectSummary]:
    """Enumerate projects on the primary that have at least one push-mirror.

    If ``namespace`` is set, scope the listing to that group (recursive into
    subgroups). Without a namespace, fall back to the token's accessible
    projects via ``/projects?membership=true`` — which can be expensive on
    large self-hosted instances, so prefer scoping by namespace when possible.

    Per-project ``/remote_mirrors`` calls run concurrently (up to ``max_workers``
    threads); pages are also fanned out when GitLab reports ``X-Total-Pages``.
    """
    headers = {"PRIVATE-TOKEN": token}
    projects = _list_projects(
        host=host,
        base_url=base_url,
        headers=headers,
        namespace=namespace,
        max_workers=max_workers,
    )

    # Map project-id → fetched mirrors (or None if forbidden / skipped).
    results: Dict[int, Optional[List[GitLabMirrorSummary]]] = {}

    def fetch(pid: int) -> Optional[List[GitLabMirrorSummary]]:
        return _fetch_project_mirrors(host=host, base_url=base_url, headers=headers, project_id=pid)

    pids = [int(p["id"]) for p in projects if p.get("id") is not None]
    workers = max(1, min(max_workers, len(pids))) if pids else 1
    if workers <= 1 or len(pids) <= 1:
        for pid in pids:
            results[pid] = fetch(pid)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(fetch, pid): pid for pid in pids}
            for fut in as_completed(futures):
                pid = futures[fut]
                results[pid] = fut.result()

    # Re-assemble in input order so callers see deterministic output.
    out: List[GitLabProjectSummary] = []
    for proj in projects:
        pid = proj.get("id")
        if pid is None:
            continue
        mirrors = results.get(int(pid))
        if not mirrors:
            continue
        out.append(
            GitLabProjectSummary(
                project_id=int(pid),
                web_url=proj.get("web_url", ""),
                name=proj.get("name", ""),
                full_path=proj.get("path_with_namespace", ""),
                mirrors=mirrors,
            )
        )
    return out


def _fetch_project_mirrors(
    *, host: str, base_url: str, headers: dict, project_id: int
) -> Optional[List[GitLabMirrorSummary]]:
    """Fetch one project's remote mirrors. Returns None on 403 (skip)."""
    resp = http.get(
        f"{base_url}/api/v4/projects/{project_id}/remote_mirrors",
        headers=headers,
    )
    if resp.status_code == 403:
        # Not the owner — skip rather than fail the whole scan.
        return None
    raise_for_response(
        resp,
        host=host,
        action=f"listing mirrors for project {project_id}",
        host_url=base_url,
    )
    return [
        GitLabMirrorSummary(id=int(m["id"]), url=m.get("url", "")) for m in resp.json() if "id" in m
    ]


def _list_projects(
    *,
    host: str,
    base_url: str,
    headers: dict,
    namespace: Optional[str],
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> List[dict]:
    """Return all visible projects in a namespace (recursive), or membership-scoped if none."""
    if namespace:
        encoded = quote(namespace, safe="")
        endpoint = f"{base_url}/api/v4/groups/{encoded}/projects"
        params = {"include_subgroups": "true", "per_page": 100, "archived": "false"}
    else:
        endpoint = f"{base_url}/api/v4/projects"
        params = {"membership": "true", "per_page": 100, "archived": "false"}
    return _paginate(
        host=host,
        endpoint=endpoint,
        headers=headers,
        params=params,
        max_workers=max_workers,
    )


def _paginate(
    *,
    host: str,
    endpoint: str,
    headers: dict,
    params: dict,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> List[dict]:
    """Walk a GitLab paginated endpoint.

    Fans out pages 2..N when ``X-Total-Pages`` is present (offset pagination);
    falls back to sequential ``X-Next-Page`` walking otherwise (keyset endpoints
    or any GitLab version that drops the total header).
    """
    page1_params = dict(params)
    page1_params["page"] = 1
    page1_params.setdefault("per_page", 100)
    resp = http.get(endpoint, headers=headers, params=page1_params)
    raise_for_response(resp, host=host, action="listing projects", host_url=endpoint)
    items = list(resp.json())

    total_pages_raw = resp.headers.get("X-Total-Pages", "").strip()
    if total_pages_raw and total_pages_raw.isdigit() and int(total_pages_raw) > 1:
        total = int(total_pages_raw)
        page_numbers = list(range(2, total + 1))
        workers = max(1, min(max_workers, len(page_numbers)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            future_to_page = {
                ex.submit(_fetch_page, host, endpoint, headers, params, n): n for n in page_numbers
            }
            pages: Dict[int, List[dict]] = {}
            for fut in as_completed(future_to_page):
                pages[future_to_page[fut]] = fut.result()
        for n in sorted(pages):
            items.extend(pages[n])
        return items

    # Fallback: walk via X-Next-Page (keyset / total-pages-absent case).
    next_page = resp.headers.get("X-Next-Page", "").strip()
    while next_page:
        page_params = dict(params)
        page_params["page"] = int(next_page)
        page_params.setdefault("per_page", 100)
        resp = http.get(endpoint, headers=headers, params=page_params)
        raise_for_response(resp, host=host, action="listing projects", host_url=endpoint)
        chunk = resp.json()
        if not chunk:
            break
        items.extend(chunk)
        next_page = resp.headers.get("X-Next-Page", "").strip()
    return items


def _fetch_page(
    host: str, endpoint: str, headers: dict, base_params: dict, page: int
) -> List[dict]:
    page_params = dict(base_params)
    page_params["page"] = page
    page_params.setdefault("per_page", 100)
    resp = http.get(endpoint, headers=headers, params=page_params)
    raise_for_response(resp, host=host, action="listing projects", host_url=endpoint)
    return list(resp.json())

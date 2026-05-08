from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import requests

from hydra.errors import raise_for_response
from hydra.utils import create_slug


@dataclass
class CreatedRepo:
    http_url: str
    project_id: int


@dataclass
class GroupResolution:
    """Result of resolving / creating a nested group path."""

    group_id: Optional[int]
    created_paths: list[str] = field(default_factory=list)


def create_repo(
    *,
    host: str,
    base_url: str,
    token: str,
    name: str,
    description: str,
    namespace_id: Optional[int] = None,
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
    raise_for_response(
        response, host=host, action=f"creating repo '{name}'", host_url=base_url
    )
    payload = response.json()
    return CreatedRepo(http_url=payload["http_url_to_repo"], project_id=payload["id"])


def get_or_create_group_path(
    *,
    host: str,
    base_url: str,
    token: str,
    group_path: Optional[str],
    add_timestamp: bool = False,
) -> GroupResolution:
    """Walk a slash-separated group path, creating any segments that don't exist.

    Returns a GroupResolution with the leaf group id and a list of full paths
    that were created (so the caller can report orphans on later failure).
    """
    if not group_path:
        return GroupResolution(group_id=None)

    headers = {"PRIVATE-TOKEN": token}
    parent_id: Optional[int] = None
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

        create_resp = requests.post(
            f"{base_url}/api/v4/groups", headers=headers, data=data
        )
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


def _find_group(
    groups: list[dict], name: str, parent_id: Optional[int]
) -> Optional[int]:
    for group in groups:
        if group.get("name") == name and group.get("parent_id") == parent_id:
            return group["id"]
    return None

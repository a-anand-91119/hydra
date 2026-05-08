from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import requests

from hydra.utils import create_slug


class GitLabError(Exception):
    pass


@dataclass
class CreatedRepo:
    http_url: str
    project_id: int


def create_repo(
    *,
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
    if response.status_code != 201:
        raise GitLabError(
            f"Failed to create repo {name} on {base_url}: "
            f"{response.status_code} {response.text}"
        )
    payload = response.json()
    return CreatedRepo(http_url=payload["http_url_to_repo"], project_id=payload["id"])


def get_or_create_group_path(
    *,
    base_url: str,
    token: str,
    group_path: Optional[str],
    add_timestamp: bool = False,
) -> Optional[int]:
    if not group_path:
        return None

    headers = {"PRIVATE-TOKEN": token}
    parent_id: Optional[int] = None

    for component in group_path.split("/"):
        if not component:
            continue

        slug = create_slug(component, add_timestamp)

        search_resp = requests.get(
            f"{base_url}/api/v4/groups", headers=headers, params={"search": component}
        )
        if search_resp.status_code != 200:
            raise GitLabError(
                f"Failed to search groups for {component}: "
                f"{search_resp.status_code} {search_resp.text}"
            )

        existing_id = _find_group(search_resp.json(), component, parent_id)
        if existing_id is not None:
            parent_id = existing_id
            continue

        data = {"name": component, "path": slug}
        if parent_id is not None:
            data["parent_id"] = parent_id

        create_resp = requests.post(
            f"{base_url}/api/v4/groups", headers=headers, data=data
        )
        if create_resp.status_code != 201:
            raise GitLabError(
                f"Failed to create group {slug}: "
                f"{create_resp.status_code} {create_resp.text}"
            )
        parent_id = create_resp.json()["id"]

    return parent_id


def _find_group(
    groups: list[dict], name: str, parent_id: Optional[int]
) -> Optional[int]:
    for group in groups:
        if group.get("name") == name and group.get("parent_id") == parent_id:
            return group["id"]
    return None

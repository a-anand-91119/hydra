"""Shared helpers for e2e tests.

True end-to-end from Hydra's perspective means:

- Real config YAML loaded by ``hydra.config.load_config``
- Real env-var token resolution via ``hydra.secrets``
- Real preflight / probe / planner / executor / journal
- Mock ONLY the HTTP transport (via ``requests_mock``)

These helpers build the config + token env + HTTP response fixtures so each
test file can focus on the contract it's exercising.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


def write_config(
    path: Path,
    *,
    primary_id: str = "primary",
    primary_url: str = "https://primary.example",
    primary_kind: str = "gitlab",
    forks: Optional[List[Dict[str, Any]]] = None,
    defaults_group: str = "",
    defaults_private: bool = True,
) -> Path:
    """Write a real Hydra config YAML and return the path.

    Default shape: 1 self-hosted GitLab primary + 1 GitLab.com fork + 1
    GitHub fork. Override ``forks`` for different fan-outs.
    """
    if forks is None:
        forks = [
            {"id": "fork_gl", "kind": "gitlab", "url": "https://gitlab.com", "options": {}},
            {
                "id": "fork_gh",
                "kind": "github",
                "url": "https://api.github.com",
                "options": {"org": None},
            },
        ]
    cfg = {
        "schema_version": 2,
        "primary": primary_id,
        "forks": [f["id"] for f in forks],
        "hosts": [
            {"id": primary_id, "kind": primary_kind, "url": primary_url, "options": {}},
            *forks,
        ],
        "defaults": {"private": defaults_private, "group": defaults_group},
    }
    path.write_text(yaml.safe_dump(cfg))
    return path


def set_tokens(monkeypatch, **tokens: str) -> None:
    """Set HYDRA_TOKEN_<UPPER_ID> env vars from kwargs.

    Example: ``set_tokens(monkeypatch, primary="p-tok", fork_gh="g-tok")``.
    """
    for host_id, token in tokens.items():
        monkeypatch.setenv(f"HYDRA_TOKEN_{host_id.upper()}", token)


# ── Response builders ──────────────────────────────────────────────────


def gitlab_pat_self(scopes: Optional[List[str]] = None) -> Dict[str, Any]:
    """Body for ``GET /api/v4/personal_access_tokens/self``."""
    return {"scopes": list(scopes) if scopes is not None else ["api"], "expires_at": None}


def gitlab_user() -> Dict[str, Any]:
    return {"id": 1, "username": "test", "name": "Test User"}


def gitlab_project(
    *,
    project_id: int,
    name: str,
    base_url: str = "https://primary.example",
    group: str = "team",
) -> Dict[str, Any]:
    """Body for ``GET /api/v4/projects/:encoded_path``."""
    return {
        "id": project_id,
        "name": name,
        "http_url_to_repo": f"{base_url}/{group}/{name}.git",
        "path_with_namespace": f"{group}/{name}",
        "web_url": f"{base_url}/{group}/{name}",
    }


def github_user(login: str = "octocat") -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Body + headers for ``GET /user``."""
    return {"login": login}, {"X-OAuth-Scopes": "repo, admin:org"}


def github_repo(*, owner: str = "octocat", name: str = "probe") -> Dict[str, Any]:
    """Body for ``GET /repos/{owner}/{name}``."""
    return {
        "name": name,
        "clone_url": f"https://github.com/{owner}/{name}.git",
        "html_url": f"https://github.com/{owner}/{name}",
        "private": True,
    }


# ── Mock registration shortcuts ────────────────────────────────────────


def register_preflight_ok(
    rmock,
    *,
    primary_url: str = "https://primary.example",
    gitlab_com_url: str = "https://gitlab.com",
    github_url: str = "https://api.github.com",
) -> None:
    """Wire up the preflight probes for the default 3-host setup so tokens
    pass the scope check.
    """
    for url in (primary_url, gitlab_com_url):
        rmock.get(f"{url}/api/v4/personal_access_tokens/self", json=gitlab_pat_self(["api"]))
    body, headers = github_user()
    rmock.get(f"{github_url}/user", json=body, headers=headers)


def register_find_repo_not_found(
    rmock,
    *,
    repo_name: str,
    group: str = "team",
    primary_url: str = "https://primary.example",
    gitlab_com_url: str = "https://gitlab.com",
    github_url: str = "https://api.github.com",
    github_owner: str = "octocat",
) -> None:
    """Wire up the existence probe (Phase 6) to return 404 for every host.

    Matches the URL ``GitLabProvider.find_repo`` actually constructs:
    when group is empty the path is just the name; otherwise it's
    ``group/name`` URL-encoded.
    """
    from urllib.parse import quote

    repo_path = f"{group}/{repo_name}" if group else repo_name
    encoded = quote(repo_path, safe="")
    rmock.get(f"{primary_url}/api/v4/projects/{encoded}", status_code=404, json={})
    rmock.get(f"{gitlab_com_url}/api/v4/projects/{encoded}", status_code=404, json={})
    # GitHub probe resolves the authenticated user's login first when no org
    # is configured — re-register /user defensively in case preflight wasn't
    # already wired up (e.g. --skip-preflight tests).
    body, headers = github_user(github_owner)
    rmock.get(f"{github_url}/user", json=body, headers=headers)
    rmock.get(
        f"{github_url}/repos/{github_owner}/{repo_name}",
        status_code=404,
        json={"message": "Not Found"},
    )

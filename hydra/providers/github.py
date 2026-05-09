from __future__ import annotations

from typing import Optional

from hydra import github as github_api
from hydra.providers.base import (
    Capabilities,
    HostSpec,
    NamespaceRef,
    RepoRef,
)

KIND = "github"

# GitHub HTTPS push uses `x-access-token` for token auth, not `oauth2`.
# Field is unused today (GitHub is fork-only) but correct for future
# inverted topologies.
CAPABILITIES = Capabilities(
    supports_mirror_source=False,
    supports_group_paths=False,
    supports_status_lookup=False,
    inbound_mirror_username="x-access-token",
)


class GitHubProvider:
    def __init__(self, spec: HostSpec) -> None:
        self.spec = spec
        self.capabilities = CAPABILITIES

    def _org(self) -> Optional[str]:
        v = self.spec.options.get("org")
        return v or None

    def ensure_namespace(
        self, *, group_path: Optional[str], token: str
    ) -> NamespaceRef:
        org = self._org()
        return NamespaceRef(namespace_id=None, created_paths=[], full_path=org)

    def create_repo(
        self,
        *,
        token: str,
        name: str,
        description: str,
        namespace: NamespaceRef,
        is_private: bool,
    ) -> RepoRef:
        url = github_api.create_repo(
            base_url=self.spec.url,
            token=token,
            name=name,
            description=description,
            org=self._org(),
            is_private=is_private,
        )
        return RepoRef(http_url=url, project_id=None, namespace_path=self._org())


def _factory(spec: HostSpec) -> GitHubProvider:
    return GitHubProvider(spec)


def install() -> None:
    from hydra.providers import register

    register(KIND, _factory, CAPABILITIES)

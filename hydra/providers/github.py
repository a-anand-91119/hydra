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
        self._login_cache: Optional[str] = None

    def _org(self) -> Optional[str]:
        v = self.spec.options.get("org")
        return v or None

    def _owner(self, token: str) -> str:
        """Return the org (if configured) or the authenticated user's login.

        Caches the login per-provider-instance so back-to-back probes don't
        each pay a ``GET /user`` round-trip.
        """
        org = self._org()
        if org:
            return org
        if self._login_cache is None:
            self._login_cache = github_api.get_authenticated_login(
                base_url=self.spec.url, token=token
            )
        return self._login_cache

    def ensure_namespace(self, *, group_path: Optional[str], token: str) -> NamespaceRef:
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

    def find_repo(
        self, *, token: str, name: str, namespace: Optional[str]
    ) -> Optional[RepoRef]:
        # GitHub doesn't use `namespace` — the host's org option determines
        # the owner. Accept the kwarg to keep the Provider contract uniform.
        del namespace
        owner = self._owner(token)
        if not owner:
            return None
        clone_url = github_api.find_repo(
            base_url=self.spec.url, token=token, owner=owner, name=name
        )
        if clone_url is None:
            return None
        return RepoRef(http_url=clone_url, project_id=None, namespace_path=owner)


def _factory(spec: HostSpec) -> GitHubProvider:
    return GitHubProvider(spec)


def install() -> None:
    from hydra.providers import register

    register(KIND, _factory, CAPABILITIES)

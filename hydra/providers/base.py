from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class Capabilities:
    """Static description of what a provider kind can do."""

    supports_mirror_source: bool  # can be the primary (push-mirror origin)
    supports_group_paths: bool  # supports nested group paths in namespace
    supports_status_lookup: bool  # implements find_project / list_mirrors
    inbound_mirror_username: str  # username injected when this host is a mirror target


@dataclass
class HostSpec:
    """Plain data describing a configured host."""

    id: str
    kind: str
    url: str
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RepoRef:
    http_url: str
    project_id: Optional[int] = None  # set for GitLab; None for GitHub
    namespace_path: Optional[str] = None


@dataclass
class NamespaceRef:
    namespace_id: Optional[int]  # GitLab group id; None for GitHub user-owned
    created_paths: List[str]  # paths newly created (for orphan reporting)
    full_path: Optional[str] = None


@dataclass
class MirrorInfo:
    id: int
    url: str
    enabled: bool
    last_update_status: Optional[str]
    last_update_at: Optional[str]
    last_error: Optional[str]


@dataclass
class PrimaryMirror:
    """One outbound push-mirror on a primary host as seen by ``hydra scan``."""

    id: int
    url: str


@dataclass
class PrimaryProject:
    """A project on a primary host plus its outbound push-mirrors.

    Returned by ``MirrorSource.list_projects_with_mirrors`` for ``hydra scan``.
    Only projects with at least one remote mirror are included.
    """

    project_id: int
    web_url: str
    name: str
    full_path: str
    mirrors: List[PrimaryMirror]

    @property
    def mirror_push_ids(self) -> List[int]:
        """Backwards-compatible accessor used by ``scan_diff``."""
        return [m.id for m in self.mirrors]


@runtime_checkable
class Provider(Protocol):
    """Minimum surface that every provider implementation exposes."""

    spec: HostSpec
    capabilities: Capabilities

    def ensure_namespace(self, *, group_path: Optional[str], token: str) -> NamespaceRef: ...

    def create_repo(
        self,
        *,
        token: str,
        name: str,
        description: str,
        namespace: NamespaceRef,
        is_private: bool,
    ) -> RepoRef: ...


@runtime_checkable
class MirrorSource(Provider, Protocol):
    """Providers that can act as the primary (push-mirror origin)."""

    def add_outbound_mirror(
        self,
        *,
        token: str,
        primary_repo: RepoRef,
        target_url: str,
        target_token: str,
        target_username: str,
        target_label: str,
    ) -> Dict[str, Any]: ...

    def replace_outbound_mirror(
        self,
        *,
        token: str,
        primary_repo: RepoRef,
        old_push_mirror_id: int,
        target_url: str,
        target_token: str,
        target_username: str,
        target_label: str,
    ) -> Dict[str, Any]: ...

    def find_project(self, *, token: str, repo_path: str) -> Optional[RepoRef]: ...

    def list_mirrors(self, *, token: str, primary_repo: RepoRef) -> List[MirrorInfo]: ...

    def list_projects_with_mirrors(
        self, *, token: str, namespace: Optional[str], max_workers: int = 8
    ) -> List[PrimaryProject]: ...

from __future__ import annotations

from typing import Any, Dict, List, Optional

from hydra import gitlab as gitlab_api
from hydra import mirrors as mirrors_api
from hydra.errors import HydraAPIError, MirrorReplaceError
from hydra.providers.base import (
    Capabilities,
    HostSpec,
    MirrorInfo,
    NamespaceRef,
    PrimaryMirror,
    PrimaryProject,
    RepoRef,
)

KIND = "gitlab"

CAPABILITIES = Capabilities(
    supports_mirror_source=True,
    supports_group_paths=True,
    supports_status_lookup=True,
    inbound_mirror_username="oauth2",
)


class GitLabProvider:
    def __init__(self, spec: HostSpec) -> None:
        self.spec = spec
        self.capabilities = CAPABILITIES

    @property
    def _add_timestamp(self) -> bool:
        return bool(self.spec.options.get("add_timestamp", False))

    @property
    def _managed_prefix(self) -> Optional[str]:
        v = self.spec.options.get("managed_group_prefix")
        return v or None

    def _effective_group_path(self, group_path: Optional[str]) -> Optional[str]:
        prefix = self._managed_prefix
        if not prefix:
            return group_path or None
        if group_path:
            return f"{prefix}/{group_path}"
        return prefix

    def ensure_namespace(self, *, group_path: Optional[str], token: str) -> NamespaceRef:
        full = self._effective_group_path(group_path)
        res = gitlab_api.get_or_create_group_path(
            host=self.spec.id,
            base_url=self.spec.url,
            token=token,
            group_path=full,
            add_timestamp=self._add_timestamp,
        )
        return NamespaceRef(
            namespace_id=res.group_id,
            created_paths=list(res.created_paths),
            full_path=full,
        )

    def create_repo(
        self,
        *,
        token: str,
        name: str,
        description: str,
        namespace: NamespaceRef,
        is_private: bool,
    ) -> RepoRef:
        created = gitlab_api.create_repo(
            host=self.spec.id,
            base_url=self.spec.url,
            token=token,
            name=name,
            description=description,
            namespace_id=namespace.namespace_id,
            is_private=is_private,
        )
        return RepoRef(
            http_url=created.http_url,
            project_id=created.project_id,
            namespace_path=namespace.full_path,
        )

    def add_outbound_mirror(
        self,
        *,
        token: str,
        primary_repo: RepoRef,
        target_url: str,
        target_token: str,
        target_username: str,
        target_label: str,
    ) -> Dict[str, Any]:
        if primary_repo.project_id is None:
            raise ValueError("GitLab mirror requires primary_repo.project_id")
        mirror_url = mirrors_api.inject_credentials(target_url, target_username, target_token)
        return mirrors_api.add_mirror(
            host_id=self.spec.id,
            base_url=self.spec.url,
            token=token,
            project_id=primary_repo.project_id,
            mirror_url=mirror_url,
            target_label=target_label,
        )

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
    ) -> Dict[str, Any]:
        """Rotate a push-mirror's embedded token.

        GitLab's PUT /remote_mirrors does not accept `url`, so the URL
        (and therefore the token) is immutable on an existing mirror. We
        DELETE the old mirror and POST a fresh one. Caller must persist
        the new mirror's id.

        On DELETE failure: raises ``HydraAPIError``; the existing mirror is
        untouched. On POST failure after DELETE succeeded: raises
        ``MirrorReplaceError`` — the mirror is now gone on the primary and
        callers must reflect that in any local state.
        """
        if primary_repo.project_id is None:
            raise ValueError("GitLab mirror requires primary_repo.project_id")
        mirrors_api.delete_mirror(
            host_id=self.spec.id,
            base_url=self.spec.url,
            token=token,
            project_id=primary_repo.project_id,
            mirror_id=old_push_mirror_id,
        )
        try:
            return self.add_outbound_mirror(
                token=token,
                primary_repo=primary_repo,
                target_url=target_url,
                target_token=target_token,
                target_username=target_username,
                target_label=target_label,
            )
        except HydraAPIError as e:
            raise MirrorReplaceError(
                message=(
                    f"{e.message} (the existing {target_label} mirror was deleted "
                    f"but the replacement could not be created)"
                ),
                host=e.host,
                status_code=e.status_code,
                hint=(
                    f"The push-mirror to {target_label} is now GONE on the primary. "
                    f"The repo is no longer mirroring there. Re-add it via "
                    f"`hydra create` (with a fresh name) or add it manually in the "
                    f"primary's project settings."
                ),
            ) from e

    def find_project(self, *, token: str, repo_path: str) -> Optional[RepoRef]:
        pid = mirrors_api.find_project_id(
            host_id=self.spec.id,
            base_url=self.spec.url,
            token=token,
            repo_path=repo_path,
        )
        if pid is None:
            return None
        return RepoRef(http_url="", project_id=pid, namespace_path=None)

    def find_repo(self, *, token: str, name: str, namespace: Optional[str]) -> Optional[RepoRef]:
        full_group = self._effective_group_path(namespace)
        repo_path = f"{full_group}/{name}" if full_group else name
        payload = mirrors_api.find_project(
            host_id=self.spec.id,
            base_url=self.spec.url,
            token=token,
            repo_path=repo_path,
        )
        if payload is None:
            return None
        pid = payload.get("id")
        return RepoRef(
            http_url=payload.get("http_url_to_repo", ""),
            project_id=int(pid) if pid is not None else None,
            namespace_path=full_group,
        )

    def list_mirrors(self, *, token: str, primary_repo: RepoRef) -> List[MirrorInfo]:
        if primary_repo.project_id is None:
            return []
        ms = mirrors_api.list_mirrors(
            host_id=self.spec.id,
            base_url=self.spec.url,
            token=token,
            project_id=primary_repo.project_id,
        )
        return [
            MirrorInfo(
                id=m.id,
                url=m.url,
                enabled=m.enabled,
                last_update_status=m.last_update_status,
                last_update_at=m.last_update_at,
                last_error=m.last_error,
            )
            for m in ms
        ]

    def list_projects_with_mirrors(
        self, *, token: str, namespace: Optional[str], max_workers: int = 8
    ) -> List[PrimaryProject]:
        raw = gitlab_api.list_projects_with_mirrors(
            host=self.spec.id,
            base_url=self.spec.url,
            token=token,
            namespace=namespace,
            max_workers=max_workers,
        )
        return [
            PrimaryProject(
                project_id=p.project_id,
                web_url=p.web_url,
                name=p.name,
                full_path=p.full_path,
                mirrors=[PrimaryMirror(id=m.id, url=m.url) for m in p.mirrors],
            )
            for p in raw
        ]


def _factory(spec: HostSpec) -> GitLabProvider:
    return GitLabProvider(spec)


def install() -> None:
    from hydra.providers import register

    register(KIND, _factory, CAPABILITIES)

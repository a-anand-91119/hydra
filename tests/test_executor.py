"""Tests for hydra.executor.apply_plan — uses real journal in tmp dir, fakes
the provider layer so no network is required.
"""

from __future__ import annotations

from typing import Dict, List

import pytest
from rich.console import Console

from hydra import executor, planner
from hydra import journal as journal_mod
from hydra.config import Config, Defaults, HostSpec
from hydra.errors import HydraAPIError
from hydra.providers.base import (
    Capabilities,
    NamespaceRef,
    RepoRef,
)


def _cfg() -> Config:
    return Config(
        hosts=[
            HostSpec(id="primary", kind="gitlab", url="https://primary.gl"),
            HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com"),
        ],
        primary="primary",
        forks=["cloud"],
        defaults=Defaults(private=True, group=""),
    )


_CAPS = Capabilities(
    supports_mirror_source=True,
    supports_group_paths=True,
    supports_status_lookup=True,
    inbound_mirror_username="oauth2",
)


class FakeProvider:
    """Implements MirrorSource sufficiently for the executor's isinstance check."""

    def __init__(self, spec: HostSpec):
        self.spec = spec
        self.capabilities = _CAPS
        self.calls: List[str] = []
        self._next_pid = 1000

    def ensure_namespace(self, *, group_path, token):
        self.calls.append(f"ns:{group_path}")
        return NamespaceRef(namespace_id=1, created_paths=[], full_path=group_path)

    def create_repo(self, *, token, name, description, namespace, is_private):
        self.calls.append(f"create:{name}")
        self._next_pid += 1
        return RepoRef(
            http_url=f"{self.spec.url}/{name}.git",
            project_id=self._next_pid,
            namespace_path=namespace.full_path,
        )

    def add_outbound_mirror(
        self, *, token, primary_repo, target_url, target_token, target_username, target_label
    ):
        self.calls.append(f"mirror:{target_label}")
        return {"id": 5000 + len(self.calls)}

    # Unused by these tests — kept so isinstance(Provider, MirrorSource) works
    def replace_outbound_mirror(self, **kwargs): ...  # pragma: no cover
    def find_project(self, **kwargs): ...  # pragma: no cover
    def list_mirrors(self, **kwargs): ...  # pragma: no cover
    def list_projects_with_mirrors(self, **kwargs): ...  # pragma: no cover


@pytest.fixture
def patched_providers(monkeypatch):
    instances: Dict[str, FakeProvider] = {}

    def factory(kind):
        def make(spec):
            inst = instances.get(spec.id)
            if inst is None:
                inst = FakeProvider(spec)
                instances[spec.id] = inst
            return inst

        return make

    monkeypatch.setattr(executor.providers_mod, "get", factory)
    yield instances


class TestApplyPlanCreate:
    def test_full_create_writes_journal(self, patched_providers):
        cfg = _cfg()
        from hydra.wizard import CreateOptions

        opts = CreateOptions(name="probe", description="d", group="t", is_private=True, mirror=True)
        plan = planner.plan_create(cfg, opts)
        result = executor.apply_plan(
            plan,
            cfg=cfg,
            tokens={"primary": "tp", "cloud": "tc"},
            console=Console(),
        )
        assert result.ok, result.error
        assert result.applied == len(plan.actions)
        with journal_mod.journal() as j:
            repos = j.list_repos()
        assert len(repos) == 1
        assert repos[0].name == "probe"
        # One mirror, targeting cloud, with the id returned by the fake.
        assert len(repos[0].mirrors) == 1
        assert repos[0].mirrors[0].target_host_id == "cloud"


class TestApplyPlanFailure:
    def test_first_failure_stops_and_records(self, patched_providers, monkeypatch):
        cfg = _cfg()
        from hydra.wizard import CreateOptions

        opts = CreateOptions(name="probe", description="", group="", is_private=True, mirror=True)
        plan = planner.plan_create(cfg, opts)

        # Force the second create_repo (the cloud fork) to fail.
        original = FakeProvider.create_repo

        def boom(self, **kwargs):
            if self.spec.id == "cloud":
                raise HydraAPIError(message="cloud boom", host="cloud", status_code=500)
            return original(self, **kwargs)

        monkeypatch.setattr(FakeProvider, "create_repo", boom)

        result = executor.apply_plan(
            plan, cfg=cfg, tokens={"primary": "t", "cloud": "t"}, console=Console()
        )
        assert not result.ok
        assert isinstance(result.error, HydraAPIError)
        assert result.failed.kind == "create_repo"
        # Primary repo was created and added to created list.
        assert any("primary repo" in label for label, _ in result.created)


class TestApplyPlanScanApply:
    def test_journal_update_push_id_handler(self, patched_providers):
        cfg = _cfg()
        # Seed a journal repo + mirror.
        with journal_mod.journal() as j:
            rid = j.record_repo(
                name="probe",
                primary_host_id="primary",
                primary_repo_id=42,
                primary_repo_url="u",
            )
            mid = j.record_mirror(
                repo_id=rid,
                target_host_id="cloud",
                target_repo_url="u",
                push_mirror_id=11,
            )

        plan = planner.Plan(
            actions=[
                planner.Action(
                    kind="journal_update_push_id",
                    host_id="primary",
                    summary="resync",
                    payload={"mirror_db_id": mid, "new_push_mirror_id": 99},
                )
            ]
        )
        result = executor.apply_plan(plan, cfg=cfg, tokens={"primary": "t"}, console=Console())
        assert result.ok
        with journal_mod.journal() as j:
            mirrors = j.list_repos()[0].mirrors
        assert mirrors[0].push_mirror_id == 99


class TestEmptyPlan:
    def test_empty_plan_returns_ok(self, patched_providers):
        cfg = _cfg()
        result = executor.apply_plan(
            planner.Plan(), cfg=cfg, tokens={"primary": "t"}, console=Console()
        )
        assert result.ok
        assert result.applied == 0

from __future__ import annotations

import pytest

from hydra.migrations import (
    CURRENT_SCHEMA_VERSION,
    Migration,
    MigrationChainError,
    MigrationContext,
    all_migrations,
    detect_version,
    pending,
    run,
)
from hydra.migrations.base import AppliedMigration  # noqa: F401
from hydra.migrations.m001_legacy_to_v2 import is_legacy_shape


@pytest.fixture
def ctx():
    return MigrationContext()


@pytest.fixture
def legacy_raw():
    return {
        "self_hosted_gitlab": {"url": "https://gl.x", "verify_ssl": False},
        "gitlab": {"url": "https://gitlab.com", "managed_group_prefix": "managed"},
        "github": {"url": "https://api.github.com", "org": "acme"},
        "defaults": {"private": True, "group": "team"},
    }


class TestChainIntegrity:
    def test_chain_covers_v1_to_current(self):
        chain = all_migrations()
        assert chain[0].from_version == 1
        assert chain[-1].to_version == CURRENT_SCHEMA_VERSION

    def test_chain_has_no_gaps(self):
        chain = all_migrations()
        for prev, nxt in zip(chain, chain[1:]):
            assert prev.to_version == nxt.from_version

    def test_unique_names(self):
        names = [m.name for m in all_migrations()]
        assert len(names) == len(set(names))


class TestDetectVersion:
    def test_explicit_schema_version_wins(self):
        assert detect_version({"schema_version": 5, "hosts": []}) == 5

    def test_hosts_without_version_is_v2(self):
        assert detect_version({"hosts": []}) == 2

    def test_legacy_shape_is_v1(self, legacy_raw):
        assert detect_version(legacy_raw) == 1

    def test_empty_dict_is_current(self):
        assert detect_version({}) == CURRENT_SCHEMA_VERSION

    def test_invalid_schema_version_falls_back(self):
        # Non-int / zero / negative should not be trusted.
        assert detect_version({"schema_version": "two", "hosts": []}) == 2
        assert detect_version({"schema_version": 0, "hosts": []}) == 2


class TestPending:
    def test_no_pending_for_current(self):
        assert pending({"schema_version": CURRENT_SCHEMA_VERSION, "hosts": []}) == []

    def test_legacy_has_one_pending(self, legacy_raw):
        p = pending(legacy_raw)
        assert len(p) == 1
        assert p[0].name == "m001-legacy-to-v2"


class TestRun:
    def test_legacy_migrates_to_v2(self, legacy_raw, ctx):
        out, applied = run(legacy_raw, ctx)
        assert out["schema_version"] == 2
        assert [h["id"] for h in out["hosts"]] == [
            "self_hosted_gitlab",
            "gitlab",
            "github",
        ]
        assert out["primary"] == "self_hosted_gitlab"
        assert out["forks"] == ["gitlab", "github"]
        assert len(applied) == 1
        assert applied[0].name == "m001-legacy-to-v2"

    def test_unknown_legacy_fields_preserved(self, legacy_raw, ctx):
        out, _ = run(legacy_raw, ctx)
        sh = next(h for h in out["hosts"] if h["id"] == "self_hosted_gitlab")
        assert sh["options"].get("verify_ssl") is False

    def test_v2_passes_through(self, ctx):
        v2 = {
            "schema_version": 2,
            "hosts": [{"id": "x", "kind": "gitlab", "url": "https://x"}],
            "primary": "x",
            "forks": [],
        }
        out, applied = run(v2, ctx)
        assert applied == []
        assert out["schema_version"] == 2

    def test_v2_without_schema_version_gets_backfilled(self, ctx):
        # Configs written by 0.0.5 between the N-fork ship and this PR.
        v2_no_version = {
            "hosts": [{"id": "x", "kind": "gitlab", "url": "https://x"}],
            "primary": "x",
            "forks": [],
        }
        out, applied = run(v2_no_version, ctx)
        assert applied == []  # detect_version says 2 already
        assert out["schema_version"] == 2

    def test_idempotent_apply(self, legacy_raw, ctx):
        once, _ = run(legacy_raw, ctx)
        twice, applied = run(once, ctx)
        assert applied == []
        # Output is structurally identical (no double-migration).
        assert once == twice


class TestM001Idempotency:
    """Direct apply of the migration on already-migrated data must be a no-op."""

    def test_apply_on_v2_returns_v2(self, ctx):
        from hydra.migrations.m001_legacy_to_v2 import apply

        v2 = {
            "schema_version": 2,
            "hosts": [{"id": "x", "kind": "gitlab", "url": "https://x"}],
            "primary": "x",
            "forks": [],
        }
        result = apply(v2, ctx)
        assert result["schema_version"] == 2
        assert result["hosts"] == v2["hosts"]


class TestRunnerDefenses:
    def test_post_condition_violation_raises(self, ctx):
        """A migration that forgets to set schema_version must be rejected."""
        from hydra.migrations import _MIGRATIONS

        bad = Migration(
            from_version=1,
            to_version=2,
            name="bad",
            description="forgets schema_version",
            apply=lambda raw, _: dict(raw),  # never sets schema_version
        )
        original = list(_MIGRATIONS)
        try:
            _MIGRATIONS.clear()
            _MIGRATIONS.append(bad)
            with pytest.raises(MigrationChainError, match="did not set schema_version"):
                run({"self_hosted_gitlab": {"url": "https://x"}}, ctx)
        finally:
            _MIGRATIONS.clear()
            _MIGRATIONS.extend(original)


def test_is_legacy_shape_helper(legacy_raw):
    assert is_legacy_shape(legacy_raw) is True
    assert is_legacy_shape({"hosts": [], "schema_version": 2}) is False

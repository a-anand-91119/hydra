from __future__ import annotations

from pathlib import Path

import pytest

from hydra import journal as journal_mod
from hydra.journal import (
    JournalMirror,
    JournalRepo,
    PrimaryRepoSnapshot,
    open_journal,
    scan_diff,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "journal.db"


class TestOpenJournal:
    def test_creates_parent_dirs(self, tmp_path: Path):
        path = tmp_path / "nested" / "deep" / "journal.db"
        j = open_journal(path)
        try:
            assert path.exists()
        finally:
            j.close()

    def test_migrates_to_current_schema(self, db_path):
        j = open_journal(db_path)
        try:
            assert journal_mod.current_schema_version(j.connection) == journal_mod.SCHEMA_VERSION
        finally:
            j.close()

    def test_idempotent_open(self, db_path):
        j1 = open_journal(db_path)
        j1.close()
        j2 = open_journal(db_path)
        try:
            # Still readable, version unchanged
            assert journal_mod.current_schema_version(j2.connection) == journal_mod.SCHEMA_VERSION
        finally:
            j2.close()


class TestRecordRepo:
    def test_round_trip(self, db_path):
        j = open_journal(db_path)
        try:
            rid = j.record_repo(
                name="probe",
                primary_host_id="self_hosted",
                primary_repo_id=42,
                primary_repo_url="https://gl.example/grp/probe.git",
            )
            repos = j.list_repos()
            assert len(repos) == 1
            r = repos[0]
            assert r.id == rid
            assert r.name == "probe"
            assert r.primary_host_id == "self_hosted"
            assert r.primary_repo_id == 42
            assert r.primary_repo_url == "https://gl.example/grp/probe.git"
            assert r.state == "active"
            assert r.mirrors == []
        finally:
            j.close()

    def test_unique_upsert(self, db_path):
        """Second insert with same (host_id, repo_id) updates instead of duplicating."""
        j = open_journal(db_path)
        try:
            r1 = j.record_repo(
                name="probe",
                primary_host_id="self_hosted",
                primary_repo_id=42,
                primary_repo_url="https://old/probe.git",
            )
            r2 = j.record_repo(
                name="probe-renamed",
                primary_host_id="self_hosted",
                primary_repo_id=42,
                primary_repo_url="https://new/probe.git",
            )
            assert r1 == r2  # same row reused via upsert
            repos = j.list_repos()
            assert len(repos) == 1
            assert repos[0].name == "probe-renamed"
            assert repos[0].primary_repo_url == "https://new/probe.git"
        finally:
            j.close()


class TestRecordMirror:
    def test_round_trip(self, db_path):
        j = open_journal(db_path)
        try:
            rid = j.record_repo(
                name="probe",
                primary_host_id="self_hosted",
                primary_repo_id=42,
                primary_repo_url="https://gl/probe.git",
            )
            j.record_mirror(
                repo_id=rid,
                target_host_id="gitlab",
                target_repo_url="https://gitlab.com/x/probe.git",
                push_mirror_id=100,
                target_repo_id="999",
            )
            j.record_mirror(
                repo_id=rid,
                target_host_id="github",
                target_repo_url="https://github.com/me/probe.git",
                push_mirror_id=101,
            )
            repos = j.list_repos()
            assert len(repos[0].mirrors) == 2
            hosts = {m.target_host_id for m in repos[0].mirrors}
            assert hosts == {"gitlab", "github"}
            push_ids = {m.push_mirror_id for m in repos[0].mirrors}
            assert push_ids == {100, 101}
        finally:
            j.close()

    def test_unique_per_target_host_upsert(self, db_path):
        j = open_journal(db_path)
        try:
            rid = j.record_repo(
                name="probe",
                primary_host_id="self_hosted",
                primary_repo_id=42,
                primary_repo_url="https://gl/probe.git",
            )
            mid1 = j.record_mirror(
                repo_id=rid,
                target_host_id="gitlab",
                target_repo_url="https://gitlab.com/x/probe.git",
                push_mirror_id=100,
            )
            # Same (repo, target_host) → same row, but push id updates.
            mid2 = j.record_mirror(
                repo_id=rid,
                target_host_id="gitlab",
                target_repo_url="https://gitlab.com/x/probe.git",
                push_mirror_id=222,
            )
            assert mid1 == mid2
            mirrors = j.list_repos()[0].mirrors
            assert len(mirrors) == 1
            assert mirrors[0].push_mirror_id == 222
        finally:
            j.close()

    def test_reupsert_same_push_id_preserves_status(self, db_path):
        """Recording a mirror with an unchanged push_mirror_id must NOT clear
        cached last_status/last_error/last_update_at. Only a genuine re-create
        (different push id) wipes status.
        """
        j = open_journal(db_path)
        try:
            rid = j.record_repo(
                name="probe",
                primary_host_id="ph",
                primary_repo_id=1,
                primary_repo_url="u",
            )
            mid = j.record_mirror(
                repo_id=rid, target_host_id="gitlab", target_repo_url="u", push_mirror_id=10
            )
            j.update_mirror_status(
                mirror_db_id=mid,
                last_status="success",
                last_error=None,
                last_update_at="2026-01-01T00:00:00Z",
            )
            # Same push_id → status preserved
            j.record_mirror(
                repo_id=rid, target_host_id="gitlab", target_repo_url="u", push_mirror_id=10
            )
            m = j.list_repos()[0].mirrors[0]
            assert m.last_status == "success"
            assert m.last_update_at == "2026-01-01T00:00:00Z"
            # Different push_id → status cleared
            j.record_mirror(
                repo_id=rid, target_host_id="gitlab", target_repo_url="u", push_mirror_id=20
            )
            m = j.list_repos()[0].mirrors[0]
            assert m.last_status is None
            assert m.last_update_at is None
        finally:
            j.close()

    def test_status_updates(self, db_path):
        j = open_journal(db_path)
        try:
            rid = j.record_repo(
                name="probe",
                primary_host_id="self_hosted",
                primary_repo_id=42,
                primary_repo_url="https://gl/probe.git",
            )
            mid = j.record_mirror(
                repo_id=rid,
                target_host_id="gitlab",
                target_repo_url="https://gitlab.com/x/probe.git",
                push_mirror_id=100,
            )
            j.update_mirror_status(
                mirror_db_id=mid,
                last_status="success",
                last_error=None,
                last_update_at="2026-01-01T00:00:00Z",
            )
            m = j.list_repos()[0].mirrors[0]
            assert m.last_status == "success"
            assert m.last_update_at == "2026-01-01T00:00:00Z"

            j.update_mirror_push_id(mirror_db_id=mid, new_push_mirror_id=999)
            m = j.list_repos()[0].mirrors[0]
            assert m.push_mirror_id == 999
        finally:
            j.close()


class TestMirrorsForTargetHost:
    def test_returns_pairs(self, db_path):
        j = open_journal(db_path)
        try:
            r1 = j.record_repo(
                name="a",
                primary_host_id="ph",
                primary_repo_id=1,
                primary_repo_url="https://ph/a.git",
            )
            r2 = j.record_repo(
                name="b",
                primary_host_id="ph",
                primary_repo_id=2,
                primary_repo_url="https://ph/b.git",
            )
            j.record_mirror(
                repo_id=r1, target_host_id="gitlab", target_repo_url="x", push_mirror_id=10
            )
            j.record_mirror(
                repo_id=r1, target_host_id="github", target_repo_url="y", push_mirror_id=11
            )
            j.record_mirror(
                repo_id=r2, target_host_id="gitlab", target_repo_url="z", push_mirror_id=12
            )
            pairs = j.mirrors_for_target_host("gitlab")
            assert sorted([p[0].name for p in pairs]) == ["a", "b"]
            assert sorted([p[1].push_mirror_id for p in pairs]) == [10, 12]
        finally:
            j.close()


class TestScanDiff:
    def _journal_repo(self, primary_repo_id, mirror_ids=()):
        mirrors = [
            JournalMirror(
                id=i + 1,
                repo_id=primary_repo_id,
                target_host_id="t",
                target_repo_id=None,
                target_repo_url="u",
                push_mirror_id=mid,
            )
            for i, mid in enumerate(mirror_ids)
        ]
        return JournalRepo(
            id=primary_repo_id,
            name=f"repo-{primary_repo_id}",
            primary_host_id="ph",
            primary_repo_id=primary_repo_id,
            primary_repo_url=f"https://ph/{primary_repo_id}.git",
            created_at="now",
            mirrors=mirrors,
        )

    def test_clean(self):
        jr = [self._journal_repo(1, mirror_ids=[10])]
        snap = [PrimaryRepoSnapshot(repo_id=1, repo_url="u", name="repo-1", mirror_push_ids=[10])]
        diff = scan_diff(jr, snap, primary_host_id="ph")
        assert diff.is_clean

    def test_unknown_repo_on_primary(self):
        snap = [
            PrimaryRepoSnapshot(repo_id=2, repo_url="u", name="new", mirror_push_ids=[20]),
        ]
        diff = scan_diff([], snap, primary_host_id="ph")
        assert len(diff.unknown) == 1
        assert diff.unknown[0].repo_id == 2
        assert not diff.is_clean

    def test_missing_repo_from_primary(self):
        jr = [self._journal_repo(1, mirror_ids=[10])]
        diff = scan_diff(jr, [], primary_host_id="ph")
        assert len(diff.missing) == 1
        assert diff.missing[0].primary_repo_id == 1

    def test_drift_when_mirror_ids_differ(self):
        jr = [self._journal_repo(1, mirror_ids=[10, 11])]
        snap = [PrimaryRepoSnapshot(repo_id=1, repo_url="u", name="repo-1", mirror_push_ids=[10])]
        diff = scan_diff(jr, snap, primary_host_id="ph")
        assert len(diff.drift) == 1
        assert diff.drift[0][0].primary_repo_id == 1

    def test_filters_by_primary_host_id(self):
        """Repos under another primary are ignored entirely."""
        jr_other = JournalRepo(
            id=99,
            name="other",
            primary_host_id="other_primary",
            primary_repo_id=1,
            primary_repo_url="x",
            created_at="now",
            mirrors=[],
        )
        diff = scan_diff([jr_other], [], primary_host_id="ph")
        assert diff.is_clean  # foreign repo doesn't count as "missing"

    def test_soft_deleted_repos_not_flagged_missing(self):
        jr = self._journal_repo(1)
        jr.state = "deleted"
        diff = scan_diff([jr], [], primary_host_id="ph")
        assert diff.missing == []  # deleted state masks missing

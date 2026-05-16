"""End-to-end tests for ``hydra repair`` — re-establishing unhealthy mirrors.

`add` vs `replace` is decided from the primary's live mirror list:
mirror gone → add; mirror still present → replace (delete + recreate).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra import journal as journal_mod
from tests.e2e_helpers import set_tokens, write_config


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch) -> Path:
    path = write_config(tmp_path / "config.yaml")
    monkeypatch.setenv("HYDRA_CONFIG", str(path))
    set_tokens(monkeypatch, primary="primary-tok", fork_gl="gl-tok", fork_gh="gh-tok")
    return path


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _seed(name, target_host_id, *, push_mirror_id, status, error=None, primary_repo_id=10):
    with journal_mod.journal() as j:
        repo_id = j.record_repo(
            name=name,
            primary_host_id="primary",
            primary_repo_id=primary_repo_id,
            primary_repo_url=f"https://primary.example/team/{name}",
        )
        mid = j.record_mirror(
            repo_id=repo_id,
            target_host_id=target_host_id,
            target_repo_url=f"https://gitlab.com/team/{name}.git",
            push_mirror_id=push_mirror_id,
            target_repo_id=None,
        )
        j.update_mirror_status(
            mirror_db_id=mid, last_status=status, last_error=error, last_update_at=None
        )


def _methods(requests_mock):
    return [h.method for h in requests_mock.request_history]


class TestRepairAddPath:
    def test_broken_mirror_gone_is_re_added(self, config_path, runner, requests_mock):
        _seed("alpha", "fork_gl", push_mirror_id=501, status="broken", error="was destroyed")
        # Mirror is GONE on the primary → empty live list → action "add".
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors", json=[]
        )
        requests_mock.post(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json={"id": 9001, "url": "https://gitlab.com/team/alpha.git"},
        )
        result = runner.invoke(cli_mod.app, ["repair", "--yes"])
        assert result.exit_code == 0, result.output
        assert "add" in result.output
        assert "DELETE" not in _methods(requests_mock)  # never deletes a gone mirror
        with journal_mod.journal() as j:
            m = j.list_repos()[0].mirrors[0]
        assert m.push_mirror_id == 9001
        assert m.last_status is None  # cleared; real state from next refresh


class TestRepairReplacePath:
    def test_failing_mirror_present_is_replaced(self, config_path, runner, requests_mock):
        _seed("beta", "fork_gl", push_mirror_id=502, status="failed", error="auth")
        # Mirror still present on the primary → action "replace" (DELETE + POST).
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json=[{"id": 502, "url": "https://gitlab.com/team/beta.git"}],
        )
        requests_mock.delete(
            "https://primary.example/api/v4/projects/10/remote_mirrors/502",
            status_code=204,
        )
        requests_mock.post(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json={"id": 9002, "url": "https://gitlab.com/team/beta.git"},
        )
        result = runner.invoke(cli_mod.app, ["repair", "--yes"])
        assert result.exit_code == 0, result.output
        assert "replace" in result.output
        assert "DELETE" in _methods(requests_mock)
        with journal_mod.journal() as j:
            m = j.list_repos()[0].mirrors[0]
        assert m.push_mirror_id == 9002
        assert m.last_status is None


class TestRepairPlanAndFilters:
    def test_dry_run_probes_but_does_not_mutate(self, config_path, runner, requests_mock):
        _seed("alpha", "fork_gl", push_mirror_id=501, status="broken")
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors", json=[]
        )
        result = runner.invoke(cli_mod.app, ["repair", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "Repair plan" in result.output
        assert "add" in result.output
        methods = _methods(requests_mock)
        assert "POST" not in methods and "DELETE" not in methods  # GET probe only

    def test_nothing_to_repair(self, config_path, runner, requests_mock):
        _seed("alpha", "fork_gl", push_mirror_id=501, status="success")
        result = runner.invoke(cli_mod.app, ["repair", "--yes"])
        assert result.exit_code == 0, result.output
        assert "Nothing to repair" in result.output
        assert requests_mock.call_count == 0

    def test_decline_prompt_makes_no_changes(self, config_path, runner, requests_mock):
        _seed("alpha", "fork_gl", push_mirror_id=501, status="broken")
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors", json=[]
        )
        result = runner.invoke(cli_mod.app, ["repair"], input="n\n")
        assert result.exit_code == 0, result.output
        assert "No changes made" in result.output
        assert "POST" not in _methods(requests_mock)

    def test_host_filter_scopes_to_one_target(self, config_path, runner, requests_mock):
        _seed("alpha", "fork_gl", push_mirror_id=501, status="broken", primary_repo_id=10)
        _seed("alpha", "fork_gh", push_mirror_id=601, status="broken", primary_repo_id=10)
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors", json=[]
        )
        requests_mock.post(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json={"id": 7000, "url": "x"},
        )
        result = runner.invoke(
            cli_mod.app, ["repair", "--host", "fork_gh", "--yes"]
        )
        assert result.exit_code == 0, result.output
        # Only the fork_gh mirror was repaired; fork_gl stays broken.
        with journal_mod.journal() as j:
            mirrors = {m.target_host_id: m for m in j.list_repos()[0].mirrors}
        assert mirrors["fork_gh"].last_status is None  # repaired
        assert mirrors["fork_gl"].last_status == "broken"  # untouched


class TestRepairDestroyed:
    def test_replace_delete_ok_post_fails_is_destroyed(
        self, config_path, runner, requests_mock
    ):
        _seed("gamma", "fork_gl", push_mirror_id=503, status="failed")
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json=[{"id": 503, "url": "https://gitlab.com/team/gamma.git"}],
        )
        requests_mock.delete(
            "https://primary.example/api/v4/projects/10/remote_mirrors/503",
            status_code=204,
        )
        requests_mock.post(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            status_code=500,
            json={"message": "boom"},
        )
        result = runner.invoke(cli_mod.app, ["repair", "--yes"])
        assert result.exit_code == 1, result.output
        assert "DELETED" in result.output  # destroyed outcome rendered
        with journal_mod.journal() as j:
            m = j.list_repos()[0].mirrors[0]
        assert m.last_status == "broken"  # reflects the now-gone mirror


class TestRepairProbeFailure:
    """Regression: when the live-mirror probe fails for a project, EVERY
    unhealthy mirror on that project must be reported as failed — none may be
    silently guessed as 'add' (which would POST a duplicate mirror).
    """

    def test_probe_failure_routes_all_mirrors_no_mutation(
        self, config_path, runner, requests_mock
    ):
        # Two unhealthy mirrors on the SAME project (id=10), different targets.
        _seed("alpha", "fork_gl", push_mirror_id=501, status="broken", primary_repo_id=10)
        _seed("alpha", "fork_gh", push_mirror_id=601, status="failed", primary_repo_id=10)
        # The probe (GET) fails for project 10.
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            status_code=500,
            json={"message": "primary on fire"},
        )
        result = runner.invoke(cli_mod.app, ["repair", "--yes"])
        assert result.exit_code == 1, result.output
        methods = _methods(requests_mock)
        # The bug would have POSTed a duplicate for the second mirror.
        assert "POST" not in methods and "DELETE" not in methods
        assert methods.count("GET") == 1  # probe cached per project
        # Both mirrors surface in the failure summary.
        assert "fork_gl" in result.output and "fork_gh" in result.output
        assert "2 failed" in result.output
        # The probe FAILURE REASON must be visible (not a bare "probe failed").
        assert "probe failed" in result.output
        assert "HTTP 500" in result.output
        # Journal untouched — statuses preserved, not cleared/guessed.
        with journal_mod.journal() as j:
            by_host = {m.target_host_id: m for m in j.list_repos()[0].mirrors}
        assert by_host["fork_gl"].last_status == "broken"
        assert by_host["fork_gh"].last_status == "failed"


class TestRepairJournalFailure:
    def test_journal_write_failure_mid_repair_exits_1(
        self, config_path, runner, requests_mock
    ):
        _seed("alpha", "fork_gl", push_mirror_id=501, status="broken")
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors", json=[]
        )
        requests_mock.post(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json={"id": 9001, "url": "x"},
        )

        def boom(*_a, **_k):
            raise RuntimeError("disk full")

        # Provider add succeeds; the journal push-id write blows up.
        with patch.object(journal_mod.Journal, "update_mirror_push_id", boom):
            result = runner.invoke(cli_mod.app, ["repair", "--yes"])
        assert result.exit_code == 1, result.output
        assert "journal write failed" in result.output.lower()

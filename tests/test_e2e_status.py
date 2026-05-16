"""End-to-end tests for ``hydra status <repo>`` — journal-backed single-repo
view, with ``--refresh`` re-querying the primary.
"""

from __future__ import annotations

from pathlib import Path

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


def _seed(name="alpha", *, push_mirror_id=501, status=None, error=None, primary_repo_id=10):
    with journal_mod.journal() as j:
        repo_id = j.record_repo(
            name=name,
            primary_host_id="primary",
            primary_repo_id=primary_repo_id,
            primary_repo_url=f"https://primary.example/team/{name}",
        )
        mid = j.record_mirror(
            repo_id=repo_id,
            target_host_id="fork_gl",
            target_repo_url=f"https://gitlab.com/team/{name}.git",
            push_mirror_id=push_mirror_id,
            target_repo_id=None,
        )
        if status is not None or error is not None:
            j.update_mirror_status(
                mirror_db_id=mid, last_status=status, last_error=error, last_update_at=None
            )


class TestStatusJournalBacked:
    def test_offline_no_network(self, config_path, runner, requests_mock):
        # No HTTP endpoints registered: requests_mock raises if any call is made.
        _seed(status="success")
        result = runner.invoke(cli_mod.app, ["status", "alpha"])
        assert result.exit_code == 0, result.output
        assert "fork_gl" in result.output
        assert "success" in result.output
        assert requests_mock.call_count == 0  # proves it never hit the network

    def test_missing_repo_exits_1(self, config_path, runner):
        result = runner.invoke(cli_mod.app, ["status", "nope"])
        assert result.exit_code == 1, result.output
        assert "not tracked" in result.output.lower()


class TestStatusRefresh:
    def test_refresh_queries_primary_and_updates_journal(
        self, config_path, runner, requests_mock
    ):
        _seed(push_mirror_id=501, status=None)  # stale, never refreshed
        # The live mirror reports a failing sync.
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json=[
                {
                    "id": 501,
                    "url": "https://gitlab.com/team/alpha.git",
                    "enabled": True,
                    "last_update_status": "failed",
                    "last_update_at": "2026-05-14T00:00:00Z",
                    "last_error": "auth failed",
                }
            ],
        )
        result = runner.invoke(cli_mod.app, ["status", "alpha", "--refresh"])
        assert result.exit_code == 1, result.output  # failed → unhealthy
        assert "failed" in result.output
        assert "error: auth failed" in result.output
        # Journal was updated by the refresh.
        with journal_mod.journal() as j:
            m = j.list_repos()[0].mirrors[0]
        assert m.last_status == "failed"
        assert m.last_error == "auth failed"

    def test_refresh_only_touches_named_repo(self, config_path, runner, requests_mock):
        _seed("alpha", push_mirror_id=501, primary_repo_id=10)
        _seed("beta", push_mirror_id=601, primary_repo_id=20)
        # Only alpha's project endpoint is mocked. If --refresh wrongly touched
        # beta (project 20) the unmocked call would raise.
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json=[{"id": 501, "url": "x", "enabled": True, "last_update_status": "success",
                   "last_update_at": None, "last_error": None}],
        )
        result = runner.invoke(cli_mod.app, ["status", "alpha", "--refresh"])
        assert result.exit_code == 0, result.output
        assert requests_mock.call_count == 1  # exactly one GET (alpha only)
        # beta's mirror was never refreshed → still stale (None), not "missing".
        with journal_mod.journal() as j:
            beta = next(r for r in j.list_repos() if r.name == "beta")
        assert beta.mirrors[0].last_status is None

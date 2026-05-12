"""End-to-end tests for ``hydra list`` (read-only journal display +
optional --refresh which probes the primary).
"""

from __future__ import annotations

import json
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


class TestList:
    def test_empty_journal_prints_hint(self, config_path, runner):
        result = runner.invoke(cli_mod.app, ["list"])
        assert result.exit_code == 0, result.output
        assert "No tracked repos" in result.output

    def test_journal_rows_render_as_table(self, config_path, runner):
        with journal_mod.journal() as j:
            repo_id = j.record_repo(
                name="alpha",
                primary_host_id="primary",
                primary_repo_id=10,
                primary_repo_url="https://primary.example/team/alpha",
            )
            j.record_mirror(
                repo_id=repo_id,
                target_host_id="fork_gl",
                target_repo_url="https://gitlab.com/team/alpha.git",
                push_mirror_id=501,
                target_repo_id=None,
            )

        result = runner.invoke(cli_mod.app, ["list"])
        assert result.exit_code == 0, result.output
        assert "alpha" in result.output
        assert "fork_gl" in result.output

    def test_json_output_is_parseable(self, config_path, runner):
        with journal_mod.journal() as j:
            j.record_repo(
                name="alpha",
                primary_host_id="primary",
                primary_repo_id=10,
                primary_repo_url="https://primary.example/team/alpha",
            )
        result = runner.invoke(cli_mod.app, ["list", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert len(payload) == 1
        assert payload[0]["name"] == "alpha"
        assert payload[0]["primary_repo_id"] == 10

    def test_refresh_hits_primary_and_updates_status(self, config_path, runner, requests_mock):
        with journal_mod.journal() as j:
            repo_id = j.record_repo(
                name="alpha",
                primary_host_id="primary",
                primary_repo_id=10,
                primary_repo_url="https://primary.example/team/alpha",
            )
            j.record_mirror(
                repo_id=repo_id,
                target_host_id="fork_gl",
                target_repo_url="https://gitlab.com/team/alpha.git",
                push_mirror_id=501,
                target_repo_id=None,
            )
        # The primary's list_mirrors call returns a "success" status for our mirror.
        requests_mock.get(
            "https://primary.example/api/v4/projects/10/remote_mirrors",
            json=[
                {
                    "id": 501,
                    "url": "https://gitlab.com/team/alpha.git",
                    "enabled": True,
                    "last_update_status": "success",
                    "last_update_at": "2026-01-01T00:00:00Z",
                    "last_error": None,
                }
            ],
        )
        result = runner.invoke(cli_mod.app, ["list", "--refresh"])
        assert result.exit_code == 0, result.output
        with journal_mod.journal() as j:
            mirror = j.list_repos()[0].mirrors[0]
        assert mirror.last_status == "success"

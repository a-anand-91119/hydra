"""End-to-end tests for ``hydra status <repo>`` — single-repo mirror lookup."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
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


class TestStatus:
    def test_existing_repo_prints_mirror_table(self, config_path, runner, requests_mock):
        # find_project for "team/alpha" returns id=42.
        encoded = quote("team/alpha", safe="")
        requests_mock.get(f"https://primary.example/api/v4/projects/{encoded}", json={"id": 42})
        requests_mock.get(
            "https://primary.example/api/v4/projects/42/remote_mirrors",
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
        result = runner.invoke(cli_mod.app, ["status", "alpha", "--group", "team"])
        assert result.exit_code == 0, result.output
        assert "fork_gl" in result.output
        assert "success" in result.output

    def test_missing_repo_exits_1(self, config_path, runner, requests_mock):
        encoded = quote("team/missing", safe="")
        requests_mock.get(
            f"https://primary.example/api/v4/projects/{encoded}",
            status_code=404,
            json={"message": "Not Found"},
        )
        result = runner.invoke(cli_mod.app, ["status", "missing", "--group", "team"])
        assert result.exit_code == 1, result.output
        assert "Project not found" in result.output

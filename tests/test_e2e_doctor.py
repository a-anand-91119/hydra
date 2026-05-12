"""End-to-end tests for ``hydra doctor`` — the diagnostic command.

Most doctor checks are offline (config parse, journal schema, etc.). The
``--check-tokens`` opt-in is the one path that hits the network — this is
where preflight's e2e shape matters most since doctor + CLI share that
code via :mod:`hydra.preflight`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from tests.e2e_helpers import (
    github_user,
    gitlab_pat_self,
    set_tokens,
    write_config,
)


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch) -> Path:
    path = write_config(tmp_path / "config.yaml")
    monkeypatch.setenv("HYDRA_CONFIG", str(path))
    set_tokens(monkeypatch, primary="primary-tok", fork_gl="gl-tok", fork_gh="gh-tok")
    return path


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


class TestDoctor:
    def test_offline_doctor_exits_clean(self, config_path, runner):
        # No --check-tokens → no network calls. Doctor just runs the offline checks.
        result = runner.invoke(cli_mod.app, ["doctor"])
        # Exit code 0 if no issues, else 1. We just verify it runs.
        assert result.exit_code in (0, 1), result.output
        assert "Config" in result.output

    def test_check_tokens_reports_missing_scope(
        self, config_path, runner, requests_mock
    ):
        # Primary token only has read_api → doctor surfaces a WARN finding.
        requests_mock.get(
            "https://primary.example/api/v4/personal_access_tokens/self",
            json=gitlab_pat_self(["read_api"]),
        )
        requests_mock.get(
            "https://gitlab.com/api/v4/personal_access_tokens/self",
            json=gitlab_pat_self(["api"]),
        )
        body, headers = github_user()
        requests_mock.get("https://api.github.com/user", json=body, headers=headers)

        result = runner.invoke(cli_mod.app, ["doctor", "--check-tokens"])
        # Doctor exits non-zero when issues are found; the missing-scope
        # finding registers as a WARN/ERROR depending on level.
        assert "missing scope" in result.output
        assert "primary" in result.output

    def test_check_tokens_all_valid_no_warnings(
        self, config_path, runner, requests_mock
    ):
        requests_mock.get(
            "https://primary.example/api/v4/personal_access_tokens/self",
            json=gitlab_pat_self(["api"]),
        )
        requests_mock.get(
            "https://gitlab.com/api/v4/personal_access_tokens/self",
            json=gitlab_pat_self(["api"]),
        )
        body, headers = github_user()
        requests_mock.get("https://api.github.com/user", json=body, headers=headers)

        result = runner.invoke(cli_mod.app, ["doctor", "--check-tokens"])
        # No "missing scope" warning when scopes line up.
        assert "missing scope" not in result.output
        assert "token valid" in result.output

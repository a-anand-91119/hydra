"""End-to-end tests for ``hydra scan`` and ``hydra scan --apply``.

Drives the full scan pipeline against a mocked GitLab primary — pagination,
per-project mirror fetch, diff computation, journal mutation under ``--apply``.
Only the HTTP transport is mocked.
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


def _wire_namespace_scan(rmock, *, namespace: str, projects: list, mirrors_by_pid: dict):
    """Wire up a /groups/{namespace}/projects listing + per-project mirrors."""
    from urllib.parse import quote

    encoded = quote(namespace, safe="")
    rmock.get(
        f"https://primary.example/api/v4/groups/{encoded}/projects",
        json=projects,
        headers={"X-Total-Pages": "1"},
    )
    for pid, mirrors in mirrors_by_pid.items():
        rmock.get(
            f"https://primary.example/api/v4/projects/{pid}/remote_mirrors",
            json=mirrors,
        )


class TestScanClean:
    def test_journal_matches_primary_exit_0(self, config_path, runner, requests_mock):
        # Journal already records both repos that the primary returns.
        with journal_mod.journal() as j:
            r1 = j.record_repo(
                name="alpha",
                primary_host_id="primary",
                primary_repo_id=10,
                primary_repo_url="https://primary.example/team/alpha",
            )
            j.record_mirror(
                repo_id=r1,
                target_host_id="fork_gl",
                target_repo_url="https://gitlab.com/team/alpha.git",
                push_mirror_id=501,
                target_repo_id=None,
            )

        _wire_namespace_scan(
            requests_mock,
            namespace="team",
            projects=[
                {
                    "id": 10,
                    "name": "alpha",
                    "web_url": "https://primary.example/team/alpha",
                    "path_with_namespace": "team/alpha",
                }
            ],
            mirrors_by_pid={10: [{"id": 501, "url": "https://gitlab.com/team/alpha.git"}]},
        )

        result = runner.invoke(cli_mod.app, ["scan", "--namespace", "team"])
        assert result.exit_code == 0, result.output
        assert "Journal matches primary" in result.output


class TestScanDrift:
    def test_unknown_repo_surfaces_without_apply(self, config_path, runner, requests_mock):
        # Journal is empty; primary has one repo. Scan without --apply should
        # surface the unknown repo and exit 1 (diff present).
        _wire_namespace_scan(
            requests_mock,
            namespace="team",
            projects=[
                {
                    "id": 10,
                    "name": "alpha",
                    "web_url": "https://primary.example/team/alpha",
                    "path_with_namespace": "team/alpha",
                }
            ],
            mirrors_by_pid={10: [{"id": 501, "url": "https://gitlab.com/team/alpha.git"}]},
        )

        result = runner.invoke(cli_mod.app, ["scan", "--namespace", "team"])
        assert result.exit_code == 1, result.output
        assert "Found 1 repo(s) on primary not in journal" in result.output
        with journal_mod.journal() as j:
            assert j.list_repos() == []  # not adopted

    def test_apply_adopts_unknown_repo(self, config_path, runner, requests_mock):
        _wire_namespace_scan(
            requests_mock,
            namespace="team",
            projects=[
                {
                    "id": 10,
                    "name": "alpha",
                    "web_url": "https://primary.example/team/alpha",
                    "path_with_namespace": "team/alpha",
                }
            ],
            mirrors_by_pid={10: [{"id": 501, "url": "https://gitlab.com/team/alpha.git"}]},
        )

        result = runner.invoke(cli_mod.app, ["scan", "--namespace", "team", "--apply", "--yes"])
        assert result.exit_code == 0, result.output
        with journal_mod.journal() as j:
            rows = j.list_repos()
        assert len(rows) == 1
        assert rows[0].name == "alpha"
        assert rows[0].primary_repo_id == 10
        assert any(m.target_host_id == "fork_gl" for m in rows[0].mirrors)

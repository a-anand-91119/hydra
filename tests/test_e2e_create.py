"""End-to-end tests for ``hydra create``.

Mocks ONLY the HTTP transport (via ``requests_mock``) — everything else
(config loading, token resolution, preflight, probe, planner, executor,
journal) runs unmocked. Exercises real URL construction, header injection,
and response parsing through the full stack.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra import journal as journal_mod
from tests.e2e_helpers import (
    github_user,
    gitlab_pat_self,
    register_find_repo_not_found,
    register_preflight_ok,
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


def _wire_create_endpoints(rmock):
    """Wire up POST endpoints for ``create_repo`` + ``add_outbound_mirror``.

    Used by happy-path and adoption tests; tests that exercise failure
    branches override individual endpoints after this setup runs.
    """
    rmock.post(
        "https://primary.example/api/v4/projects",
        json={"id": 100, "http_url_to_repo": "https://primary.example/probe.git"},
    )
    rmock.post(
        "https://gitlab.com/api/v4/projects",
        json={"id": 200, "http_url_to_repo": "https://gitlab.com/probe.git"},
    )
    rmock.post(
        "https://api.github.com/user/repos",
        json={"clone_url": "https://github.com/octocat/probe.git"},
    )
    # Two add_outbound_mirror calls on the primary (one per fork)
    rmock.post(
        "https://primary.example/api/v4/projects/100/remote_mirrors",
        [{"json": {"id": 7001}}, {"json": {"id": 7002}}],
    )


class TestGoldenPath:
    def test_clean_create_writes_journal_and_calls_every_endpoint(
        self, config_path, runner, requests_mock
    ):
        register_preflight_ok(requests_mock)
        register_find_repo_not_found(requests_mock, repo_name="probe", group="")
        _wire_create_endpoints(requests_mock)

        result = runner.invoke(
            cli_mod.app,
            ["create", "probe", "--yes"],
        )
        assert result.exit_code == 0, result.output
        # Real journal write happened via real executor.
        with journal_mod.journal() as j:
            rows = j.list_repos()
        assert len(rows) == 1
        repo = rows[0]
        assert repo.name == "probe"
        assert repo.primary_host_id == "primary"
        assert repo.primary_repo_id == 100
        mirror_hosts = {m.target_host_id for m in repo.mirrors}
        assert mirror_hosts == {"fork_gl", "fork_gh"}

        # Verify the real outbound calls happened.
        history = requests_mock.request_history
        created_repos = [
            h
            for h in history
            if h.method == "POST" and "projects" in h.url and "remote_mirrors" not in h.url
        ]
        assert len(created_repos) == 2  # primary + gitlab.com
        assert any(h.url.endswith("/user/repos") for h in history)
        # Two mirror creates on the primary.
        mirror_posts = [h for h in history if h.method == "POST" and "remote_mirrors" in h.url]
        assert len(mirror_posts) == 2

    def test_clean_create_with_dry_run_makes_no_mutations(self, config_path, runner, requests_mock):
        register_preflight_ok(requests_mock)
        register_find_repo_not_found(requests_mock, repo_name="probe", group="")
        # Intentionally do NOT wire any POST endpoints. If the executor ran,
        # requests_mock would raise NoMockAddress.

        result = runner.invoke(
            cli_mod.app,
            ["create", "probe", "--dry-run"],
        )
        assert result.exit_code == 0, result.output
        with journal_mod.journal() as j:
            assert j.list_repos() == []


class TestAdoption:
    def test_primary_exists_journal_empty_offers_adoption_accept(
        self, config_path, runner, requests_mock
    ):
        register_preflight_ok(requests_mock)
        # Primary's find_repo returns 200 (exists). Forks 404.
        encoded = quote("probe", safe="")
        requests_mock.get(
            f"https://primary.example/api/v4/projects/{encoded}",
            json={
                "id": 100,
                "http_url_to_repo": "https://primary.example/probe.git",
                "path_with_namespace": "probe",
                "name": "probe",
                "web_url": "https://primary.example/probe",
            },
        )
        requests_mock.get(f"https://gitlab.com/api/v4/projects/{encoded}", status_code=404, json={})
        requests_mock.get(
            "https://api.github.com/repos/octocat/probe",
            status_code=404,
            json={"message": "Not Found"},
        )
        # Primary has no mirrors configured yet.
        requests_mock.get("https://primary.example/api/v4/projects/100/remote_mirrors", json=[])
        # Forks will be created (skip-create for primary), mirrors added.
        requests_mock.post(
            "https://gitlab.com/api/v4/projects",
            json={"id": 200, "http_url_to_repo": "https://gitlab.com/probe.git"},
        )
        requests_mock.post(
            "https://api.github.com/user/repos",
            json={"clone_url": "https://github.com/octocat/probe.git"},
        )
        requests_mock.post(
            "https://primary.example/api/v4/projects/100/remote_mirrors",
            [{"json": {"id": 7001}}, {"json": {"id": 7002}}],
        )

        result = runner.invoke(
            cli_mod.app,
            ["create", "probe", "--adopt-existing", "--yes"],
        )
        assert result.exit_code == 0, result.output
        assert "skip_create_repo" in result.output
        assert "already exists on primary" in result.output
        with journal_mod.journal() as j:
            rows = j.list_repos()
        assert len(rows) == 1
        assert rows[0].primary_repo_id == 100

    def test_all_exists_and_journaled_is_noop(self, config_path, runner, requests_mock):
        register_preflight_ok(requests_mock)
        encoded = quote("probe", safe="")
        # All three hosts find the repo.
        requests_mock.get(
            f"https://primary.example/api/v4/projects/{encoded}",
            json={
                "id": 100,
                "http_url_to_repo": "https://primary.example/probe.git",
                "path_with_namespace": "probe",
                "name": "probe",
                "web_url": "https://primary.example/probe",
            },
        )
        requests_mock.get(
            f"https://gitlab.com/api/v4/projects/{encoded}",
            json={
                "id": 200,
                "http_url_to_repo": "https://gitlab.com/probe.git",
                "path_with_namespace": "probe",
                "name": "probe",
            },
        )
        requests_mock.get(
            "https://api.github.com/repos/octocat/probe",
            json={"clone_url": "https://github.com/octocat/probe.git", "name": "probe"},
        )
        # Primary's existing mirrors are probed when primary is found.
        requests_mock.get("https://primary.example/api/v4/projects/100/remote_mirrors", json=[])
        # And the journal already records the primary.
        with journal_mod.journal() as j:
            j.record_repo(
                name="probe",
                primary_host_id="primary",
                primary_repo_id=100,
                primary_repo_url="https://primary.example/probe.git",
            )

        result = runner.invoke(
            cli_mod.app,
            ["create", "probe"],
        )
        assert result.exit_code == 0, result.output
        assert "already exists on every configured host" in result.output
        # Verify no POSTs ran.
        posts = [h for h in requests_mock.request_history if h.method == "POST"]
        assert posts == []


class TestPreflightFailure:
    def test_missing_api_scope_aborts_before_any_mutation(self, config_path, runner, requests_mock):
        # Primary token has only `read_api` — missing `api`.
        requests_mock.get(
            "https://primary.example/api/v4/personal_access_tokens/self",
            json=gitlab_pat_self(["read_api"]),
        )
        # Other hosts respond fine — irrelevant once primary fails preflight.
        requests_mock.get(
            "https://gitlab.com/api/v4/personal_access_tokens/self",
            json=gitlab_pat_self(["api"]),
        )
        body, headers = github_user()
        requests_mock.get("https://api.github.com/user", json=body, headers=headers)

        result = runner.invoke(
            cli_mod.app,
            ["create", "probe", "--yes"],
        )
        assert result.exit_code == 1, result.output
        assert "Token preflight failed" in result.output
        assert "missing scope" in result.output
        # No find_repo, no create_repo — preflight bailed first.
        non_preflight = [
            h
            for h in requests_mock.request_history
            if "personal_access_tokens" not in h.url and h.url != "https://api.github.com/user"
        ]
        assert non_preflight == [], f"unexpected calls after preflight: {non_preflight}"

    def test_skip_preflight_lets_bad_token_through(self, config_path, runner, requests_mock):
        # Primary token is missing api scope — but --skip-preflight should
        # bypass the check entirely so the next mutation gets a chance to fail.
        # No preflight endpoints registered; probe + creates wired normally.
        register_find_repo_not_found(requests_mock, repo_name="probe", group="")
        _wire_create_endpoints(requests_mock)

        result = runner.invoke(
            cli_mod.app,
            ["create", "probe", "--yes", "--skip-preflight"],
        )
        # We expect success here because the mock returns 201 from POST endpoints
        # — proves that --skip-preflight reaches the executor.
        assert result.exit_code == 0, result.output

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra import journal as journal_mod
from tests.e2e_helpers import github_repo, gitlab_project, set_tokens, write_config


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _configure(
    tmp_path: Path,
    monkeypatch,
    *,
    forks=None,
) -> Path:
    path = write_config(tmp_path / "config.yaml", forks=forks)
    monkeypatch.setenv("HYDRA_CONFIG", str(path))
    set_tokens(
        monkeypatch,
        primary="primary-tok",
        fork_a="fork-a-tok",
        fork_b="fork-b-tok",
        fork_gl="fork-gl-tok",
        fork_gh="fork-gh-tok",
    )
    return path


def _seed_repo(*, name: str = "probe", primary_repo_id: int = 10) -> int:
    with journal_mod.journal() as j:
        return j.record_repo(
            name=name,
            primary_host_id="primary",
            primary_repo_id=primary_repo_id,
            primary_repo_url=f"https://primary.example/team/{name}.git",
        )


def _seed_mirror(
    repo_id: int,
    *,
    host_id: str,
    target_repo_id: str | None,
    target_repo_url: str | None = None,
) -> None:
    with journal_mod.journal() as j:
        j.record_mirror(
            repo_id=repo_id,
            target_host_id=host_id,
            target_repo_id=target_repo_id,
            target_repo_url=target_repo_url or f"https://{host_id}.example/team/probe.git",
            push_mirror_id=500 + repo_id,
        )


def _gitlab_fork(host_id: str, url: str):
    return {"id": host_id, "kind": "gitlab", "url": url, "options": {}}


def _github_fork(host_id: str = "fork_gh", org: str | None = "acme"):
    return {
        "id": host_id,
        "kind": "github",
        "url": "https://api.github.com",
        "options": {"org": org},
    }


class TestDestroyHappyPath:
    def test_journal_primary_and_mirrors_are_deleted(
        self, tmp_path, monkeypatch, runner, requests_mock
    ):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[
                _gitlab_fork("fork_a", "https://fork-a.example"),
                _gitlab_fork("fork_b", "https://fork-b.example"),
            ],
        )
        repo_id = _seed_repo()
        _seed_mirror(
            repo_id,
            host_id="fork_a",
            target_repo_id="20",
            target_repo_url="https://fork-a.example/team/probe.git",
        )
        _seed_mirror(
            repo_id,
            host_id="fork_b",
            target_repo_id="30",
            target_repo_url="https://fork-b.example/team/probe.git",
        )
        requests_mock.delete("https://fork-a.example/api/v4/projects/20", status_code=202)
        requests_mock.delete("https://fork-b.example/api/v4/projects/30", status_code=202)
        requests_mock.delete("https://primary.example/api/v4/projects/10", status_code=202)

        result = runner.invoke(cli_mod.app, ["destroy", "probe", "--yes"])

        assert result.exit_code == 0, result.output
        assert [r.method for r in requests_mock.request_history] == ["DELETE", "DELETE", "DELETE"]
        assert [r.url for r in requests_mock.request_history] == [
            "https://fork-a.example/api/v4/projects/20",
            "https://fork-b.example/api/v4/projects/30",
            "https://primary.example/api/v4/projects/10",
        ]
        with journal_mod.journal() as j:
            assert j.list_repos() == []

    def test_github_fork_is_deleted_by_owner_and_name(
        self, tmp_path, monkeypatch, runner, requests_mock
    ):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_github_fork()],
        )
        repo_id = _seed_repo()
        _seed_mirror(
            repo_id,
            host_id="fork_gh",
            target_repo_id=None,
            target_repo_url="https://github.com/acme/probe.git",
        )
        requests_mock.get(
            "https://api.github.com/repos/acme/probe",
            json=github_repo(owner="acme", name="probe"),
        )
        requests_mock.delete("https://api.github.com/repos/acme/probe", status_code=204)
        requests_mock.delete("https://primary.example/api/v4/projects/10", status_code=202)

        result = runner.invoke(cli_mod.app, ["destroy", "probe", "--yes"])

        assert result.exit_code == 0, result.output
        assert "provider cannot delete repos" not in result.output
        assert [r.method for r in requests_mock.request_history] == ["GET", "DELETE", "DELETE"]
        assert [r.url for r in requests_mock.request_history] == [
            "https://api.github.com/repos/acme/probe",
            "https://api.github.com/repos/acme/probe",
            "https://primary.example/api/v4/projects/10",
        ]
        with journal_mod.journal() as j:
            assert j.list_repos() == []

    def test_delete_namespace_flag_deletes_inferred_gitlab_groups(
        self, tmp_path, monkeypatch, runner, requests_mock
    ):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )
        repo_id = _seed_repo()
        _seed_mirror(
            repo_id,
            host_id="fork_gl",
            target_repo_id="20",
            target_repo_url="https://gitlab.com/team/probe.git",
        )
        requests_mock.delete("https://gitlab.com/api/v4/projects/20", status_code=202)
        requests_mock.delete("https://primary.example/api/v4/projects/10", status_code=202)
        requests_mock.delete("https://primary.example/api/v4/groups/team", status_code=202)
        requests_mock.delete("https://gitlab.com/api/v4/groups/team", status_code=202)

        result = runner.invoke(cli_mod.app, ["destroy", "probe", "--delete-namespace", "--yes"])

        assert result.exit_code == 0, result.output
        assert "namespace" in result.output
        assert [r.url for r in requests_mock.request_history] == [
            "https://gitlab.com/api/v4/projects/20",
            "https://primary.example/api/v4/projects/10",
            "https://primary.example/api/v4/groups/team",
            "https://gitlab.com/api/v4/groups/team",
        ]
        with journal_mod.journal() as j:
            assert j.list_repos() == []

    def test_yes_skips_prompt(self, tmp_path, monkeypatch, runner, requests_mock):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )
        repo_id = _seed_repo()
        _seed_mirror(repo_id, host_id="fork_gl", target_repo_id="20")
        requests_mock.delete("https://gitlab.com/api/v4/projects/20", status_code=202)
        requests_mock.delete("https://primary.example/api/v4/projects/10", status_code=202)

        with patch("hydra.cli.destroy.typer.confirm") as confirm:
            result = runner.invoke(cli_mod.app, ["destroy", "probe", "--yes"])

        assert result.exit_code == 0, result.output
        confirm.assert_not_called()

    def test_decline_prompt_makes_no_changes(self, tmp_path, monkeypatch, runner, requests_mock):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )
        repo_id = _seed_repo()
        _seed_mirror(repo_id, host_id="fork_gl", target_repo_id="20")

        result = runner.invoke(cli_mod.app, ["destroy", "probe"], input="n\n")

        assert result.exit_code == 0, result.output
        assert "No changes made" in result.output
        assert requests_mock.request_history == []
        with journal_mod.journal() as j:
            assert len(j.list_repos()) == 1

    def test_gitlab_already_marked_for_deletion_is_success(
        self, tmp_path, monkeypatch, runner, requests_mock
    ):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )
        repo_id = _seed_repo()
        _seed_mirror(repo_id, host_id="fork_gl", target_repo_id="83318931")
        requests_mock.delete(
            "https://gitlab.com/api/v4/projects/83318931",
            status_code=400,
            json={"message": "Project has already been marked for deletion"},
        )
        requests_mock.delete("https://primary.example/api/v4/projects/10", status_code=202)

        result = runner.invoke(cli_mod.app, ["destroy", "probe", "--yes"])

        assert result.exit_code == 0, result.output
        with journal_mod.journal() as j:
            assert j.list_repos() == []


class TestDestroyProbing:
    def test_partial_journal_probes_and_deletes_orphaned_fork(
        self, tmp_path, monkeypatch, runner, requests_mock
    ):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )
        _seed_repo()
        requests_mock.get(
            "https://gitlab.com/api/v4/projects/team%2Fprobe",
            json=gitlab_project(
                project_id=20,
                name="probe",
                base_url="https://gitlab.com",
                group="team",
            ),
        )
        requests_mock.delete("https://gitlab.com/api/v4/projects/20", status_code=202)
        requests_mock.delete("https://primary.example/api/v4/projects/10", status_code=202)

        result = runner.invoke(cli_mod.app, ["destroy", "probe", "--yes"])

        assert result.exit_code == 0, result.output
        assert "probed" in result.output
        assert [r.method for r in requests_mock.request_history] == ["GET", "DELETE", "DELETE"]
        with journal_mod.journal() as j:
            assert j.list_repos() == []

    def test_probe_failure_warns_and_continues(self, tmp_path, monkeypatch, runner, requests_mock):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )
        _seed_repo()
        requests_mock.get(
            "https://gitlab.com/api/v4/projects/team%2Fprobe",
            status_code=500,
            json={"message": "boom"},
        )
        requests_mock.delete("https://primary.example/api/v4/projects/10", status_code=202)

        result = runner.invoke(cli_mod.app, ["destroy", "probe", "--yes"])

        assert result.exit_code == 0, result.output
        assert "probe failed for fork_gl" in result.output
        assert [r.method for r in requests_mock.request_history] == ["GET", "DELETE"]
        with journal_mod.journal() as j:
            assert j.list_repos() == []


class TestDestroyFailures:
    def test_name_not_in_journal_exits_1(self, tmp_path, monkeypatch, runner):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )

        result = runner.invoke(cli_mod.app, ["destroy", "missing", "--yes"])

        assert result.exit_code == 1, result.output
        assert "No journal entry" in result.output

    def test_delete_failure_exits_1_and_preserves_journal(
        self, tmp_path, monkeypatch, runner, requests_mock
    ):
        _configure(
            tmp_path,
            monkeypatch,
            forks=[_gitlab_fork("fork_gl", "https://gitlab.com")],
        )
        repo_id = _seed_repo()
        _seed_mirror(repo_id, host_id="fork_gl", target_repo_id="20")
        requests_mock.delete(
            "https://gitlab.com/api/v4/projects/20",
            status_code=500,
            json={"message": "boom"},
        )

        result = runner.invoke(cli_mod.app, ["destroy", "probe", "--yes"])

        assert result.exit_code == 1, result.output
        assert "journal entry preserved" in result.output
        assert [r.url for r in requests_mock.request_history] == [
            "https://gitlab.com/api/v4/projects/20"
        ]
        with journal_mod.journal() as j:
            repos = j.list_repos()
        assert len(repos) == 1
        assert repos[0].name == "probe"

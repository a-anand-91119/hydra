"""Regression test for cli._execute_create — covers 1 primary + N forks via the
provider abstraction. Mocks the underlying HTTP-calling functions in
hydra.gitlab / hydra.github / hydra.mirrors.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from rich.console import Console

from hydra import journal as journal_mod
from hydra.cli import _execute_create
from hydra.config import Config, Defaults, HostSpec
from hydra.errors import HydraAPIError
from hydra.gitlab import CreatedRepo, GroupResolution
from hydra.wizard import CreateOptions


@pytest.fixture
def cfg():
    return Config(
        hosts=[
            HostSpec(
                id="self_hosted_gitlab",
                kind="gitlab",
                url="https://gitlab.example.com",
                options={"add_timestamp": False},
            ),
            HostSpec(
                id="gitlab",
                kind="gitlab",
                url="https://gitlab.com",
                options={"managed_group_prefix": "managed", "add_timestamp": True},
            ),
            HostSpec(
                id="github",
                kind="github",
                url="https://api.github.com",
                options={"org": None},
            ),
        ],
        primary="self_hosted_gitlab",
        forks=["gitlab", "github"],
        defaults=Defaults(private=True, group=""),
    )


@pytest.fixture
def opts():
    return CreateOptions(
        name="probe",
        description="d",
        group="myteam",
        is_private=True,
        mirror=True,
        dry_run=False,
    )


@pytest.fixture
def console():
    return Console(record=True, width=120)


@pytest.fixture
def patches():
    with (
        patch("hydra.cli.secrets_mod.get_token", side_effect=lambda hid, **_: f"tok-{hid}"),
        patch("hydra.gitlab.get_or_create_group_path") as gl_groups,
        patch("hydra.gitlab.create_repo") as gl_create,
        patch("hydra.github.create_repo") as gh_create,
        patch("hydra.mirrors.add_mirror") as mi_add,
    ):
        yield {
            "gl_groups": gl_groups,
            "gl_create": gl_create,
            "gh_create": gh_create,
            "mi_add": mi_add,
        }


def _stub_happy(p):
    p["gl_groups"].side_effect = [
        # primary (self_hosted_gitlab)
        GroupResolution(group_id=10, created_paths=["myteam"]),
        # fork: gitlab.com (with managed prefix)
        GroupResolution(group_id=20, created_paths=["managed/myteam"]),
    ]
    p["gl_create"].side_effect = [
        CreatedRepo(http_url="https://gitlab.example.com/myteam/probe.git", project_id=999),
        CreatedRepo(http_url="https://gitlab.com/managed/myteam/probe.git", project_id=888),
    ]
    p["gh_create"].return_value = "https://github.com/me/probe.git"
    # Distinct ids per call so journal rows are uniquely identifiable.
    p["mi_add"].side_effect = [{"id": 7001}, {"id": 7002}]


class TestExecuteCreateHappyPath:
    def test_creates_primary_plus_forks_and_mirrors(self, cfg, opts, console, patches):
        _stub_happy(patches)

        _execute_create(cfg=cfg, opts=opts, verbose=False, console=console)

        # 2 gitlab repos created (primary + gitlab.com fork)
        assert patches["gl_create"].call_count == 2
        # 1 github repo created
        assert patches["gh_create"].call_count == 1
        # 2 mirrors added: one per fork
        assert patches["mi_add"].call_count == 2

        # Mirrors are added on the primary's project_id
        for call in patches["mi_add"].call_args_list:
            assert call.kwargs["project_id"] == 999
            assert call.kwargs["base_url"] == "https://gitlab.example.com"

    def test_primary_no_timestamp_fork_gitlab_com_timestamped(self, cfg, opts, console, patches):
        _stub_happy(patches)
        _execute_create(cfg=cfg, opts=opts, verbose=False, console=console)

        primary_call = patches["gl_groups"].call_args_list[0]
        fork_call = patches["gl_groups"].call_args_list[1]
        assert primary_call.kwargs["add_timestamp"] is False
        assert primary_call.kwargs["host"] == "self_hosted_gitlab"
        assert primary_call.kwargs["group_path"] == "myteam"
        assert fork_call.kwargs["add_timestamp"] is True
        assert fork_call.kwargs["host"] == "gitlab"
        assert fork_call.kwargs["group_path"] == "managed/myteam"

    def test_no_mirror_flag_skips_mirror_setup(self, cfg, opts, console, patches):
        opts.mirror = False
        _stub_happy(patches)

        _execute_create(cfg=cfg, opts=opts, verbose=False, console=console)
        patches["mi_add"].assert_not_called()

    def test_writes_journal_rows(self, cfg, opts, console, patches):
        """After a successful create, the journal carries one repo row and one
        mirror row per fork, each tagged with the push_mirror_id returned by
        the GitLab API."""
        _stub_happy(patches)
        _execute_create(cfg=cfg, opts=opts, verbose=False, console=console)

        with journal_mod.journal() as j:
            repos = j.list_repos()

        assert len(repos) == 1
        r = repos[0]
        assert r.name == "probe"
        assert r.primary_host_id == "self_hosted_gitlab"
        assert r.primary_repo_id == 999
        by_host = {m.target_host_id: m for m in r.mirrors}
        assert set(by_host) == {"gitlab", "github"}
        # push_mirror_id from add_mirror's payload[id]
        assert {by_host["gitlab"].push_mirror_id, by_host["github"].push_mirror_id} == {
            7001,
            7002,
        }


class TestExecuteCreatePartialFailure:
    def test_github_failure_after_gitlab_repos_reports_orphans(self, cfg, opts, console, patches):
        import typer

        _stub_happy(patches)
        patches["gh_create"].side_effect = HydraAPIError(message="github boom", hint="check token")

        with pytest.raises(typer.Exit):
            _execute_create(cfg=cfg, opts=opts, verbose=False, console=console)

        out = console.export_text()
        assert "github boom" in out
        assert "self_hosted_gitlab repo" in out
        assert "gitlab repo" in out
        # The plan interleaves mirror setup with each fork, so the gitlab.com
        # fork's mirror is added before github's create_repo runs. Confirm the
        # one completed mirror is reported as an orphan.
        assert patches["mi_add"].call_count == 1
        assert "mirror → gitlab" in out

    def test_partial_mirror_failure_lists_succeeded_mirrors(self, cfg, opts, console, patches):
        import typer

        _stub_happy(patches)
        # The plan order is: gitlab fork repo → gitlab mirror → github fork
        # repo → github mirror. Make github's mirror call fail.
        patches["mi_add"].side_effect = [
            {"id": 7001},
            HydraAPIError(message="mirror boom", hint=""),
        ]

        with pytest.raises(typer.Exit):
            _execute_create(cfg=cfg, opts=opts, verbose=False, console=console)

        out = console.export_text()
        assert "mirror boom" in out
        # The first fork's mirror succeeded — its orphan entry surfaces in
        # the partial-progress block.
        assert "mirror → gitlab" in out


class TestDryRunAndConfirm:
    """End-to-end CLI behavior for --dry-run / --yes / declined confirm."""

    def _setup(self, patches):
        # Stubs return well-formed URLs so the mirror credential-injection
        # path doesn't blow up; dry-run shouldn't reach them, but the
        # confirm-yes test does.
        patches["gl_groups"].side_effect = lambda **kw: GroupResolution(
            group_id=1, created_paths=[]
        )
        patches["gl_create"].side_effect = lambda **kw: CreatedRepo(
            http_url="https://gl.example/team/probe.git", project_id=1
        )
        patches["gh_create"].return_value = "https://github.com/me/probe.git"
        patches["mi_add"].return_value = {"id": 1}

    def test_dry_run_makes_no_provider_calls(self, cfg, console, patches, monkeypatch):
        from typer.testing import CliRunner

        from hydra import cli as cli_mod

        self._setup(patches)
        monkeypatch.setattr(cli_mod, "_load_or_die", lambda *a, **k: cfg)
        runner = CliRunner()
        result = runner.invoke(cli_mod.app, ["create", "probe", "--dry-run"])
        assert result.exit_code == 0, result.output
        patches["gl_create"].assert_not_called()
        patches["gh_create"].assert_not_called()
        patches["mi_add"].assert_not_called()

    def test_decline_confirm_aborts(self, cfg, console, patches, monkeypatch):
        from typer.testing import CliRunner

        from hydra import cli as cli_mod

        self._setup(patches)
        monkeypatch.setattr(cli_mod, "_load_or_die", lambda *a, **k: cfg)
        runner = CliRunner()
        result = runner.invoke(cli_mod.app, ["create", "probe"], input="n\n")
        assert result.exit_code == 0, result.output
        assert "No changes made" in result.output
        patches["gl_create"].assert_not_called()

    def test_yes_skips_prompt(self, cfg, console, patches, monkeypatch):
        from typer.testing import CliRunner

        from hydra import cli as cli_mod

        self._setup(patches)
        # Distinct ids per call so journal stitching works.
        patches["gl_create"].side_effect = [
            CreatedRepo(http_url="https://a.example/probe.git", project_id=11),
            CreatedRepo(http_url="https://b.example/probe.git", project_id=22),
        ]
        patches["mi_add"].side_effect = [{"id": 91}, {"id": 92}]
        monkeypatch.setattr(cli_mod, "_load_or_die", lambda *a, **k: cfg)
        # patches["..."] already stubs secrets.get_token via hydra.cli.secrets_mod
        runner = CliRunner()
        result = runner.invoke(cli_mod.app, ["create", "probe", "--yes"])
        assert result.exit_code == 0, result.output
        assert patches["gl_create"].call_count >= 1


class TestNForks:
    """Verify the abstraction handles N!=2 forks."""

    def test_three_forks(self, opts, console, patches):
        cfg = Config(
            hosts=[
                HostSpec(id="primary", kind="gitlab", url="https://primary.gl"),
                HostSpec(id="gh", kind="github", url="https://api.github.com"),
                HostSpec(
                    id="cloud",
                    kind="gitlab",
                    url="https://gitlab.com",
                    options={"managed_group_prefix": "mp"},
                ),
                HostSpec(id="extra", kind="gitlab", url="https://extra.gl"),
            ],
            primary="primary",
            forks=["gh", "cloud", "extra"],
            defaults=Defaults(private=True, group=""),
        )
        opts.group = ""
        # 3 group calls (primary + 2 gitlab forks; github skips)
        patches["gl_groups"].side_effect = [
            GroupResolution(group_id=1, created_paths=[]),
            GroupResolution(group_id=2, created_paths=[]),
            GroupResolution(group_id=3, created_paths=[]),
        ]
        # 3 gitlab repos: primary + cloud + extra
        patches["gl_create"].side_effect = [
            CreatedRepo(http_url="https://primary.gl/probe.git", project_id=100),
            CreatedRepo(http_url="https://gitlab.com/mp/probe.git", project_id=200),
            CreatedRepo(http_url="https://extra.gl/probe.git", project_id=300),
        ]
        patches["gh_create"].return_value = "https://github.com/me/probe.git"
        patches["mi_add"].return_value = {}

        _execute_create(cfg=cfg, opts=opts, verbose=False, console=console)

        assert patches["gh_create"].call_count == 1
        assert patches["gl_create"].call_count == 3
        # Three mirrors, each with primary project_id=100
        assert patches["mi_add"].call_count == 3
        for call in patches["mi_add"].call_args_list:
            assert call.kwargs["project_id"] == 100

"""Phase 6 (Item 6): adoption-aware re-run for `hydra create`.

Exercises the full ``create`` CLI path with mocked providers so we can
verify:
- All-exist-and-journaled → no-op exit 0
- Primary exists, journal empty → prompt → adopt path
- Primary exists, journal empty → declined → exit 1
- Partial fork-exists → real create_repo for missing, skip for existing
- Existing mirror → skip add_outbound_mirror
- --adopt-existing skips the prompt
- --no-probe bypasses find_repo entirely
- --dry-run with existing state shows the transformed plan
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra import journal as journal_mod
from hydra.config import Config, Defaults, HostSpec
from hydra.gitlab import CreatedRepo, GroupResolution
from hydra.providers.base import MirrorInfo, RepoRef


@pytest.fixture
def cfg():
    return Config(
        hosts=[
            HostSpec(
                id="primary",
                kind="gitlab",
                url="https://primary.example",
                options={},
            ),
            HostSpec(
                id="fork_gl",
                kind="gitlab",
                url="https://gitlab.com",
                options={},
            ),
        ],
        primary="primary",
        forks=["fork_gl"],
        defaults=Defaults(private=True, group=""),
    )


@pytest.fixture
def patches(cfg):
    """Stub every outbound provider call. Each test re-configures the side
    effects it cares about.
    """
    with (
        patch("hydra.cli.secrets_mod.get_token", side_effect=lambda hid, **_: f"tok-{hid}"),
        patch.object(cli_mod, "_load_or_die", lambda *a, **k: cfg),
        patch("hydra.cli._preflight_or_die"),  # Phase 7 — assume tokens OK
        patch("hydra.gitlab.get_or_create_group_path") as gl_groups,
        patch("hydra.gitlab.create_repo") as gl_create,
        patch("hydra.mirrors.add_mirror") as mi_add,
        patch("hydra.providers.gitlab.GitLabProvider.find_repo") as gl_find,
        patch("hydra.providers.gitlab.GitLabProvider.list_mirrors") as gl_list_mirrors,
    ):
        # Sensible defaults — tests override.
        gl_groups.return_value = GroupResolution(group_id=1, created_paths=[])
        gl_create.side_effect = [
            CreatedRepo(http_url="https://primary.example/probe.git", project_id=100),
            CreatedRepo(http_url="https://gitlab.com/probe.git", project_id=200),
        ]
        mi_add.return_value = {"id": 9000}
        gl_find.return_value = None  # default: nothing exists
        gl_list_mirrors.return_value = []
        yield {
            "gl_groups": gl_groups,
            "gl_create": gl_create,
            "mi_add": mi_add,
            "gl_find": gl_find,
            "gl_list_mirrors": gl_list_mirrors,
        }


class TestAllHostsHaveRepo:
    def test_all_hosts_have_repo_and_journal_matches_is_noop(self, cfg, patches, tmp_path):
        # Both hosts return an existing RepoRef…
        patches["gl_find"].side_effect = [
            RepoRef(http_url="https://primary.example/probe.git", project_id=100, namespace_path=None),
            RepoRef(http_url="https://gitlab.com/probe.git", project_id=200, namespace_path=None),
        ]
        # …and the journal already records the primary.
        with journal_mod.journal() as j:
            j.record_repo(
                name="probe",
                primary_host_id="primary",
                primary_repo_id=100,
                primary_repo_url="https://primary.example/probe.git",
            )

        runner = CliRunner()
        result = runner.invoke(
            cli_mod.app, ["create", "probe", "--yes"]
        )
        assert result.exit_code == 0, result.output
        assert "already exists on every configured host" in result.output
        patches["gl_create"].assert_not_called()
        patches["mi_add"].assert_not_called()


class TestAdoptionPrompt:
    def test_primary_exists_journal_empty_offers_adoption_accept(self, cfg, patches):
        # Primary exists, fork doesn't.
        patches["gl_find"].side_effect = [
            RepoRef(http_url="https://primary.example/probe.git", project_id=100, namespace_path=None),
            None,
        ]
        patches["gl_create"].side_effect = [
            CreatedRepo(http_url="https://gitlab.com/probe.git", project_id=200),
        ]
        runner = CliRunner()
        # Two y's: first to adopt, second to confirm the transformed plan.
        result = runner.invoke(
            cli_mod.app, ["create", "probe"], input="y\ny\n"
        )
        assert result.exit_code == 0, result.output
        assert "already exists on primary" in result.output
        # Primary is adopted → skip_create_repo, only fork actually created.
        assert patches["gl_create"].call_count == 1
        # And the journal was populated retroactively.
        with journal_mod.journal() as j:
            rows = j.list_repos()
        assert len(rows) == 1
        assert rows[0].name == "probe"

    def test_primary_exists_journal_empty_decline_exits_1(self, cfg, patches):
        patches["gl_find"].side_effect = [
            RepoRef(http_url="https://primary.example/probe.git", project_id=100, namespace_path=None),
            None,
        ]
        runner = CliRunner()
        result = runner.invoke(cli_mod.app, ["create", "probe"], input="n\n")
        assert result.exit_code == 1, result.output
        assert "No changes made" in result.output
        patches["gl_create"].assert_not_called()

    def test_adopt_existing_flag_skips_prompt(self, cfg, patches):
        patches["gl_find"].side_effect = [
            RepoRef(http_url="https://primary.example/probe.git", project_id=100, namespace_path=None),
            None,
        ]
        patches["gl_create"].side_effect = [
            CreatedRepo(http_url="https://gitlab.com/probe.git", project_id=200),
        ]
        runner = CliRunner()
        # Just one y for the apply-plan confirm — no adoption prompt because
        # --adopt-existing was passed.
        result = runner.invoke(
            cli_mod.app, ["create", "probe", "--adopt-existing"], input="y\n"
        )
        assert result.exit_code == 0, result.output
        # No "Adopt it?" prompt in the output.
        assert "Adopt it?" not in result.output


class TestPartialState:
    def test_one_fork_exists_skips_create_for_that_fork_only(self, cfg, patches):
        # Primary doesn't exist, fork does.
        patches["gl_find"].side_effect = [
            None,
            RepoRef(http_url="https://gitlab.com/probe.git", project_id=200, namespace_path=None),
        ]
        patches["gl_create"].side_effect = [
            CreatedRepo(http_url="https://primary.example/probe.git", project_id=100),
        ]
        runner = CliRunner()
        result = runner.invoke(
            cli_mod.app, ["create", "probe", "--yes"]
        )
        assert result.exit_code == 0, result.output
        # Only one create_repo call — the primary.
        assert patches["gl_create"].call_count == 1
        # Plan renderer surfaces the skip row.
        assert "skip_create_repo" in result.output


class TestExistingMirror:
    def test_existing_mirror_skips_add_mirror(self, cfg, patches):
        # Both hosts already have the repo…
        patches["gl_find"].side_effect = [
            RepoRef(http_url="https://primary.example/probe.git", project_id=100, namespace_path=None),
            RepoRef(http_url="https://gitlab.com/probe.git", project_id=200, namespace_path=None),
        ]
        # …and the primary already has a push-mirror to the fork.
        patches["gl_list_mirrors"].return_value = [
            MirrorInfo(
                id=4242,
                url="https://gitlab.com/probe.git",
                enabled=True,
                last_update_status="success",
                last_update_at=None,
                last_error=None,
            )
        ]
        runner = CliRunner()
        # adopt + apply plan
        result = runner.invoke(
            cli_mod.app, ["create", "probe", "--adopt-existing", "--yes"]
        )
        assert result.exit_code == 0, result.output
        assert "skip_add_mirror" in result.output
        patches["mi_add"].assert_not_called()


class TestNoProbeFlag:
    def test_no_probe_skips_find_repo_calls(self, cfg, patches):
        runner = CliRunner()
        result = runner.invoke(
            cli_mod.app,
            ["create", "probe", "--yes", "--no-probe", "--skip-preflight"],
        )
        assert result.exit_code == 0, result.output
        patches["gl_find"].assert_not_called()
        patches["gl_list_mirrors"].assert_not_called()
        # Original plan executed unchanged.
        assert patches["gl_create"].call_count == 2


class TestDryRunWithExistingState:
    def test_dry_run_renders_transformed_plan(self, cfg, patches):
        patches["gl_find"].side_effect = [
            RepoRef(http_url="https://primary.example/probe.git", project_id=100, namespace_path=None),
            None,
        ]
        runner = CliRunner()
        result = runner.invoke(
            cli_mod.app, ["create", "probe", "--dry-run", "--adopt-existing"]
        )
        assert result.exit_code == 0, result.output
        # The dry-run still shows the transformed plan with skip rows.
        assert "skip_create_repo" in result.output
        # And does not attempt the real create.
        patches["gl_create"].assert_not_called()

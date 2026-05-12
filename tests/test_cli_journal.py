"""Integration tests for the `list`, `scan`, and `rotate-token` CLI commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra import journal as journal_mod
from hydra.config import Config, Defaults, HostSpec
from hydra.preflight import PreflightReport
from hydra.providers.base import MirrorInfo, PrimaryMirror, PrimaryProject


def _clean_preflight() -> PreflightReport:
    """Empty PreflightReport — used as a return value when patching the
    network probe in rotate-token tests that don't exercise scope-checking.
    """
    return PreflightReport()


def _proj(*, project_id, web_url, name, full_path, mirrors):
    """Helper: build a PrimaryProject from (id, url) pairs."""
    return PrimaryProject(
        project_id=project_id,
        web_url=web_url,
        name=name,
        full_path=full_path,
        mirrors=[PrimaryMirror(id=mid, url=url) for mid, url in mirrors],
    )


@pytest.fixture
def cfg():
    return Config(
        hosts=[
            HostSpec(
                id="self_hosted",
                kind="gitlab",
                url="https://gl.example",
                options={"managed_group_prefix": "team"},
            ),
            HostSpec(id="gitlab", kind="gitlab", url="https://gitlab.com"),
            HostSpec(id="github", kind="github", url="https://api.github.com"),
        ],
        primary="self_hosted",
        forks=["gitlab", "github"],
        defaults=Defaults(private=True, group=""),
    )


@pytest.fixture
def seeded_journal(tmp_path: Path, monkeypatch):
    """Point HYDRA_JOURNAL to a temp DB and pre-populate one repo + two mirrors."""
    db = tmp_path / "journal.db"
    monkeypatch.setenv("HYDRA_JOURNAL", str(db))
    with journal_mod.journal() as j:
        rid = j.record_repo(
            name="probe",
            primary_host_id="self_hosted",
            primary_repo_id=42,
            primary_repo_url="https://gl.example/team/probe.git",
        )
        j.record_mirror(
            repo_id=rid,
            target_host_id="gitlab",
            target_repo_url="https://gitlab.com/team/probe.git",
            push_mirror_id=100,
        )
        j.record_mirror(
            repo_id=rid,
            target_host_id="github",
            target_repo_url="https://github.com/me/probe.git",
            push_mirror_id=101,
        )
    return db


# ──────────────────────────── list ────────────────────────────


class TestListCommand:
    def test_empty_journal(self, cfg):
        runner = CliRunner()
        with patch.object(cli_mod, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["list"])
        assert result.exit_code == 0, result.output
        assert "No tracked repos" in result.output

    def test_lists_seeded_repos(self, cfg, seeded_journal):
        runner = CliRunner()
        with patch.object(cli_mod, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["list"])
        assert result.exit_code == 0, result.output
        assert "probe" in result.output
        assert "gitlab" in result.output
        assert "github" in result.output
        # Never scanned → status shows as "stale"
        assert "stale" in result.output

    def test_host_filter(self, cfg, seeded_journal):
        runner = CliRunner()
        with patch.object(cli_mod, "_load_or_die", return_value=cfg):
            # Real repo has mirrors targeting gitlab + github; filter excludes neither
            result = runner.invoke(cli_mod.app, ["list", "--host", "gitlab"])
        assert result.exit_code == 0, result.output
        assert "probe" in result.output

    def test_host_filter_no_match(self, cfg, seeded_journal):
        runner = CliRunner()
        with patch.object(cli_mod, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["list", "--host", "nowhere"])
        assert result.exit_code == 0, result.output
        assert "No tracked repos" in result.output

    def test_name_filter_glob(self, cfg, seeded_journal):
        runner = CliRunner()
        with patch.object(cli_mod, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["list", "--filter", "probe*"])
        assert result.exit_code == 0, result.output
        assert "probe" in result.output

    def test_json_output(self, cfg, seeded_journal):
        runner = CliRunner()
        with patch.object(cli_mod, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["list", "--json"])
        assert result.exit_code == 0, result.output
        import json as json_mod

        payload = json_mod.loads(result.output)
        assert len(payload) == 1
        assert payload[0]["name"] == "probe"
        assert {m["target_host_id"] for m in payload[0]["mirrors"]} == {"gitlab", "github"}

    def test_refresh_updates_status_from_provider(self, cfg, seeded_journal):
        runner = CliRunner()
        live = [
            MirrorInfo(
                id=100,
                url="https://oauth2:tok@gitlab.com/team/probe.git",
                enabled=True,
                last_update_status="success",
                last_update_at="2026-05-01T00:00:00Z",
                last_error=None,
            ),
            MirrorInfo(
                id=101,
                url="https://x-access-token:tok@github.com/me/probe.git",
                enabled=True,
                last_update_status="failed",
                last_update_at="2026-05-01T00:00:00Z",
                last_error="auth rejected",
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="primary-tok"),
            patch("hydra.providers.gitlab.GitLabProvider.list_mirrors", return_value=live),
        ):
            result = runner.invoke(cli_mod.app, ["list", "--refresh"])
        assert result.exit_code == 0, result.output
        # post-refresh: status flips from stale to success/failed
        assert "success" in result.output
        assert "failed" in result.output

        # Verify journal got written
        with journal_mod.journal() as j:
            r = j.list_repos()[0]
        by_host = {m.target_host_id: m for m in r.mirrors}
        assert by_host["gitlab"].last_status == "success"
        assert by_host["github"].last_status == "failed"
        assert by_host["github"].last_error == "auth rejected"
        assert r.last_scanned_at is not None


# ──────────────────────────── scan ────────────────────────────


class TestScanCommand:
    def test_clean_when_journal_matches_primary(self, cfg, seeded_journal):
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=42,
                web_url="https://gl.example/team/probe.git",
                name="probe",
                full_path="team/probe",
                mirrors=[
                    (100, "https://oauth2:tok@gitlab.com/team/probe.git"),
                    (101, "https://x-access-token:tok@github.com/me/probe.git"),
                ],
            )
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan"])
        assert result.exit_code == 0, result.output
        assert "matches primary" in result.output

    def test_drift_exits_non_zero(self, cfg, seeded_journal):
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=42,
                web_url="https://gl.example/team/probe.git",
                name="probe",
                full_path="team/probe",
                mirrors=[
                    (100, "https://oauth2:tok@gitlab.com/team/probe.git"),
                    # github push id changed: 101 → 200
                    (200, "https://x-access-token:tok@github.com/me/probe.git"),
                ],
            )
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan"])
        assert result.exit_code == 1, result.output
        assert "drift" in result.output.lower()
        # New richer output shows journal vs primary ids
        assert "101" in result.output and "200" in result.output

    def test_unknown_repo_shows_mirror_count(self, cfg, seeded_journal):
        """Every unknown-repo line carries the mirror count."""
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra",
                name="extra",
                full_path="team/extra",
                mirrors=[
                    (500, "https://oauth2:tok@gitlab.com/team/extra.git"),
                    (501, "https://x-access-token:tok@github.com/me/extra.git"),
                ],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan"])
        assert result.exit_code == 1, result.output
        # "2 mirrors" must appear in the unknown-repo line
        assert "2 mirrors" in result.output

    def test_drift_line_shows_primary_vs_journal_counts(self, cfg, seeded_journal):
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=42,
                web_url="https://gl.example/team/probe.git",
                name="probe",
                full_path="team/probe",
                mirrors=[
                    (100, "https://oauth2:tok@gitlab.com/team/probe.git"),
                    (999, "https://x-access-token:tok@github.com/me/probe.git"),  # drifted
                ],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan"])
        assert "2 on primary" in result.output
        assert "2 in journal" in result.output

    def test_interactive_prompt_shows_mirror_count_and_targets(self, cfg, seeded_journal):
        """Interactive mode prints a count + matched-fork list before the prompt."""
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra",
                name="extra",
                full_path="team/extra",
                mirrors=[
                    (500, "https://oauth2:tok@gitlab.com/team/extra.git"),
                    (501, "https://x-access-token:tok@github.com/me/extra.git"),
                ],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan", "--interactive"], input="y\n")
        # Count + breakdown shown adjacent to the prompt
        assert "2 mirrors" in result.output
        # Both target ids visible
        assert "gitlab" in result.output and "github" in result.output

    def test_interactive_prompt_flags_unknown_mirror_targets(self, cfg, seeded_journal):
        """When a mirror doesn't match any configured fork, the summary marks it."""
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra",
                name="extra",
                full_path="team/extra",
                mirrors=[
                    (500, "https://oauth2:tok@gitlab.com/team/extra.git"),
                    (501, "https://example.org/random.git"),
                ],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan", "--interactive"], input="n\n")
        assert "1 unknown" in result.output

    def test_unknown_repo_reported(self, cfg, seeded_journal):
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=42,
                web_url="https://gl.example/team/probe.git",
                name="probe",
                full_path="team/probe",
                mirrors=[
                    (100, "https://oauth2:tok@gitlab.com/team/probe.git"),
                    (101, "https://x-access-token:tok@github.com/me/probe.git"),
                ],
            ),
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra.git",
                name="extra",
                full_path="team/extra",
                mirrors=[(500, "https://oauth2:tok@gitlab.com/team/extra.git")],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan"])
        assert result.exit_code == 1, result.output
        assert "extra" in result.output
        assert "not in journal" in result.output.lower()
        # Mirror line surfaces matched fork id + URL
        assert "gitlab:" in result.output
        assert "gitlab.com/team/extra.git" in result.output

    def test_requires_namespace_or_all(self, seeded_journal):
        """No managed_group_prefix, no defaults.group, no --namespace, no --all → exit 1."""
        runner = CliRunner()
        cfg_no_scope = Config(
            hosts=[
                HostSpec(id="self_hosted", kind="gitlab", url="https://gl.example"),
                HostSpec(id="gitlab", kind="gitlab", url="https://gitlab.com"),
                HostSpec(id="github", kind="github", url="https://api.github.com"),
            ],
            primary="self_hosted",
            forks=["gitlab", "github"],
            defaults=Defaults(private=True, group=""),
        )
        with patch.object(cli_mod, "_load_or_die", return_value=cfg_no_scope):
            result = runner.invoke(cli_mod.app, ["scan"])
        assert result.exit_code == 1, result.output
        assert "--namespace" in result.output

    def test_apply_adopts_unknown_repo_into_journal(self, cfg, seeded_journal):
        """--apply: unknown repo with matchable mirrors is added; rows persist."""
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=42,
                web_url="https://gl.example/team/probe.git",
                name="probe",
                full_path="team/probe",
                mirrors=[
                    (100, "https://oauth2:tok@gitlab.com/team/probe.git"),
                    (101, "https://x-access-token:tok@github.com/me/probe.git"),
                ],
            ),
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra.git",
                name="extra",
                full_path="team/extra",
                mirrors=[
                    (500, "https://oauth2:tok@gitlab.com/team/extra.git"),
                    (501, "https://x-access-token:tok@github.com/me/extra.git"),
                ],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan", "--apply", "--yes"])
        assert result.exit_code == 0, result.output
        assert "adopt" in result.output.lower()
        with journal_mod.journal() as j:
            repos = {r.name: r for r in j.list_repos()}
        assert "extra" in repos
        extra = repos["extra"]
        assert extra.primary_repo_id == 77
        hosts = {m.target_host_id: m.push_mirror_id for m in extra.mirrors}
        assert hosts == {"gitlab": 500, "github": 501}

    def test_apply_resyncs_drifted_push_ids(self, cfg, seeded_journal):
        """--apply: a drifted github push_mirror_id (101 → 999) is updated in-journal."""
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=42,
                web_url="https://gl.example/team/probe.git",
                name="probe",
                full_path="team/probe",
                mirrors=[
                    (100, "https://oauth2:tok@gitlab.com/team/probe.git"),
                    # github changed from 101 → 999 on the primary
                    (999, "https://x-access-token:tok@github.com/me/probe.git"),
                ],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan", "--apply", "--yes"])
        assert result.exit_code == 0, result.output
        with journal_mod.journal() as j:
            mirrors = {m.target_host_id: m for m in j.list_repos()[0].mirrors}
        assert mirrors["github"].push_mirror_id == 999
        assert mirrors["gitlab"].push_mirror_id == 100  # unchanged

    def test_apply_skips_mirrors_with_no_matching_fork(self, cfg, seeded_journal):
        """A mirror whose URL doesn't match any fork host is skipped, not stored."""
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra.git",
                name="extra",
                full_path="team/extra",
                mirrors=[
                    (500, "https://oauth2:tok@gitlab.com/team/extra.git"),
                    (501, "https://example.org/random.git"),  # no matching fork
                ],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan", "--apply", "--yes"])
        assert result.exit_code == 1, result.output  # 'probe' still missing → not clean
        with journal_mod.journal() as j:
            extra = next(r for r in j.list_repos() if r.name == "extra")
        hosts = {m.target_host_id for m in extra.mirrors}
        assert hosts == {"gitlab"}  # only the matched one

    def test_interactive_yes_adopts(self, cfg, seeded_journal):
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra.git",
                name="extra",
                full_path="team/extra",
                mirrors=[(500, "https://oauth2:tok@gitlab.com/team/extra.git")],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            # Two prompts now: per-repo "Adopt 'extra'?" then a final
            # "Apply N action(s)?". Answer y to both.
            result = runner.invoke(cli_mod.app, ["scan", "--interactive"], input="y\ny\n")
        assert result.exit_code in (0, 1), result.output  # 'probe' missing → 1; 'extra' adopted
        with journal_mod.journal() as j:
            names = {r.name for r in j.list_repos()}
        assert "extra" in names

    def test_interactive_no_skips(self, cfg, seeded_journal):
        runner = CliRunner()
        snapshot = [
            _proj(
                project_id=77,
                web_url="https://gl.example/team/extra.git",
                name="extra",
                full_path="team/extra",
                mirrors=[(500, "https://oauth2:tok@gitlab.com/team/extra.git")],
            ),
        ]
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=snapshot,
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan", "--interactive"], input="n\n")
        assert result.exit_code == 1, result.output
        assert "skipped" in result.output.lower()
        with journal_mod.journal() as j:
            names = {r.name for r in j.list_repos()}
        assert "extra" not in names

    def test_all_flag_enables_membership_listing(self, seeded_journal):
        runner = CliRunner()
        cfg_no_scope = Config(
            hosts=[
                HostSpec(id="self_hosted", kind="gitlab", url="https://gl.example"),
                HostSpec(id="gitlab", kind="gitlab", url="https://gitlab.com"),
                HostSpec(id="github", kind="github", url="https://api.github.com"),
            ],
            primary="self_hosted",
            forks=["gitlab", "github"],
            defaults=Defaults(private=True, group=""),
        )
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg_no_scope),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=[],
            ) as lp,
        ):
            result = runner.invoke(cli_mod.app, ["scan", "--all"])
        # Empty primary + 1 journal repo → 'missing' reported
        assert result.exit_code == 1, result.output
        assert lp.call_args.kwargs["namespace"] is None

    def test_missing_repo_reported(self, cfg, seeded_journal):
        runner = CliRunner()
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.list_projects_with_mirrors",
                return_value=[],
            ),
        ):
            result = runner.invoke(cli_mod.app, ["scan"])
        assert result.exit_code == 1, result.output
        assert "probe" in result.output
        assert "no longer on primary" in result.output.lower()


# ──────────────────────────── rotate-token ────────────────────────────


class TestRotateToken:
    def test_unknown_host(self, cfg):
        runner = CliRunner()
        with patch.object(cli_mod, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["rotate-token", "ghost", "--token", "new"])
        assert result.exit_code == 1
        assert "Unknown host" in result.output

    def test_primary_host_short_circuits(self, cfg, seeded_journal):
        """Rotating the primary's PAT should NOT touch any mirror."""
        runner = CliRunner()
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.gitlab.verify_token") as verify,
            patch("hydra.cli.preflight_mod.check_tokens", return_value=_clean_preflight()),
            patch("hydra.secrets.set_token") as set_token,
            patch("hydra.providers.gitlab.GitLabProvider.replace_outbound_mirror") as replace_call,
        ):
            result = runner.invoke(cli_mod.app, ["rotate-token", "self_hosted", "--token", "new"])
        assert result.exit_code == 0, result.output
        verify.assert_called_once()
        set_token.assert_called_once_with("self_hosted", "new")
        replace_call.assert_not_called()
        assert "primary token rotated" in result.output.lower()

    def test_fork_host_replaces_all_matching_mirrors(self, cfg, seeded_journal):
        runner = CliRunner()
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.github.verify_token") as verify,
            patch("hydra.cli.preflight_mod.check_tokens", return_value=_clean_preflight()),
            patch("hydra.secrets.set_token") as set_token,
            patch("hydra.cli.secrets_mod.get_token", return_value="primary-tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.replace_outbound_mirror",
                return_value={"id": 7777},
            ) as replace_call,
        ):
            result = runner.invoke(cli_mod.app, ["rotate-token", "github", "--token", "ghp_new"])
        assert result.exit_code == 0, result.output
        verify.assert_called_once()
        set_token.assert_called_once_with("github", "ghp_new")
        # Only one mirror targets github → exactly one replace call
        assert replace_call.call_count == 1
        call = replace_call.call_args
        assert call.kwargs["old_push_mirror_id"] == 101
        assert call.kwargs["target_token"] == "ghp_new"
        assert call.kwargs["target_username"] == "x-access-token"
        # Journal updated to new push id
        with journal_mod.journal() as j:
            mirrors = {m.target_host_id: m for m in j.list_repos()[0].mirrors}
        assert mirrors["github"].push_mirror_id == 7777

    def test_dry_run_makes_no_changes(self, cfg, seeded_journal):
        runner = CliRunner()
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.github.verify_token") as verify,
            patch("hydra.cli.preflight_mod.check_tokens", return_value=_clean_preflight()),
            patch("hydra.secrets.set_token") as set_token,
            patch("hydra.providers.gitlab.GitLabProvider.replace_outbound_mirror") as replace_call,
        ):
            result = runner.invoke(
                cli_mod.app,
                ["rotate-token", "github", "--token", "x", "--dry-run"],
            )
        assert result.exit_code == 0, result.output
        verify.assert_called_once()
        set_token.assert_not_called()
        replace_call.assert_not_called()
        assert "dry-run" in result.output.lower()

    def test_mirror_replace_error_marks_journal_broken(self, cfg, seeded_journal):
        """When POST fails after DELETE succeeded, the mirror is gone on the
        primary. The journal row must be marked 'broken' so `list` surfaces it.
        """
        from hydra.errors import MirrorReplaceError

        runner = CliRunner()
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.github.verify_token"),
            patch("hydra.cli.preflight_mod.check_tokens", return_value=_clean_preflight()),
            patch("hydra.secrets.set_token"),
            patch("hydra.cli.secrets_mod.get_token", return_value="primary-tok"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.replace_outbound_mirror",
                side_effect=MirrorReplaceError(message="recreate failed", hint="re-add manually"),
            ),
        ):
            result = runner.invoke(cli_mod.app, ["rotate-token", "github", "--token", "x"])
        assert result.exit_code == 1, result.output
        assert "deleted with no replacement" in result.output.lower()
        with journal_mod.journal() as j:
            mirrors = {m.target_host_id: m for m in j.list_repos()[0].mirrors}
        assert mirrors["github"].last_status == "broken"
        assert "recreate failed" in (mirrors["github"].last_error or "")

    def test_skip_verify_skips_probe(self, cfg, seeded_journal):
        runner = CliRunner()
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.github.verify_token") as verify,
            patch("hydra.cli.preflight_mod.check_tokens") as preflight,
            patch("hydra.secrets.set_token"),
            patch("hydra.cli.secrets_mod.get_token", return_value="t"),
            patch(
                "hydra.providers.gitlab.GitLabProvider.replace_outbound_mirror",
                return_value={"id": 1},
            ),
        ):
            result = runner.invoke(
                cli_mod.app,
                ["rotate-token", "github", "--token", "x", "--skip-verify"],
            )
        assert result.exit_code == 0, result.output
        verify.assert_not_called()
        # --skip-verify bypasses the preflight too (same code path).
        preflight.assert_not_called()

    def test_rotate_token_bails_on_scope_mismatch_before_keyring_write(self, cfg, seeded_journal):
        """If preflight's scope check fails, secrets.set_token must NOT be called."""
        from hydra.preflight import PreflightFinding, PreflightReport

        bad = PreflightReport(
            errors=[
                PreflightFinding(
                    host_id="github",
                    message="github — token valid but missing scope(s): repo",
                    hint="mint a new PAT",
                )
            ]
        )
        runner = CliRunner()
        with (
            patch.object(cli_mod, "_load_or_die", return_value=cfg),
            patch("hydra.github.verify_token"),
            patch("hydra.cli.preflight_mod.check_tokens", return_value=bad),
            patch("hydra.secrets.set_token") as set_token,
            patch("hydra.providers.gitlab.GitLabProvider.replace_outbound_mirror") as replace_call,
        ):
            result = runner.invoke(cli_mod.app, ["rotate-token", "github", "--token", "ghp_x"])
        assert result.exit_code == 1, result.output
        set_token.assert_not_called()
        replace_call.assert_not_called()
        assert "missing scope" in result.output.lower()

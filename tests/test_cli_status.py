from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra import journal as journal_mod
from hydra.config import Config, Defaults, HostSpec
from hydra.hostspec_utils import match_fork


@pytest.fixture
def cfg():
    return Config(
        hosts=[
            HostSpec(id="self_hosted_gitlab", kind="gitlab", url="https://gitlab.example.com"),
            HostSpec(id="gitlab", kind="gitlab", url="https://gitlab.com"),
            HostSpec(id="github", kind="github", url="https://api.github.com"),
        ],
        primary="self_hosted_gitlab",
        forks=["gitlab", "github"],
        defaults=Defaults(private=True, group=""),
    )


def _seed(name="alpha", *, mirrors):
    """Seed one journal repo with the given (host_id, status, error) mirrors."""
    with journal_mod.journal() as j:
        repo_id = j.record_repo(
            name=name,
            primary_host_id="self_hosted_gitlab",
            primary_repo_id=10,
            primary_repo_url=f"https://gitlab.example.com/team/{name}",
        )
        for host_id, status, err in mirrors:
            mid = j.record_mirror(
                repo_id=repo_id,
                target_host_id=host_id,
                target_repo_url=f"https://{host_id}.example/team/{name}.git",
                push_mirror_id=500 + len(host_id),
                target_repo_id=None,
            )
            if status is not None or err is not None:
                j.update_mirror_status(
                    mirror_db_id=mid,
                    last_status=status,
                    last_error=err,
                    last_update_at=None,
                )


class TestStatusJournalBacked:
    def test_inline_error_and_unhealthy_exit_1(self, cfg):
        _seed(mirrors=[("gitlab", "success", None), ("github", "broken", "replace failed: gone")])
        runner = CliRunner()
        with patch.object(cli_mod._common, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["status", "alpha"])

        assert result.exit_code == 1, result.output
        out = result.output
        assert "alpha" in out
        assert "gitlab" in out and "success" in out
        assert "github" in out and "broken" in out
        assert "error: replace failed: gone" in out
        assert "journal cache" in out  # footer present (no --refresh)

    def test_all_healthy_exits_0(self, cfg):
        _seed(mirrors=[("gitlab", "success", None), ("github", "success", None)])
        runner = CliRunner()
        with patch.object(cli_mod._common, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["status", "alpha"])
        assert result.exit_code == 0, result.output

    def test_never_refreshed_is_stale_not_unhealthy(self, cfg):
        # last_status None → "stale", exit 0 (not a failure).
        _seed(mirrors=[("gitlab", None, None)])
        runner = CliRunner()
        with patch.object(cli_mod._common, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["status", "alpha"])
        assert result.exit_code == 0, result.output
        assert "stale" in result.output

    def test_not_tracked_exits_1(self, cfg):
        runner = CliRunner()
        with patch.object(cli_mod._common, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["status", "ghost"])
        assert result.exit_code == 1
        assert "not tracked" in result.output.lower()

    def test_ambiguous_name_exits_1(self, cfg):
        # Same name under two different primary (host, repo_id) keys → two rows.
        with journal_mod.journal() as j:
            j.record_repo(
                name="dup",
                primary_host_id="self_hosted_gitlab",
                primary_repo_id=1,
                primary_repo_url="https://gitlab.example.com/a/dup",
            )
            j.record_repo(
                name="dup",
                primary_host_id="gitlab",
                primary_repo_id=2,
                primary_repo_url="https://gitlab.com/b/dup",
            )
        runner = CliRunner()
        with patch.object(cli_mod._common, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["status", "dup"])
        assert result.exit_code == 1, result.output
        assert "ambiguous" in result.output.lower()
        assert "https://gitlab.example.com/a/dup" in result.output
        assert "https://gitlab.com/b/dup" in result.output

    def test_json_shape_matches_list(self, cfg):
        _seed(mirrors=[("github", "broken", "boom")])
        runner = CliRunner()
        with patch.object(cli_mod._common, "_load_or_die", return_value=cfg):
            result = runner.invoke(cli_mod.app, ["status", "alpha", "--json"])
        assert result.exit_code == 1, result.output  # broken mirror → nonzero
        payload = json.loads(result.output)
        assert payload["name"] == "alpha"
        assert payload["mirrors"][0]["target_host_id"] == "github"
        assert payload["mirrors"][0]["last_status"] == "broken"
        assert payload["mirrors"][0]["last_error"] == "boom"


class TestMatchFork:
    def test_exact_host_matches(self):
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com")
        other = HostSpec(id="gh", kind="github", url="https://api.github.com")
        assert (
            match_fork("https://oauth2:tok@gitlab.com/foo/bar.git", [fork, other]).id
            == "cloud"
        )

    def test_different_host_does_not_match(self):
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com")
        # api.github.com vs github.com → distinct hosts.
        assert match_fork("https://oauth2:tok@api.github.com/foo/bar.git", [fork]) is None

    def test_substring_attack_does_not_match(self):
        # Substring match would be unsafe — ensure exact equality.
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com")
        assert (
            match_fork(
                "https://oauth2:tok@evilgitlab.com.attacker.example/foo.git", [fork]
            )
            is None
        )
        assert (
            match_fork("https://oauth2:tok@gitlab.com.evil.example/foo.git", [fork])
            is None
        )

    def test_case_insensitive_host(self):
        fork = HostSpec(id="cloud", kind="gitlab", url="https://GitLab.com")
        assert match_fork("https://oauth2:tok@gitlab.com/foo.git", [fork]).id == "cloud"

    def test_port_independent(self):
        # Same host, different ports — current behavior matches by hostname only.
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com:443")
        assert (
            match_fork("https://oauth2:tok@gitlab.com:8443/foo.git", [fork]).id == "cloud"
        )

    def test_github_api_url_matches_git_url(self):
        """api.github.com (config) ↔ github.com (mirror push URL) — special case
        for the github kind so scan / status can recognise the fork."""
        fork = HostSpec(id="gh", kind="github", url="https://api.github.com")
        assert (
            match_fork("https://x-access-token:tok@github.com/me/probe.git", [fork]).id
            == "gh"
        )

    def test_github_enterprise_uses_configured_hostname(self):
        """Self-hosted GHE: api URL and git URL share a hostname, no special case."""
        fork = HostSpec(id="ghe", kind="github", url="https://github.acme.internal")
        assert (
            match_fork(
                "https://x-access-token:tok@github.acme.internal/team/r.git", [fork]
            ).id
            == "ghe"
        )
        # And api.github.com is not magically accepted for an enterprise host:
        assert match_fork("https://x-access-token:tok@github.com/me/r.git", [fork]) is None


class TestParseHostOptions:
    def test_simple(self):
        out = cli_mod._parse_host_options(["github.org=acme"])
        assert out == {"github": {"org": "acme"}}

    def test_multiple_yaml_parsed(self):
        # YAML parsing means "1" becomes int 1, "true" becomes True, etc.
        out = cli_mod._parse_host_options(["a.x=1", "b.y=hello", "a.z=true"])
        assert out == {"a": {"x": 1, "z": True}, "b": {"y": "hello"}}

    def test_value_can_contain_equals(self):
        # YAML doesn't parse "abc=def" specially; string fallthrough.
        out = cli_mod._parse_host_options(["gh.token=abc=def"])
        assert out["gh"]["token"] == "abc=def"

    def test_null_value(self):
        out = cli_mod._parse_host_options(["gh.org=null"])
        assert out["gh"]["org"] is None

    def test_missing_dot_raises(self):
        import typer

        with pytest.raises(typer.BadParameter):
            cli_mod._parse_host_options(["nodot=value"])

    def test_missing_equals_raises(self):
        import typer

        with pytest.raises(typer.BadParameter):
            cli_mod._parse_host_options(["a.b"])

    def test_empty_id_raises(self):
        import typer

        with pytest.raises(typer.BadParameter):
            cli_mod._parse_host_options([".key=val"])

    def test_empty_key_raises(self):
        import typer

        with pytest.raises(typer.BadParameter):
            cli_mod._parse_host_options(["host.=val"])

    def test_repeated_same_key_last_wins(self):
        out = cli_mod._parse_host_options(["a.x=1", "a.x=2"])
        assert out == {"a": {"x": 2}}

    def test_invalid_format_raises(self):
        import typer

        with pytest.raises(typer.BadParameter):
            cli_mod._parse_host_options(["nodot"])


class TestApplyOverrides:
    def test_overrides_merge(self, cfg):
        new = cli_mod._apply_overrides(cfg, {"github": {"org": "acme"}})
        assert new.host("github").options["org"] == "acme"
        # Original untouched
        assert cfg.host("github").options.get("org") is None

    def test_unknown_host_id_raises(self, cfg):
        import typer

        with pytest.raises(typer.BadParameter):
            cli_mod._apply_overrides(cfg, {"ghost": {"x": "y"}})

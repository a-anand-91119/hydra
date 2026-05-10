from __future__ import annotations

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra.config import Config, Defaults, HostSpec
from hydra.providers.base import MirrorInfo, RepoRef


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


def test_status_reconciles_mirror_urls_with_fork_ids(cfg):
    runner = CliRunner()
    mirrors = [
        MirrorInfo(
            url="https://oauth2:xxx@gitlab.com/managed/foo/probe.git",
            enabled=True,
            last_update_status="success",
            last_update_at="2025-01-01T00:00:00Z",
            last_error=None,
        ),
        MirrorInfo(
            url="https://oauth2:xxx@github.com/me/probe.git",
            enabled=True,
            last_update_status="success",
            last_update_at=None,
            last_error=None,
        ),
        MirrorInfo(
            url="https://example.org/random.git",
            enabled=False,
            last_update_status=None,
            last_update_at=None,
            last_error=None,
        ),
    ]

    with (
        patch.object(cli_mod, "_load_or_die", return_value=cfg),
        patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
        patch(
            "hydra.providers.gitlab.GitLabProvider.find_project",
            return_value=RepoRef(http_url="", project_id=42),
        ),
        patch("hydra.providers.gitlab.GitLabProvider.list_mirrors", return_value=mirrors),
    ):
        result = runner.invoke(cli_mod.app, ["status", "probe"])

    assert result.exit_code == 0, result.output
    # Note: github URL is api.github.com in config but mirror points to github.com,
    # so the substring match doesn't catch it — that's expected. gitlab.com matches.
    assert "gitlab" in result.output  # the fork id label
    assert "(unconfigured)" in result.output  # third mirror has no matching fork


def test_status_scrubs_credentials_from_mirror_url(cfg):
    """Regression: GitLab API echoes the URL we sent — including the token —
    in /remote_mirrors responses. status must NOT print the raw token.
    """
    runner = CliRunner()
    leaky = [
        MirrorInfo(
            url="https://oauth2:glpat-SECRET-TOKEN-DO-NOT-LEAK@gitlab.com/foo/bar.git",
            enabled=True,
            last_update_status="success",
            last_update_at=None,
            last_error=None,
        ),
    ]
    with (
        patch.object(cli_mod, "_load_or_die", return_value=cfg),
        patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
        patch(
            "hydra.providers.gitlab.GitLabProvider.find_project",
            return_value=RepoRef(http_url="", project_id=42),
        ),
        patch("hydra.providers.gitlab.GitLabProvider.list_mirrors", return_value=leaky),
    ):
        result = runner.invoke(cli_mod.app, ["status", "probe"])

    assert result.exit_code == 0, result.output
    assert "glpat-SECRET-TOKEN-DO-NOT-LEAK" not in result.output
    assert "oauth2:" not in result.output
    # Hostname is still shown so users can identify the fork.
    assert "gitlab.com" in result.output


def test_status_blocks_when_primary_lacks_status_capability(cfg):
    runner = CliRunner()
    # Swap in a fake provider kind without status support.
    from hydra import providers as providers_mod
    from hydra.providers.base import Capabilities

    fake_caps = Capabilities(
        supports_mirror_source=True,
        supports_group_paths=False,
        supports_status_lookup=False,
        inbound_mirror_username="oauth2",
    )
    with (
        patch.object(providers_mod, "capabilities_for", return_value=fake_caps),
        patch.object(cli_mod, "_load_or_die", return_value=cfg),
    ):
        result = runner.invoke(cli_mod.app, ["status", "probe"])
    assert result.exit_code == 1
    assert "not supported" in result.output.lower()


def test_status_unknown_repo_exits_1(cfg):
    runner = CliRunner()
    with (
        patch.object(cli_mod, "_load_or_die", return_value=cfg),
        patch("hydra.cli.secrets_mod.get_token", return_value="tok"),
        patch("hydra.providers.gitlab.GitLabProvider.find_project", return_value=None),
    ):
        result = runner.invoke(cli_mod.app, ["status", "missing"])

    assert result.exit_code == 1
    assert "not found" in result.output.lower()


class TestMatchFork:
    def test_exact_host_matches(self):
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com")
        other = HostSpec(id="gh", kind="github", url="https://api.github.com")
        assert (
            cli_mod._match_fork("https://oauth2:tok@gitlab.com/foo/bar.git", [fork, other]).id
            == "cloud"
        )

    def test_different_host_does_not_match(self):
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com")
        # api.github.com vs github.com → distinct hosts.
        assert cli_mod._match_fork("https://oauth2:tok@api.github.com/foo/bar.git", [fork]) is None

    def test_substring_attack_does_not_match(self):
        # Substring match would be unsafe — ensure exact equality.
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com")
        assert (
            cli_mod._match_fork(
                "https://oauth2:tok@evilgitlab.com.attacker.example/foo.git", [fork]
            )
            is None
        )
        assert (
            cli_mod._match_fork("https://oauth2:tok@gitlab.com.evil.example/foo.git", [fork])
            is None
        )

    def test_case_insensitive_host(self):
        fork = HostSpec(id="cloud", kind="gitlab", url="https://GitLab.com")
        assert cli_mod._match_fork("https://oauth2:tok@gitlab.com/foo.git", [fork]).id == "cloud"

    def test_port_independent(self):
        # Same host, different ports — current behavior matches by hostname only.
        fork = HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com:443")
        assert (
            cli_mod._match_fork("https://oauth2:tok@gitlab.com:8443/foo.git", [fork]).id == "cloud"
        )


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

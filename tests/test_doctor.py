from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from rich.console import Console
from typer.testing import CliRunner

from hydra import cli as cli_mod
from hydra.doctor import EXIT_ISSUES, EXIT_OK, run_doctor
from hydra.doctor.findings import Level
from hydra.doctor.fixes import all_handlers, get_handler

# ──────────────────────────── Fixtures ────────────────────────────


@pytest.fixture
def legacy_path(tmp_path: Path) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(
        yaml.safe_dump(
            {
                "self_hosted_gitlab": {"url": "https://gl.x"},
                "gitlab": {"url": "https://gitlab.com"},
                "github": {"url": "https://api.github.com"},
            }
        )
    )
    return p


@pytest.fixture
def v2_path(tmp_path: Path) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(
        yaml.safe_dump(
            {
                "schema_version": 2,
                "hosts": [
                    {"id": "self_hosted_gitlab", "kind": "gitlab", "url": "https://gl.x"},
                    {"id": "gitlab", "kind": "gitlab", "url": "https://gitlab.com"},
                    {"id": "github", "kind": "github", "url": "https://api.github.com"},
                ],
                "primary": "self_hosted_gitlab",
                "forks": ["gitlab", "github"],
                "defaults": {"private": True, "group": ""},
            }
        )
    )
    return p


@pytest.fixture
def quiet_console():
    # Capture output but discard for tests that care about Report only.
    return Console(file=open("/dev/null", "w"), force_terminal=False)


# ──────────────────────────── Individual checks ────────────────────────────


class TestCheckSchemaVersion:
    def test_v2_clean(self, v2_path, quiet_console):
        result = run_doctor(config_path=v2_path, console=quiet_console)
        config_findings = [f for f in result.report.findings if f.section == "Config"]
        assert config_findings[0].level is Level.OK
        assert "schema_version: 2" in config_findings[0].message

    def test_legacy_reports_pending(self, legacy_path, quiet_console):
        result = run_doctor(config_path=legacy_path, console=quiet_console)
        config = [f for f in result.report.findings if f.section == "Config"]
        warnings = [f for f in config if f.level is Level.WARN]
        assert warnings, "expected a pending-migration warning"
        assert "m001-legacy-to-v2" in warnings[0].message
        assert warnings[0].fix_id == "run-migrations"


class TestCheckPrimaryCapable:
    def test_clean_for_v2(self, v2_path, quiet_console):
        result = run_doctor(config_path=v2_path, console=quiet_console)
        host_oks = [
            f for f in result.report.findings if f.section == "Hosts" and f.level is Level.OK
        ]
        assert any("primary:" in f.message for f in host_oks)


class TestCheckLegacyEnvVars:
    def test_warns_when_legacy_var_set(self, v2_path, quiet_console, monkeypatch):
        monkeypatch.setenv("HYDRA_GITHUB_TOKEN", "legacy-tok")
        result = run_doctor(config_path=v2_path, console=quiet_console)
        legacy = [
            f for f in result.report.findings
            if "HYDRA_GITHUB_TOKEN" in f.message and f.level is Level.WARN
        ]
        assert legacy, "expected a legacy-env-var warning"

    def test_silent_when_only_modern(self, v2_path, quiet_console, monkeypatch):
        monkeypatch.delenv("HYDRA_GITHUB_TOKEN", raising=False)
        monkeypatch.setenv("HYDRA_TOKEN_GITHUB", "tok")
        result = run_doctor(config_path=v2_path, console=quiet_console)
        legacy = [f for f in result.report.findings if "HYDRA_GITHUB_TOKEN" in f.message]
        assert legacy == []


class TestCheckTokenResolvable:
    def test_env_token_reported_ok(self, v2_path, quiet_console, monkeypatch):
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB"):
            monkeypatch.setenv(name, "tok")
        result = run_doctor(config_path=v2_path, console=quiet_console)
        token_findings = [f for f in result.report.findings if f.section == "Tokens"]
        assert all(f.level is Level.OK for f in token_findings if "—" in f.message)

    def test_missing_token_warns(self, v2_path, quiet_console, monkeypatch):
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB",
                     "HYDRA_GITHUB_TOKEN", "HYDRA_GITLAB_TOKEN", "HYDRA_SELF_HOSTED_GITLAB_TOKEN"):
            monkeypatch.delenv(name, raising=False)
        result = run_doctor(config_path=v2_path, console=quiet_console)
        token_warnings = [
            f for f in result.report.findings
            if f.section == "Tokens" and f.level is Level.WARN
        ]
        assert len(token_warnings) >= 3  # one per host

    def test_keyring_disabled_by_default(self, v2_path, quiet_console, monkeypatch):
        # If check_keyring stays False, doctor must not call into keyring at
        # all (would block on macOS Keychain otherwise).
        called = []
        monkeypatch.setattr(
            "keyring.get_password", lambda *a, **kw: called.append(a) or "should-not-be-called"
        )
        run_doctor(config_path=v2_path, console=quiet_console)
        assert called == []


# ──────────────────────────── --fix flow ────────────────────────────


class TestFixFlow:
    def test_legacy_to_clean_via_fix(self, legacy_path, quiet_console, monkeypatch):
        # Provide tokens so the post-fix run is fully clean.
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB"):
            monkeypatch.setenv(name, "tok")

        result = run_doctor(config_path=legacy_path, fix=True, console=quiet_console)

        # Migration was applied
        applied_ids = [o.fix_id for o in result.fixes_applied]
        assert "run-migrations" in applied_ids
        # File rewritten in v2 shape
        rewritten = yaml.safe_load(legacy_path.read_text())
        assert rewritten.get("schema_version") == 2
        assert "self_hosted_gitlab" not in rewritten  # legacy top-level keys gone
        # Backup file exists alongside
        siblings = list(legacy_path.parent.glob("config.yaml.bak-*"))
        assert siblings, "expected a backup file"
        # Final report has no remaining pending-migration warning
        config_warnings = [
            f for f in result.report.findings
            if f.section == "Config" and f.level is Level.WARN
        ]
        assert config_warnings == []

    def test_fix_is_idempotent_after_clean(self, v2_path, quiet_console, monkeypatch):
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB"):
            monkeypatch.setenv(name, "tok")
        result = run_doctor(config_path=v2_path, fix=True, console=quiet_console)
        # No fixes attempted because nothing was fixable.
        assert result.fixes_applied == []
        assert result.exit_code == EXIT_OK


# ──────────────────────────── Exit codes via CLI ────────────────────────────


class TestCliExitCodes:
    def test_clean_v2_with_tokens_exits_0(self, v2_path, monkeypatch):
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB"):
            monkeypatch.setenv(name, "tok")
        runner = CliRunner()
        result = runner.invoke(cli_mod.app, ["doctor", "--config", str(v2_path)])
        assert result.exit_code == EXIT_OK, result.output

    def test_pending_migration_without_fix_exits_1(self, legacy_path, monkeypatch):
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB"):
            monkeypatch.setenv(name, "tok")
        runner = CliRunner()
        result = runner.invoke(cli_mod.app, ["doctor", "--config", str(legacy_path)])
        assert result.exit_code == EXIT_ISSUES

    def test_pending_migration_with_fix_exits_0(self, legacy_path, monkeypatch):
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB"):
            monkeypatch.setenv(name, "tok")
        runner = CliRunner()
        result = runner.invoke(
            cli_mod.app, ["doctor", "--fix", "--config", str(legacy_path)]
        )
        assert result.exit_code == EXIT_OK, result.output


# ──────────────────────────── Fix handler registry ────────────────────────────


class TestFixHandlers:
    def test_run_migrations_handler_registered(self):
        h = get_handler("run-migrations")
        assert h.fix_id == "run-migrations"

    def test_unknown_handler_raises(self):
        with pytest.raises(KeyError):
            get_handler("does-not-exist")

    def test_all_handlers_have_unique_ids(self):
        ids = [h.fix_id for h in all_handlers()]
        assert len(ids) == len(set(ids))

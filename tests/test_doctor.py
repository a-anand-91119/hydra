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
            f
            for f in result.report.findings
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
        for name in (
            "HYDRA_TOKEN_SELF_HOSTED_GITLAB",
            "HYDRA_TOKEN_GITLAB",
            "HYDRA_TOKEN_GITHUB",
            "HYDRA_GITHUB_TOKEN",
            "HYDRA_GITLAB_TOKEN",
            "HYDRA_SELF_HOSTED_GITLAB_TOKEN",
        ):
            monkeypatch.delenv(name, raising=False)
        result = run_doctor(config_path=v2_path, console=quiet_console)
        token_warnings = [
            f for f in result.report.findings if f.section == "Tokens" and f.level is Level.WARN
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


class TestTokenSourceAttribution:
    def test_shell_env_reported_distinctly(self, v2_path, quiet_console, monkeypatch):
        monkeypatch.setenv("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "tok")
        result = run_doctor(config_path=v2_path, console=quiet_console)
        msgs = [
            f.message
            for f in result.report.findings
            if f.section == "Tokens" and "self_hosted_gitlab" in f.message
        ]
        assert any("shell env" in m for m in msgs), msgs

    def test_dotenv_reported_distinctly(self, v2_path, quiet_console, monkeypatch, tmp_path):
        # The autouse conftest chdirs to a clean dir; write a .env there.
        (Path.cwd() / ".env").write_text("HYDRA_TOKEN_GITLAB=fromdotenv\n")
        monkeypatch.delenv("HYDRA_TOKEN_GITLAB", raising=False)
        result = run_doctor(config_path=v2_path, console=quiet_console)
        msgs = [
            f.message
            for f in result.report.findings
            if f.section == "Tokens" and "gitlab" in f.message and "self_hosted" not in f.message
        ]
        assert any(".env" in m for m in msgs), msgs

    def test_keyring_reported_distinctly(self, v2_path, quiet_console, monkeypatch):
        for name in (
            "HYDRA_TOKEN_GITHUB",
            "HYDRA_GITHUB_TOKEN",
        ):
            monkeypatch.delenv(name, raising=False)
        # Stub keyring directly via the state's keyring_get is harder; easier
        # to use --check-keyring and patch keyring.get_password.
        monkeypatch.setattr(
            "hydra.doctor.checks._safe_keyring_get",
            lambda host_id: "from-keyring" if host_id == "github" else None,
        )
        result = run_doctor(config_path=v2_path, console=quiet_console, check_keyring=True)
        github_msgs = [
            f.message
            for f in result.report.findings
            if f.section == "Tokens" and "github" in f.message and "GITHUB" not in f.message
        ]
        assert any("keyring" in m.lower() for m in github_msgs), github_msgs

    def test_shadowing_warns(self, v2_path, quiet_console, monkeypatch):
        """Shell env with a DIFFERENT value than .env for the same key → WARN."""
        (Path.cwd() / ".env").write_text("HYDRA_TOKEN_GITLAB=fromdotenv\n")
        monkeypatch.setenv("HYDRA_TOKEN_GITLAB", "fromshell")
        result = run_doctor(config_path=v2_path, console=quiet_console)
        shadow_warnings = [
            f
            for f in result.report.findings
            if f.section == "Tokens" and f.level is Level.WARN and "shadowing" in f.message.lower()
        ]
        assert len(shadow_warnings) == 1, [
            f.message for f in result.report.findings if f.section == "Tokens"
        ]

    def test_no_shadow_when_values_match(self, v2_path, quiet_console, monkeypatch):
        """Same value in shell and .env → no warning."""
        (Path.cwd() / ".env").write_text("HYDRA_TOKEN_GITLAB=same\n")
        monkeypatch.setenv("HYDRA_TOKEN_GITLAB", "same")
        result = run_doctor(config_path=v2_path, console=quiet_console)
        shadow = [
            f
            for f in result.report.findings
            if f.section == "Tokens" and "shadowing" in f.message.lower()
        ]
        assert shadow == []

    def test_dotenv_presence_finding_when_present(self, v2_path, quiet_console):
        (Path.cwd() / ".env").write_text("HYDRA_TOKEN_GITLAB=x\nUNRELATED=y\n")
        result = run_doctor(config_path=v2_path, console=quiet_console)
        presence = [
            f for f in result.report.findings if f.section == "Tokens" and ".env at" in f.message
        ]
        assert len(presence) == 1
        # The count is HYDRA_* keys only
        assert "1 HYDRA_* key" in presence[0].message

    def test_dotenv_presence_finding_when_absent(self, v2_path, quiet_console):
        # Conftest leaves cwd empty by default
        result = run_doctor(config_path=v2_path, console=quiet_console)
        absent = [
            f
            for f in result.report.findings
            if f.section == "Tokens" and "no .env in cwd" in f.message
        ]
        assert len(absent) == 1


class TestCheckTokenPermissions:
    """`--check-tokens` makes one network call per host; tests stub the probes."""

    def _set_all_env(self, monkeypatch):
        for name in ("HYDRA_TOKEN_SELF_HOSTED_GITLAB", "HYDRA_TOKEN_GITLAB", "HYDRA_TOKEN_GITHUB"):
            monkeypatch.setenv(name, "tok")

    def test_disabled_by_default(self, v2_path, quiet_console, monkeypatch):
        """No network probes happen unless --check-tokens is passed."""
        self._set_all_env(monkeypatch)
        called: list = []
        monkeypatch.setattr(
            "hydra.gitlab.inspect_token",
            lambda **kw: called.append(("gl", kw)) or None,
        )
        monkeypatch.setattr(
            "hydra.github.inspect_token",
            lambda **kw: called.append(("gh", kw)) or None,
        )
        run_doctor(config_path=v2_path, console=quiet_console)
        assert called == []

    def test_valid_token_with_required_scope_is_ok(self, v2_path, quiet_console, monkeypatch):
        from hydra.secrets import TokenScopes

        self._set_all_env(monkeypatch)
        monkeypatch.setattr(
            "hydra.gitlab.inspect_token",
            lambda **kw: TokenScopes(scopes=["api", "read_repository"], expires_at=None),
        )
        monkeypatch.setattr(
            "hydra.github.inspect_token",
            lambda **kw: TokenScopes(scopes=["repo", "user"], expires_at=None),
        )
        result = run_doctor(config_path=v2_path, console=quiet_console, check_tokens=True)
        ok = [
            f.message
            for f in result.report.findings
            if f.section == "Tokens" and "valid (scopes:" in f.message
        ]
        assert len(ok) == 3, ok

    def test_missing_scope_warns(self, v2_path, quiet_console, monkeypatch):
        from hydra.secrets import TokenScopes

        self._set_all_env(monkeypatch)
        monkeypatch.setattr(
            "hydra.gitlab.inspect_token",
            lambda **kw: TokenScopes(scopes=["read_repository"], expires_at=None),
        )
        monkeypatch.setattr(
            "hydra.github.inspect_token",
            lambda **kw: TokenScopes(scopes=["repo"], expires_at=None),
        )
        result = run_doctor(config_path=v2_path, console=quiet_console, check_tokens=True)
        warns = [
            f
            for f in result.report.findings
            if f.section == "Tokens" and f.level is Level.WARN and "missing scope" in f.message
        ]
        assert len(warns) == 2  # two GitLab hosts missing 'api'
        assert all("api" in w.message for w in warns)

    def test_rejected_token_errors(self, v2_path, quiet_console, monkeypatch):
        from hydra.errors import HydraAPIError

        self._set_all_env(monkeypatch)

        def _reject(**kw):
            raise HydraAPIError(message="auth failed", status_code=401, hint="rotate")

        monkeypatch.setattr("hydra.gitlab.inspect_token", _reject)
        monkeypatch.setattr("hydra.github.inspect_token", _reject)
        result = run_doctor(config_path=v2_path, console=quiet_console, check_tokens=True)
        errors = [
            f
            for f in result.report.findings
            if f.section == "Tokens" and f.level is Level.ERROR and "rejected" in f.message
        ]
        assert len(errors) == 3  # one per host

    def test_unknown_scopes_treated_as_ok(self, v2_path, quiet_console, monkeypatch):
        """Fine-grained GitHub PAT / older GitLab — token valid but scopes
        aren't introspectable; doctor should not penalise."""
        from hydra.secrets import TokenScopes

        self._set_all_env(monkeypatch)
        monkeypatch.setattr(
            "hydra.gitlab.inspect_token",
            lambda **kw: TokenScopes(scopes=[], expires_at=None, scopes_known=False),
        )
        monkeypatch.setattr(
            "hydra.github.inspect_token",
            lambda **kw: TokenScopes(scopes=[], expires_at=None, scopes_known=False),
        )
        result = run_doctor(config_path=v2_path, console=quiet_console, check_tokens=True)
        non_ok = [
            f for f in result.report.findings if f.section == "Tokens" and f.level is not Level.OK
        ]
        # The probe yields OK for each; the only WARN would be missing scope,
        # which we explicitly suppress when scopes aren't known.
        assert not any("missing scope" in f.message for f in non_ok)

    def test_skipped_when_no_token_resolvable(self, v2_path, quiet_console, monkeypatch):
        """If a host has no token, the permissions check skips it silently —
        the missing-token warning from check_token_resolvable already covers it."""
        from hydra.secrets import TokenScopes

        # Only set env for github; gitlab hosts have no resolvable token.
        monkeypatch.delenv("HYDRA_TOKEN_SELF_HOSTED_GITLAB", raising=False)
        monkeypatch.delenv("HYDRA_TOKEN_GITLAB", raising=False)
        monkeypatch.delenv("HYDRA_GITLAB_TOKEN", raising=False)
        monkeypatch.delenv("HYDRA_SELF_HOSTED_GITLAB_TOKEN", raising=False)
        monkeypatch.setenv("HYDRA_TOKEN_GITHUB", "tok")
        probed: list = []
        monkeypatch.setattr(
            "hydra.gitlab.inspect_token",
            lambda **kw: probed.append("gl") or TokenScopes(scopes=["api"]),
        )
        monkeypatch.setattr(
            "hydra.github.inspect_token",
            lambda **kw: probed.append("gh") or TokenScopes(scopes=["repo"]),
        )
        run_doctor(config_path=v2_path, console=quiet_console, check_tokens=True)
        # GitLab probes skipped (no token); GitHub probed.
        assert probed == ["gh"]

    def test_github_org_requires_admin_org(self, v2_path, quiet_console, monkeypatch, tmp_path):
        """When github host has options.org set, doctor must also require an
        org-management scope; warn if absent."""
        from hydra.secrets import TokenScopes

        # Override v2_path to set options.org on github
        cfg_path = tmp_path / "with-org.yaml"
        cfg_path.write_text(
            yaml.safe_dump(
                {
                    "schema_version": 2,
                    "hosts": [
                        {"id": "self_hosted_gitlab", "kind": "gitlab", "url": "https://gl.x"},
                        {"id": "gitlab", "kind": "gitlab", "url": "https://gitlab.com"},
                        {
                            "id": "github",
                            "kind": "github",
                            "url": "https://api.github.com",
                            "options": {"org": "acme"},
                        },
                    ],
                    "primary": "self_hosted_gitlab",
                    "forks": ["gitlab", "github"],
                    "defaults": {"private": True, "group": ""},
                }
            )
        )
        self._set_all_env(monkeypatch)
        monkeypatch.setattr(
            "hydra.gitlab.inspect_token",
            lambda **kw: TokenScopes(scopes=["api"]),
        )
        # GitHub has 'repo' but no admin:org → should warn.
        monkeypatch.setattr(
            "hydra.github.inspect_token",
            lambda **kw: TokenScopes(scopes=["repo"]),
        )
        result = run_doctor(config_path=cfg_path, console=quiet_console, check_tokens=True)
        gh_warns = [
            f
            for f in result.report.findings
            if f.section == "Tokens"
            and f.level is Level.WARN
            and "github" in f.message
            and "missing scope" in f.message
        ]
        assert len(gh_warns) == 1
        assert "admin:org" in gh_warns[0].message

    def test_github_org_accepts_write_org(self, v2_path, quiet_console, monkeypatch, tmp_path):
        """write:org is an acceptable substitute for admin:org."""
        from hydra.secrets import TokenScopes

        cfg_path = tmp_path / "with-org.yaml"
        cfg_path.write_text(
            yaml.safe_dump(
                {
                    "schema_version": 2,
                    "hosts": [
                        {"id": "self_hosted_gitlab", "kind": "gitlab", "url": "https://gl.x"},
                        {"id": "gitlab", "kind": "gitlab", "url": "https://gitlab.com"},
                        {
                            "id": "github",
                            "kind": "github",
                            "url": "https://api.github.com",
                            "options": {"org": "acme"},
                        },
                    ],
                    "primary": "self_hosted_gitlab",
                    "forks": ["gitlab", "github"],
                    "defaults": {"private": True, "group": ""},
                }
            )
        )
        self._set_all_env(monkeypatch)
        monkeypatch.setattr(
            "hydra.gitlab.inspect_token",
            lambda **kw: TokenScopes(scopes=["api"]),
        )
        monkeypatch.setattr(
            "hydra.github.inspect_token",
            lambda **kw: TokenScopes(scopes=["repo", "write:org"]),
        )
        result = run_doctor(config_path=cfg_path, console=quiet_console, check_tokens=True)
        gh = [
            f
            for f in result.report.findings
            if f.section == "Tokens" and "github" in f.message and "valid" in f.message
        ]
        assert len(gh) == 1
        assert "missing scope" not in gh[0].message


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
            f for f in result.report.findings if f.section == "Config" and f.level is Level.WARN
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
        result = runner.invoke(cli_mod.app, ["doctor", "--fix", "--config", str(legacy_path)])
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

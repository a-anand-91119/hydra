from __future__ import annotations

import pytest

from hydra import secrets as secrets_mod


@pytest.fixture(autouse=True)
def _reset_dotenv(monkeypatch):
    monkeypatch.setattr(secrets_mod, "_dotenv_loaded", True)


class TestEnvVarFor:
    def test_simple_id(self):
        assert secrets_mod.env_var_for("internal") == "HYDRA_TOKEN_INTERNAL"

    def test_dashes_to_underscores(self):
        assert secrets_mod.env_var_for("my-host") == "HYDRA_TOKEN_MY_HOST"

    def test_unicode_replaced(self):
        assert secrets_mod.env_var_for("ho.st") == "HYDRA_TOKEN_HO_ST"


class TestGetToken:
    def test_modern_env_var_wins(self, monkeypatch):
        monkeypatch.setattr("keyring.get_password", lambda *a, **kw: None)
        monkeypatch.setenv("HYDRA_TOKEN_INTERNAL", "modern-tok")
        assert secrets_mod.get_token("internal", allow_prompt=False) == "modern-tok"

    def test_legacy_fallback_for_known_id(self, monkeypatch):
        monkeypatch.setattr("keyring.get_password", lambda *a, **kw: None)
        monkeypatch.delenv("HYDRA_TOKEN_GITHUB", raising=False)
        monkeypatch.setenv("HYDRA_GITHUB_TOKEN", "legacy-gh")
        assert secrets_mod.get_token("github", allow_prompt=False) == "legacy-gh"

    def test_no_legacy_fallback_for_arbitrary_id(self, monkeypatch):
        monkeypatch.setattr("keyring.get_password", lambda *a, **kw: None)
        # Should not look for HYDRA_INTERNAL_TOKEN style — only modern.
        monkeypatch.delenv("HYDRA_TOKEN_RANDOM", raising=False)
        with pytest.raises(secrets_mod.SecretError):
            secrets_mod.get_token("random", allow_prompt=False)

    def test_env_wins_over_keyring(self, monkeypatch):
        # Env-first lets users override stale keyring entries (CI use case).
        monkeypatch.setattr("keyring.get_password", lambda *a, **kw: "keyring-tok")
        monkeypatch.setenv("HYDRA_TOKEN_INTERNAL", "env-tok")
        assert secrets_mod.get_token("internal", allow_prompt=False) == "env-tok"

    def test_keyring_used_when_no_env(self, monkeypatch):
        monkeypatch.delenv("HYDRA_TOKEN_INTERNAL", raising=False)
        monkeypatch.setattr("keyring.get_password", lambda *a, **kw: "keyring-tok")
        assert secrets_mod.get_token("internal", allow_prompt=False) == "keyring-tok"

    def test_arbitrary_id_passes_validation(self, monkeypatch):
        monkeypatch.setattr("keyring.get_password", lambda *a, **kw: None)
        monkeypatch.setenv("HYDRA_TOKEN_CODEFORGE", "tok")
        # Plain `codeforge` (a future provider id) is accepted.
        assert secrets_mod.get_token("codeforge", allow_prompt=False) == "tok"

    def test_empty_id_rejected(self):
        with pytest.raises(secrets_mod.SecretError):
            secrets_mod.get_token("", allow_prompt=False)


class TestExportLines:
    def test_emits_modern_form_for_arbitrary_ids(self):
        out = secrets_mod.export_lines({"internal": "a", "codeforge": "b"})
        assert "export HYDRA_TOKEN_INTERNAL=a" in out
        assert "export HYDRA_TOKEN_CODEFORGE=b" in out

    def test_skips_empty_id(self):
        assert secrets_mod.export_lines({"": "x"}) == ""

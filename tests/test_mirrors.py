from __future__ import annotations

from urllib.parse import urlparse

import pytest

from hydra.mirrors import _inject_credentials


class TestInjectCredentials:
    def test_simple_https(self):
        out = _inject_credentials("https://github.com/user/repo.git", "oauth2", "ghp_abcdef")
        assert out == "https://oauth2:ghp_abcdef@github.com/user/repo.git"

    def test_preserves_path(self):
        out = _inject_credentials("https://gitlab.com/group/sub/repo.git", "oauth2", "tok")
        parsed = urlparse(out)
        assert parsed.path == "/group/sub/repo.git"

    def test_preserves_explicit_port(self):
        out = _inject_credentials("https://gitlab.example.com:8443/foo/bar.git", "oauth2", "tok")
        parsed = urlparse(out)
        assert parsed.port == 8443

    def test_quotes_at_sign_in_token(self):
        # An '@' in the token would otherwise produce two '@' chars and break
        # the URL parser into thinking the token is the host.
        out = _inject_credentials("https://github.com/foo/bar.git", "oauth2", "ab@cd")
        # Hostname must remain github.com; token gets percent-encoded.
        parsed = urlparse(out)
        assert parsed.hostname == "github.com"
        assert "ab%40cd" in out
        assert "@github.com" in out  # exactly one @ separator

    def test_quotes_colon_in_token(self):
        # Colons would be parsed as user:pass:extra otherwise.
        out = _inject_credentials("https://github.com/foo/bar.git", "oauth2", "ab:cd")
        parsed = urlparse(out)
        assert parsed.hostname == "github.com"
        assert "ab%3Acd" in out

    def test_quotes_slash_in_token(self):
        out = _inject_credentials("https://github.com/foo/bar.git", "oauth2", "ab/cd")
        parsed = urlparse(out)
        assert parsed.hostname == "github.com"
        assert "ab%2Fcd" in out

    def test_alphanumeric_passes_through_unchanged(self):
        # PATs use safe chars (letters, digits, '_', '-', '.') — no escaping.
        out = _inject_credentials("https://gitlab.com/x.git", "oauth2", "glpat-ABC_123.456-xyz")
        assert "glpat-ABC_123.456-xyz" in out

    def test_raises_on_url_without_host(self):
        with pytest.raises(ValueError):
            _inject_credentials("not-a-url", "oauth2", "tok")

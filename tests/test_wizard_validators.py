from __future__ import annotations

from unittest.mock import patch

import pytest

from hydra.config import HostSpec
from hydra.wizard import (
    WizardCancelled,
    _looks_like_url,
    _pick_forks,
    _required,
    _valid_host_id,
    _valid_repo_name,
)


class TestValidHostId:
    def test_accepts_simple(self):
        assert _valid_host_id("internal", taken=set()) is True

    def test_accepts_hyphens_underscores(self):
        assert _valid_host_id("my-host_1", taken=set()) is True

    def test_rejects_empty(self):
        assert _valid_host_id("", taken=set()) == "Required"

    def test_rejects_special_chars(self):
        result = _valid_host_id("ho.st", taken=set())
        assert isinstance(result, str) and "letters" in result

    def test_rejects_taken(self):
        result = _valid_host_id("dup", taken={"dup"})
        assert isinstance(result, str) and "already" in result


class TestRequired:
    def test_accepts_non_empty(self):
        assert _required("hello") is True

    def test_accepts_string_with_inner_whitespace(self):
        assert _required("hello world") is True

    def test_rejects_empty(self):
        assert _required("") == "Required"

    def test_rejects_whitespace_only(self):
        assert _required("   ") == "Required"
        assert _required("\t\n") == "Required"


class TestLooksLikeUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com",
            "http://localhost:8080",
            "https://gitlab.notyouraverage.dev",
            "http://192.168.1.1:1234/path",
        ],
    )
    def test_accepts_well_formed_urls(self, url):
        assert _looks_like_url(url) is True

    def test_rejects_empty(self):
        assert _looks_like_url("") == "Required"
        assert _looks_like_url("   ") == "Required"

    @pytest.mark.parametrize("url", ["example.com", "ftp://example.com", "git@example.com:foo.git"])
    def test_rejects_missing_scheme(self, url):
        result = _looks_like_url(url)
        assert isinstance(result, str)
        assert "http" in result.lower()


class TestValidRepoName:
    @pytest.mark.parametrize(
        "name",
        [
            "repo",
            "my-cool-repo",
            "with_underscore",
            "dots.in.name",
            "Mixed-CASE_123",
            "a",  # single char
        ],
    )
    def test_accepts_valid_names(self, name):
        assert _valid_repo_name(name) is True

    def test_rejects_empty(self):
        assert _valid_repo_name("") == "Required"
        assert _valid_repo_name("   ") == "Required"

    def test_rejects_too_long(self):
        result = _valid_repo_name("a" * 101)
        assert isinstance(result, str)
        assert "100" in result

    def test_accepts_exactly_100_chars(self):
        assert _valid_repo_name("a" * 100) is True

    @pytest.mark.parametrize(
        "name",
        ["foo bar", "foo!bar", "foo/bar", "foo@bar", "café", "foo+bar"],
    )
    def test_rejects_invalid_characters(self, name):
        result = _valid_repo_name(name)
        assert isinstance(result, str)
        assert "letters" in result.lower() or "characters" in result.lower()

    @pytest.mark.parametrize("name", [".foo", "-foo", "..foo"])
    def test_rejects_leading_special_chars(self, name):
        result = _valid_repo_name(name)
        assert isinstance(result, str)
        assert "start" in result or "end" in result

    @pytest.mark.parametrize("name", ["foo.", "foo-", "foo--"])
    def test_rejects_trailing_special_chars(self, name):
        result = _valid_repo_name(name)
        assert isinstance(result, str)
        assert "end" in result


class TestPickForks:
    def test_empty_pool_raises(self):
        # Only the primary host exists — no candidates for forks.
        hosts = [HostSpec(id="only", kind="gitlab", url="https://gl.x")]
        with pytest.raises(WizardCancelled, match="another host"):
            _pick_forks(hosts, primary="only", default=[])

    def test_persistent_empty_selection_raises(self, capsys):
        hosts = [
            HostSpec(id="primary", kind="gitlab", url="https://gl.x"),
            HostSpec(id="fork", kind="gitlab", url="https://gl.y"),
        ]
        # _ask returns [] every time → eventual WizardCancelled (no infinite loop).
        with patch("hydra.wizard._ask", return_value=[]):
            with pytest.raises(WizardCancelled, match="multiple attempts"):
                _pick_forks(hosts, primary="primary", default=[], max_attempts=2)

    def test_first_nonempty_selection_returns(self):
        hosts = [
            HostSpec(id="primary", kind="gitlab", url="https://gl.x"),
            HostSpec(id="a", kind="gitlab", url="https://gl.a"),
            HostSpec(id="b", kind="github", url="https://api.github.com"),
        ]
        with patch("hydra.wizard._ask", return_value=["a", "b"]):
            assert _pick_forks(hosts, primary="primary", default=[]) == ["a", "b"]

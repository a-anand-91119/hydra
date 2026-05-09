from __future__ import annotations

import pytest

from hydra.wizard import _looks_like_url, _required, _valid_repo_name


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

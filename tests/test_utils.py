from __future__ import annotations

import re

import pytest

from hydra.utils import create_slug


class TestCreateSlug:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Hello World", "hello-world"),
            ("Hello   World", "hello-world"),
            ("Foo!@#Bar", "foo-bar"),
            ("ABC", "abc"),
            ("Already-Slug", "already-slug"),
            ("with_under_score", "with-under-score"),
            ("dots.in.it", "dots-in-it"),
            ("alpha123beta", "alpha123beta"),
        ],
    )
    def test_basic_slugification(self, raw, expected):
        assert create_slug(raw) == expected

    def test_strips_leading_and_trailing_separators(self):
        assert create_slug("  Hello World  ") == "hello-world"
        assert create_slug("---foo---") == "foo"

    def test_collapses_runs_of_separators(self):
        assert create_slug("foo!!!@@@bar") == "foo-bar"

    def test_empty_input_returns_empty(self):
        assert create_slug("") == ""
        assert create_slug("   ") == ""

    def test_timestamp_suffix_appended(self):
        slug = create_slug("foo bar", add_timestamp=True)
        assert slug.startswith("foo-bar-")
        # timestamp format: YYYYMMDDHHMMSS — 14 digits at the end
        match = re.search(r"-(\d{14})$", slug)
        assert match is not None

    def test_no_timestamp_when_flag_false(self):
        assert create_slug("foo bar", add_timestamp=False) == "foo-bar"

    def test_unicode_characters_treated_as_separators(self):
        # Non-ASCII letters get replaced by '-' (current regex is ASCII-only)
        assert create_slug("café-au-lait") == "caf-au-lait"

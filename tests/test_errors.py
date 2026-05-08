from __future__ import annotations

import pytest

from hydra.errors import (
    BODY_SNIPPET_LIMIT,
    HOST_LABELS,
    HydraAPIError,
    _short_body,
    _truncate,
    raise_for_response,
)


# ── _short_body ─────────────────────────────────────────────────────────────


class TestShortBody:
    def test_extracts_message_key_from_dict(self, fake_response):
        r = fake_response(401, {"message": "Unauthorized"})
        assert _short_body(r) == "Unauthorized"

    def test_extracts_error_key_from_dict(self, fake_response):
        r = fake_response(400, {"error": "bad request"})
        assert _short_body(r) == "bad request"

    def test_extracts_error_description_key(self, fake_response):
        r = fake_response(400, {"error_description": "explained"})
        assert _short_body(r) == "explained"

    def test_extracts_detail_key(self, fake_response):
        r = fake_response(400, {"detail": "details here"})
        assert _short_body(r) == "details here"

    def test_joins_list_value_with_semicolons(self, fake_response):
        r = fake_response(422, {"message": ["a", "b", "c"]})
        assert _short_body(r) == "a; b; c"

    def test_falls_back_to_compact_json_for_unrecognised_dict(self, fake_response):
        r = fake_response(400, {"foo": "bar"})
        out = _short_body(r)
        assert "foo" in out and "bar" in out

    def test_returns_text_when_body_is_not_json(self, fake_response):
        r = fake_response(500, "Internal Server Error")
        assert _short_body(r) == "Internal Server Error"

    def test_truncates_long_text(self, fake_response):
        big = "x" * 500
        r = fake_response(500, big)
        snippet = _short_body(r)
        assert len(snippet) == BODY_SNIPPET_LIMIT + 1  # plus the ellipsis
        assert snippet.endswith("…")

    def test_truncate_preserves_short_strings(self):
        assert _truncate("hello") == "hello"

    def test_first_matching_key_wins(self, fake_response):
        r = fake_response(400, {"detail": "second", "message": "first"})
        # 'message' is checked first
        assert _short_body(r) == "first"


# ── raise_for_response ──────────────────────────────────────────────────────


class TestRaiseForResponse:
    def test_passes_through_200(self, fake_response):
        r = fake_response(200, {"ok": True})
        assert raise_for_response(r, host="github", action="poking") is r

    def test_passes_through_201(self, fake_response):
        r = fake_response(201, {"created": True})
        assert raise_for_response(r, host="github", action="poking") is r

    @pytest.mark.parametrize("host", list(HOST_LABELS.keys()))
    def test_401_uses_host_label(self, fake_response, host):
        r = fake_response(401, {"message": "Unauthorized"})
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host=host, action="searching")
        err = exc_info.value
        assert err.status_code == 401
        assert HOST_LABELS[host] in err.message
        assert "401" in err.message
        assert err.hint is not None
        assert "rotate" in err.hint.lower() or "Rotate" in err.hint

    def test_401_substitutes_host_url_for_self_hosted(self, fake_response):
        r = fake_response(401)
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(
                r,
                host="self_hosted_gitlab",
                action="x",
                host_url="https://gitlab.example.com",
            )
        assert "https://gitlab.example.com" in exc_info.value.hint

    def test_401_falls_back_when_no_host_url(self, fake_response):
        r = fake_response(401)
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="self_hosted_gitlab", action="x")
        assert "<your self-hosted GitLab>" in exc_info.value.hint

    def test_401_uses_canonical_url_for_gitlab_com(self, fake_response):
        r = fake_response(401)
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="gitlab", action="x")
        assert "https://gitlab.com/" in exc_info.value.hint

    def test_401_uses_canonical_url_for_github(self, fake_response):
        r = fake_response(401)
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="github", action="x")
        assert "https://github.com/settings/tokens" in exc_info.value.hint

    def test_403_mentions_scope(self, fake_response):
        r = fake_response(403, {"message": "Forbidden"})
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="github", action="creating repo")
        err = exc_info.value
        assert err.status_code == 403
        assert "scope" in err.hint.lower()

    def test_404_returns_not_found(self, fake_response):
        r = fake_response(404, {"message": "Not found"})
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="github", action="searching")
        assert exc_info.value.status_code == 404
        assert "404" in exc_info.value.message

    @pytest.mark.parametrize("code", [409, 422])
    def test_409_and_422_signal_conflict(self, fake_response, code):
        r = fake_response(code, {"message": "name has already been taken"})
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="gitlab", action="creating repo 'x'")
        err = exc_info.value
        assert err.status_code == code
        assert "already exists" in err.hint
        # Snippet of the body should appear in message
        assert "already been taken" in err.message

    @pytest.mark.parametrize("code", [500, 502, 503, 504])
    def test_5xx_marked_as_server_problem(self, fake_response, code):
        r = fake_response(code, "boom")
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="github", action="creating repo")
        err = exc_info.value
        assert err.status_code == code
        assert "Server-side" in err.hint

    def test_other_4xx_uses_generic_message(self, fake_response):
        r = fake_response(418, "I'm a teapot")
        with pytest.raises(HydraAPIError) as exc_info:
            raise_for_response(r, host="github", action="brewing")
        err = exc_info.value
        assert err.status_code == 418
        assert "418" in err.message
        assert "teapot" in err.message
        assert err.hint is None

    def test_str_returns_message(self):
        err = HydraAPIError(message="boom", host="github", status_code=500)
        assert str(err) == "boom"

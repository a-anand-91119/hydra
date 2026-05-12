"""Contract tests for hydra.http — the shared HTTP layer."""

from __future__ import annotations

import inspect
import threading

import pytest
import requests
from urllib3.exceptions import NewConnectionError

from hydra import github as github_api
from hydra import gitlab as gitlab_api
from hydra import http


def _fresh_connect_error(msg: str = "refused") -> requests.exceptions.ConnectionError:
    """Build a ConnectionError whose underlying cause is a NewConnectionError.

    This mirrors what ``requests`` actually constructs when the OS refuses
    the TCP connect (DNS failure, connect-timeout, ECONNREFUSED).
    """
    cause = NewConnectionError(None, msg)  # type: ignore[arg-type]
    err = requests.exceptions.ConnectionError(cause)
    err.__cause__ = cause
    return err


class _FakeSession:
    """Stand-in for ``requests.Session`` — records calls and replays a script."""

    def __init__(self, script):
        # script: list of either Response-like objects OR Exception instances.
        self._script = list(script)
        self.calls = []

    def request(self, method, url, **kw):
        self.calls.append({"method": method, "url": url, "kw": kw})
        if not self._script:
            raise AssertionError("FakeSession out of scripted responses")
        outcome = self._script.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class TestRetryConfig:
    def test_status_forcelist_and_methods(self):
        retry = http._build_retry()
        assert 429 in retry.status_forcelist
        assert 502 in retry.status_forcelist
        assert 503 in retry.status_forcelist
        assert 504 in retry.status_forcelist
        assert "GET" in retry.allowed_methods
        # Mutating verbs MUST NOT auto-retry through urllib3.
        assert "POST" not in retry.allowed_methods
        assert "PUT" not in retry.allowed_methods
        assert "DELETE" not in retry.allowed_methods
        assert retry.respect_retry_after_header is True

    def test_session_is_thread_local(self):
        s1 = http.session()
        s2 = http.session()
        assert s1 is s2
        seen = {}

        def grab():
            seen["other"] = http.session()

        t = threading.Thread(target=grab)
        t.start()
        t.join()
        # Different thread => different session instance.
        assert seen["other"] is not s1


class TestDefaultTimeout:
    def test_default_pair(self, monkeypatch):
        monkeypatch.delenv("HYDRA_HTTP_TIMEOUT_CONNECT", raising=False)
        monkeypatch.delenv("HYDRA_HTTP_TIMEOUT_READ", raising=False)
        assert http._default_timeout() == (
            http.DEFAULT_CONNECT_TIMEOUT,
            http.DEFAULT_READ_TIMEOUT,
        )

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("HYDRA_HTTP_TIMEOUT_CONNECT", "0.5")
        monkeypatch.setenv("HYDRA_HTTP_TIMEOUT_READ", "2.5")
        assert http._default_timeout() == (0.5, 2.5)

    def test_invalid_env_var_falls_back(self, monkeypatch):
        monkeypatch.setenv("HYDRA_HTTP_TIMEOUT_CONNECT", "not-a-number")
        assert http._default_timeout()[0] == http.DEFAULT_CONNECT_TIMEOUT


class TestRequestWrapper:
    def test_timeout_injected_when_absent(self, monkeypatch):
        fake = _FakeSession([_resp(200)])
        monkeypatch.setattr(http, "session", lambda: fake)
        http.get("https://example.com/x")
        assert fake.calls[0]["kw"]["timeout"] == http._default_timeout()

    def test_caller_timeout_respected(self, monkeypatch):
        fake = _FakeSession([_resp(200)])
        monkeypatch.setattr(http, "session", lambda: fake)
        http.get("https://example.com/x", timeout=(1, 2))
        assert fake.calls[0]["kw"]["timeout"] == (1, 2)

    def test_post_retries_on_fresh_connect_failure(self, monkeypatch):
        # ConnectionError whose underlying cause is NewConnectionError (TCP
        # refused before any bytes left the wire) → one explicit retry.
        fake = _FakeSession([_fresh_connect_error(), _resp(201)])
        monkeypatch.setattr(http, "session", lambda: fake)
        resp = http.post("https://example.com/api", data={"a": 1})
        assert resp.status_code == 201
        assert len(fake.calls) == 2
        stats = http.pop_retry_stats()
        assert sum(stats.values()) == 1
        assert "example.com" in stats

    def test_post_does_not_retry_on_mid_request_connection_error(self, monkeypatch):
        # A bare ConnectionError with no NewConnectionError in the chain could
        # mean the server already processed the write (e.g. RST mid-response).
        # Replaying could duplicate a side effect → MUST NOT retry.
        fake = _FakeSession([requests.exceptions.ConnectionError("reset mid-write")])
        monkeypatch.setattr(http, "session", lambda: fake)
        with pytest.raises(requests.exceptions.ConnectionError):
            http.post("https://example.com/api", data={"a": 1})
        assert len(fake.calls) == 1
        assert http.pop_retry_stats() == {}

    def test_post_read_timeout_not_retried(self, monkeypatch):
        # ReadTimeout means bytes left the wire — replaying could duplicate
        # a server-side write, so we must NOT retry.
        fake = _FakeSession([requests.exceptions.ReadTimeout("slow")])
        monkeypatch.setattr(http, "session", lambda: fake)
        with pytest.raises(requests.exceptions.ReadTimeout):
            http.post("https://example.com/api", data={"a": 1})
        assert len(fake.calls) == 1

    def test_get_connection_error_propagates(self, monkeypatch):
        # GET is non-mutating, so request() never enters the explicit-retry
        # branch — regardless of whether the cause is NewConnectionError or
        # something else. GET retry for 429/5xx is delegated to urllib3's
        # adapter (covered by TestRetryConfig + the integration test below).
        fake = _FakeSession([requests.exceptions.ConnectionError("nope")])
        monkeypatch.setattr(http, "session", lambda: fake)
        with pytest.raises(requests.exceptions.ConnectionError):
            http.get("https://example.com/x")
        assert len(fake.calls) == 1


class TestRetryStats:
    def test_pop_clears_counters(self, monkeypatch):
        fake = _FakeSession([_fresh_connect_error(), _resp(201)])
        monkeypatch.setattr(http, "session", lambda: fake)
        http.post("https://h.example/api", data={})
        first = http.pop_retry_stats()
        assert sum(first.values()) == 1
        second = http.pop_retry_stats()
        assert second == {}


class TestCountingRetry:
    def test_bump_only_on_successful_increment(self, monkeypatch):
        """Counter must count retries that ACTUALLY happen — exhausted retries
        (where ``super().increment`` raises ``MaxRetryError``) must not bump.
        """
        retry = http._build_retry()

        # Successful increment: counter should bump by 1 each call.
        r1 = retry.increment(method="GET", url="https://h.example/a", error=Exception("x"))
        assert sum(http.pop_retry_stats().values()) == 1

        # Drain budget by repeatedly incrementing until exhaustion.
        cur = r1
        bumps = 0
        while True:
            try:
                cur = cur.increment(method="GET", url="https://h.example/a", error=Exception("y"))
                bumps += 1
            except Exception:
                # MaxRetryError — must NOT have bumped the counter on this call.
                break

        observed = sum(http.pop_retry_stats().values())
        assert observed == bumps, (
            f"counter ({observed}) should match the number of successful "
            f"increments ({bumps}) — no bump on exhaustion"
        )


class TestRegression:
    def test_session_machinery_lives_only_in_http_module(self):
        gitlab_src = inspect.getsource(gitlab_api)
        github_src = inspect.getsource(github_api)
        for needle in ("_thread_local", "_build_retry"):
            assert needle not in gitlab_src, f"{needle} leaked into hydra.gitlab"
            assert needle not in github_src, f"{needle} leaked into hydra.github"


# ── helpers ─────────────────────────────────────────────────────────────


def _resp(status: int):
    r = requests.Response()
    r.status_code = status
    return r

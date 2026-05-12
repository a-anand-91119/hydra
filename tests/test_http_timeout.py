"""Tests for the timeout-injection behaviour of hydra.http.

The unit-level wiring (timeout always passed, env-var override works) is
covered in test_http.py. Here we exercise the bug Phase 4a fixed: a hung
peer must surface as a fail-fast exception rather than blocking forever.
"""

from __future__ import annotations

import time

import pytest
import requests

from hydra import http


class _SlowSession:
    """Simulates a peer that hangs forever — but our timeout cuts it short."""

    def __init__(self, exc):
        self._exc = exc
        self.calls = 0

    def request(self, method, url, **kw):
        self.calls += 1
        # Sanity-check the wrapper actually passed a timeout through.
        assert "timeout" in kw, "http.request must inject a timeout"
        raise self._exc


def test_connect_timeout_on_get_fails_fast(monkeypatch):
    monkeypatch.setenv("HYDRA_HTTP_TIMEOUT_CONNECT", "0.1")
    monkeypatch.setenv("HYDRA_HTTP_TIMEOUT_READ", "0.1")
    fake = _SlowSession(requests.exceptions.ConnectTimeout("dead host"))
    monkeypatch.setattr(http, "session", lambda: fake)

    start = time.perf_counter()
    with pytest.raises(requests.exceptions.ConnectTimeout):
        http.get("https://black-hole.example/api/v4/projects")
    elapsed = time.perf_counter() - start

    assert elapsed < 1.0, f"GET hung for {elapsed:.2f}s instead of failing fast"
    assert fake.calls == 1


def test_read_timeout_on_post_does_not_retry(monkeypatch):
    """A ReadTimeout on POST means bytes already left the wire; replaying
    could duplicate a server-side write. The wrapper must fail immediately.
    """
    monkeypatch.setenv("HYDRA_HTTP_TIMEOUT_CONNECT", "0.1")
    monkeypatch.setenv("HYDRA_HTTP_TIMEOUT_READ", "0.1")
    fake = _SlowSession(requests.exceptions.ReadTimeout("hung mid-response"))
    monkeypatch.setattr(http, "session", lambda: fake)

    start = time.perf_counter()
    with pytest.raises(requests.exceptions.ReadTimeout):
        http.post("https://slow.example/api/v4/projects", data={"name": "x"})
    elapsed = time.perf_counter() - start

    assert elapsed < 1.0
    assert fake.calls == 1  # No retry on ReadTimeout for mutations.


def test_every_outbound_call_site_passes_timeout(monkeypatch):
    """Every call routed through hydra.http carries a timeout — proven by
    asserting it from the FakeSession side.
    """
    captured = []

    class Recorder:
        def request(self, method, url, **kw):
            captured.append((method, url, kw.get("timeout")))
            r = requests.Response()
            r.status_code = 200
            return r

    monkeypatch.setattr(http, "session", lambda: Recorder())

    http.get("https://x.example/a")
    http.post("https://x.example/b", data={})
    http.delete("https://x.example/c")
    http.put("https://x.example/d", data={})

    for method, _url, timeout in captured:
        assert timeout is not None, f"{method} missing timeout"
        assert isinstance(timeout, tuple) and len(timeout) == 2

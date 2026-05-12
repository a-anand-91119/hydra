"""End-to-end integration tests for hydra.http through a real urllib3 stack.

These tests spin up a tiny local HTTP server so the full
``HTTPAdapter → urllib3.PoolManager → Retry`` path is exercised. That's the
only way to validate ``_CountingRetry`` for real — ``requests-mock`` and
similar tools replace the adapter and skip urllib3's retry loop entirely.
"""

from __future__ import annotations

import socketserver
import threading
from http.server import BaseHTTPRequestHandler
from typing import List

import pytest

from hydra import http as hydra_http
from hydra.errors import HydraAPIError, raise_for_response


class _ScriptedHandler(BaseHTTPRequestHandler):
    """Per-subclass class-var state lets each test get its own response script."""

    sequence: List[int] = []
    cursor = 0

    def do_GET(self):  # noqa: N802 — stdlib API
        cls = type(self)
        idx = cls.cursor
        cls.cursor += 1
        code = cls.sequence[idx] if idx < len(cls.sequence) else 200
        self.send_response(code)
        # Retry-After: 0 short-circuits urllib3's backoff sleep so the test
        # finishes in milliseconds rather than seconds.
        if code in (429, 503):
            self.send_header("Retry-After", "0")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args, **kwargs):  # silence stderr noise
        pass


def _spawn_server(sequence):
    """Bind to an ephemeral port and serve `sequence` then 200s forever."""
    handler_cls = type(
        "Handler",
        (_ScriptedHandler,),
        {"sequence": list(sequence), "cursor": 0},
    )
    server = socketserver.TCPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, handler_cls


@pytest.fixture
def scripted_server():
    """Yields ``make(sequence) -> base_url, handler``. Tears the server down."""
    spawned = []

    def make(sequence):
        server, handler = _spawn_server(sequence)
        spawned.append(server)
        host, port = server.server_address
        return f"http://{host}:{port}", handler

    yield make

    for server in spawned:
        server.shutdown()
        server.server_close()


def test_429_then_200_retries_once_and_bumps_counter(scripted_server):
    """The acceptance test from the plan: one 429, then 200; counter shows 1."""
    base, handler = scripted_server([429, 200])

    response = hydra_http.get(f"{base}/x")

    assert response.status_code == 200
    assert handler.cursor == 2, "expected exactly one retry"
    stats = hydra_http.pop_retry_stats()
    assert sum(stats.values()) == 1
    assert "127.0.0.1" in stats


def test_three_503s_exhaust_retries_and_surface_server_side_hint(scripted_server):
    """Plan acceptance: three 503s → HydraAPIError, hint mentions server-side."""
    base, handler = scripted_server([503, 503, 503, 503])

    response = hydra_http.get(f"{base}/x")
    # raise_on_status=False → urllib3 returns the final 503 instead of raising.
    assert response.status_code == 503
    assert handler.cursor == 4, "expected 1 initial + 3 retries"

    with pytest.raises(HydraAPIError) as excinfo:
        raise_for_response(response, host="gitlab", action="listing projects")
    assert excinfo.value.status_code == 503
    assert "Server-side" in (excinfo.value.hint or "")

    # _CountingRetry bumps once per *successful* increment. The 4th attempt's
    # increment raises MaxRetryError, but raise_on_status=False catches it
    # internally — that path still calls increment one extra time, so we
    # expect 3 retries actually performed.
    assert sum(hydra_http.pop_retry_stats().values()) == 3

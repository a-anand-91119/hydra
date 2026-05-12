from __future__ import annotations

import json

import pytest

# Ensure built-in providers are registered before any test loads config or
# instantiates a provider. Idempotent.
from hydra import providers as _providers  # noqa: E402

_providers.bootstrap()


@pytest.fixture(autouse=True)
def _isolate_state_paths(tmp_path, monkeypatch):
    """Pin XDG_STATE_HOME + HYDRA_JOURNAL to per-test tmp dirs, and chdir to a
    clean working directory.

    Stops tests from reading or writing the real ~/.local/state/hydra/journal.db,
    and prevents doctor from picking up a developer's local .env file.
    """
    state_dir = tmp_path / "xdg-state"
    state_dir.mkdir()
    cwd_dir = tmp_path / "cwd"
    cwd_dir.mkdir()
    monkeypatch.setenv("XDG_STATE_HOME", str(state_dir))
    monkeypatch.delenv("HYDRA_JOURNAL", raising=False)
    monkeypatch.chdir(cwd_dir)
    yield


@pytest.fixture(autouse=True)
def _reset_http_retry_stats():
    """Zero the global retry counter before each test so retry telemetry
    assertions don't leak across tests, regardless of test order.
    """
    from hydra import http as _http

    _http.reset_retry_stats()
    yield
    _http.reset_retry_stats()


class FakeResponse:
    """Minimal stand-in for requests.Response used by error tests."""

    def __init__(
        self,
        status_code: int,
        body: dict | list | str | None = None,
        headers: dict | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = dict(headers or {})
        if isinstance(body, (dict, list)):
            self._payload = body
            self.text = json.dumps(body)
            self._is_json = True
        else:
            self._payload = None
            self.text = body or ""
            self._is_json = False

    def json(self):
        if not self._is_json:
            raise ValueError("not JSON")
        return self._payload


@pytest.fixture
def fake_response():
    return FakeResponse

from __future__ import annotations

import json

import pytest

# Ensure built-in providers are registered before any test loads config or
# instantiates a provider. Idempotent.
from hydra import providers as _providers  # noqa: E402

_providers.bootstrap()


class FakeResponse:
    """Minimal stand-in for requests.Response used by error tests."""

    def __init__(
        self,
        status_code: int,
        body: dict | list | str | None = None,
    ) -> None:
        self.status_code = status_code
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

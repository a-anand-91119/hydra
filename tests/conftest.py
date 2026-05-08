from __future__ import annotations

import json
from typing import Optional

import pytest


class FakeResponse:
    """Minimal stand-in for requests.Response used by error tests."""

    def __init__(
        self,
        status_code: int,
        body: Optional[dict | list | str] = None,
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

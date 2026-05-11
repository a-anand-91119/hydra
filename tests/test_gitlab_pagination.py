"""Tests for hydra.gitlab._paginate and list_projects_with_mirrors:
- multi-page fan-out via X-Total-Pages preserves order
- single-page fast path makes one GET
- keyset fallback (X-Next-Page only) walks sequentially
"""

from __future__ import annotations

from hydra import gitlab as gitlab_api
from tests.conftest import FakeResponse


def _resp(items, *, total_pages=None, next_page=""):
    headers = {}
    if total_pages is not None:
        headers["X-Total-Pages"] = str(total_pages)
    if next_page:
        headers["X-Next-Page"] = str(next_page)
    return FakeResponse(200, items, headers=headers)


class _SessionStub:
    """Minimal stand-in for the per-thread requests.Session."""

    def __init__(self, get_handler=None, post_handler=None):
        self._get = get_handler
        self._post = post_handler
        self.get_calls = []
        self.post_calls = []

    def get(self, url, headers=None, params=None):
        self.get_calls.append({"url": url, "headers": headers, "params": params or {}})
        return self._get(url, params or {})

    def post(self, url, headers=None, data=None):
        self.post_calls.append({"url": url, "headers": headers, "data": data})
        return self._post(url, data or {})


class TestPaginateConcurrent:
    def test_three_pages_fan_out_preserves_order(self, monkeypatch):
        # Pages 1..3, each with two items. Out-of-order completion should still
        # produce 1,2,3,4,5,6 in the final list because we re-sort by page.
        page1 = _resp([{"i": 1}, {"i": 2}], total_pages=3)
        page2 = _resp([{"i": 3}, {"i": 4}])
        page3 = _resp([{"i": 5}, {"i": 6}])

        pages = {1: page1, 2: page2, 3: page3}

        def get(url, params):
            return pages[int(params["page"])]

        sess = _SessionStub(get_handler=get)
        monkeypatch.setattr(gitlab_api, "_session", lambda: sess)

        out = gitlab_api._paginate(
            host="h",
            endpoint="https://gl/api/v4/projects",
            headers={},
            params={"per_page": 100},
            max_workers=4,
        )
        assert [d["i"] for d in out] == [1, 2, 3, 4, 5, 6]
        # Each page fetched exactly once.
        seen_pages = sorted(c["params"]["page"] for c in sess.get_calls)
        assert seen_pages == [1, 2, 3]

    def test_single_page_does_no_fan_out(self, monkeypatch):
        sess = _SessionStub(get_handler=lambda u, p: _resp([{"i": 1}], total_pages=1))
        monkeypatch.setattr(gitlab_api, "_session", lambda: sess)
        out = gitlab_api._paginate(host="h", endpoint="x", headers={}, params={}, max_workers=8)
        assert out == [{"i": 1}]
        assert len(sess.get_calls) == 1

    def test_keyset_fallback_walks_sequentially(self, monkeypatch):
        # X-Total-Pages absent; X-Next-Page is the only signal.
        responses = [
            _resp([{"i": 1}], next_page="2"),
            _resp([{"i": 2}], next_page="3"),
            _resp([{"i": 3}], next_page=""),
        ]
        idx = {"n": 0}

        def get(url, params):
            r = responses[idx["n"]]
            idx["n"] += 1
            return r

        sess = _SessionStub(get_handler=get)
        monkeypatch.setattr(gitlab_api, "_session", lambda: sess)
        out = gitlab_api._paginate(host="h", endpoint="x", headers={}, params={}, max_workers=8)
        assert [d["i"] for d in out] == [1, 2, 3]
        assert len(sess.get_calls) == 3

    def test_total_pages_zero_or_one_uses_keyset_path(self, monkeypatch):
        sess = _SessionStub(get_handler=lambda u, p: _resp([{"i": 1}], total_pages=0))
        monkeypatch.setattr(gitlab_api, "_session", lambda: sess)
        out = gitlab_api._paginate(host="h", endpoint="x", headers={}, params={})
        assert out == [{"i": 1}]


class TestListProjectsWithMirrors:
    def test_concurrent_per_project_mirror_fetch(self, monkeypatch):
        projects = [
            {"id": 1, "name": "a", "path_with_namespace": "g/a", "web_url": "u1"},
            {"id": 2, "name": "b", "path_with_namespace": "g/b", "web_url": "u2"},
            {"id": 3, "name": "c", "path_with_namespace": "g/c", "web_url": "u3"},
        ]

        def get(url, params):
            if url.endswith("/projects"):
                return _resp(projects, total_pages=1)
            # /projects/<id>/remote_mirrors
            pid = int(url.rstrip("/").split("/")[-2])
            return _resp([{"id": 100 + pid, "url": f"https://mirror/{pid}.git"}])

        sess = _SessionStub(get_handler=get)
        monkeypatch.setattr(gitlab_api, "_session", lambda: sess)

        out = gitlab_api.list_projects_with_mirrors(
            host="h", base_url="https://gl", token="t", namespace="g", max_workers=4
        )
        # Order should follow input projects (deterministic).
        assert [p.project_id for p in out] == [1, 2, 3]
        # And mirror id was stitched per project.
        assert [p.mirrors[0].id for p in out] == [101, 102, 103]

    def test_403_skips_project(self, monkeypatch):
        projects = [
            {"id": 1, "name": "a", "path_with_namespace": "g/a", "web_url": "u1"},
            {"id": 2, "name": "b", "path_with_namespace": "g/b", "web_url": "u2"},
        ]

        def get(url, params):
            if url.endswith("/projects"):
                return _resp(projects, total_pages=1)
            pid = int(url.rstrip("/").split("/")[-2])
            if pid == 2:
                return FakeResponse(403, {"message": "no"})
            return _resp([{"id": 7, "url": "https://m/1.git"}])

        sess = _SessionStub(get_handler=get)
        monkeypatch.setattr(gitlab_api, "_session", lambda: sess)
        out = gitlab_api.list_projects_with_mirrors(
            host="h", base_url="https://gl", token="t", namespace="g"
        )
        assert [p.project_id for p in out] == [1]


class TestRetryConfig:
    def test_retry_status_forcelist_is_honored(self):
        retry = gitlab_api._build_retry()
        assert 429 in retry.status_forcelist
        assert 502 in retry.status_forcelist
        assert 503 in retry.status_forcelist
        assert 504 in retry.status_forcelist
        # Mutating verbs MUST NOT auto-retry.
        assert "POST" not in retry.allowed_methods
        assert "DELETE" not in retry.allowed_methods
        assert "GET" in retry.allowed_methods

    def test_session_is_thread_local(self):
        # A given thread always gets the same session back.
        s1 = gitlab_api._session()
        s2 = gitlab_api._session()
        assert s1 is s2

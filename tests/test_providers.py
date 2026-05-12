from __future__ import annotations

import pytest

from hydra.providers import HostSpec, capabilities_for, get, kinds


class TestRegistry:
    def test_builtins_registered(self):
        assert "gitlab" in kinds()
        assert "github" in kinds()

    def test_get_returns_factory(self):
        factory = get("gitlab")
        prov = factory(HostSpec(id="x", kind="gitlab", url="https://gl"))
        assert prov.spec.id == "x"
        assert prov.capabilities.supports_mirror_source is True

    def test_unknown_kind_raises(self):
        with pytest.raises(KeyError):
            get("nope")

    def test_double_register_rejected(self):
        from hydra.providers import (
            ProviderRegistrationError,
            _reset_for_tests,
            bootstrap,
            register,
        )
        from hydra.providers.base import Capabilities

        try:
            _reset_for_tests()
            bootstrap()
            with pytest.raises(ProviderRegistrationError):
                register(
                    "gitlab",
                    lambda spec: object(),
                    Capabilities(
                        supports_mirror_source=False,
                        supports_group_paths=False,
                        supports_status_lookup=False,
                        inbound_mirror_username="x",
                    ),
                )
        finally:
            _reset_for_tests()
            bootstrap()

    def test_bootstrap_is_idempotent(self):
        from hydra.providers import bootstrap

        before = sorted(kinds())
        bootstrap()
        bootstrap()
        assert sorted(kinds()) == before


class TestCapabilities:
    def test_gitlab_can_be_primary(self):
        caps = capabilities_for("gitlab")
        assert caps.supports_mirror_source is True
        assert caps.supports_group_paths is True
        assert caps.supports_status_lookup is True
        assert caps.inbound_mirror_username == "oauth2"

    def test_github_cannot_be_primary(self):
        caps = capabilities_for("github")
        assert caps.supports_mirror_source is False
        assert caps.supports_group_paths is False
        assert caps.supports_status_lookup is False
        # GitHub HTTPS push uses x-access-token, not oauth2.
        assert caps.inbound_mirror_username == "x-access-token"


class TestGitLabProvider:
    def test_managed_prefix_wraps_group_path(self):
        spec = HostSpec(
            id="cloud",
            kind="gitlab",
            url="https://gitlab.com",
            options={"managed_group_prefix": "managed", "add_timestamp": True},
        )
        prov = get("gitlab")(spec)
        assert prov._effective_group_path("team") == "managed/team"
        assert prov._effective_group_path(None) == "managed"

    def test_no_prefix_passthrough(self):
        spec = HostSpec(id="self", kind="gitlab", url="https://gl")
        prov = get("gitlab")(spec)
        assert prov._effective_group_path("team") == "team"
        assert prov._effective_group_path(None) is None


class TestGitHubProvider:
    def test_org_passthrough(self):
        spec = HostSpec(
            id="gh", kind="github", url="https://api.github.com", options={"org": "acme"}
        )
        prov = get("github")(spec)
        assert prov._org() == "acme"

    def test_no_org(self):
        spec = HostSpec(id="gh", kind="github", url="https://api.github.com")
        prov = get("github")(spec)
        assert prov._org() is None

    def test_no_mirror_method(self):
        # GitHub provider doesn't implement add_outbound_mirror at all (it
        # doesn't satisfy the MirrorSource Protocol). Config validation
        # rejects github-as-primary, so the method is never called.
        spec = HostSpec(id="gh", kind="github", url="https://api.github.com")
        prov = get("github")(spec)
        from hydra.providers import MirrorSource

        assert not isinstance(prov, MirrorSource)
        assert not hasattr(prov, "add_outbound_mirror")


class TestFindRepo:
    """Phase 6: existence-probe used by `hydra create` to detect re-runs."""

    def test_gitlab_find_repo_returns_full_ref(self, monkeypatch):
        from hydra import mirrors as mirrors_api

        spec = HostSpec(id="gl", kind="gitlab", url="https://gl.example")
        prov = get("gitlab")(spec)

        captured = {}

        def fake_find_project(*, host_id, base_url, token, repo_path):
            captured["repo_path"] = repo_path
            return {"id": 42, "http_url_to_repo": "https://gl.example/team/probe.git"}

        monkeypatch.setattr(mirrors_api, "find_project", fake_find_project)
        ref = prov.find_repo(token="t", name="probe", namespace="team")
        assert ref is not None
        assert ref.project_id == 42
        assert ref.http_url == "https://gl.example/team/probe.git"
        assert ref.namespace_path == "team"
        assert captured["repo_path"] == "team/probe"

    def test_gitlab_find_repo_returns_none_on_404(self, monkeypatch):
        from hydra import mirrors as mirrors_api

        spec = HostSpec(id="gl", kind="gitlab", url="https://gl.example")
        prov = get("gitlab")(spec)
        monkeypatch.setattr(mirrors_api, "find_project", lambda **kw: None)
        assert prov.find_repo(token="t", name="probe", namespace="team") is None

    def test_gitlab_find_repo_honours_managed_group_prefix(self, monkeypatch):
        from hydra import mirrors as mirrors_api

        spec = HostSpec(
            id="gl",
            kind="gitlab",
            url="https://gl.example",
            options={"managed_group_prefix": "managed"},
        )
        prov = get("gitlab")(spec)

        captured = {}

        def fake_find_project(**kw):
            captured["repo_path"] = kw["repo_path"]
            return None

        monkeypatch.setattr(mirrors_api, "find_project", fake_find_project)
        prov.find_repo(token="t", name="probe", namespace="team")
        assert captured["repo_path"] == "managed/team/probe"

    def test_github_find_repo_handles_404(self, monkeypatch):
        from hydra import github as github_api

        spec = HostSpec(id="gh", kind="github", url="https://api.github.com", options={"org": "acme"})
        prov = get("github")(spec)
        monkeypatch.setattr(github_api, "find_repo", lambda **kw: None)
        assert prov.find_repo(token="t", name="probe", namespace=None) is None

    def test_github_find_repo_uses_org_when_configured(self, monkeypatch):
        from hydra import github as github_api

        spec = HostSpec(id="gh", kind="github", url="https://api.github.com", options={"org": "acme"})
        prov = get("github")(spec)

        captured = {}

        def fake_find_repo(*, base_url, token, owner, name):
            captured["owner"] = owner
            return "https://github.com/acme/probe.git"

        monkeypatch.setattr(github_api, "find_repo", fake_find_repo)
        ref = prov.find_repo(token="t", name="probe", namespace=None)
        assert ref is not None
        assert ref.http_url == "https://github.com/acme/probe.git"
        assert captured["owner"] == "acme"

    def test_github_find_repo_caches_user_login(self, monkeypatch):
        """For user-owned repos, GET /user must only be called once."""
        from hydra import github as github_api

        spec = HostSpec(id="gh", kind="github", url="https://api.github.com", options={"org": None})
        prov = get("github")(spec)

        login_calls = {"n": 0}

        def fake_login(*, base_url, token):
            login_calls["n"] += 1
            return "octocat"

        monkeypatch.setattr(github_api, "get_authenticated_login", fake_login)
        monkeypatch.setattr(github_api, "find_repo", lambda **kw: None)
        prov.find_repo(token="t", name="probe", namespace=None)
        prov.find_repo(token="t", name="probe2", namespace=None)
        assert login_calls["n"] == 1

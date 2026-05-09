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

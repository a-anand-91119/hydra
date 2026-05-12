"""Unit tests for hydra.preflight — the shared token-scope probe."""

from __future__ import annotations

from unittest.mock import patch

from hydra import preflight
from hydra.config import HostSpec
from hydra.errors import HydraAPIError
from hydra.secrets import TokenScopes


def _gitlab(host_id: str = "gitlab", url: str = "https://gitlab.example") -> HostSpec:
    return HostSpec(id=host_id, kind="gitlab", url=url, options={})


def _github(host_id: str = "github", **opts) -> HostSpec:
    return HostSpec(id=host_id, kind="github", url="https://api.github.com", options=opts)


class TestCheckTokens:
    def test_errors_on_missing_api_scope(self):
        host = _gitlab()
        with patch(
            "hydra.preflight.inspect_for_host",
            return_value=TokenScopes(scopes=["read_api"], scopes_known=True),
        ):
            report = preflight.check_tokens([host], {"gitlab": "tok"})
        assert len(report.errors) == 1
        assert "missing scope" in report.errors[0].message
        assert "api" in report.errors[0].message
        assert not report.warnings
        assert not report.oks

    def test_warns_on_unknown_scopes(self):
        # Fine-grained PAT path: scopes_known=False → goes to warnings.
        host = _gitlab()
        with patch(
            "hydra.preflight.inspect_for_host",
            return_value=TokenScopes(scopes=[], scopes_known=False),
        ):
            report = preflight.check_tokens([host], {"gitlab": "tok"})
        assert not report.errors
        assert len(report.warnings) == 1
        assert "scopes not exposed" in report.warnings[0].message

    def test_errors_on_rejected_token(self):
        host = _gitlab()
        with patch(
            "hydra.preflight.inspect_for_host",
            side_effect=HydraAPIError(
                message="auth failed", host="gitlab", status_code=401, hint="rotate me"
            ),
        ):
            report = preflight.check_tokens([host], {"gitlab": "tok"})
        assert len(report.errors) == 1
        assert "token rejected" in report.errors[0].message
        assert report.errors[0].hint == "rotate me"

    def test_substitutes_org_scopes_for_github(self):
        host = _github(org="acme")
        # 'admin:org' substitutes for the synthetic '_org' requirement.
        with patch(
            "hydra.preflight.inspect_for_host",
            return_value=TokenScopes(scopes=["repo", "admin:org"], scopes_known=True),
        ):
            report = preflight.check_tokens([host], {"github": "tok"})
        assert not report.errors
        assert len(report.oks) == 1

    def test_ok_for_valid_token_with_required_scopes(self):
        host = _gitlab()
        with patch(
            "hydra.preflight.inspect_for_host",
            return_value=TokenScopes(scopes=["api", "read_user"], expires_at="2099-01-01"),
        ):
            report = preflight.check_tokens([host], {"gitlab": "tok"})
        assert report.ok
        assert len(report.oks) == 1
        assert "expires 2099-01-01" in report.oks[0].message

    def test_skips_hosts_without_tokens(self):
        # Only probes hosts that appear in the tokens dict.
        gitlab = _gitlab()
        github = _github()
        with patch(
            "hydra.preflight.inspect_for_host",
            return_value=TokenScopes(scopes=["api"], scopes_known=True),
        ) as inspect:
            preflight.check_tokens([gitlab, github], {"gitlab": "tok"})
        # GitHub never probed because its token wasn't provided.
        called_hosts = [call.args[0].id for call in inspect.call_args_list]
        assert called_hosts == ["gitlab"]

    def test_parallel_probe_aggregates_results(self):
        """Two hosts with mixed outcomes: stable ordering + both surface."""
        gl = _gitlab(host_id="g1")
        gh = _github(host_id="gh")

        def fake_inspect(host, token):
            if host.id == "g1":
                return TokenScopes(scopes=["read_api"], scopes_known=True)
            return TokenScopes(scopes=["repo"], scopes_known=True)

        with patch("hydra.preflight.inspect_for_host", side_effect=fake_inspect):
            report = preflight.check_tokens([gh, gl], {"g1": "a", "gh": "b"})
        # gl missing 'api' → error; gh has 'repo' → ok.
        assert len(report.errors) == 1
        assert report.errors[0].host_id == "g1"
        assert len(report.oks) == 1
        assert report.oks[0].host_id == "gh"


class TestRequiredScopes:
    def test_gitlab_needs_api(self):
        assert preflight.required_scopes_for(_gitlab()) == {"api"}

    def test_github_needs_repo(self):
        assert preflight.required_scopes_for(_github()) == {"repo"}

    def test_github_with_org_adds_org_sentinel(self):
        scopes = preflight.required_scopes_for(_github(org="acme"))
        assert scopes == {"repo", "_org"}

    def test_unknown_kind_yields_empty(self):
        unknown = HostSpec(id="x", kind="bitbucket", url="https://bb", options={})
        assert preflight.required_scopes_for(unknown) == set()


class TestMissingScopes:
    def test_returns_unsatisfied_required_scopes(self):
        assert preflight.missing_scopes({"api"}, ["read_api"]) == {"api"}

    def test_handles_org_sentinel_substitution(self):
        # _org satisfied by admin:org.
        assert preflight.missing_scopes({"_org"}, ["admin:org"]) == set()
        # _org satisfied by write:org too.
        assert preflight.missing_scopes({"_org"}, ["write:org"]) == set()
        # Without either, missing surface mentions both substitutes.
        missing = preflight.missing_scopes({"_org"}, ["repo"])
        assert len(missing) == 1
        assert "admin:org" in next(iter(missing))

    def test_all_satisfied_returns_empty(self):
        assert preflight.missing_scopes({"api"}, ["api", "read_user"]) == set()

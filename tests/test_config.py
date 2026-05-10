from __future__ import annotations

import pytest

from hydra.config import ConfigError, _from_dict, _is_legacy_shape

# ──────────────────────────── New schema parsing ────────────────────────────


def _new_shape(**overrides):
    raw = {
        "hosts": [
            {"id": "internal", "kind": "gitlab", "url": "https://gl.internal"},
            {
                "id": "cloud",
                "kind": "gitlab",
                "url": "https://gitlab.com",
                "options": {"managed_group_prefix": "mp"},
            },
            {
                "id": "gh",
                "kind": "github",
                "url": "https://api.github.com",
                "options": {"org": "acme"},
            },
        ],
        "primary": "internal",
        "forks": ["cloud", "gh"],
        "defaults": {"private": True, "group": "team"},
    }
    raw.update(overrides)
    return raw


class TestNewSchema:
    def test_parses_valid_config(self):
        cfg = _from_dict(_new_shape())
        assert cfg.primary == "internal"
        assert cfg.forks == ["cloud", "gh"]
        assert len(cfg.hosts) == 3
        assert cfg.host("cloud").options["managed_group_prefix"] == "mp"

    def test_primary_host_helper(self):
        cfg = _from_dict(_new_shape())
        assert cfg.primary_host().id == "internal"
        assert [h.id for h in cfg.fork_hosts()] == ["cloud", "gh"]


# ──────────────────────────── Validation ────────────────────────────


class TestValidation:
    def test_empty_hosts_rejected(self):
        with pytest.raises(ConfigError, match="at least one host"):
            _from_dict({"hosts": [], "primary": "x", "forks": []})

    def test_duplicate_id_rejected(self):
        raw = _new_shape()
        raw["hosts"][1]["id"] = "internal"
        with pytest.raises(ConfigError, match="duplicate host id"):
            _from_dict(raw)

    def test_unknown_kind_rejected(self):
        raw = _new_shape()
        raw["hosts"][0]["kind"] = "codeforge"
        with pytest.raises(ConfigError, match="not a registered provider"):
            _from_dict(raw)

    def test_primary_must_reference_existing_host(self):
        raw = _new_shape(primary="ghost")
        with pytest.raises(ConfigError, match="does not match"):
            _from_dict(raw)

    def test_primary_must_be_mirror_capable(self):
        raw = _new_shape(primary="gh")
        with pytest.raises(ConfigError, match="cannot be a mirror source"):
            _from_dict(raw)

    def test_empty_forks_rejected(self):
        raw = _new_shape(forks=[])
        with pytest.raises(ConfigError, match="at least one host id"):
            _from_dict(raw)

    def test_fork_cannot_be_primary(self):
        raw = _new_shape(forks=["internal", "cloud"])
        with pytest.raises(ConfigError, match="cannot also be the primary"):
            _from_dict(raw)

    def test_unknown_fork_id_rejected(self):
        raw = _new_shape(forks=["cloud", "ghost"])
        with pytest.raises(ConfigError, match="unknown host id"):
            _from_dict(raw)

    def test_duplicate_forks_rejected(self):
        raw = _new_shape(forks=["cloud", "cloud"])
        with pytest.raises(ConfigError, match="duplicate"):
            _from_dict(raw)

    def test_missing_url_rejected(self):
        raw = _new_shape()
        raw["hosts"][0]["url"] = ""
        with pytest.raises(ConfigError, match="url is required"):
            _from_dict(raw)


# ──────────────────────────── Legacy migration ────────────────────────────


@pytest.fixture
def legacy_raw():
    return {
        "self_hosted_gitlab": {"url": "https://gitlab.example.com"},
        "gitlab": {
            "url": "https://gitlab.com",
            "managed_group_prefix": "my-managed",
        },
        "github": {"url": "https://api.github.com", "org": "acme"},
        "defaults": {"private": False, "group": "team"},
    }


class TestLegacyMigration:
    def test_detects_legacy_shape(self, legacy_raw):
        assert _is_legacy_shape(legacy_raw) is True
        assert _is_legacy_shape({"hosts": [], "primary": "x"}) is False

    def test_migration_creates_three_hosts(self, legacy_raw):
        cfg = _from_dict(legacy_raw)
        ids = [h.id for h in cfg.hosts]
        assert ids == ["self_hosted_gitlab", "gitlab", "github"]

    def test_self_hosted_url_preserved(self, legacy_raw):
        cfg = _from_dict(legacy_raw)
        assert cfg.host("self_hosted_gitlab").url == "https://gitlab.example.com"
        assert cfg.host("self_hosted_gitlab").options["add_timestamp"] is False

    def test_gitlab_managed_prefix_preserved(self, legacy_raw):
        cfg = _from_dict(legacy_raw)
        assert cfg.host("gitlab").options["managed_group_prefix"] == "my-managed"
        assert cfg.host("gitlab").options["add_timestamp"] is True

    def test_github_org_preserved(self, legacy_raw):
        cfg = _from_dict(legacy_raw)
        assert cfg.host("github").options["org"] == "acme"

    def test_implicit_primary_and_forks(self, legacy_raw):
        cfg = _from_dict(legacy_raw)
        assert cfg.primary == "self_hosted_gitlab"
        assert cfg.forks == ["gitlab", "github"]

    def test_defaults_preserved(self, legacy_raw):
        cfg = _from_dict(legacy_raw)
        assert cfg.defaults.private is False
        assert cfg.defaults.group == "team"

    def test_legacy_without_self_hosted_url_rejected(self):
        with pytest.raises(ConfigError, match="self_hosted_gitlab.url"):
            _from_dict({"self_hosted_gitlab": {}, "gitlab": {}, "github": {}})

    def test_legacy_defaults_for_optional_fields(self):
        cfg = _from_dict({"self_hosted_gitlab": {"url": "https://gl.x"}})
        assert cfg.host("gitlab").url == "https://gitlab.com"
        assert cfg.host("github").url == "https://api.github.com"
        assert cfg.host("gitlab").options["managed_group_prefix"] == "repo-syncer-managed-groups"

    def test_unknown_legacy_fields_preserved_in_options(self):
        # User had a custom field on a legacy host block — it should land in
        # `options` so future provider versions can pick it up.
        raw = {
            "self_hosted_gitlab": {"url": "https://gl.x", "verify_ssl": False},
            "github": {"url": "https://api.github.com", "org": "acme", "topic_prefix": "infra"},
        }
        cfg = _from_dict(raw)
        assert cfg.host("self_hosted_gitlab").options.get("verify_ssl") is False
        assert cfg.host("github").options.get("topic_prefix") == "infra"

    def test_legacy_add_timestamp_override_respected(self):
        # If the user set add_timestamp on the legacy self_hosted_gitlab block,
        # don't silently flip it back to the default.
        raw = {
            "self_hosted_gitlab": {"url": "https://gl.x", "add_timestamp": True},
            "gitlab": {"url": "https://gitlab.com", "add_timestamp": False},
        }
        cfg = _from_dict(raw)
        assert cfg.host("self_hosted_gitlab").options["add_timestamp"] is True
        assert cfg.host("gitlab").options["add_timestamp"] is False


class TestMigrationRoundTrip:
    def test_load_migrate_save_reload(self, tmp_path, legacy_raw):
        import yaml

        from hydra.config import load_config, save_config

        legacy_path = tmp_path / "config.yaml"
        legacy_path.write_text(yaml.safe_dump(legacy_raw))

        first = load_config(legacy_path)
        # After load, the file on disk should be in the new shape.
        rewritten = yaml.safe_load(legacy_path.read_text())
        assert "hosts" in rewritten
        assert "self_hosted_gitlab" not in rewritten
        # Re-load — should be a pure new-shape parse, no migration triggered.
        second = load_config(legacy_path)
        assert second.primary == first.primary
        assert second.forks == first.forks
        assert [h.id for h in second.hosts] == [h.id for h in first.hosts]
        # Save round-trip is idempotent.
        save_config(second, legacy_path)
        third = load_config(legacy_path)
        assert third.to_dict() == second.to_dict()

"""Migration 001: legacy 3-host shape → v2 N-fork shape.

Legacy shape (pre-0.0.5):
    self_hosted_gitlab: { url: ..., ... }
    gitlab: { url: ..., managed_group_prefix: ..., ... }
    github: { url: ..., org: ..., ... }
    defaults: { ... }

V2 shape:
    schema_version: 2
    hosts:
      - id: self_hosted_gitlab, kind: gitlab, url: ..., options: {...}
      - id: gitlab, kind: gitlab, url: ..., options: {...}
      - id: github, kind: github, url: ..., options: {...}
    primary: self_hosted_gitlab
    forks: [gitlab, github]
    defaults: { ... }

Unknown fields under each legacy host block are preserved verbatim under the
new `options` mapping.
"""

from __future__ import annotations

from typing import Any, Dict, List

from hydra.migrations.base import Migration, MigrationContext

LEGACY_TOP_LEVEL_KEYS = {"self_hosted_gitlab", "gitlab", "github"}
_LEGACY_KNOWN_FIELDS = {
    "self_hosted_gitlab": {"url"},
    "gitlab": {"url", "managed_group_prefix"},
    "github": {"url", "org"},
}


def is_legacy_shape(raw: Dict[str, Any]) -> bool:
    """True if `raw` looks like the pre-0.0.5 shape."""
    return "hosts" not in raw and any(k in raw for k in LEGACY_TOP_LEVEL_KEYS)


def _extras(block: Dict[str, Any], section: str) -> Dict[str, Any]:
    known = _LEGACY_KNOWN_FIELDS[section]
    return {k: v for k, v in block.items() if k not in known}


def apply(raw: Dict[str, Any], ctx: MigrationContext) -> Dict[str, Any]:
    # Idempotency: if this is invoked on already-migrated data, return it.
    if not is_legacy_shape(raw):
        out = dict(raw)
        out.setdefault("schema_version", 2)
        return out

    sh = raw.get("self_hosted_gitlab") or {}
    gl = raw.get("gitlab") or {}
    gh = raw.get("github") or {}
    df = raw.get("defaults") or {}

    if not sh.get("url"):
        # Surface as an explicit migration error rather than silently producing
        # a config that fails downstream validation with a vague message.
        from hydra.config import ConfigError

        raise ConfigError("self_hosted_gitlab.url is required in legacy config")

    sh_options: Dict[str, Any] = {"add_timestamp": bool(sh.get("add_timestamp", False))}
    sh_options.update(_extras(sh, "self_hosted_gitlab"))

    gl_options: Dict[str, Any] = {
        "managed_group_prefix": gl.get("managed_group_prefix", "repo-syncer-managed-groups"),
        "add_timestamp": bool(gl.get("add_timestamp", True)),
    }
    gl_options.update(_extras(gl, "gitlab"))

    gh_options: Dict[str, Any] = {"org": gh.get("org")}
    gh_options.update(_extras(gh, "github"))

    hosts: List[Dict[str, Any]] = [
        {"id": "self_hosted_gitlab", "kind": "gitlab", "url": sh["url"], "options": sh_options},
        {
            "id": "gitlab",
            "kind": "gitlab",
            "url": gl.get("url", "https://gitlab.com"),
            "options": gl_options,
        },
        {
            "id": "github",
            "kind": "github",
            "url": gh.get("url", "https://api.github.com"),
            "options": gh_options,
        },
    ]
    return {
        "schema_version": 2,
        "hosts": hosts,
        "primary": "self_hosted_gitlab",
        "forks": ["gitlab", "github"],
        "defaults": {
            "private": bool(df.get("private", True)),
            "group": df.get("group", "") or "",
        },
    }


MIGRATION = Migration(
    from_version=1,
    to_version=2,
    name="m001-legacy-to-v2",
    description="Convert pre-0.0.5 three-host config to N-fork shape with primary/forks.",
    apply=apply,
)

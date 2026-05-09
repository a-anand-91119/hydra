"""Versioned migration framework for hydra config files.

Each migration moves a raw config dict from `from_version` → `to_version`.
Migrations are append-only and idempotent: bugs are fixed by adding *another*
migration, never by editing a released one. This file's `_MIGRATIONS` list
is the canonical, ordered chain.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from hydra.migrations import m001_legacy_to_v2
from hydra.migrations.base import (
    AppliedMigration,
    Migration,
    MigrationContext,
)

# Bumped on every schema-changing migration. Token / keyring / non-shape
# changes do NOT bump this — they belong in `hydra/doctor/` checks instead.
CURRENT_SCHEMA_VERSION = 2


class MigrationChainError(Exception):
    pass


# Ordered chain. Append new migrations here as the schema evolves.
_MIGRATIONS: List[Migration] = [
    m001_legacy_to_v2.MIGRATION,
]


def _validate_chain() -> None:
    """Sanity-check the registry at import time. Surfaces dev errors loudly."""
    if not _MIGRATIONS:
        return  # nothing to validate
    seen_names: set = set()
    expected_from = _MIGRATIONS[0].from_version
    for m in _MIGRATIONS:
        if m.name in seen_names:
            raise MigrationChainError(f"duplicate migration name: {m.name!r}")
        seen_names.add(m.name)
        if m.from_version != expected_from:
            raise MigrationChainError(
                f"migration chain gap: expected from_version={expected_from}, "
                f"got {m.from_version} at {m.name!r}"
            )
        if m.to_version != m.from_version + 1:
            raise MigrationChainError(
                f"{m.name!r}: to_version ({m.to_version}) must be from_version + 1 "
                f"({m.from_version + 1})"
            )
        expected_from = m.to_version
    if _MIGRATIONS[-1].to_version != CURRENT_SCHEMA_VERSION:
        raise MigrationChainError(
            f"chain ends at version {_MIGRATIONS[-1].to_version}, "
            f"but CURRENT_SCHEMA_VERSION is {CURRENT_SCHEMA_VERSION}"
        )


_validate_chain()


def detect_version(raw: Dict[str, Any]) -> int:
    """Determine the schema version of a raw config dict.

    Rules:
      - Explicit `schema_version: N` wins.
      - Otherwise, presence of `hosts:` implies version 2 (back-compat for
        configs written by 0.0.5 between the N-fork ship and this PR).
      - Otherwise, presence of any legacy top-level key implies version 1.
      - Empty / unknown shapes default to CURRENT_SCHEMA_VERSION (so a fresh
        `Config()` with no file doesn't trigger a migration).
    """
    explicit = raw.get("schema_version")
    if isinstance(explicit, int) and explicit > 0:
        return explicit
    if "hosts" in raw:
        return 2
    if any(k in raw for k in m001_legacy_to_v2.LEGACY_TOP_LEVEL_KEYS):
        return 1
    return CURRENT_SCHEMA_VERSION


def pending(raw: Dict[str, Any]) -> List[Migration]:
    """Migrations that would run on `raw` to reach CURRENT_SCHEMA_VERSION."""
    current = detect_version(raw)
    return [m for m in _MIGRATIONS if m.from_version >= current]


def run(
    raw: Dict[str, Any], ctx: MigrationContext
) -> Tuple[Dict[str, Any], List[AppliedMigration]]:
    """Apply all pending migrations. Idempotent.

    Returns the final dict and an ordered list of what ran. After the chain,
    ``schema_version`` always equals ``CURRENT_SCHEMA_VERSION``.
    """
    applied: List[AppliedMigration] = []
    out: Dict[str, Any] = dict(raw)
    current = detect_version(out)
    for migration in _MIGRATIONS:
        if migration.from_version < current:
            continue
        if migration.from_version > current:
            # Should never happen given _validate_chain() — defensive.
            raise MigrationChainError(
                f"cannot reach migration {migration.name!r} from version {current}"
            )
        out = migration.apply(out, ctx)
        # Post-condition: migration must set schema_version to its to_version.
        if out.get("schema_version") != migration.to_version:
            raise MigrationChainError(
                f"{migration.name!r} did not set schema_version to "
                f"{migration.to_version} (got {out.get('schema_version')!r})"
            )
        applied.append(
            AppliedMigration(
                name=migration.name,
                from_version=migration.from_version,
                to_version=migration.to_version,
            )
        )
        current = migration.to_version

    # Backfill: a v2 file written before schema_version was added still needs
    # the field set so future loads skip detection-by-shape.
    if "schema_version" not in out:
        out["schema_version"] = CURRENT_SCHEMA_VERSION
    return out, applied


def all_migrations() -> List[Migration]:
    """Read-only view of the registered chain (for diagnostics)."""
    return list(_MIGRATIONS)


__all__ = [
    "AppliedMigration",
    "CURRENT_SCHEMA_VERSION",
    "Migration",
    "MigrationChainError",
    "MigrationContext",
    "all_migrations",
    "detect_version",
    "pending",
    "run",
]

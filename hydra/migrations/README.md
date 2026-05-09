# Hydra config migrations

Each file `mNNN_<short-name>.py` defines exactly one `Migration` that moves a
raw config dict from `from_version` → `from_version + 1`. The chain is
registered (in order) inside `hydra/migrations/__init__.py:_MIGRATIONS`.

## Rules

1. **Append-only.** Never edit a migration after release. Bugs are fixed by
   adding *another* migration (see Django/Alembic for the model).
2. **Idempotent.** `apply()` called twice in a row must produce the same
   result as one call. Defensive against crashed/partial runs.
3. **Sets `schema_version`.** Every `apply()` must set
   `out["schema_version"] = migration.to_version`. The runner asserts this.
4. **No network or external services.** Migrations are pure dict
   transforms (with the optional `MigrationContext` for keyring/env access).
5. **Bumps go in `__init__.py`.** Add the new migration to `_MIGRATIONS`
   and bump `CURRENT_SCHEMA_VERSION`. The chain is validated at import — a
   gap or off-by-one raises `MigrationChainError` immediately.

## Bumping `CURRENT_SCHEMA_VERSION`

Bump it **only** for shape changes that the parser must understand
differently. Token storage changes, keyring renames, env-var hygiene — none
of these are schema changes. They go through `hydra/doctor/` checks/fixes.

## Adding a new migration

```python
# hydra/migrations/m002_rename_managed_prefix.py
from hydra.migrations.base import Migration, MigrationContext


def apply(raw, ctx: MigrationContext):
    out = dict(raw)
    for host in out.get("hosts", []):
        opts = host.setdefault("options", {})
        if "managed_group_prefix" in opts:
            opts["managed_namespace_prefix"] = opts.pop("managed_group_prefix")
    out["schema_version"] = 3
    return out


MIGRATION = Migration(
    from_version=2,
    to_version=3,
    name="m002-rename-managed-prefix",
    description="Rename managed_group_prefix → managed_namespace_prefix.",
    apply=apply,
)
```

Then in `__init__.py`:

```python
from hydra.migrations import m002_rename_managed_prefix

CURRENT_SCHEMA_VERSION = 3

_MIGRATIONS: List[Migration] = [
    m001_legacy_to_v2.MIGRATION,
    m002_rename_managed_prefix.MIGRATION,
]
```

## Testing

Each new migration must come with:
- A round-trip test: input pre-state → `apply()` → expected post-state.
- An idempotency test: applying twice equals applying once.
- An end-to-end doctor test verifying `hydra doctor --fix` upgrades a config
  written at the previous version.

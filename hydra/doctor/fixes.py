"""Fix handlers — applied with `hydra doctor --fix`.

Each handler corresponds to one or more `Finding.fix_id` values. Handlers
mutate state and return a one-line description of what they did.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List

import yaml

from hydra.config import _parse as _parse_config
from hydra.config import save_config, write_backup


@dataclass
class FixOutcome:
    fix_id: str
    success: bool
    message: str


@dataclass
class FixHandler:
    fix_id: str
    description: str
    apply: Callable[[FixContext], FixOutcome]


@dataclass
class FixContext:
    cfg_path: Path
    raw: Dict[str, Any]


# ──────────────────────────── Handler implementations ──────────────────


def _apply_run_migrations(ctx: FixContext) -> FixOutcome:
    from hydra.migrations import MigrationContext
    from hydra.migrations import run as run_migrations

    backup = write_backup(ctx.cfg_path)
    mig_ctx = MigrationContext(cfg_path=ctx.cfg_path)
    migrated, applied = run_migrations(ctx.raw, mig_ctx)
    if not applied:
        return FixOutcome(
            fix_id="run-migrations",
            success=True,
            message="no migrations were pending",
        )
    cfg = _parse_config(migrated)
    save_config(cfg, ctx.cfg_path)
    names = ", ".join(a.name for a in applied)
    suffix = f" (backup: {backup.name})" if backup else ""
    return FixOutcome(
        fix_id="run-migrations",
        success=True,
        message=f"applied {len(applied)} migration(s): {names}{suffix}",
    )


def _apply_show_legacy_env_rename(ctx: FixContext) -> FixOutcome:
    # Doctor never mutates the user's shell. Just confirm the rename
    # snippet was already shown in the warning's `details`.
    return FixOutcome(
        fix_id="show-legacy-env-rename",
        success=True,
        message="rename snippet shown in finding details (no automatic action taken)",
    )


_HANDLERS: List[FixHandler] = [
    FixHandler(
        fix_id="run-migrations",
        description="Apply all pending schema migrations.",
        apply=_apply_run_migrations,
    ),
    FixHandler(
        fix_id="show-legacy-env-rename",
        description="Display the env-var rename snippet (no mutation).",
        apply=_apply_show_legacy_env_rename,
    ),
]


def get_handler(fix_id: str) -> FixHandler:
    for h in _HANDLERS:
        if h.fix_id == fix_id:
            return h
    raise KeyError(f"no fix handler registered for {fix_id!r}")


def all_handlers() -> List[FixHandler]:
    return list(_HANDLERS)


def reload_raw(cfg_path: Path) -> Dict[str, Any]:
    """Re-read the file from disk after fixes have run."""
    if not cfg_path.exists():
        return {}
    with cfg_path.open("r") as f:
        return yaml.safe_load(f) or {}

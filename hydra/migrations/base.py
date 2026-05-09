from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional


@dataclass
class MigrationContext:
    """Side-channel state a migration may need beyond the raw config dict."""

    cfg_path: Optional[object] = None  # pathlib.Path | None — kept loose to avoid import cycles
    keyring_get: Optional[Callable[[str], Optional[str]]] = None
    keyring_set: Optional[Callable[[str, str], None]] = None
    keyring_delete: Optional[Callable[[str], None]] = None
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass
class Migration:
    """A single, idempotent step that moves a config from N → N+1."""

    from_version: int
    to_version: int
    name: str
    description: str
    apply: Callable[[Dict[str, Any], MigrationContext], Dict[str, Any]]


@dataclass
class AppliedMigration:
    """Record of a migration that ran during this load."""

    name: str
    from_version: int
    to_version: int
    notes: List[str] = field(default_factory=list)

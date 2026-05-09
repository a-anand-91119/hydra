from __future__ import annotations

import os
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

from hydra.providers.base import HostSpec

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "hydra" / "config.yaml"


class ConfigError(Exception):
    pass


# Re-export so callers can `from hydra.config import HostSpec` without crossing
# into the providers package.
__all__ = [
    "Config",
    "ConfigError",
    "DEFAULT_CONFIG_PATH",
    "Defaults",
    "HostSpec",
    "load_config",
    "load_config_or_default",
    "resolve_config_path",
    "save_config",
    "write_backup",
]


@dataclass
class Defaults:
    private: bool = True
    group: str = ""


@dataclass
class Config:
    hosts: List[HostSpec] = field(default_factory=list)
    primary: str = ""
    forks: List[str] = field(default_factory=list)
    defaults: Defaults = field(default_factory=Defaults)

    def host(self, host_id: str) -> HostSpec:
        for h in self.hosts:
            if h.id == host_id:
                return h
        raise KeyError(host_id)

    def primary_host(self) -> HostSpec:
        return self.host(self.primary)

    def fork_hosts(self) -> List[HostSpec]:
        return [self.host(fid) for fid in self.forks]

    def to_dict(self) -> Dict[str, Any]:
        # Lazy import — `migrations` package transitively imports config.
        from hydra.migrations import CURRENT_SCHEMA_VERSION

        return {
            "schema_version": CURRENT_SCHEMA_VERSION,
            "hosts": [asdict(h) for h in self.hosts],
            "primary": self.primary,
            "forks": list(self.forks),
            "defaults": asdict(self.defaults),
        }


# ──────────────────────────── Path resolution ────────────────────────────


def resolve_config_path(explicit: Optional[Path] = None) -> Path:
    if explicit is not None:
        return explicit
    env = os.environ.get("HYDRA_CONFIG")
    if env:
        return Path(env).expanduser()
    return DEFAULT_CONFIG_PATH


def write_backup(cfg_path: Path) -> Optional[Path]:
    """Copy the existing config to a timestamped sibling. Returns the backup path
    (or None if the source doesn't exist)."""
    if not cfg_path.exists():
        return None
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = cfg_path.with_suffix(cfg_path.suffix + f".bak-{ts}")
    # If two writes happen within the same second, append a counter.
    counter = 1
    while backup_path.exists():
        backup_path = cfg_path.with_suffix(cfg_path.suffix + f".bak-{ts}.{counter}")
        counter += 1
    shutil.copy2(cfg_path, backup_path)
    return backup_path


def load_config(path: Optional[Path] = None) -> Config:
    cfg_path = resolve_config_path(path)
    if not cfg_path.exists():
        raise ConfigError(f"No config file at {cfg_path}. Run `hydra configure` to create one.")
    with cfg_path.open("r") as f:
        raw = yaml.safe_load(f) or {}
    cfg, applied = _from_dict_with_migration(raw, cfg_path=cfg_path)
    if applied:
        # Persist the migrated shape so future loads skip the chain.
        try:
            write_backup(cfg_path)
            save_config(cfg, path)
        except OSError:
            # Read-only filesystem etc. — keep the in-memory result valid;
            # `hydra doctor` will surface the unwritten state.
            pass
    return cfg


def load_config_or_default(path: Optional[Path] = None) -> Config:
    cfg_path = resolve_config_path(path)
    if not cfg_path.exists():
        return Config()
    with cfg_path.open("r") as f:
        raw = yaml.safe_load(f) or {}
    cfg, _ = _from_dict_with_migration(raw, cfg_path=cfg_path)
    return cfg


def save_config(cfg: Config, path: Optional[Path] = None) -> Path:
    cfg_path = resolve_config_path(path)
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    with cfg_path.open("w") as f:
        yaml.safe_dump(cfg.to_dict(), f, sort_keys=False)
    return cfg_path


# ──────────────────────────── Parsing ────────────────────────────


def _from_dict_with_migration(
    raw: Dict[str, Any], *, cfg_path: Optional[Path] = None
) -> tuple[Config, list]:
    """Run pending migrations then parse to Config. Returns (cfg, applied)."""
    from hydra import providers as providers_mod
    from hydra.migrations import MigrationContext
    from hydra.migrations import run as run_migrations

    if not providers_mod.kinds():
        providers_mod.bootstrap()

    ctx = MigrationContext(cfg_path=cfg_path, env=os.environ)
    migrated, applied = run_migrations(raw, ctx)
    return _parse(migrated), applied


def _from_dict(raw: Dict[str, Any]) -> Config:
    """Parse raw dict (after any migrations) into Config.

    Kept for tests and any caller that already has migrated data.
    """
    cfg, _ = _from_dict_with_migration(raw)
    return cfg


def _parse(raw: Dict[str, Any]) -> Config:
    """Validate + construct a Config from a *post-migration* raw dict."""
    from hydra import providers as providers_mod

    hosts_raw = raw.get("hosts") or []
    if not hosts_raw:
        raise ConfigError("config must declare at least one host under `hosts`")

    seen_ids: Set[str] = set()
    hosts: List[HostSpec] = []
    for i, h in enumerate(hosts_raw):
        hid = (h.get("id") or "").strip()
        kind = (h.get("kind") or "").strip()
        url = (h.get("url") or "").strip()
        opts = h.get("options") or {}
        if not hid:
            raise ConfigError(f"hosts[{i}].id is required")
        if hid in seen_ids:
            raise ConfigError(f"duplicate host id: {hid!r}")
        seen_ids.add(hid)
        if not kind:
            raise ConfigError(f"hosts[{hid}].kind is required")
        if kind not in providers_mod.kinds():
            raise ConfigError(
                f"hosts[{hid}].kind={kind!r} is not a registered provider "
                f"(known: {providers_mod.kinds()})"
            )
        if not url:
            raise ConfigError(f"hosts[{hid}].url is required")
        if not isinstance(opts, dict):
            raise ConfigError(f"hosts[{hid}].options must be a mapping")
        hosts.append(HostSpec(id=hid, kind=kind, url=url, options=dict(opts)))

    primary = (raw.get("primary") or "").strip()
    forks = list(raw.get("forks") or [])
    if not primary:
        raise ConfigError("`primary` is required and must reference a host id")
    if primary not in seen_ids:
        raise ConfigError(f"primary={primary!r} does not match any configured host id")
    primary_kind = next(h.kind for h in hosts if h.id == primary)
    if not providers_mod.capabilities_for(primary_kind).supports_mirror_source:
        raise ConfigError(
            f"primary host {primary!r} (kind={primary_kind}) cannot be a mirror source. "
            f"Pick a GitLab-family host as primary."
        )
    if not forks:
        raise ConfigError("`forks` must declare at least one host id")
    if len(forks) != len(set(forks)):
        raise ConfigError("`forks` contains duplicate ids")
    for fid in forks:
        if fid not in seen_ids:
            raise ConfigError(f"forks contains unknown host id: {fid!r}")
        if fid == primary:
            raise ConfigError(f"fork {fid!r} cannot also be the primary")

    df = raw.get("defaults") or {}
    defaults = Defaults(
        private=bool(df.get("private", True)),
        group=df.get("group", "") or "",
    )
    return Config(hosts=hosts, primary=primary, forks=forks, defaults=defaults)


# ──────────────────────────── Back-compat shims ────────────────────────────
# Older test imports may still reference these — keep stable, but defer to
# the migration module for the actual logic.


def _is_legacy_shape(raw: Dict[str, Any]) -> bool:
    from hydra.migrations.m001_legacy_to_v2 import is_legacy_shape

    return is_legacy_shape(raw)

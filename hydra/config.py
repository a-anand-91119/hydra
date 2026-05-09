from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "hydra" / "config.yaml"


class ConfigError(Exception):
    pass


@dataclass
class HostConfig:
    url: str


@dataclass
class GitLabCloudConfig(HostConfig):
    url: str = "https://gitlab.com"
    managed_group_prefix: str = "repo-syncer-managed-groups"


@dataclass
class GitHubConfig(HostConfig):
    url: str = "https://api.github.com"
    org: str | None = None


@dataclass
class Defaults:
    private: bool = True
    group: str = ""


@dataclass
class Config:
    self_hosted_gitlab: HostConfig = field(default_factory=lambda: HostConfig(url=""))
    gitlab: GitLabCloudConfig = field(default_factory=GitLabCloudConfig)
    github: GitHubConfig = field(default_factory=GitHubConfig)
    defaults: Defaults = field(default_factory=Defaults)

    def to_dict(self) -> dict:
        return asdict(self)


def resolve_config_path(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit
    env = os.environ.get("HYDRA_CONFIG")
    if env:
        return Path(env).expanduser()
    return DEFAULT_CONFIG_PATH


def load_config(path: Path | None = None) -> Config:
    cfg_path = resolve_config_path(path)
    if not cfg_path.exists():
        raise ConfigError(f"No config file at {cfg_path}. Run `hydra configure` to create one.")
    with cfg_path.open("r") as f:
        raw = yaml.safe_load(f) or {}
    return _from_dict(raw)


def load_config_or_default(path: Path | None = None) -> Config:
    cfg_path = resolve_config_path(path)
    if not cfg_path.exists():
        return Config()
    with cfg_path.open("r") as f:
        raw = yaml.safe_load(f) or {}
    return _from_dict(raw)


def save_config(cfg: Config, path: Path | None = None) -> Path:
    cfg_path = resolve_config_path(path)
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    with cfg_path.open("w") as f:
        yaml.safe_dump(cfg.to_dict(), f, sort_keys=False)
    return cfg_path


def _from_dict(raw: dict) -> Config:
    sh = raw.get("self_hosted_gitlab") or {}
    gl = raw.get("gitlab") or {}
    gh = raw.get("github") or {}
    df = raw.get("defaults") or {}

    if not sh.get("url"):
        raise ConfigError("self_hosted_gitlab.url is required in config")

    return Config(
        self_hosted_gitlab=HostConfig(url=sh["url"]),
        gitlab=GitLabCloudConfig(
            url=gl.get("url", "https://gitlab.com"),
            managed_group_prefix=gl.get("managed_group_prefix", "repo-syncer-managed-groups"),
        ),
        github=GitHubConfig(
            url=gh.get("url", "https://api.github.com"),
            org=gh.get("org"),
        ),
        defaults=Defaults(
            private=bool(df.get("private", True)),
            group=df.get("group", "") or "",
        ),
    )

"""Helpers that operate on configured HostSpecs and mirror URLs.

Lives in its own module (rather than utils.py) because both the CLI layer
and the planner need these — and neither is the natural owner. Keeps the
planner free of CLI imports without resorting to per-module duplication.
"""

from __future__ import annotations

from typing import List, Optional
from urllib.parse import urlparse

from hydra.config import HostSpec


def spec_mirror_hostname(spec: HostSpec) -> Optional[str]:
    """The hostname mirror URLs use for this host (NOT the API base).

    GitHub's API lives at api.github.com but git push URLs use github.com.
    GitLab uses the same hostname for both. Self-hosted GitHub Enterprise
    typically also uses the same hostname for API and git, so the special
    case is limited to the public api.github.com.
    """
    try:
        host = (urlparse(spec.url).hostname or "").lower()
    except ValueError:
        return None
    if not host:
        return None
    if spec.kind == "github" and host == "api.github.com":
        return "github.com"
    return host


def match_fork(mirror_url: str, forks: List[HostSpec]) -> Optional[HostSpec]:
    """Match a mirror URL to a configured fork by exact hostname (case-insensitive).

    Substring matching would be unsafe (e.g. ``gitlab.com`` would match
    ``evilgitlab.com.attacker.example``).
    """
    try:
        mirror_host = (urlparse(mirror_url).hostname or "").lower()
    except ValueError:
        return None
    if not mirror_host:
        return None
    for spec in forks:
        spec_host = spec_mirror_hostname(spec)
        if spec_host and spec_host == mirror_host:
            return spec
    return None

"""Filesystem paths for mutable runtime state.

The journal lives under $XDG_STATE_HOME (default ~/.local/state), not under
~/.config — XDG reserves the config dir for static, user-edited files.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def xdg_state_home() -> Path:
    explicit = os.environ.get("XDG_STATE_HOME")
    if explicit:
        return Path(explicit).expanduser()
    return Path.home() / ".local" / "state"


def journal_path(explicit: Optional[Path] = None) -> Path:
    if explicit is not None:
        return explicit
    env = os.environ.get("HYDRA_JOURNAL")
    if env:
        return Path(env).expanduser()
    return xdg_state_home() / "hydra" / "journal.db"

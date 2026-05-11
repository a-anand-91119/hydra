"""Tests for cli._refresh_status thread-pool behavior:
- mirror fetches happen across threads
- journal writes stay on the calling (main) thread
- max_workers=1 reproduces sequential behavior
"""

from __future__ import annotations

import threading
import time
from typing import List

from rich.console import Console

from hydra import cli as cli_mod
from hydra import journal as journal_mod
from hydra.config import Config, Defaults, HostSpec
from hydra.providers.base import MirrorInfo


def _make_cfg() -> Config:
    return Config(
        hosts=[
            HostSpec(id="primary", kind="gitlab", url="https://primary.gl"),
            HostSpec(id="gh", kind="github", url="https://api.github.com"),
        ],
        primary="primary",
        forks=["gh"],
        defaults=Defaults(private=True, group=""),
    )


def _seed_journal(n: int) -> List[int]:
    """Insert n journal repos with one mirror each. Returns repo db ids."""
    ids: List[int] = []
    with journal_mod.journal() as j:
        for i in range(n):
            rid = j.record_repo(
                name=f"r{i}",
                primary_host_id="primary",
                primary_repo_id=1000 + i,
                primary_repo_url=f"https://primary.gl/r{i}.git",
            )
            j.record_mirror(
                repo_id=rid,
                target_host_id="gh",
                target_repo_url=f"https://github.com/me/r{i}.git",
                push_mirror_id=2000 + i,
            )
            ids.append(rid)
    return ids


class _FakePrimary:
    """Implements enough of MirrorSource for the runtime_checkable isinstance."""

    def __init__(self, *, sleep_s: float = 0.0):
        self.sleep_s = sleep_s
        self.fetch_threads: List[int] = []
        self.spec = HostSpec(id="primary", kind="gitlab", url="https://primary.gl")
        self.capabilities = None  # not consulted in this test path

    def ensure_namespace(self, *, group_path, token): ...  # pragma: no cover
    def create_repo(self, **kwargs): ...  # pragma: no cover
    def add_outbound_mirror(self, **kwargs): ...  # pragma: no cover
    def replace_outbound_mirror(self, **kwargs): ...  # pragma: no cover
    def find_project(self, **kwargs): ...  # pragma: no cover
    def list_projects_with_mirrors(self, **kwargs): ...  # pragma: no cover

    def list_mirrors(self, *, token, primary_repo):
        if self.sleep_s:
            time.sleep(self.sleep_s)
        self.fetch_threads.append(threading.get_ident())
        # Return a "matching" mirror for whatever the journal recorded.
        idx = primary_repo.project_id - 1000
        return [
            MirrorInfo(
                id=2000 + idx,
                url=f"https://github.com/me/r{idx}.git",
                enabled=True,
                last_update_status="success",
                last_update_at="2026-01-01T00:00:00Z",
                last_error=None,
            )
        ]


def test_parallel_refresh_uses_multiple_threads(monkeypatch):
    cfg = _make_cfg()
    _seed_journal(8)

    # Sleep is here only to widen the window for thread interleaving — we no
    # longer assert wall-clock, just that the pool actually used >1 thread.
    # CI runners can be wildly contended; timing-based assertions go flaky.
    primary = _FakePrimary(sleep_s=0.05)
    monkeypatch.setattr(cli_mod.providers_mod, "get", lambda kind: lambda spec: primary)
    monkeypatch.setattr(cli_mod.secrets_mod, "get_token", lambda *a, **k: "tok")

    console = Console(record=True, width=120)
    with journal_mod.journal() as j:
        cli_mod._refresh_status(cfg=cfg, journal=j, console=console, max_workers=8)

    assert len(primary.fetch_threads) == 8
    assert len(set(primary.fetch_threads)) > 1, "all fetches landed on one thread"


def test_journal_writes_stay_on_main_thread(monkeypatch):
    cfg = _make_cfg()
    _seed_journal(4)

    primary = _FakePrimary(sleep_s=0.01)
    monkeypatch.setattr(cli_mod.providers_mod, "get", lambda kind: lambda spec: primary)
    monkeypatch.setattr(cli_mod.secrets_mod, "get_token", lambda *a, **k: "tok")

    main_thread = threading.get_ident()
    write_threads: List[int] = []

    real_update = journal_mod.Journal.update_mirror_status
    real_touch = journal_mod.Journal.touch_repo_scanned

    def spy_update(self, **kwargs):
        write_threads.append(threading.get_ident())
        return real_update(self, **kwargs)

    def spy_touch(self, **kwargs):
        write_threads.append(threading.get_ident())
        return real_touch(self, **kwargs)

    monkeypatch.setattr(journal_mod.Journal, "update_mirror_status", spy_update)
    monkeypatch.setattr(journal_mod.Journal, "touch_repo_scanned", spy_touch)

    with journal_mod.journal() as j:
        cli_mod._refresh_status(cfg=cfg, journal=j, console=Console(), max_workers=4)

    assert write_threads, "no journal writes recorded"
    assert all(tid == main_thread for tid in write_threads)


def test_max_workers_one_is_sequential(monkeypatch):
    cfg = _make_cfg()
    _seed_journal(3)

    primary = _FakePrimary(sleep_s=0.0)
    monkeypatch.setattr(cli_mod.providers_mod, "get", lambda kind: lambda spec: primary)
    monkeypatch.setattr(cli_mod.secrets_mod, "get_token", lambda *a, **k: "tok")

    with journal_mod.journal() as j:
        cli_mod._refresh_status(cfg=cfg, journal=j, console=Console(), max_workers=1)

    assert len(set(primary.fetch_threads)) == 1

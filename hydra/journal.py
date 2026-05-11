"""Local SQLite journal of hydra-created repos and their mirrors.

The journal is pure local state — never a source of truth. Cross-check against
the primary host with `hydra scan` or `hydra list --refresh`.

Schema is migrated forward on open(). To change shape, append a tuple to
`_MIGRATIONS` — never edit a released one.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Tuple

from hydra.paths import journal_path

# Each migration is (target_version, [statements]). Append-only.
_MIGRATIONS: List[Tuple[int, List[str]]] = [
    (
        1,
        [
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)",
            """CREATE TABLE IF NOT EXISTS repos (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                primary_host_id TEXT NOT NULL,
                primary_repo_id INTEGER NOT NULL,
                primary_repo_url TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_scanned_at TEXT,
                state TEXT NOT NULL DEFAULT 'active',
                UNIQUE(primary_host_id, primary_repo_id)
            )""",
            """CREATE TABLE IF NOT EXISTS mirrors (
                id INTEGER PRIMARY KEY,
                repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
                target_host_id TEXT NOT NULL,
                target_repo_id TEXT,
                target_repo_url TEXT NOT NULL,
                push_mirror_id INTEGER NOT NULL,
                last_status TEXT,
                last_error TEXT,
                last_update_at TEXT,
                UNIQUE(repo_id, target_host_id)
            )""",
        ],
    ),
]


SCHEMA_VERSION = _MIGRATIONS[-1][0] if _MIGRATIONS else 0


@dataclass
class JournalMirror:
    id: int
    repo_id: int
    target_host_id: str
    target_repo_id: Optional[str]
    target_repo_url: str
    push_mirror_id: int
    last_status: Optional[str] = None
    last_error: Optional[str] = None
    last_update_at: Optional[str] = None


@dataclass
class JournalRepo:
    id: int
    name: str
    primary_host_id: str
    primary_repo_id: int
    primary_repo_url: str
    created_at: str
    last_scanned_at: Optional[str] = None
    state: str = "active"
    mirrors: List[JournalMirror] = field(default_factory=list)


# Scan diff result types — kept here so cli/scan can build them in tests without
# hitting any network.
@dataclass
class PrimaryRepoSnapshot:
    """What the primary host says about one project, captured during scan."""

    repo_id: int
    repo_url: str
    name: str
    mirror_push_ids: List[int]


@dataclass
class ScanDiff:
    unknown: List[PrimaryRepoSnapshot] = field(default_factory=list)  # on primary, not in journal
    missing: List[JournalRepo] = field(default_factory=list)  # in journal, gone from primary
    drift: List[Tuple[JournalRepo, PrimaryRepoSnapshot]] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return not (self.unknown or self.missing or self.drift)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Journal:
    """Thin wrapper around a SQLite connection. Migrates schema on open()."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    # ── Writes ───────────────────────────────────────────────────────────

    def record_repo(
        self,
        *,
        name: str,
        primary_host_id: str,
        primary_repo_id: int,
        primary_repo_url: str,
    ) -> int:
        """Insert (or update) a repo row. Returns the journal row id."""
        now = _now_iso()
        cur = self._conn.execute(
            """INSERT INTO repos
                   (name, primary_host_id, primary_repo_id, primary_repo_url,
                    created_at, state)
               VALUES (?, ?, ?, ?, ?, 'active')
               ON CONFLICT(primary_host_id, primary_repo_id) DO UPDATE SET
                   name = excluded.name,
                   primary_repo_url = excluded.primary_repo_url,
                   state = 'active'
               RETURNING id""",
            (name, primary_host_id, primary_repo_id, primary_repo_url, now),
        )
        row = cur.fetchone()
        self._conn.commit()
        return int(row[0])

    def record_mirror(
        self,
        *,
        repo_id: int,
        target_host_id: str,
        target_repo_url: str,
        push_mirror_id: int,
        target_repo_id: Optional[str] = None,
    ) -> int:
        # On conflict, only wipe cached status when push_mirror_id actually
        # changed (a genuine re-create). Re-recording the same mirror
        # preserves history.
        cur = self._conn.execute(
            """INSERT INTO mirrors
                   (repo_id, target_host_id, target_repo_id, target_repo_url,
                    push_mirror_id)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(repo_id, target_host_id) DO UPDATE SET
                   target_repo_url = excluded.target_repo_url,
                   target_repo_id = excluded.target_repo_id,
                   push_mirror_id = excluded.push_mirror_id,
                   last_status = CASE
                       WHEN mirrors.push_mirror_id = excluded.push_mirror_id
                       THEN mirrors.last_status ELSE NULL
                   END,
                   last_error = CASE
                       WHEN mirrors.push_mirror_id = excluded.push_mirror_id
                       THEN mirrors.last_error ELSE NULL
                   END,
                   last_update_at = CASE
                       WHEN mirrors.push_mirror_id = excluded.push_mirror_id
                       THEN mirrors.last_update_at ELSE NULL
                   END
               RETURNING id""",
            (repo_id, target_host_id, target_repo_id, target_repo_url, push_mirror_id),
        )
        row = cur.fetchone()
        self._conn.commit()
        return int(row[0])

    def update_mirror_status(
        self,
        *,
        mirror_db_id: int,
        last_status: Optional[str],
        last_error: Optional[str],
        last_update_at: Optional[str],
    ) -> None:
        self._conn.execute(
            """UPDATE mirrors
                  SET last_status = ?, last_error = ?, last_update_at = ?
                WHERE id = ?""",
            (last_status, last_error, last_update_at, mirror_db_id),
        )
        self._conn.commit()

    def update_mirror_push_id(self, *, mirror_db_id: int, new_push_mirror_id: int) -> None:
        self._conn.execute(
            "UPDATE mirrors SET push_mirror_id = ? WHERE id = ?",
            (new_push_mirror_id, mirror_db_id),
        )
        self._conn.commit()

    def touch_repo_scanned(self, *, repo_db_id: int) -> None:
        self._conn.execute(
            "UPDATE repos SET last_scanned_at = ? WHERE id = ?",
            (_now_iso(), repo_db_id),
        )
        self._conn.commit()

    # ── Reads ────────────────────────────────────────────────────────────

    def list_repos(self) -> List[JournalRepo]:
        repo_rows = self._conn.execute(
            """SELECT id, name, primary_host_id, primary_repo_id, primary_repo_url,
                      created_at, last_scanned_at, state
                 FROM repos
                ORDER BY name COLLATE NOCASE"""
        ).fetchall()
        if not repo_rows:
            return []
        ids = [r[0] for r in repo_rows]
        # Single query, then bucket by repo id. Avoids N+1.
        mirror_rows = self._conn.execute(
            f"""SELECT id, repo_id, target_host_id, target_repo_id, target_repo_url,
                       push_mirror_id, last_status, last_error, last_update_at
                  FROM mirrors
                 WHERE repo_id IN ({",".join("?" * len(ids))})
                 ORDER BY target_host_id COLLATE NOCASE""",
            ids,
        ).fetchall()
        mirrors_by_repo: dict = {}
        for m in mirror_rows:
            mirrors_by_repo.setdefault(m[1], []).append(
                JournalMirror(
                    id=m[0],
                    repo_id=m[1],
                    target_host_id=m[2],
                    target_repo_id=m[3],
                    target_repo_url=m[4],
                    push_mirror_id=m[5],
                    last_status=m[6],
                    last_error=m[7],
                    last_update_at=m[8],
                )
            )
        return [
            JournalRepo(
                id=r[0],
                name=r[1],
                primary_host_id=r[2],
                primary_repo_id=r[3],
                primary_repo_url=r[4],
                created_at=r[5],
                last_scanned_at=r[6],
                state=r[7],
                mirrors=mirrors_by_repo.get(r[0], []),
            )
            for r in repo_rows
        ]

    def mirrors_for_target_host(
        self, target_host_id: str
    ) -> List[Tuple[JournalRepo, JournalMirror]]:
        out: List[Tuple[JournalRepo, JournalMirror]] = []
        for repo in self.list_repos():
            for m in repo.mirrors:
                if m.target_host_id == target_host_id:
                    out.append((repo, m))
        return out


# ── Open / migrate ──────────────────────────────────────────────────────


def open_journal(path: Optional[Path] = None) -> Journal:
    """Open (creating if needed) and migrate the journal."""
    p = journal_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.execute("PRAGMA foreign_keys = ON")
    _migrate(conn)
    return Journal(conn)


@contextmanager
def journal(path: Optional[Path] = None) -> Iterator[Journal]:
    j = open_journal(path)
    try:
        yield j
    finally:
        j.close()


def _migrate(conn: sqlite3.Connection) -> None:
    # Always-create the version table so we can read current.
    conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
    row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    current = int(row[0]) if row and row[0] is not None else 0
    for target_version, statements in _MIGRATIONS:
        if target_version <= current:
            continue
        for stmt in statements:
            conn.execute(stmt)
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (target_version,))
        conn.commit()
        current = target_version


def current_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


# ── Pure diff logic ─────────────────────────────────────────────────────


def scan_diff(
    journal_repos: Sequence[JournalRepo],
    primary_snapshot: Sequence[PrimaryRepoSnapshot],
    *,
    primary_host_id: str,
) -> ScanDiff:
    """Compute a diff between the journal and what the primary host reports.

    `journal_repos`        — only rows whose primary_host_id == primary_host_id are considered.
    `primary_snapshot`     — every project on the primary that has push-mirrors.
    """
    journal_by_id: dict = {
        r.primary_repo_id: r for r in journal_repos if r.primary_host_id == primary_host_id
    }
    primary_by_id: dict = {p.repo_id: p for p in primary_snapshot}

    diff = ScanDiff()
    for repo_id, snap in primary_by_id.items():
        if repo_id not in journal_by_id:
            diff.unknown.append(snap)
            continue
        jrepo = journal_by_id[repo_id]
        journal_push_ids = {m.push_mirror_id for m in jrepo.mirrors}
        primary_push_ids = set(snap.mirror_push_ids)
        if journal_push_ids != primary_push_ids:
            diff.drift.append((jrepo, snap))

    for repo_id, jrepo in journal_by_id.items():
        if repo_id not in primary_by_id:
            # Only flag active repos as missing; soft-deleted ones are fine.
            if jrepo.state == "active":
                diff.missing.append(jrepo)
    return diff


__all__ = [
    "Journal",
    "JournalMirror",
    "JournalRepo",
    "PrimaryRepoSnapshot",
    "SCHEMA_VERSION",
    "ScanDiff",
    "current_schema_version",
    "journal",
    "open_journal",
    "scan_diff",
]

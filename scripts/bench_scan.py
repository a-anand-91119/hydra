"""Synthetic benchmark: old (serial) vs new (concurrent) scan paths.

Runs the same hydra entry points twice — once with `max_workers=1` (serial,
matches old behaviour) and once with `max_workers=8` (the new default) — and
reports wall-clock + speedup. A `latency_s` knob simulates per-request RTT;
real GitLab calls usually sit between 30 ms and 300 ms.

Usage:
    python scripts/bench_scan.py
    python scripts/bench_scan.py --projects 100 --pages 5 --latency 0.05

Nothing here hits the network; we monkey-patch ``hydra.gitlab._session`` with
a fake that sleeps for ``latency_s`` per call.
"""

from __future__ import annotations

import argparse
import time
from contextlib import contextmanager
from typing import List

from rich.console import Console
from rich.table import Table

from hydra import cli as cli_mod
from hydra import gitlab as gitlab_api
from hydra import journal as journal_mod
from hydra.config import Config, Defaults, HostSpec
from hydra.providers.base import MirrorInfo


class _FakeResponse:
    def __init__(self, status_code: int, body, headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self.text = ""

    def json(self):
        return self._body


class _LatencySession:
    """Fake requests.Session that sleeps on every call.

    Routes /api/v4/groups/.../projects → paginated project list, and
    /api/v4/projects/<pid>/remote_mirrors → per-project mirrors.
    """

    def __init__(self, *, projects: List[dict], pages: int, latency_s: float):
        self._projects = projects
        self._pages = max(1, pages)
        self._latency = latency_s
        self.calls = 0

    def get(self, url, headers=None, params=None):
        self.calls += 1
        time.sleep(self._latency)
        params = params or {}
        if "/remote_mirrors" in url:
            pid = int(url.rstrip("/").split("/")[-2])
            return _FakeResponse(200, [{"id": 100_000 + pid, "url": f"https://m/{pid}.git"}])
        # Paginated project listing.
        page = int(params.get("page", 1))
        per_page = int(params.get("per_page", 100))
        chunk = self._projects[(page - 1) * per_page : page * per_page]
        headers_out = {"X-Total-Pages": str(self._pages)}
        if page < self._pages:
            headers_out["X-Next-Page"] = str(page + 1)
        return _FakeResponse(200, chunk, headers=headers_out)

    def post(self, *a, **kw):  # pragma: no cover — unused
        raise AssertionError("benchmark must not POST")


@contextmanager
def _patched_session(session):
    original = gitlab_api._session
    gitlab_api._session = lambda: session
    try:
        yield
    finally:
        gitlab_api._session = original


def _make_projects(n: int) -> List[dict]:
    return [
        {
            "id": i,
            "name": f"proj-{i}",
            "path_with_namespace": f"team/proj-{i}",
            "web_url": f"https://gl/team/proj-{i}",
        }
        for i in range(1, n + 1)
    ]


def _bench_list_projects(*, projects, pages, latency_s, max_workers, runs: int) -> float:
    timings: List[float] = []
    for _ in range(runs):
        sess = _LatencySession(projects=projects, pages=pages, latency_s=latency_s)
        with _patched_session(sess):
            t0 = time.monotonic()
            gitlab_api.list_projects_with_mirrors(
                host="h",
                base_url="https://gl",
                token="t",
                namespace="team",
                max_workers=max_workers,
            )
            timings.append(time.monotonic() - t0)
    return min(timings)  # report best run — least noisy


class _LatencyPrimary:
    """Stand-in MirrorSource used by _refresh_status."""

    def __init__(self, latency_s: float):
        self._latency = latency_s
        self.spec = HostSpec(id="primary", kind="gitlab", url="https://primary.gl")
        self.capabilities = None

    def list_mirrors(self, *, token, primary_repo):
        time.sleep(self._latency)
        idx = primary_repo.project_id - 1000
        return [
            MirrorInfo(
                id=2000 + idx,
                url=f"https://github.com/me/r{idx}.git",
                enabled=True,
                last_update_status="success",
                last_update_at=None,
                last_error=None,
            )
        ]

    # MirrorSource protocol stubs — not invoked by _refresh_status.
    def ensure_namespace(self, **k): ...
    def create_repo(self, **k): ...
    def add_outbound_mirror(self, **k): ...
    def replace_outbound_mirror(self, **k): ...
    def find_project(self, **k): ...
    def list_projects_with_mirrors(self, **k): ...


def _bench_refresh(*, repo_count, latency_s, max_workers, runs: int) -> float:
    cfg = Config(
        hosts=[
            HostSpec(id="primary", kind="gitlab", url="https://primary.gl"),
            HostSpec(id="gh", kind="github", url="https://api.github.com"),
        ],
        primary="primary",
        forks=["gh"],
        defaults=Defaults(private=True, group=""),
    )
    # Seed the journal once per run; the autouse-style isolation isn't active
    # in a bare script, so we wipe + reseed each time.
    timings: List[float] = []
    for _ in range(runs):
        with journal_mod.journal() as j:
            j.connection.execute("DELETE FROM mirrors")
            j.connection.execute("DELETE FROM repos")
            j.connection.commit()
            for i in range(repo_count):
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

        primary = _LatencyPrimary(latency_s)

        # Patch provider factory + token resolution.
        orig_get = cli_mod.providers_mod.get
        orig_tok = cli_mod.secrets_mod.get_token
        cli_mod.providers_mod.get = lambda kind: lambda spec: primary
        cli_mod.secrets_mod.get_token = lambda *a, **k: "t"
        try:
            with journal_mod.journal() as j:
                t0 = time.monotonic()
                cli_mod._refresh_status(
                    cfg=cfg, journal=j, console=Console(quiet=True), max_workers=max_workers
                )
                timings.append(time.monotonic() - t0)
        finally:
            cli_mod.providers_mod.get = orig_get
            cli_mod.secrets_mod.get_token = orig_tok
    return min(timings)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--projects", type=int, default=50, help="project count")
    ap.add_argument("--pages", type=int, default=2, help="pagination depth")
    ap.add_argument("--latency", type=float, default=0.05, help="simulated per-request RTT (s)")
    ap.add_argument("--workers", type=int, default=8, help="parallel worker count")
    ap.add_argument("--runs", type=int, default=3, help="repeats; report best")
    args = ap.parse_args()

    projects = _make_projects(args.projects)
    console = Console()

    console.print(
        f"[bold]Synthetic scan benchmark[/bold] — {args.projects} projects, "
        f"{args.pages} list page(s), {int(args.latency * 1000)}ms latency, "
        f"best of {args.runs}"
    )

    # Stage 1 — list_projects_with_mirrors
    serial = _bench_list_projects(
        projects=projects, pages=args.pages, latency_s=args.latency, max_workers=1, runs=args.runs
    )
    parallel = _bench_list_projects(
        projects=projects,
        pages=args.pages,
        latency_s=args.latency,
        max_workers=args.workers,
        runs=args.runs,
    )

    # Stage 2 — _refresh_status
    refresh_serial = _bench_refresh(
        repo_count=args.projects, latency_s=args.latency, max_workers=1, runs=args.runs
    )
    refresh_parallel = _bench_refresh(
        repo_count=args.projects, latency_s=args.latency, max_workers=args.workers, runs=args.runs
    )

    table = Table(show_header=True, header_style="bold")
    table.add_column("Stage")
    table.add_column("Serial (max_workers=1)", justify="right")
    table.add_column(f"Parallel (max_workers={args.workers})", justify="right")
    table.add_column("Speedup", justify="right", style="green")
    for label, s, p in (
        ("scan: list+mirrors", serial, parallel),
        ("list --refresh", refresh_serial, refresh_parallel),
    ):
        speedup = s / p if p else float("inf")
        table.add_row(label, f"{s * 1000:.0f} ms", f"{p * 1000:.0f} ms", f"{speedup:.1f}×")
    console.print(table)


if __name__ == "__main__":
    main()

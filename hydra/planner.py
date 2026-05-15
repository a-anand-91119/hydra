"""Plan model for mutating hydra operations.

A ``Plan`` is an ordered list of ``Action`` records describing every side
effect a command intends to make — repo creates, mirror adds, journal writes.
Plans are built by pure functions (``plan_create``, ``plan_scan_apply``) so
tests can assert exact action sequences without touching the network or the
journal. The plan is then executed by :mod:`hydra.executor`.

The plan is the single source of truth for both ``--dry-run`` (render and
exit) and a normal apply (render, confirm, then execute). One rendering
function means dry-run output and live output never drift.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional

from rich.console import Console
from rich.table import Table

from hydra.config import Config
from hydra.hostspec_utils import match_fork
from hydra.journal import ScanDiff
from hydra.providers.base import PrimaryMirror, PrimaryProject, RepoRef

if TYPE_CHECKING:
    from hydra.wizard import CreateOptions

ActionKind = Literal[
    "ensure_namespace",
    "create_repo",
    "skip_create_repo",
    "add_outbound_mirror",
    "skip_add_mirror",
    "journal_record_repo",
    "journal_record_mirror",
    "journal_update_push_id",
]


@dataclass(frozen=True)
class Action:
    """One side-effect the executor will perform.

    ``kind`` discriminates the handler; ``payload`` carries handler kwargs
    plus any cross-action symbolic references (see ``ref`` / ``repo_ref``).
    """

    kind: ActionKind
    host_id: str
    summary: str
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Plan:
    actions: List[Action] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.actions

    def summary_counts(self) -> Dict[str, int]:
        return dict(Counter(a.kind for a in self.actions))

    def group_by_host(self) -> Dict[str, List[Action]]:
        out: Dict[str, List[Action]] = {}
        for a in self.actions:
            out.setdefault(a.host_id, []).append(a)
        return out


# ── Plan builders ────────────────────────────────────────────────────────


def plan_create(cfg: Config, opts: CreateOptions) -> Plan:
    """Pure plan for ``hydra create``. No network, no journal.

    Order:
      1. ensure_namespace (primary)
      2. create_repo (primary)
      3. journal_record_repo (primary)
      4. for each fork: ensure_namespace, create_repo, add_outbound_mirror,
         journal_record_mirror.
    """
    actions: List[Action] = []
    primary = cfg.primary_host()

    actions.append(
        Action(
            kind="ensure_namespace",
            host_id=primary.id,
            summary=f"ensure group '{opts.group or '(none)'}'",
            payload={"group": opts.group or None, "role": "primary"},
        )
    )
    actions.append(
        Action(
            kind="create_repo",
            host_id=primary.id,
            summary=f"create repo '{opts.name}'",
            payload={
                "name": opts.name,
                "description": opts.description,
                "is_private": opts.is_private,
                "role": "primary",
                "ref": "primary",  # later actions reference this symbolic id
            },
        )
    )
    actions.append(
        Action(
            kind="journal_record_repo",
            host_id=primary.id,
            summary=f"journal repo '{opts.name}'",
            payload={
                "name": opts.name,
                "repo_ref": "primary",  # source RepoRef
                "ref": "db:primary",  # store repo_db_id under this symbol
            },
        )
    )

    for fork in cfg.fork_hosts():
        actions.append(
            Action(
                kind="ensure_namespace",
                host_id=fork.id,
                summary=f"ensure group '{opts.group or '(none)'}'",
                payload={"group": opts.group or None, "role": "fork"},
            )
        )
        actions.append(
            Action(
                kind="create_repo",
                host_id=fork.id,
                summary=f"create repo '{opts.name}'",
                payload={
                    "name": opts.name,
                    "description": opts.description,
                    "is_private": opts.is_private,
                    "role": "fork",
                    "ref": f"fork:{fork.id}",
                },
            )
        )
        if opts.mirror:
            actions.append(
                Action(
                    kind="add_outbound_mirror",
                    host_id=primary.id,
                    summary=f"mirror primary → {fork.id}",
                    payload={
                        "target_host_id": fork.id,
                        "primary_ref": "primary",
                        "target_ref": f"fork:{fork.id}",
                    },
                )
            )
            actions.append(
                Action(
                    kind="journal_record_mirror",
                    host_id=primary.id,
                    summary=f"journal mirror → {fork.id}",
                    payload={
                        "target_host_id": fork.id,
                        "repo_ref": "db:primary",
                        "target_ref": f"fork:{fork.id}",
                    },
                )
            )
    return Plan(actions=actions)


def plan_create_with_existing(
    plan: Plan,
    *,
    existing_repos: Dict[str, RepoRef],
    existing_mirrors: Optional[Dict[str, PrimaryMirror]] = None,
) -> Plan:
    """Rewrite a `create` plan to skip work that already exists.

    For every host in ``existing_repos``, replace its ``create_repo`` action
    with ``skip_create_repo`` (carrying the existing ``RepoRef`` in the
    payload). For every fork in ``existing_mirrors``, replace the matching
    ``add_outbound_mirror`` with ``skip_add_mirror``.

    Other actions (``ensure_namespace``, journal records, etc.) are left
    untouched — namespaces are idempotent on GitLab and no-ops on GitHub.
    """
    if existing_mirrors is None:
        existing_mirrors = {}
    out: List[Action] = []
    for action in plan.actions:
        if action.kind == "create_repo" and action.host_id in existing_repos:
            ref = action.payload["ref"]
            existing = existing_repos[action.host_id]
            out.append(
                Action(
                    kind="skip_create_repo",
                    host_id=action.host_id,
                    summary=f"= adopt existing repo on {action.host_id}",
                    payload={
                        "ref": ref,
                        "http_url": existing.http_url,
                        "project_id": existing.project_id,
                        "namespace_path": existing.namespace_path,
                    },
                )
            )
            continue
        if action.kind == "add_outbound_mirror":
            target = action.payload.get("target_host_id")
            if target in existing_mirrors:
                existing_mirror = existing_mirrors[target]
                out.append(
                    Action(
                        kind="skip_add_mirror",
                        host_id=action.host_id,
                        summary=f"= mirror to {target} already configured",
                        payload={
                            "target_host_id": target,
                            "push_mirror_id": existing_mirror.id,
                            "mirror_url": existing_mirror.url,
                        },
                    )
                )
                continue
        out.append(action)
    return Plan(actions=out)


def plan_scan_apply(
    diff: ScanDiff,
    cfg: Config,
    *,
    by_repo_id: Dict[int, PrimaryProject],
    accept_unknown_ids: Optional[Iterable[int]] = None,
) -> Plan:
    """Pure plan for ``hydra scan --apply``.

    ``accept_unknown_ids`` lets ``--interactive`` filter the plan down to
    only the unknown repos a user confirmed. ``None`` means accept all.
    Drift actions are always included — an id change is not a semantic change.
    """
    primary_host_id = cfg.primary
    fork_specs = cfg.fork_hosts()
    actions: List[Action] = []
    accepted = set(accept_unknown_ids) if accept_unknown_ids is not None else None

    for snap in diff.unknown:
        if accepted is not None and snap.repo_id not in accepted:
            continue
        proj = by_repo_id.get(snap.repo_id)
        if proj is None:
            continue
        label = proj.name or proj.full_path or proj.web_url
        repo_ref = f"adopt:{snap.repo_id}"
        actions.append(
            Action(
                kind="journal_record_repo",
                host_id=primary_host_id,
                summary=f"adopt '{label}' (id={snap.repo_id})",
                payload={
                    "name": label,
                    "primary_repo_id": proj.project_id,
                    "primary_repo_url": proj.web_url,
                    "ref": repo_ref,
                },
            )
        )
        for m in proj.mirrors:
            fork = match_fork(m.url, fork_specs)
            if fork is None:
                # Reporting-only — we still surface skipped mirrors at execute
                # time. Don't emit a no-op action.
                continue
            actions.append(
                Action(
                    kind="journal_record_mirror",
                    host_id=primary_host_id,
                    summary=f"journal mirror {label} → {fork.id} (id={m.id})",
                    payload={
                        "repo_ref": repo_ref,
                        "target_host_id": fork.id,
                        "target_repo_url": m.url,
                        "push_mirror_id": m.id,
                    },
                )
            )

    for jrepo, snap in diff.drift:
        proj = by_repo_id.get(snap.repo_id)
        if proj is None:
            continue
        j_by_host = {jm.target_host_id: jm for jm in jrepo.mirrors}
        for m in proj.mirrors:
            fork = match_fork(m.url, fork_specs)
            if fork is None or fork.id not in j_by_host:
                continue
            jm = j_by_host[fork.id]
            if jm.push_mirror_id == m.id:
                continue
            actions.append(
                Action(
                    kind="journal_update_push_id",
                    host_id=primary_host_id,
                    summary=(
                        f"resync {jrepo.name} → {fork.id} "
                        f"(journal={jm.push_mirror_id} → primary={m.id})"
                    ),
                    payload={"mirror_db_id": jm.id, "new_push_mirror_id": m.id},
                )
            )

    return Plan(actions=actions)


# ── Rendering ────────────────────────────────────────────────────────────


def render_plan(
    plan: Plan, console: Console, *, dry_run: bool = False, title: str = "Plan"
) -> None:
    if plan.is_empty:
        console.print("[dim](nothing to do)[/dim]")
        return

    table = Table(title=title, show_header=True, header_style="bold")
    table.add_column("Host")
    table.add_column("Action")
    table.add_column("Summary")
    for action in plan.actions:
        table.add_row(action.host_id, action.kind, action.summary)
    console.print(table)

    counts = plan.summary_counts()
    pretty = ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
    suffix = "  [dim](dry-run — no changes made)[/dim]" if dry_run else ""
    console.print(f"[dim]{pretty}[/dim]{suffix}")


# Forward reference resolution. CreateOptions lives in hydra.wizard; keep this
# import deferred to avoid the wizard pulling in questionary at planner import.
def __getattr__(name: str):  # pragma: no cover — Python <3.7 won't hit this
    if name == "CreateOptions":
        from hydra.wizard import CreateOptions

        return CreateOptions
    raise AttributeError(name)


__all__ = [
    "Action",
    "Plan",
    "plan_create",
    "plan_scan_apply",
    "render_plan",
]

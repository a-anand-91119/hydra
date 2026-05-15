"""Apply a :class:`hydra.planner.Plan` against real providers + the journal.

The executor is the only place that issues mutating provider calls or journal
writes. It iterates the plan in order, dispatches per-action handlers, stops
on first failure, and reports an :class:`ApplyResult`.

In-flight cross-action references (``ref`` / ``repo_ref`` / ``primary_ref``
fields on action payloads) are resolved through a small symbol-table that
maps a symbolic name → the runtime value produced by an earlier action
(a :class:`RepoRef` for ``create_repo`` actions, or an ``int`` repo db id
for ``journal_record_repo`` actions).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from rich.console import Console

from hydra import journal as journal_mod
from hydra import providers as providers_mod
from hydra.config import Config
from hydra.errors import HydraAPIError
from hydra.planner import Action, Plan
from hydra.providers.base import MirrorSource, RepoRef
from hydra.utils import safe_int


@dataclass
class ApplyResult:
    applied: int = 0
    failed: Optional[Action] = None
    error: Optional[Exception] = None
    created: List[Tuple[str, str]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed is None


def apply_plan(
    plan: Plan,
    *,
    cfg: Config,
    tokens: Dict[str, str],
    console: Console,
    verbose: bool = False,
) -> ApplyResult:
    """Execute ``plan`` against real providers + the journal.

    Stops on first :class:`HydraAPIError`. The journal stays open for the
    full apply so all writes share one connection (and SQLite stays
    single-writer).
    """
    result = ApplyResult()
    if plan.is_empty:
        return result

    providers: Dict[str, Any] = {h.id: providers_mod.get(h.kind)(h) for h in cfg.hosts}
    symbols: Dict[str, Any] = {}

    handlers: Dict[str, Callable[..., None]] = {
        "ensure_namespace": _h_ensure_namespace,
        "create_repo": _h_create_repo,
        "skip_create_repo": _h_skip_create_repo,
        "add_outbound_mirror": _h_add_outbound_mirror,
        "skip_add_mirror": _h_skip_add_mirror,
        "journal_record_repo": _h_journal_record_repo,
        "journal_record_mirror": _h_journal_record_mirror,
        "journal_update_push_id": _h_journal_update_push_id,
    }

    with journal_mod.journal() as journal:
        ctx = _Ctx(
            cfg=cfg,
            providers=providers,
            tokens=tokens,
            journal=journal,
            console=console,
            symbols=symbols,
            result=result,
            verbose=verbose,
        )
        for action in plan.actions:
            handler = handlers.get(action.kind)
            if handler is None:
                result.failed = action
                result.error = RuntimeError(f"unknown action kind: {action.kind}")
                return result
            try:
                handler(ctx, action)
            except HydraAPIError as e:
                result.failed = action
                result.error = e
                return result
            result.applied += 1

    return result


# ── Internal context + handlers ─────────────────────────────────────────


@dataclass
class _Ctx:
    cfg: Config
    providers: Dict[str, Any]
    tokens: Dict[str, str]
    journal: journal_mod.Journal
    console: Console
    symbols: Dict[str, Any]
    result: ApplyResult
    verbose: bool


def _h_ensure_namespace(ctx: _Ctx, action: Action) -> None:
    prov = ctx.providers[action.host_id]
    spec = ctx.cfg.host(action.host_id)
    token = ctx.tokens[action.host_id]
    ns = prov.ensure_namespace(group_path=action.payload.get("group"), token=token)
    ctx.symbols[f"ns:{action.host_id}"] = ns
    for path in ns.created_paths:
        ctx.result.created.append((f"{action.host_id} group", f"{spec.url}/{path}"))
    if ctx.verbose and ns.namespace_id is not None:
        ctx.console.print(f"[dim]{action.host_id} group id: {ns.namespace_id}[/dim]")


def _h_create_repo(ctx: _Ctx, action: Action) -> None:
    prov = ctx.providers[action.host_id]
    token = ctx.tokens[action.host_id]
    ns = ctx.symbols.get(f"ns:{action.host_id}")
    if ns is None:
        # Defensive; a well-formed plan always emits ensure_namespace first.
        raise HydraAPIError(message=f"create_repo for {action.host_id} ran before ensure_namespace")
    repo = prov.create_repo(
        token=token,
        name=action.payload["name"],
        description=action.payload.get("description", ""),
        namespace=ns,
        is_private=action.payload.get("is_private", True),
    )
    ctx.symbols[action.payload["ref"]] = repo
    ctx.result.created.append((f"{action.host_id} repo", repo.http_url))
    ctx.console.print(f"[green]✓[/green] {action.host_id}: {repo.http_url}")


def _h_skip_create_repo(ctx: _Ctx, action: Action) -> None:
    """Adoption-path counterpart to ``_h_create_repo``.

    No HTTP call — the repo already exists. Populate the symbol so downstream
    actions (mirror setup, journal record) resolve correctly.
    """
    payload = action.payload
    repo = RepoRef(
        http_url=payload["http_url"],
        project_id=payload.get("project_id"),
        namespace_path=payload.get("namespace_path"),
    )
    ctx.symbols[payload["ref"]] = repo
    ctx.console.print(f"[dim]= adopted existing[/dim] {action.host_id}: {repo.http_url}")


def _h_skip_add_mirror(ctx: _Ctx, action: Action) -> None:
    """Adoption-path counterpart to ``_h_add_outbound_mirror``.

    Reuses the existing push-mirror's id so the downstream
    ``journal_record_mirror`` handler captures the correct value.
    """
    payload = action.payload
    target = payload["target_host_id"]
    ctx.symbols[f"mirror:{target}"] = {
        "id": payload["push_mirror_id"],
        "url": payload.get("mirror_url", ""),
    }
    ctx.console.print(f"[dim]= mirror to {target} already configured[/dim]")


def _h_add_outbound_mirror(ctx: _Ctx, action: Action) -> None:
    primary_spec = ctx.cfg.host(action.host_id)
    primary_prov = ctx.providers[action.host_id]
    if not isinstance(primary_prov, MirrorSource):
        raise HydraAPIError(message=f"{primary_spec.id} cannot be a mirror source")

    target_host_id = action.payload["target_host_id"]
    primary_repo: RepoRef = ctx.symbols[action.payload["primary_ref"]]
    target_repo: RepoRef = ctx.symbols[action.payload["target_ref"]]
    target_caps = providers_mod.capabilities_for(ctx.cfg.host(target_host_id).kind)

    payload = primary_prov.add_outbound_mirror(
        token=ctx.tokens[action.host_id],
        primary_repo=primary_repo,
        target_url=target_repo.http_url,
        target_token=ctx.tokens[target_host_id],
        target_username=target_caps.inbound_mirror_username,
        target_label=target_host_id,
    )
    ctx.symbols[f"mirror:{target_host_id}"] = payload or {}
    ctx.result.created.append((f"{action.host_id} mirror → {target_host_id}", target_repo.http_url))
    ctx.console.print(f"[green]✓[/green] mirror configured: {target_host_id}")


def _h_journal_record_repo(ctx: _Ctx, action: Action) -> None:
    payload = action.payload
    primary_repo_id = payload.get("primary_repo_id")
    primary_repo_url = payload.get("primary_repo_url")
    if primary_repo_id is None:
        # `create` flow: pull from the just-created RepoRef.
        repo: RepoRef = ctx.symbols[payload["repo_ref"]]
        primary_repo_id = repo.project_id
        primary_repo_url = repo.http_url
    if primary_repo_id is None:
        # Provider didn't return a project_id (e.g. GitHub primary in some
        # future world). Skip the journal write rather than failing the apply.
        ctx.result.notes.append(
            f"skipped journal entry for {payload.get('name')} — no primary_repo_id"
        )
        return
    repo_db_id = ctx.journal.record_repo(
        name=payload["name"],
        primary_host_id=action.host_id,
        primary_repo_id=int(primary_repo_id),
        primary_repo_url=str(primary_repo_url or ""),
    )
    if "ref" in payload:
        ctx.symbols[payload["ref"]] = repo_db_id


def _h_journal_record_mirror(ctx: _Ctx, action: Action) -> None:
    payload = action.payload
    repo_db_id = ctx.symbols.get(payload["repo_ref"])
    if repo_db_id is None:
        ctx.result.notes.append(f"no repo_db_id for {payload.get('target_host_id')}")
        return
    target_repo_url = payload.get("target_repo_url")
    push_mirror_id = payload.get("push_mirror_id")
    target_repo_id = payload.get("target_repo_id")
    if target_repo_url is None or push_mirror_id is None:
        # `create` flow: pull from the just-added mirror payload + RepoRef.
        target_ref = payload.get("target_ref")
        target_repo: Optional[RepoRef] = ctx.symbols.get(target_ref) if target_ref else None
        mirror_payload = ctx.symbols.get(f"mirror:{payload['target_host_id']}", {})
        target_repo_url = target_repo.http_url if target_repo else ""
        push_mirror_id = safe_int(mirror_payload.get("id"))
        if push_mirror_id is None:
            ctx.result.notes.append(f"no push_mirror_id returned for {payload['target_host_id']}")
            return
        if target_repo and target_repo.project_id is not None:
            target_repo_id = str(target_repo.project_id)
    ctx.journal.record_mirror(
        repo_id=int(repo_db_id),
        target_host_id=payload["target_host_id"],
        target_repo_url=str(target_repo_url),
        push_mirror_id=int(push_mirror_id),
        target_repo_id=target_repo_id,
    )


def _h_journal_update_push_id(ctx: _Ctx, action: Action) -> None:
    ctx.journal.update_mirror_push_id(
        mirror_db_id=int(action.payload["mirror_db_id"]),
        new_push_mirror_id=int(action.payload["new_push_mirror_id"]),
    )


__all__ = ["ApplyResult", "apply_plan"]

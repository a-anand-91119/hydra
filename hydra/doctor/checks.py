"""Diagnostic checks. Each is a pure function over `DoctorState` returning
zero or more `Finding`s. Checks must be side-effect free — fixes live in
`fixes.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import keyring

from hydra import providers as providers_mod
from hydra import secrets as secrets_mod
from hydra.config import Config
from hydra.doctor.findings import Finding, Level, Report
from hydra.migrations import detect_version, pending


@dataclass
class DoctorState:
    """Bundle of inputs every check sees. Built once at the top of run_doctor."""

    cfg_path: Path
    raw: Dict[str, Any]  # post-migration, post-default raw dict — None-safe
    cfg: Optional[Config]  # parsed config; None if parse failed
    parse_error: Optional[Exception] = None
    env: Dict[str, str] = field(default_factory=dict)
    # Keyring access can block on macOS (Keychain prompts for user approval),
    # so it's opt-in — pass `check_keyring=True` from run_doctor.
    check_keyring: bool = False
    # Injected so tests can stub keyring without monkeypatching the module.
    keyring_get: Callable[[str], Optional[str]] = field(
        default_factory=lambda: lambda host_id: _safe_keyring_get(host_id)
    )


def _safe_keyring_get(host_id: str) -> Optional[str]:
    try:
        return keyring.get_password(secrets_mod.KEYRING_SERVICE, host_id)
    except keyring.errors.KeyringError:
        return None


# ──────────────────────────── Schema / migration checks ─────────────────


def check_schema_version(state: DoctorState) -> List[Finding]:
    out: List[Finding] = []
    pending_migrations = pending(state.raw)
    if not pending_migrations:
        out.append(
            Finding(
                section="Config",
                level=Level.OK,
                message=f"schema_version: {detect_version(state.raw)} (current)",
            )
        )
        return out
    names = ", ".join(m.name for m in pending_migrations)
    detail = "\n".join(f"  - {m.name}: {m.description}" for m in pending_migrations)
    out.append(
        Finding(
            section="Config",
            level=Level.WARN,
            message=f"{len(pending_migrations)} pending migration(s): {names}",
            fix_id="run-migrations",
            details=detail,
        )
    )
    return out


def check_parse_error(state: DoctorState) -> List[Finding]:
    if state.parse_error is None:
        return []
    return [
        Finding(
            section="Config",
            level=Level.ERROR,
            message=f"config failed to parse: {state.parse_error}",
            details=str(state.parse_error),
        )
    ]


# ──────────────────────────── Provider / topology checks ────────────────


def check_provider_kinds(state: DoctorState) -> List[Finding]:
    if state.cfg is None:
        return []
    out: List[Finding] = []
    known = set(providers_mod.kinds())
    for host in state.cfg.hosts:
        if host.kind not in known:
            out.append(
                Finding(
                    section="Hosts",
                    level=Level.ERROR,
                    message=f"host {host.id!r} uses unknown provider kind {host.kind!r}",
                    details=f"Known kinds: {sorted(known)}",
                )
            )
    return out


def check_primary_capable(state: DoctorState) -> List[Finding]:
    if state.cfg is None:
        return []
    try:
        primary = state.cfg.primary_host()
    except KeyError:
        return [
            Finding(
                section="Hosts",
                level=Level.ERROR,
                message=f"primary {state.cfg.primary!r} does not match any configured host",
            )
        ]
    try:
        caps = providers_mod.capabilities_for(primary.kind)
    except KeyError:
        return []  # already reported by check_provider_kinds
    if not caps.supports_mirror_source:
        return [
            Finding(
                section="Hosts",
                level=Level.ERROR,
                message=(f"primary {primary.id!r} (kind={primary.kind}) cannot be a mirror source"),
            )
        ]
    return [
        Finding(
            section="Hosts",
            level=Level.OK,
            message=f"primary: {primary.id} ({primary.kind})",
        )
    ]


def check_fork_references(state: DoctorState) -> List[Finding]:
    if state.cfg is None:
        return []
    out: List[Finding] = []
    host_ids = {h.id for h in state.cfg.hosts}
    seen: set = set()
    for fid in state.cfg.forks:
        if fid in seen:
            out.append(
                Finding(
                    section="Hosts",
                    level=Level.ERROR,
                    message=f"fork {fid!r} listed more than once",
                )
            )
        seen.add(fid)
        if fid not in host_ids:
            out.append(
                Finding(
                    section="Hosts",
                    level=Level.ERROR,
                    message=f"fork {fid!r} does not match any configured host",
                )
            )
    if not out and state.cfg.forks:
        out.append(
            Finding(
                section="Hosts",
                level=Level.OK,
                message=f"forks: {', '.join(state.cfg.forks)}",
            )
        )
    return out


# ──────────────────────────── Token / secret checks ─────────────────────


def check_token_resolvable(state: DoctorState) -> List[Finding]:
    """Probe presence of a token for each host id without actually fetching one
    from a prompt (allow_prompt=False).
    """
    if state.cfg is None:
        return []
    out: List[Finding] = []
    for host in state.cfg.hosts:
        candidates = secrets_mod._candidate_env_vars(host.id)
        in_env = any(state.env.get(name) for name in candidates)
        in_keyring = state.check_keyring and bool(state.keyring_get(host.id))
        if in_env:
            out.append(
                Finding(
                    section="Tokens",
                    level=Level.OK,
                    message=f"{host.id} — token found in environment",
                )
            )
        elif in_keyring:
            out.append(
                Finding(
                    section="Tokens",
                    level=Level.OK,
                    message=f"{host.id} — token found in keyring",
                )
            )
        else:
            hint = (
                f"set {secrets_mod.env_var_for(host.id)} or run `hydra configure`"
                if state.check_keyring
                else (
                    f"set {secrets_mod.env_var_for(host.id)} "
                    f"(or rerun with --check-keyring to also probe the OS keyring)"
                )
            )
            out.append(
                Finding(
                    section="Tokens",
                    level=Level.WARN,
                    message=f"{host.id} — no token found in environment ({hint})",
                )
            )
    return out


def check_legacy_env_vars(state: DoctorState) -> List[Finding]:
    """Warn if any legacy `HYDRA_*_TOKEN` env vars are set; advise renaming."""
    out: List[Finding] = []
    legacy_names = {
        "github": "HYDRA_GITHUB_TOKEN",
        "gitlab": "HYDRA_GITLAB_TOKEN",
        "self_hosted_gitlab": "HYDRA_SELF_HOSTED_GITLAB_TOKEN",
    }
    for host_id, legacy in legacy_names.items():
        if state.env.get(legacy):
            modern = secrets_mod.env_var_for(host_id)
            out.append(
                Finding(
                    section="Tokens",
                    level=Level.WARN,
                    message=f"legacy env var {legacy} is set; prefer {modern}",
                    fix_id="show-legacy-env-rename",
                    details=(
                        f"The legacy variable still works (back-compat fallback) but "
                        f"will be removed in a future release. To rename:\n"
                        f"  export {modern}=${legacy}\n"
                        f"  unset {legacy}"
                    ),
                )
            )
    return out


def check_keyring_orphans(state: DoctorState) -> List[Finding]:
    """The keyring API exposes no portable enumeration, so true orphan
    detection is impossible. This check is a no-op placeholder so its omission
    is documented rather than silent.
    """
    if not state.check_keyring:
        return []
    return [
        Finding(
            section="Tokens",
            level=Level.OK,
            message="keyring orphan check skipped (no portable enumeration)",
        )
    ]


# ──────────────────────────── Aggregation ───────────────────────────────


ALL_CHECKS: List[Callable[[DoctorState], List[Finding]]] = [
    check_parse_error,
    check_schema_version,
    check_provider_kinds,
    check_primary_capable,
    check_fork_references,
    check_token_resolvable,
    check_legacy_env_vars,
    check_keyring_orphans,
]


def collect(state: DoctorState) -> Report:
    report = Report()
    for check in ALL_CHECKS:
        for finding in check(state):
            report.add(finding)
    return report

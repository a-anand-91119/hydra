"""Diagnostic checks. Each is a pure function over `DoctorState` returning
zero or more `Finding`s. Checks must be side-effect free — fixes live in
`fixes.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import keyring

from hydra import journal as journal_mod
from hydra import paths as paths_mod
from hydra import preflight as preflight_mod
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
    # `.env` in the cwd where doctor ran. dotenv_values is what get_token() would
    # see after _ensure_dotenv_loaded(); empty if the file doesn't exist.
    dotenv_path: Optional[Path] = None
    dotenv_exists: bool = False
    dotenv_values: Dict[str, str] = field(default_factory=dict)
    # Keyring access can block on macOS (Keychain prompts for user approval),
    # so it's opt-in — pass `check_keyring=True` from run_doctor.
    check_keyring: bool = False
    # Network probes for token validity + scope. Opt-in via `check_tokens=True`.
    check_tokens: bool = False
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


def check_dotenv_presence(state: DoctorState) -> List[Finding]:
    """One-line note about whether a `.env` file would be loaded from cwd.

    Loaded order is fixed in ``secrets.get_token``: shell env → .env in cwd →
    keyring → prompt. This finding surfaces the cwd-resolved path so users
    understand which file (if any) is in play.
    """
    if state.dotenv_path is None:
        return []
    if state.dotenv_exists:
        n = sum(1 for k in state.dotenv_values if k.startswith("HYDRA_"))
        return [
            Finding(
                section="Tokens",
                level=Level.OK,
                message=(
                    f".env at {state.dotenv_path} "
                    f"({n} HYDRA_* key(s); loaded only when cwd is this directory)"
                ),
            )
        ]
    return [
        Finding(
            section="Tokens",
            level=Level.OK,
            message=(
                f"no .env in cwd ({state.dotenv_path.parent}) — "
                f"only shell env and keyring will resolve tokens"
            ),
        )
    ]


def check_token_resolvable(state: DoctorState) -> List[Finding]:
    """Identify which source would resolve each host's token.

    Source priority matches ``secrets.get_token``: shell env → .env (cwd) →
    keyring. Surfaces shadowing (shell overriding a different .env value) as
    a warning so users notice why their .env edits don't take effect.
    """
    if state.cfg is None:
        return []
    out: List[Finding] = []
    for host in state.cfg.hosts:
        candidates = secrets_mod._candidate_env_vars(host.id)
        shell_match = next((n for n in candidates if state.env.get(n)), None)
        dotenv_match = next((n for n in candidates if state.dotenv_values.get(n)), None)
        keyring_value = state.keyring_get(host.id) if state.check_keyring else None

        if shell_match is not None:
            msg = f"{host.id} — shell env [{shell_match}]"
            # Detect shadowing: same var also in .env but with a different value.
            shadowed = dotenv_match == shell_match and state.env.get(
                shell_match
            ) != state.dotenv_values.get(shell_match)
            if shadowed:
                out.append(
                    Finding(
                        section="Tokens",
                        level=Level.WARN,
                        message=(
                            f"{host.id} — shell env [{shell_match}] is "
                            f"shadowing a different value in .env"
                        ),
                        details=(
                            "Your shell has this variable exported with a different "
                            "value than the .env file. Hydra uses the shell value. "
                            f"Run `unset {shell_match}` (and re-source your shell) "
                            "if you meant the .env value."
                        ),
                    )
                )
            else:
                out.append(Finding(section="Tokens", level=Level.OK, message=msg))
        elif dotenv_match is not None:
            out.append(
                Finding(
                    section="Tokens",
                    level=Level.OK,
                    message=(f"{host.id} — .env [{dotenv_match}] at {state.dotenv_path}"),
                )
            )
        elif keyring_value:
            out.append(
                Finding(
                    section="Tokens",
                    level=Level.OK,
                    message=f"{host.id} — OS keyring",
                )
            )
        else:
            hint = (
                f"set {secrets_mod.env_var_for(host.id)}, add it to "
                f"{state.dotenv_path}, or run `hydra configure`"
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
                    message=f"{host.id} — no token resolvable ({hint})",
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


def _resolve_token_for_doctor(state: DoctorState, host_id: str) -> Optional[str]:
    """Mirror ``secrets.get_token`` precedence using only the captured state:
    shell env → .env (cwd) → keyring (if check_keyring)."""
    candidates = secrets_mod._candidate_env_vars(host_id)
    for name in candidates:
        if state.env.get(name):
            return state.env[name]
    for name in candidates:
        if state.dotenv_values.get(name):
            return state.dotenv_values[name]
    if state.check_keyring:
        return state.keyring_get(host_id)
    return None


def check_token_permissions(state: DoctorState) -> List[Finding]:
    """Opt-in network probe: validates each token and reports its scopes.

    Only runs when ``state.check_tokens`` is True. Delegates the actual
    probe + scope-diff to :mod:`hydra.preflight` (shared with the CLI's
    pre-mutation preflight); doctor just translates :class:`PreflightFinding`
    records into ``Finding`` rows with the right ``Level``.

    Hosts whose token cannot be resolved are skipped (already warned by
    ``check_token_resolvable``).
    """
    if not state.check_tokens or state.cfg is None:
        return []
    tokens: Dict[str, str] = {}
    for host in state.cfg.hosts:
        token = _resolve_token_for_doctor(state, host.id)
        if token is not None:
            tokens[host.id] = token
    if not tokens:
        return []

    report = preflight_mod.check_tokens(state.cfg.hosts, tokens)
    out: List[Finding] = []
    for finding in report.errors:
        out.append(
            Finding(
                section="Tokens",
                level=Level.ERROR if "token rejected" in finding.message else Level.WARN,
                message=finding.message,
                details=finding.hint or "",
            )
        )
    for finding in report.warnings + report.oks:
        out.append(
            Finding(
                section="Tokens",
                level=Level.OK,
                message=finding.message,
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


def check_journal(state: DoctorState) -> List[Finding]:
    """Verify the journal opens and is at the expected schema version."""
    path = paths_mod.journal_path()
    if not path.exists():
        return [
            Finding(
                section="Journal",
                level=Level.OK,
                message=f"journal not yet created at {path} (created on first `hydra create`)",
            )
        ]
    try:
        j = journal_mod.open_journal(path)
    except Exception as e:  # noqa: BLE001
        return [
            Finding(
                section="Journal",
                level=Level.ERROR,
                message=f"journal at {path} is unreadable: {e}",
                details=str(e),
            )
        ]
    try:
        version = journal_mod.current_schema_version(j.connection)
        n_repos = j.connection.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
    finally:
        j.close()
    if version != journal_mod.SCHEMA_VERSION:
        return [
            Finding(
                section="Journal",
                level=Level.WARN,
                message=(
                    f"journal schema version {version} != expected "
                    f"{journal_mod.SCHEMA_VERSION} (re-open to auto-migrate)"
                ),
            )
        ]
    return [
        Finding(
            section="Journal",
            level=Level.OK,
            message=f"journal OK ({n_repos} repo(s) tracked at {path})",
        )
    ]


ALL_CHECKS: List[Callable[[DoctorState], List[Finding]]] = [
    check_parse_error,
    check_schema_version,
    check_provider_kinds,
    check_primary_capable,
    check_fork_references,
    check_dotenv_presence,
    check_token_resolvable,
    check_token_permissions,
    check_legacy_env_vars,
    check_keyring_orphans,
    check_journal,
]


def collect(state: DoctorState) -> Report:
    report = Report()
    for check in ALL_CHECKS:
        for finding in check(state):
            report.add(finding)
    return report

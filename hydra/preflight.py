"""Pre-mutation token preflight.

Probes each host's token *before* any mutating command runs its first side
effect, so a wrong-scope or rejected token fails fast instead of leaving
orphan groups / repos / mirrors behind.

Shared between :mod:`hydra.doctor` (where it powers the opt-in
``--check-tokens`` probe) and :mod:`hydra.cli` (where it gates ``create`` and
``rotate-token``). The doctor entry point keeps the old behaviour intact —
it just translates :class:`PreflightFinding` records into ``Finding``s.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Tuple

from hydra.config import HostSpec
from hydra.errors import HydraAPIError
from hydra.secrets import TokenScopes

Severity = Literal["error", "warning", "ok"]

# Minimum machine-readable scopes hydra needs per provider kind. Tokens may
# carry more (and usually do); the missing-scope warning only fires when one
# of these is absent.
REQUIRED_SCOPES: Dict[str, set] = {
    "gitlab": {"api"},
    "github": {"repo"},
}

# Acceptable substitutes — if any is present, the corresponding required
# scope is considered satisfied (lets users grant least-privilege org admin
# scopes interchangeably).
GITHUB_ORG_SCOPES = {"admin:org", "write:org"}


@dataclass
class PreflightFinding:
    """One issue found by :func:`check_tokens` for a single host."""

    host_id: str
    message: str
    hint: Optional[str] = None


@dataclass
class PreflightReport:
    """Result of probing every configured host's token.

    - ``errors``: token rejected or missing a required scope (bail).
    - ``warnings``: token valid but scopes unknown (proceed with caution).
    - ``oks``: token valid with all required scopes (no action). Doctor
      surfaces these as positive findings; the CLI ignores them.
    """

    errors: List[PreflightFinding] = field(default_factory=list)
    warnings: List[PreflightFinding] = field(default_factory=list)
    oks: List[PreflightFinding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def inspect_for_host(host: HostSpec, token: str) -> Optional[TokenScopes]:
    """Dispatch to the provider's token-inspection probe.

    Returns ``None`` for unknown provider kinds — caller decides what to do.
    Raises :class:`HydraAPIError` if the host rejects the token (e.g. 401).
    """
    if host.kind == "gitlab":
        from hydra import gitlab as gitlab_api

        return gitlab_api.inspect_token(host=host.id, base_url=host.url, token=token)
    if host.kind == "github":
        from hydra import github as github_api

        return github_api.inspect_token(base_url=host.url, token=token)
    return None


def required_scopes_for(host: HostSpec) -> set:
    """Scopes hydra needs on this host, accounting for ``host.options``."""
    base = set(REQUIRED_SCOPES.get(host.kind, set()))
    if host.kind == "github" and host.options.get("org"):
        base = base | {"_org"}  # sentinel — resolved against GITHUB_ORG_SCOPES
    return base


def missing_scopes(required: set, have: List[str]) -> set:
    """Compute missing scopes, treating GitHub org permissions as substitutable."""
    have_set = set(have)
    missing: set = set()
    for req in required:
        if req == "_org":
            if not (have_set & GITHUB_ORG_SCOPES):
                missing.add(f"admin:org (or {'/'.join(sorted(GITHUB_ORG_SCOPES))})")
        elif req not in have_set:
            missing.add(req)
    return missing


def _probe_one_host(host: HostSpec, token: str) -> Tuple[Severity, PreflightFinding]:
    """Probe one host's token and classify the outcome.

    Returns ``(severity, finding)`` so the caller can route it to the right
    bucket without re-parsing the message string.
    """
    try:
        info = inspect_for_host(host, token)
    except HydraAPIError as e:
        return "error", PreflightFinding(
            host_id=host.id,
            message=f"{host.id} — token rejected ({e.status_code or '?'}): {e.message}",
            hint=e.hint,
        )
    if info is None:
        return "warning", PreflightFinding(
            host_id=host.id,
            message=(
                f"{host.id} — no token-introspection probe for kind "
                f"{host.kind!r}; permissions not verified"
            ),
        )
    if not info.scopes_known:
        return "warning", PreflightFinding(
            host_id=host.id,
            message=(
                f"{host.id} — token valid (scopes not exposed by host; "
                f"fine-grained PAT or older GitLab)"
            ),
        )
    missing = missing_scopes(required_scopes_for(host), info.scopes)
    if missing:
        return "error", PreflightFinding(
            host_id=host.id,
            message=(
                f"{host.id} — token valid but missing scope(s): "
                f"{', '.join(sorted(missing))} (have: {', '.join(info.scopes)})"
            ),
            hint=(
                f"Mint a new {host.kind} token with the required scopes "
                f"and re-run `hydra configure`."
            ),
        )
    line = f"scopes: {', '.join(info.scopes)}"
    if info.expires_at:
        line += f"; expires {info.expires_at}"
    return "ok", PreflightFinding(
        host_id=host.id, message=f"{host.id} — token valid ({line})"
    )


def check_tokens(hosts: List[HostSpec], tokens: Dict[str, str]) -> PreflightReport:
    """Validate every host's token in parallel.

    For each host present in ``tokens``:

    - 401 / 403 from the host → an ``errors`` entry (token rejected).
    - Token valid but missing a required scope → an ``errors`` entry.
    - Token valid but the host doesn't expose scopes (fine-grained PATs,
      older GitLab) → a ``warnings`` entry.
    - Token valid with all required scopes → an ``oks`` entry.

    Hosts whose kind has no inspection probe yield a warning so callers
    know preflight was skipped for them.
    """
    report = PreflightReport()
    targets = [h for h in hosts if h.id in tokens]
    if not targets:
        return report

    bucket = {"error": report.errors, "warning": report.warnings, "ok": report.oks}

    workers = max(1, min(len(targets), 8))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_probe_one_host, host, tokens[host.id]) for host in targets]
        for fut in as_completed(futures):
            severity, finding = fut.result()
            bucket[severity].append(finding)

    # Stable order for deterministic output.
    report.errors.sort(key=lambda f: f.host_id)
    report.warnings.sort(key=lambda f: f.host_id)
    report.oks.sort(key=lambda f: f.host_id)
    return report

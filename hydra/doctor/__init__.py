"""hydra doctor — diagnose configuration, tokens, and topology."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from rich.console import Console

from hydra.config import (
    Config,
    ConfigError,
    _parse,
    resolve_config_path,
)
from hydra.doctor.checks import DoctorState, collect
from hydra.doctor.findings import Finding, Level, Report
from hydra.doctor.fixes import FixContext, FixOutcome, get_handler

# Exit codes
EXIT_OK = 0
EXIT_ISSUES = 1
EXIT_INTERNAL = 2

# Glyphs (no emojis — terminal-portable).
_GLYPHS = {
    Level.OK: "[green]✓[/green]",
    Level.WARN: "[yellow]⚠[/yellow]",
    Level.ERROR: "[red]✗[/red]",
}


@dataclass
class DoctorResult:
    report: Report
    fixes_applied: List[FixOutcome]
    exit_code: int


def run_doctor(
    *,
    config_path: Optional[Path] = None,
    fix: bool = False,
    verbose: bool = False,
    check_keyring: bool = False,
    console: Optional[Console] = None,
) -> DoctorResult:
    """Run all checks; optionally apply fixes. Pure-ish: returns a result the
    CLI renders. Tests can call this directly.

    `check_keyring` is opt-in because keyring access can block on macOS
    (Keychain prompts the user for permission to read each entry).
    """
    console = console or Console()
    cfg_path = resolve_config_path(config_path)

    state = _build_state(cfg_path, check_keyring=check_keyring)
    report = collect(state)

    _render_report(console, report, verbose=verbose)

    fixes_applied: List[FixOutcome] = []
    if fix and report.fixable:
        # Apply unique fix_ids in registration order.
        seen: set = set()
        ordered_ids: List[str] = []
        for f in report.fixable:
            if f.fix_id and f.fix_id not in seen:
                seen.add(f.fix_id)
                ordered_ids.append(f.fix_id)

        if ordered_ids:
            console.print()
            console.print("[bold]Applying fixes…[/bold]")

        for fix_id in ordered_ids:
            try:
                handler = get_handler(fix_id)
            except KeyError:
                continue
            try:
                ctx = FixContext(cfg_path=cfg_path, raw=state.raw)
                outcome = handler.apply(ctx)
            except Exception as e:  # noqa: BLE001 — surface any handler error
                outcome = FixOutcome(
                    fix_id=fix_id,
                    success=False,
                    message=f"fix failed: {e}",
                )
            fixes_applied.append(outcome)
            glyph = "[green]✓[/green]" if outcome.success else "[red]✗[/red]"
            console.print(f"  {glyph} [bold]{fix_id}[/]: {outcome.message}")

        # Re-run checks against the post-fix state.
        post_state = _build_state(cfg_path, check_keyring=check_keyring)
        post_report = collect(post_state)
        console.print()
        console.print("[bold]Re-checking after fixes…[/bold]")
        _render_report(console, post_report, verbose=verbose)
        report = post_report  # caller-visible report reflects final state

    exit_code = _exit_code(report, internal_error=False)
    if state.parse_error is not None and not report.errors:
        # Defensive: parse_error always produces an ERROR finding via
        # check_parse_error, but if a future refactor lets it slip past,
        # don't report success.
        exit_code = EXIT_INTERNAL

    return DoctorResult(report=report, fixes_applied=fixes_applied, exit_code=exit_code)


def _build_state(cfg_path: Path, *, check_keyring: bool = False) -> DoctorState:
    raw: Dict = {}
    parse_error: Optional[Exception] = None
    cfg: Optional[Config] = None
    if cfg_path.exists():
        try:
            with cfg_path.open("r") as f:
                raw = yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError) as e:
            parse_error = e
            raw = {}
    else:
        parse_error = ConfigError(f"no config file at {cfg_path}")

    # Try to parse using post-migration view: if the file is at an old
    # version, the schema check will surface the pending migration; we still
    # want the *current* parsed Config (if any) for the other checks.
    if not parse_error:
        try:
            from hydra.migrations import MigrationContext
            from hydra.migrations import run as run_migrations

            ctx = MigrationContext(cfg_path=cfg_path, env=os.environ)
            migrated, _ = run_migrations(raw, ctx)
            cfg = _parse(migrated)
            # Doctor checks operate on the *on-disk* raw so pending migrations
            # are correctly detected; cfg is just the materialized view.
        except ConfigError as e:
            parse_error = e
        except Exception as e:  # noqa: BLE001
            parse_error = e

    return DoctorState(
        cfg_path=cfg_path,
        raw=raw,
        cfg=cfg,
        parse_error=parse_error,
        env=dict(os.environ),
        check_keyring=check_keyring,
    )


def _render_report(console: Console, report: Report, *, verbose: bool) -> None:
    console.print()
    console.print("[bold]hydra doctor — system check[/bold]")

    sections: Dict[str, List[Finding]] = {}
    for f in report.findings:
        sections.setdefault(f.section, []).append(f)

    for section, findings in sections.items():
        console.print()
        console.print(f"  [bold]{section}[/]")
        for f in findings:
            glyph = _GLYPHS[f.level]
            console.print(f"    {glyph} {f.message}")
            if verbose and f.details:
                for line in f.details.splitlines():
                    console.print(f"        [dim]{line}[/dim]")

    console.print()
    n_warn = len(report.warnings)
    n_err = len(report.errors)
    n_fix = len({f.fix_id for f in report.fixable if f.fix_id})

    if report.is_clean:
        console.print("[green]All checks passed.[/green]")
    else:
        parts = []
        if n_warn:
            parts.append(f"{n_warn} warning(s)")
        if n_err:
            parts.append(f"{n_err} error(s)")
        console.print(", ".join(parts) + ".")
        if n_fix:
            console.print(
                f"Run [bold]hydra doctor --fix[/] to apply {n_fix} safe fix(es)."
            )


def _exit_code(report: Report, *, internal_error: bool) -> int:
    if internal_error:
        return EXIT_INTERNAL
    if report.errors:
        return EXIT_ISSUES
    if report.warnings and any(f.fix_id is None for f in report.warnings):
        # Warnings with no fix path → user must act manually → still exit 1
        # so CI catches drift.
        return EXIT_ISSUES
    if report.warnings:
        return EXIT_ISSUES
    return EXIT_OK


__all__ = [
    "DoctorResult",
    "EXIT_INTERNAL",
    "EXIT_ISSUES",
    "EXIT_OK",
    "run_doctor",
]

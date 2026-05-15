from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from hydra import doctor as doctor_mod
from hydra.cli import app


@app.command()
def doctor(
    fix: bool = typer.Option(
        False, "--fix", help="Apply safe fixes (run pending migrations, etc.)"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show full details for each finding"
    ),
    check_keyring: bool = typer.Option(
        False,
        "--check-keyring",
        help="Probe the OS keyring for stored tokens. May prompt for "
        "Keychain access on macOS — disabled by default.",
    ),
    check_tokens: bool = typer.Option(
        False,
        "--check-tokens",
        help="Make one network call per host to validate each token and "
        "report its scopes/expiry. Disabled by default to keep doctor offline.",
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Diagnose configuration, tokens, and topology. Use --fix to apply
    pending migrations and other safe automatic fixes.
    """
    console = Console()
    result = doctor_mod.run_doctor(
        config_path=config_path,
        fix=fix,
        verbose=verbose,
        check_keyring=check_keyring,
        check_tokens=check_tokens,
        console=console,
    )
    if result.exit_code != 0:
        raise typer.Exit(code=result.exit_code)

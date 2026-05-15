from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from hydra import paths as paths_mod
from hydra.cli import app
from hydra.config import resolve_config_path


@app.command("config-path")
def show_config_path(
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Print the resolved config file path."""
    typer.echo(str(resolve_config_path(config_path)))


@app.command("journal-path")
def show_journal_path() -> None:
    """Print the resolved journal database path."""
    typer.echo(str(paths_mod.journal_path()))

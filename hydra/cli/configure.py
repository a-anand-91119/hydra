from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from hydra.cli import app
from hydra.wizard import WizardCancelled, apply_token_storage, run_wizard


@app.command()
def configure(
    config_path: Optional[Path] = typer.Option(None, "--config"),
) -> None:
    """Guided wizard for hosts, defaults, and API tokens."""
    console = Console()

    try:
        result = run_wizard(config_path=config_path, console=console)
    except WizardCancelled as e:
        console.print(f"\n[yellow]Configuration not saved:[/yellow] {e or 'aborted'}")
        raise typer.Exit(code=1) from None
    except KeyboardInterrupt:
        console.print("\n[yellow]Configuration not saved:[/yellow] aborted")
        raise typer.Exit(code=1) from None

    console.print(f"\n[green]✓[/green] Config saved to [bold]{result.config_path}[/bold]")
    apply_token_storage(result, console=console)
    console.print(
        "\n[dim]Next:[/dim] try [bold]hydra create my-repo --dry-run[/bold] to verify the setup."
    )

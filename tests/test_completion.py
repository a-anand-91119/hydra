"""Tab-completion is enabled (Typer's add_completion=True), so the root
command must offer --install-completion. README Quickstart documents it.
"""

from __future__ import annotations

from typer.testing import CliRunner

from hydra import cli as cli_mod


def test_install_completion_option_is_offered():
    result = CliRunner().invoke(cli_mod.app, ["--help"])
    assert result.exit_code == 0, result.output
    assert "--install-completion" in result.output

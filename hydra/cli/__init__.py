"""Typer CLI entry point. Each command lives in its own submodule; importing
this package registers all of them on ``app`` via ``@app.command()`` side
effects.

The bottom of this module re-exports a few names (``preflight_mod``,
``secrets_mod``, ``_execute_create``, ``_journal_records_primary``, etc.)
so existing ``hydra.cli.X`` mock paths in tests keep resolving after the
1,400-line cli.py was split into submodules.
"""

from __future__ import annotations

import typer

from hydra import __version__
from hydra import providers as providers_mod

# Register built-in providers exactly once at CLI entry.
providers_mod.bootstrap()

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Hydra — provision a repo across one primary and N forks.",
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"hydra {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(False, "--version", callback=_version_callback, is_eager=True),
) -> None:
    pass


# ── Command registration + test-facing re-exports ─────────────────────────
# Each command submodule decorates its function(s) with @app.command(...) on
# import, so importing them here is what registers them on `app`.
#
# The ``hydra.X as X_mod`` re-imports below preserve module-attribute paths
# tests patch through (e.g. ``hydra.cli.secrets_mod.get_token``) so the
# subpackage refactor is transparent to existing test code. The function
# re-exports cover direct-import patterns like
# ``from hydra.cli import _execute_create``.
from hydra import doctor as doctor_mod  # noqa: E402, F401
from hydra import (  # noqa: E402, F401
    executor,
    http,
)
from hydra import journal as journal_mod  # noqa: E402, F401
from hydra import paths as paths_mod  # noqa: E402, F401
from hydra import preflight as preflight_mod  # noqa: E402, F401
from hydra import secrets as secrets_mod  # noqa: E402, F401
from hydra.cli import (  # noqa: E402, F401
    configure,
    create,
    doctor,
    paths,
    rotate,
    scan,
    status,
)
from hydra.cli import list as list_cmd  # noqa: E402, F401
from hydra.cli._common import (  # noqa: E402, F401
    _apply_overrides,
    _load_or_die,
    _parse_host_options,
    _preflight_or_die,
    _resolve_token_or_die,
    _resolve_tokens_or_die,
    _verify_token,
)
from hydra.cli.create import (  # noqa: E402, F401
    _execute_create,
    _journal_records_primary,
)
from hydra.cli.list import _refresh_status  # noqa: E402, F401

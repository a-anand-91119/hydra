from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import questionary
from questionary import Choice, Style
from rich.align import Align
from rich.console import Console, Group
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from hydra import secrets as secrets_mod
from hydra.config import (
    Config,
    Defaults,
    GitHubConfig,
    GitLabCloudConfig,
    HostConfig,
    load_config_or_default,
    save_config,
)


@dataclass
class CreateOptions:
    name: str
    description: str
    group: str
    is_private: bool
    github_org: str | None
    mirror: bool
    dry_run: bool = False


# ── Palette ────────────────────────────────────────────────────────────────
ACCENT = "#dc2626"  # crimson — primary brand color
ACCENT_DIM = "#7f1d1d"
MUTED = "#9ca3af"
ASH = "#6b7280"

QSTYLE = Style(
    [
        ("qmark", f"fg:{ACCENT} bold"),
        ("question", "bold"),
        ("answer", f"fg:{ACCENT} bold"),
        ("pointer", f"fg:{ACCENT} bold"),
        ("highlighted", f"fg:{ACCENT} bold"),
        ("selected", f"fg:{ACCENT}"),
        ("instruction", f"fg:{ASH} italic"),
        ("separator", f"fg:{ACCENT_DIM}"),
    ]
)

BANNER = r"""
 _   _ _   _ ____  ____      _
| | | | | | |  _ \|  _ \    / \
| |_| | |_| | | | | |_) |  / _ \
|  _  |  _  | |_| |  _ <  / ___ \
|_| |_|_| |_|____/|_| \_\/_/   \_\
"""

TAGLINE = "one source · many mirrors"
GLYPH = "❦❦❦"


@dataclass
class WizardResult:
    config: Config
    tokens: dict[str, str]
    store: str  # "keyring" | "env" | "skip"
    config_path: Path


class WizardCancelled(Exception):
    pass


# ── Helpers ────────────────────────────────────────────────────────────────


def _ask(prompt) -> Any:
    answer = prompt.ask()
    if answer is None:
        raise WizardCancelled()
    return answer


def _required(value: str) -> bool | str:
    return True if value.strip() else "Required"


def _looks_like_url(value: str) -> bool | str:
    v = value.strip()
    if not v:
        return "Required"
    if not (v.startswith("http://") or v.startswith("https://")):
        return "Must start with http:// or https://"
    return True


def _intro(console: Console, subtitle: str) -> None:
    console.print()
    console.print(
        Align.center(
            Group(
                Text(BANNER.strip("\n"), style=f"bold {ACCENT}"),
                Text(TAGLINE, style=f"italic {ASH}"),
                Text(),
                Text(f"⟶  {subtitle}  ⟵", style=f"bold {ACCENT_DIM}"),
            )
        )
    )
    console.print()


def _section(console: Console, n: int, total: int, title: str) -> None:
    line = Text()
    line.append(f"{GLYPH}  ", style=f"bold {ACCENT}")
    line.append("Forging head ", style=f"italic {ASH}")
    line.append(f"{n}", style=f"bold {ACCENT}")
    line.append(" of ", style=f"italic {ASH}")
    line.append(f"{total}", style=f"bold {ACCENT}")
    line.append("  ·  ", style=ASH)
    line.append(title, style="bold")
    line.append(f"  {GLYPH}", style=f"bold {ACCENT}")
    console.print()
    console.print(Align.center(line))
    console.print()


def _review_rule(console: Console, label: str = "Review") -> None:
    console.print()
    console.print(Rule(f" {label} ", style=ACCENT, characters="═"))
    console.print()


# ── Configure wizard ──────────────────────────────────────────────────────


def run_wizard(*, config_path: Path | None = None, console: Console | None = None) -> WizardResult:
    if not sys.stdin.isatty():
        raise WizardCancelled(
            "configure must be run from a terminal (no TTY detected). "
            "Use environment variables or .env for non-interactive setup."
        )

    console = console or Console()
    existing = load_config_or_default(config_path)

    _intro(console, "configure")
    console.print(
        Align.center(
            Text(
                "Press Enter to accept defaults · Ctrl-C to abort.",
                style=f"dim {ASH}",
            )
        )
    )

    total_steps = 4

    _section(console, 1, total_steps, "Hosts")

    sh_url = _ask(
        questionary.text(
            "Self-hosted GitLab URL:",
            default=existing.self_hosted_gitlab.url or "https://gitlab.example.com",
            validate=_looks_like_url,
            style=QSTYLE,
        )
    )
    gl_url = _ask(
        questionary.text(
            "GitLab.com URL:",
            default=existing.gitlab.url,
            validate=_looks_like_url,
            style=QSTYLE,
        )
    )
    gl_prefix = _ask(
        questionary.text(
            "GitLab.com managed group prefix:",
            default=existing.gitlab.managed_group_prefix,
            instruction="(repos created on gitlab.com land under <prefix>/<group>)",
            validate=_required,
            style=QSTYLE,
        )
    )
    gh_url = _ask(
        questionary.text(
            "GitHub API URL:",
            default=existing.github.url,
            instruction="(use a GHE base URL if applicable)",
            validate=_looks_like_url,
            style=QSTYLE,
        )
    )

    _section(console, 2, total_steps, "GitHub account")

    gh_account = _ask(
        questionary.select(
            "Where should GitHub repos be created?",
            choices=[
                Choice("Under my personal user account", value="user"),
                Choice("Under an organization", value="org"),
            ],
            default="org" if existing.github.org else "user",
            style=QSTYLE,
        )
    )
    gh_org: str | None = None
    if gh_account == "org":
        gh_org = _ask(
            questionary.text(
                "GitHub organization name:",
                default=existing.github.org or "",
                validate=_required,
                style=QSTYLE,
            )
        )

    _section(console, 3, total_steps, "Defaults")

    default_group = _ask(
        questionary.text(
            "Default group path on the self-hosted GitLab:",
            default=existing.defaults.group,
            instruction="(blank = none. Override per-invocation with --group.)",
            style=QSTYLE,
        )
    )
    visibility = _ask(
        questionary.select(
            "Default repository visibility:",
            choices=[
                Choice("Private", value=True),
                Choice("Public", value=False),
            ],
            default=existing.defaults.private,
            style=QSTYLE,
        )
    )

    cfg = Config(
        self_hosted_gitlab=HostConfig(url=sh_url.strip()),
        gitlab=GitLabCloudConfig(url=gl_url.strip(), managed_group_prefix=gl_prefix.strip()),
        github=GitHubConfig(url=gh_url.strip(), org=gh_org.strip() if gh_org else None),
        defaults=Defaults(private=bool(visibility), group=default_group.strip()),
    )

    _section(console, 4, total_steps, "API tokens")

    store = _ask(
        questionary.select(
            "How would you like to store API tokens?",
            choices=[
                Choice("OS keyring (recommended on macOS/Linux desktops)", value="keyring"),
                Choice("Print export lines for my shell or .env", value="env"),
                Choice("Skip — I'll set HYDRA_*_TOKEN env vars myself", value="skip"),
            ],
            style=QSTYLE,
        )
    )

    tokens: dict[str, str] = {}
    if store != "skip":
        console.print(f"  [italic {ASH}]Input is hidden. Press Enter to skip a host.[/]")
        for service, label in (
            ("self_hosted_gitlab", "Self-hosted GitLab"),
            ("gitlab", "GitLab.com"),
            ("github", "GitHub"),
        ):
            token = _ask(questionary.password(f"{label} token:", style=QSTYLE))
            if token.strip():
                tokens[service] = token.strip()

    _review_rule(console)
    console.print(_summary_table(cfg, tokens, store))
    console.print()

    confirmed = _ask(questionary.confirm("Save this configuration?", default=True, style=QSTYLE))
    if not confirmed:
        raise WizardCancelled("aborted by user")

    saved_path = save_config(cfg, config_path)

    return WizardResult(config=cfg, tokens=tokens, store=store, config_path=saved_path)


def _summary_table(cfg: Config, tokens: dict[str, str], store: str) -> Table:
    t = Table(show_header=False, box=None, pad_edge=False, padding=(0, 2))
    t.add_column(style="bold")
    t.add_column()
    t.add_row("self-hosted GitLab", cfg.self_hosted_gitlab.url)
    t.add_row(
        "gitlab.com",
        f"{cfg.gitlab.url}  [dim](prefix: {cfg.gitlab.managed_group_prefix})[/dim]",
    )
    gh_owner = cfg.github.org or "[dim]<your user>[/dim]"
    t.add_row("github", f"{cfg.github.url}  [dim](owner: {gh_owner})[/dim]")
    t.add_row(
        "defaults",
        f"group=[{ACCENT}]{cfg.defaults.group or '∅'}[/], "
        f"visibility=[{ACCENT}]{'private' if cfg.defaults.private else 'public'}[/]",
    )
    if store == "skip":
        token_summary = "[yellow]none — set HYDRA_*_TOKEN env vars[/yellow]"
    elif not tokens:
        token_summary = "[yellow]none entered[/yellow]"
    else:
        targets = ", ".join(sorted(tokens.keys()))
        dest = "keyring" if store == "keyring" else "env-export"
        token_summary = f"[green]{targets}[/green] [dim]→ {dest}[/dim]"
    t.add_row("tokens", token_summary)
    return t


# ── Create wizard ─────────────────────────────────────────────────────────


_REPO_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _valid_repo_name(value: str) -> bool | str:
    v = value.strip()
    if not v:
        return "Required"
    if len(v) > 100:
        return "Must be 100 characters or fewer"
    if not _REPO_NAME_RE.match(v):
        return "Use letters, numbers, hyphens, underscores, periods only"
    if v.startswith((".", "-")) or v.endswith((".", "-")):
        return "Cannot start or end with '.' or '-'"
    return True


def run_create_wizard(*, cfg: Config, console: Console) -> CreateOptions:
    if not sys.stdin.isatty():
        raise WizardCancelled("create wizard needs a terminal — pass a repo name to use flag mode.")

    _intro(console, "create")

    total_steps = 3

    _section(console, 1, total_steps, "Repository")

    name = _ask(
        questionary.text(
            "Repository name:",
            validate=_valid_repo_name,
            instruction="(letters, numbers, '-', '_', '.')",
            style=QSTYLE,
        )
    ).strip()

    description = _ask(questionary.text("Description (optional):", style=QSTYLE)).strip()

    visibility = _ask(
        questionary.select(
            "Visibility:",
            choices=[Choice("Private", value=True), Choice("Public", value=False)],
            default=cfg.defaults.private,
            style=QSTYLE,
        )
    )

    _section(console, 2, total_steps, "Placement")

    group = _ask(
        questionary.text(
            "Self-hosted GitLab group path:",
            default=cfg.defaults.group,
            instruction="(blank = no group; nested paths like 'platform/services' OK)",
            style=QSTYLE,
        )
    ).strip()

    gh_org_default = cfg.github.org or ""
    gh_org_input = _ask(
        questionary.text(
            "GitHub org (blank = your user account):",
            default=gh_org_default,
            style=QSTYLE,
        )
    ).strip()
    github_org = gh_org_input or None

    _section(console, 3, total_steps, "Mirrors")

    mirror = _ask(
        questionary.confirm(
            "Configure push mirrors from self-hosted GitLab to GitLab.com and GitHub?",
            default=True,
            style=QSTYLE,
        )
    )

    opts = CreateOptions(
        name=name,
        description=description,
        group=group,
        is_private=bool(visibility),
        github_org=github_org,
        mirror=mirror,
    )

    _review_rule(console)
    console.print(_create_summary(cfg, opts))
    console.print()

    decision = _ask(
        questionary.select(
            "Proceed?",
            choices=[
                Choice("Unleash the hydra (create now)", value="go"),
                Choice("Dry-run (show plan, no API calls)", value="dry"),
                Choice("Cancel", value="cancel"),
            ],
            style=QSTYLE,
        )
    )

    if decision == "cancel":
        raise WizardCancelled("aborted at review")

    opts.dry_run = decision == "dry"
    return opts


def _create_summary(cfg: Config, opts: CreateOptions) -> Table:
    t = Table(show_header=False, box=None, pad_edge=False, padding=(0, 2))
    t.add_column(style="bold")
    t.add_column()

    t.add_row("repo", f"[{ACCENT}]{opts.name}[/]")
    if opts.description:
        t.add_row("description", opts.description)
    t.add_row("visibility", "private" if opts.is_private else "public")

    sh_path = f"{opts.group}/{opts.name}" if opts.group else opts.name
    t.add_row("self-hosted GitLab", f"{cfg.self_hosted_gitlab.url}/{sh_path}")

    gl_group = (
        f"{cfg.gitlab.managed_group_prefix}/{opts.group}"
        if opts.group
        else cfg.gitlab.managed_group_prefix
    )
    t.add_row(
        "gitlab.com",
        f"{cfg.gitlab.url}/{gl_group}-<timestamp>/{opts.name}  [dim](timestamped leaf)[/dim]",
    )

    gh_owner = opts.github_org or "[dim]<your user>[/dim]"
    t.add_row("github", f"{cfg.github.url} → {gh_owner}/{opts.name}")

    t.add_row(
        "mirrors",
        "[green]configure[/green]" if opts.mirror else "[yellow]skip[/yellow]",
    )
    return t


def apply_token_storage(result: WizardResult, *, console: Console) -> None:
    if not result.tokens:
        return

    if result.store == "keyring":
        for service, token in result.tokens.items():
            secrets_mod.set_token(service, token)
            console.print(f"  [green]✓[/green] stored [bold]{service}[/bold] in OS keyring")
    elif result.store == "env":
        console.print("\n[bold]Add these to your shell rc or a .env file:[/bold]")
        console.print(secrets_mod.export_lines(result.tokens))

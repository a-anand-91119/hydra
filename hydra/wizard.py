from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import questionary
from questionary import Choice, Style
from rich.align import Align
from rich.console import Console, Group
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from hydra import providers as providers_mod
from hydra import secrets as secrets_mod
from hydra.config import (
    Config,
    Defaults,
    HostSpec,
    load_config_or_default,
    save_config,
)


@dataclass
class CreateOptions:
    name: str
    description: str
    group: str
    is_private: bool
    mirror: bool
    dry_run: bool = False


# ── Palette ────────────────────────────────────────────────────────────────
ACCENT = "#dc2626"
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
    tokens: Dict[str, str]  # keyed by host id
    store: str  # "keyring" | "env" | "skip"
    config_path: Path


class WizardCancelled(Exception):
    pass


# ── Validators (unit-tested in test_wizard_validators.py) ─────────────────


def _ask(prompt) -> Any:
    answer = prompt.ask()
    if answer is None:
        raise WizardCancelled()
    return answer


def _required(value: str) -> Union[bool, str]:
    return True if value.strip() else "Required"


def _looks_like_url(value: str) -> Union[bool, str]:
    v = value.strip()
    if not v:
        return "Required"
    if not (v.startswith("http://") or v.startswith("https://")):
        return "Must start with http:// or https://"
    return True


_REPO_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _valid_repo_name(value: str) -> Union[bool, str]:
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


_HOST_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _valid_host_id(value: str, *, taken: Set[str]) -> Union[bool, str]:
    v = value.strip()
    if not v:
        return "Required"
    if not _HOST_ID_RE.match(v):
        return "Use letters, numbers, '-', '_' only"
    if v in taken:
        return f"Host id {v!r} already in use"
    return True


# ── UI helpers ────────────────────────────────────────────────────────────


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


# ── Per-kind option prompts ───────────────────────────────────────────────


def _options_for_gitlab(*, existing: Dict[str, Any]) -> Dict[str, Any]:
    prefix = _ask(
        questionary.text(
            "Managed group prefix (blank = none):",
            default=existing.get("managed_group_prefix") or "",
            instruction="(repos created here land under <prefix>/<group>)",
            style=QSTYLE,
        )
    ).strip()
    add_ts = _ask(
        questionary.confirm(
            "Append a timestamp to created group leaves on this host?",
            default=bool(existing.get("add_timestamp", False)),
            style=QSTYLE,
        )
    )
    out: Dict[str, Any] = {"add_timestamp": bool(add_ts)}
    if prefix:
        out["managed_group_prefix"] = prefix
    return out


def _options_for_github(*, existing: Dict[str, Any]) -> Dict[str, Any]:
    use_org = _ask(
        questionary.confirm(
            "Create repos under an organization (vs. your user account)?",
            default=bool(existing.get("org")),
            style=QSTYLE,
        )
    )
    if not use_org:
        return {"org": None}
    org = _ask(
        questionary.text(
            "GitHub organization name:",
            default=existing.get("org") or "",
            validate=_required,
            style=QSTYLE,
        )
    ).strip()
    return {"org": org}


_OPTIONS_PROMPTS = {
    "gitlab": _options_for_gitlab,
    "github": _options_for_github,
}


def _prompt_host(*, existing: Optional[HostSpec], taken: Set[str]) -> HostSpec:
    """Collect id/kind/url + per-kind options for one host."""
    default_id = existing.id if existing else ""
    default_kind = existing.kind if existing else "gitlab"
    default_url = existing.url if existing else ""
    default_opts = dict(existing.options) if existing else {}

    if existing:
        # Editing — id is fixed.
        host_id = existing.id
    else:
        host_id = _ask(
            questionary.text(
                "Host id (used as the keyring/env key):",
                default=default_id,
                validate=lambda v: _valid_host_id(v, taken=taken),
                style=QSTYLE,
            )
        ).strip()

    kind = _ask(
        questionary.select(
            "Provider kind:",
            choices=[Choice(k, value=k) for k in providers_mod.kinds()],
            default=default_kind,
            style=QSTYLE,
        )
    )
    url = _ask(
        questionary.text(
            "URL:",
            default=default_url,
            validate=_looks_like_url,
            style=QSTYLE,
        )
    ).strip()

    prompt_fn = _OPTIONS_PROMPTS.get(kind)
    options = prompt_fn(existing=default_opts) if prompt_fn else {}

    return HostSpec(id=host_id, kind=kind, url=url, options=options)


def _manage_hosts(
    *, existing: List[HostSpec], console: Console
) -> List[HostSpec]:
    hosts = list(existing)
    while True:
        if hosts:
            console.print()
            for h in hosts:
                caps = providers_mod.capabilities_for(h.kind)
                tag = " [primary-eligible]" if caps.supports_mirror_source else ""
                console.print(f"  • [bold]{h.id}[/]  ({h.kind}){tag}  {h.url}")
            console.print()

        choices = [Choice("Add a host", value="add")]
        if hosts:
            choices += [
                Choice("Edit a host", value="edit"),
                Choice("Remove a host", value="remove"),
                Choice("Done", value="done"),
            ]
        action = _ask(
            questionary.select(
                "Hosts:" if hosts else "No hosts yet — add the first.",
                choices=choices,
                style=QSTYLE,
            )
        )
        if action == "done":
            if not hosts:
                console.print("[yellow]Need at least one host.[/yellow]")
                continue
            return hosts
        if action == "add":
            taken = {h.id for h in hosts}
            hosts.append(_prompt_host(existing=None, taken=taken))
        elif action == "edit":
            target_id = _ask(
                questionary.select(
                    "Edit which host?",
                    choices=[Choice(h.id, value=h.id) for h in hosts],
                    style=QSTYLE,
                )
            )
            for i, h in enumerate(hosts):
                if h.id == target_id:
                    taken = {x.id for x in hosts if x.id != h.id}
                    hosts[i] = _prompt_host(existing=h, taken=taken)
                    break
        elif action == "remove":
            target_id = _ask(
                questionary.select(
                    "Remove which host?",
                    choices=[Choice(h.id, value=h.id) for h in hosts],
                    style=QSTYLE,
                )
            )
            hosts = [h for h in hosts if h.id != target_id]


def _pick_primary(hosts: List[HostSpec], *, default: Optional[str]) -> str:
    candidates = [
        h for h in hosts if providers_mod.capabilities_for(h.kind).supports_mirror_source
    ]
    if not candidates:
        raise WizardCancelled(
            "no host with mirror-source capability — add a GitLab-family host first"
        )
    default_id = default if default and any(h.id == default for h in candidates) else candidates[0].id
    return _ask(
        questionary.select(
            "Primary (source) host:",
            choices=[Choice(f"{h.id}  ({h.kind})", value=h.id) for h in candidates],
            default=default_id,
            style=QSTYLE,
        )
    )


def _pick_forks(
    hosts: List[HostSpec],
    *,
    primary: str,
    default: List[str],
    console: Optional[Console] = None,
    max_attempts: int = 3,
) -> List[str]:
    pool = [h for h in hosts if h.id != primary]
    if not pool:
        raise WizardCancelled("at least one fork is required (add another host first)")
    default_set = set(default)
    choices = [
        Choice(f"{h.id}  ({h.kind})", value=h.id, checked=h.id in default_set)
        for h in pool
    ]
    for _attempt in range(max_attempts):
        picked = _ask(
            questionary.checkbox(
                "Forks (mirror destinations):",
                choices=choices,
                style=QSTYLE,
            )
        )
        if picked:
            return list(picked)
        msg = "[yellow]Pick at least one fork (use Space to select).[/yellow]"
        if console is not None:
            console.print(msg)
        else:
            # Fallback when no console plumbed through (used by some tests).
            print(msg)
    raise WizardCancelled(
        "no forks selected after multiple attempts — re-run `hydra configure`"
    )


# ── Configure wizard ──────────────────────────────────────────────────────


def run_wizard(
    *, config_path: Optional[Path] = None, console: Optional[Console] = None
) -> WizardResult:
    if not sys.stdin.isatty():
        raise WizardCancelled(
            "configure must be run from a terminal (no TTY detected). "
            "Use environment variables or .env for non-interactive setup."
        )

    providers_mod.bootstrap()
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
    hosts = _manage_hosts(existing=list(existing.hosts), console=console)

    _section(console, 2, total_steps, "Topology")
    primary = _pick_primary(hosts, default=existing.primary or None)
    forks = _pick_forks(hosts, primary=primary, default=existing.forks, console=console)

    _section(console, 3, total_steps, "Defaults")
    default_group = _ask(
        questionary.text(
            "Default group path (on the primary host):",
            default=existing.defaults.group,
            instruction="(blank = none. Override per-invocation with --group.)",
            style=QSTYLE,
        )
    )
    visibility = _ask(
        questionary.select(
            "Default repository visibility:",
            choices=[Choice("Private", value=True), Choice("Public", value=False)],
            default=existing.defaults.private,
            style=QSTYLE,
        )
    )

    cfg = Config(
        hosts=hosts,
        primary=primary,
        forks=forks,
        defaults=Defaults(private=bool(visibility), group=default_group.strip()),
    )

    _section(console, 4, total_steps, "API tokens")
    store = _ask(
        questionary.select(
            "How would you like to store API tokens?",
            choices=[
                Choice("OS keyring (recommended on macOS/Linux desktops)", value="keyring"),
                Choice("Print export lines for my shell or .env", value="env"),
                Choice("Skip — I'll set HYDRA_TOKEN_<ID> env vars myself", value="skip"),
            ],
            style=QSTYLE,
        )
    )

    tokens: Dict[str, str] = {}
    if store != "skip":
        console.print(f"  [italic {ASH}]Input is hidden. Press Enter to skip a host.[/]")
        for host in hosts:
            token = _ask(questionary.password(f"{host.id} token:", style=QSTYLE))
            if token.strip():
                tokens[host.id] = token.strip()

    _review_rule(console)
    console.print(_summary_table(cfg, tokens, store))
    console.print()

    confirmed = _ask(questionary.confirm("Save this configuration?", default=True, style=QSTYLE))
    if not confirmed:
        raise WizardCancelled("aborted by user")

    saved_path = save_config(cfg, config_path)
    return WizardResult(config=cfg, tokens=tokens, store=store, config_path=saved_path)


def _summary_table(cfg: Config, tokens: Dict[str, str], store: str) -> Table:
    t = Table(show_header=False, box=None, pad_edge=False, padding=(0, 2))
    t.add_column(style="bold")
    t.add_column()
    for h in cfg.hosts:
        role = []
        if h.id == cfg.primary:
            role.append("primary")
        if h.id in cfg.forks:
            role.append("fork")
        role_str = f" [dim]({'/'.join(role)})[/dim]" if role else ""
        t.add_row(h.id, f"{h.url}{role_str}")
    t.add_row(
        "defaults",
        f"group=[{ACCENT}]{cfg.defaults.group or '∅'}[/], "
        f"visibility=[{ACCENT}]{'private' if cfg.defaults.private else 'public'}[/]",
    )
    if store == "skip":
        token_summary = "[yellow]none — set HYDRA_TOKEN_<ID> env vars[/yellow]"
    elif not tokens:
        token_summary = "[yellow]none entered[/yellow]"
    else:
        targets = ", ".join(sorted(tokens.keys()))
        dest = "keyring" if store == "keyring" else "env-export"
        token_summary = f"[green]{targets}[/green] [dim]→ {dest}[/dim]"
    t.add_row("tokens", token_summary)
    return t


# ── Create wizard ─────────────────────────────────────────────────────────


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
            f"Group path on the primary host ({cfg.primary}):",
            default=cfg.defaults.group,
            instruction="(blank = no group; nested paths like 'platform/services' OK)",
            style=QSTYLE,
        )
    ).strip()

    _section(console, 3, total_steps, "Mirrors")

    fork_ids = ", ".join(cfg.forks)
    mirror = _ask(
        questionary.confirm(
            f"Configure push mirrors from {cfg.primary} to: {fork_ids}?",
            default=True,
            style=QSTYLE,
        )
    )

    opts = CreateOptions(
        name=name,
        description=description,
        group=group,
        is_private=bool(visibility),
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

    primary = cfg.primary_host()
    primary_path = f"{opts.group}/{opts.name}" if opts.group else opts.name
    t.add_row(f"{primary.id} (primary)", f"{primary.url}/{primary_path}")

    for fork in cfg.fork_hosts():
        suffix = ""
        if fork.kind == "gitlab":
            prefix = fork.options.get("managed_group_prefix") or ""
            base = "/".join(p for p in [prefix, opts.group] if p)
            ts_hint = " [dim](timestamped leaf)[/dim]" if fork.options.get("add_timestamp") else ""
            leaf = f"{base}/{opts.name}" if base else opts.name
            suffix = f"/{leaf}{ts_hint}"
        elif fork.kind == "github":
            owner = fork.options.get("org") or "<your user>"
            suffix = f" → {owner}/{opts.name}"
        t.add_row(f"{fork.id} (fork)", f"{fork.url}{suffix}")

    t.add_row(
        "mirrors",
        "[green]configure[/green]" if opts.mirror else "[yellow]skip[/yellow]",
    )
    return t


def apply_token_storage(result: WizardResult, *, console: Console) -> None:
    if not result.tokens:
        return

    if result.store == "keyring":
        for host_id, token in result.tokens.items():
            secrets_mod.set_token(host_id, token)
            console.print(f"  [green]✓[/green] stored [bold]{host_id}[/bold] in OS keyring")
    elif result.store == "env":
        console.print("\n[bold]Add these to your shell rc or a .env file:[/bold]")
        console.print(secrets_mod.export_lines(result.tokens))

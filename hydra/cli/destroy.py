from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import typer
from rich.console import Console
from rich.table import Table

from hydra import journal as journal_mod
from hydra import providers as providers_mod
from hydra.cli import _common, app
from hydra.config import HostSpec
from hydra.errors import HydraAPIError
from hydra.providers.base import RepoRef
from hydra.utils import safe_int


@dataclass
class _DestroyTarget:
    host_id: str
    name: str
    url: str
    source: str
    project_id: Optional[int]
    is_primary: bool = False


@dataclass
class _NamespaceTarget:
    host_id: str
    group_path: str
    url: str
    source: str = "parsed"


@app.command("destroy")
def destroy(
    name: str = typer.Argument(..., help="Journaled repo name to destroy."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
    delete_namespace: bool = typer.Option(
        False,
        "--delete-namespace",
        "--delete-group",
        help=(
            "Also delete inferred GitLab namespaces after repo deletion. "
            "Use only for groups created by hydra."
        ),
    ),
    config_path: Optional[Path] = typer.Option(None, "--config"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Delete a hydra-created repo and its tracked/probed forks."""
    console = Console()
    cfg = _common._load_or_die(config_path, console)
    primary_spec = cfg.primary_host()
    fork_specs = cfg.fork_hosts()
    specs_by_id = {h.id: h for h in [primary_spec, *fork_specs]}
    providers: Dict[str, Any] = {
        host_id: providers_mod.get(spec.kind)(spec) for host_id, spec in specs_by_id.items()
    }

    try:
        with journal_mod.journal() as j:
            matches = [
                r for r in j.list_repos() if r.name == name and r.primary_host_id == primary_spec.id
            ]
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Could not open journal: {e}[/red]")
        raise typer.Exit(code=1) from None

    if not matches:
        console.print(
            f"[red]No journal entry for '{name}' on primary host {primary_spec.id}.[/red]"
        )
        raise typer.Exit(code=1)
    if len(matches) > 1:
        console.print(
            f"[red]Multiple journal entries named '{name}' exist on "
            f"{primary_spec.id}; destroy by name would be ambiguous.[/red]"
        )
        raise typer.Exit(code=1)

    repo = matches[0]
    tokens = _common._resolve_tokens_or_die(cfg, console=console)
    targets = _discover_targets(
        repo=repo,
        primary_spec=primary_spec,
        fork_specs=fork_specs,
        providers=providers,
        tokens=tokens,
        verbose=verbose,
        console=console,
    )
    namespace_targets = (
        _namespace_targets_for_repos(targets, providers=providers) if delete_namespace else []
    )

    _render_destroy_plan(console, targets, namespace_targets)

    target_count = len(targets) + len(namespace_targets)
    if not yes and not typer.confirm(f"Destroy {target_count} resource(s)?", default=False):
        console.print("[dim]No changes made.[/dim]")
        raise typer.Exit(code=0)

    fork_failed = False
    for target in [t for t in targets if not t.is_primary]:
        if not _delete_target(target, providers=providers, tokens=tokens, console=console):
            fork_failed = True

    if fork_failed:
        console.print("[red]Destroy failed; journal entry preserved.[/red]")
        raise typer.Exit(code=1)

    primary = next((t for t in targets if t.is_primary), None)
    if primary is not None and not _delete_target(
        primary, providers=providers, tokens=tokens, console=console
    ):
        console.print("[red]Destroy failed; journal entry preserved.[/red]")
        raise typer.Exit(code=1)

    namespace_failed = False
    for target in namespace_targets:
        if not _delete_namespace(target, providers=providers, tokens=tokens, console=console):
            namespace_failed = True

    try:
        with journal_mod.journal() as j:
            j.delete_repo_by_project_id(
                primary_host_id=repo.primary_host_id,
                primary_repo_id=repo.primary_repo_id,
            )
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Deleted remote repos but failed to update journal: {e}[/red]")
        raise typer.Exit(code=1) from None

    if namespace_failed:
        console.print(
            "[red]Destroyed repo and removed journal entry, but namespace cleanup failed.[/red]"
        )
        raise typer.Exit(code=1)

    console.print("[green]Destroyed resources and removed journal entry.[/green]")


def _discover_targets(
    *,
    repo: journal_mod.JournalRepo,
    primary_spec: HostSpec,
    fork_specs: List[HostSpec],
    providers: Dict[str, Any],
    tokens: Dict[str, str],
    verbose: bool,
    console: Console,
) -> List[_DestroyTarget]:
    targets = [
        _DestroyTarget(
            host_id=primary_spec.id,
            name=repo.name,
            url=repo.primary_repo_url,
            source="journal",
            project_id=repo.primary_repo_id,
            is_primary=True,
        )
    ]
    primary_namespace = _parse_namespace_from_url(repo.primary_repo_url, repo.name)
    mirrors_by_host = {m.target_host_id: m for m in repo.mirrors}

    for spec in fork_specs:
        mirror = mirrors_by_host.get(spec.id)
        target_id = safe_int(mirror.target_repo_id) if mirror is not None else None
        if target_id is not None:
            targets.append(
                _DestroyTarget(
                    host_id=spec.id,
                    name=repo.name,
                    url=mirror.target_repo_url,
                    source="journal",
                    project_id=target_id,
                )
            )
            continue

        ref = _probe_orphan(
            spec=spec,
            provider=providers[spec.id],
            token=tokens[spec.id],
            name=repo.name,
            namespace=primary_namespace,
            verbose=verbose,
            console=console,
        )
        if ref is None:
            continue
        targets.append(
            _DestroyTarget(
                host_id=spec.id,
                name=repo.name,
                url=ref.http_url,
                source="probed",
                project_id=ref.project_id,
            )
        )
    return targets


def _probe_orphan(
    *,
    spec: HostSpec,
    provider: Any,
    token: str,
    name: str,
    namespace: Optional[str],
    verbose: bool,
    console: Console,
) -> Optional[RepoRef]:
    try:
        ref = provider.find_repo(token=token, name=name, namespace=namespace)
    except HydraAPIError as e:
        console.print(f"[yellow]Warning:[/yellow] probe failed for {spec.id}: {e.message}")
        return None
    except Exception as e:  # noqa: BLE001
        console.print(f"[yellow]Warning:[/yellow] probe failed for {spec.id}: {e}")
        return None

    if ref is None and verbose:
        console.print(f"[dim]not found on {spec.id}[/dim]")
    return ref


def _namespace_targets_for_repos(
    repo_targets: List[_DestroyTarget], *, providers: Dict[str, Any]
) -> List[_NamespaceTarget]:
    out: List[_NamespaceTarget] = []
    seen: set = set()
    for target in repo_targets:
        delete_namespace = getattr(providers[target.host_id], "delete_namespace", None)
        if not callable(delete_namespace):
            continue
        group_path = _parse_namespace_from_url(target.url, "")
        if group_path is None:
            continue
        key = (target.host_id, group_path)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            _NamespaceTarget(
                host_id=target.host_id,
                group_path=group_path,
                url=_namespace_url_from_repo_url(target.url, group_path),
            )
        )
    return sorted(out, key=lambda t: t.group_path.count("/"), reverse=True)


def _render_destroy_plan(
    console: Console,
    targets: List[_DestroyTarget],
    namespace_targets: List[_NamespaceTarget],
) -> None:
    table = Table(show_header=True, header_style="bold", title="Destroy plan")
    table.add_column("Kind")
    table.add_column("Host")
    table.add_column("Target")
    table.add_column("Source")
    for target in targets:
        table.add_row(
            "repo",
            target.host_id,
            target.url or "[dim](unknown)[/dim]",
            target.source,
        )
    for target in namespace_targets:
        table.add_row("namespace", target.host_id, target.url, target.source)
    console.print(table)


def _delete_target(
    target: _DestroyTarget,
    *,
    providers: Dict[str, Any],
    tokens: Dict[str, str],
    console: Console,
) -> bool:
    provider = providers[target.host_id]
    delete_repo = getattr(provider, "delete_repo", None)
    if not callable(delete_repo):
        console.print(
            f"[yellow]Warning:[/yellow] {target.host_id} provider cannot delete repos; skipping."
        )
        return True
    if target.project_id is None:
        try:
            delete_repo(
                token=tokens[target.host_id],
                repo_url=target.url,
                name=target.name,
            )
        except HydraAPIError as e:
            console.print(f"[red]Failed to delete {target.host_id}: {e.message}[/red]")
            return False
        except Exception as e:  # noqa: BLE001
            console.print(f"[red]Failed to delete {target.host_id}: {e}[/red]")
            return False
        console.print(f"[green]Deleted[/green] {target.host_id}: {target.url}")
        return True

    try:
        delete_repo(token=tokens[target.host_id], project_id=target.project_id)
    except HydraAPIError as e:
        console.print(f"[red]Failed to delete {target.host_id}: {e.message}[/red]")
        return False
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Failed to delete {target.host_id}: {e}[/red]")
        return False

    console.print(f"[green]Deleted[/green] {target.host_id}: {target.url}")
    return True


def _delete_namespace(
    target: _NamespaceTarget,
    *,
    providers: Dict[str, Any],
    tokens: Dict[str, str],
    console: Console,
) -> bool:
    provider = providers[target.host_id]
    delete_namespace = getattr(provider, "delete_namespace", None)
    if not callable(delete_namespace):
        console.print(
            f"[yellow]Warning:[/yellow] {target.host_id} provider cannot delete namespaces; "
            "skipping."
        )
        return True

    try:
        delete_namespace(token=tokens[target.host_id], group_path=target.group_path)
    except HydraAPIError as e:
        console.print(f"[red]Failed to delete {target.host_id} namespace: {e.message}[/red]")
        return False
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Failed to delete {target.host_id} namespace: {e}[/red]")
        return False

    console.print(f"[green]Deleted[/green] {target.host_id} namespace: {target.url}")
    return True


def _namespace_url_from_repo_url(url: str, group_path: str) -> str:
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}/{group_path}"
    except Exception:
        pass
    return group_path


def _parse_namespace_from_url(url: str, name: str) -> Optional[str]:
    """Return the path before the final repo segment in a clone URL."""
    del name
    try:
        path = urlparse(url).path.lstrip("/")
        if path.endswith(".git"):
            path = path[:-4]
        if "/" in path:
            return path[: path.rfind("/")]
        return None
    except Exception:
        return None

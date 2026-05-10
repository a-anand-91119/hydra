"""Provider registry. New provider kinds register themselves via `register()`.

Built-in providers are registered by `bootstrap()` rather than at module import
to avoid circular import surprises (config.py imports this module, and
providers must in turn import API modules that may transitively pull in
hydra.config). Call `bootstrap()` once from your entry point (cli, tests).
"""

from __future__ import annotations

from typing import Callable, Dict, List

from hydra.providers.base import (
    Capabilities,
    HostSpec,
    MirrorInfo,
    MirrorSource,
    NamespaceRef,
    Provider,
    RepoRef,
)

ProviderFactory = Callable[[HostSpec], Provider]

_REGISTRY: Dict[str, ProviderFactory] = {}
_CAPABILITIES: Dict[str, Capabilities] = {}
_BOOTSTRAPPED = False


class ProviderRegistrationError(Exception):
    pass


def register(kind: str, factory: ProviderFactory, capabilities: Capabilities) -> None:
    if kind in _REGISTRY:
        raise ProviderRegistrationError(f"provider kind {kind!r} already registered")
    _REGISTRY[kind] = factory
    _CAPABILITIES[kind] = capabilities


def get(kind: str) -> ProviderFactory:
    if kind not in _REGISTRY:
        raise KeyError(f"Unknown provider kind: {kind!r}. Registered: {sorted(_REGISTRY)}")
    return _REGISTRY[kind]


def kinds() -> List[str]:
    return sorted(_REGISTRY)


def capabilities_for(kind: str) -> Capabilities:
    if kind not in _CAPABILITIES:
        raise KeyError(
            f"No capabilities registered for kind: {kind!r}. "
            f"Did you forget to call hydra.providers.bootstrap()?"
        )
    return _CAPABILITIES[kind]


def bootstrap() -> None:
    """Register the built-in providers. Idempotent."""
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    # Imports kept inside the function to defer module loading until
    # registration is actually requested (avoids circular imports at startup).
    from hydra.providers import github as _github
    from hydra.providers import gitlab as _gitlab

    _gitlab.install()
    _github.install()
    _BOOTSTRAPPED = True


def _reset_for_tests() -> None:
    """Drop registrations so tests can re-bootstrap with isolated state."""
    global _BOOTSTRAPPED
    _REGISTRY.clear()
    _CAPABILITIES.clear()
    _BOOTSTRAPPED = False


__all__ = [
    "Capabilities",
    "HostSpec",
    "MirrorInfo",
    "MirrorSource",
    "NamespaceRef",
    "Provider",
    "ProviderFactory",
    "ProviderRegistrationError",
    "RepoRef",
    "bootstrap",
    "capabilities_for",
    "get",
    "kinds",
    "register",
]

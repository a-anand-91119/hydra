from __future__ import annotations

import json
from dataclasses import dataclass

import requests

HOST_LABELS = {
    "self_hosted_gitlab": "self-hosted GitLab",
    "gitlab": "GitLab.com",
    "github": "GitHub",
}

ENV_VAR = {
    "self_hosted_gitlab": "HYDRA_SELF_HOSTED_GITLAB_TOKEN",
    "gitlab": "HYDRA_GITLAB_TOKEN",
    "github": "HYDRA_GITHUB_TOKEN",
}


def _token_page(host: str, host_url: str | None) -> str:
    """Where the user should mint a fresh token."""
    if host == "github":
        return "https://github.com/settings/tokens"
    if host == "gitlab":
        return "https://gitlab.com/-/user_settings/personal_access_tokens"
    if host_url:
        return f"{host_url.rstrip('/')}/-/user_settings/personal_access_tokens"
    return "<your self-hosted GitLab>/-/user_settings/personal_access_tokens"


SCOPE_REQUIREMENT = {
    "self_hosted_gitlab": "'api' scope",
    "gitlab": "'api' scope",
    "github": "'repo' scope (and 'admin:org' if creating under an org)",
}

BODY_SNIPPET_LIMIT = 140


@dataclass
class HydraAPIError(Exception):
    message: str
    host: str | None = None
    status_code: int | None = None
    hint: str | None = None

    def __str__(self) -> str:
        return self.message


class MirrorReplaceError(HydraAPIError):
    """Raised when a mirror replace operation fails *after* the original
    mirror was deleted on the primary — the host now has no mirror for that
    target. Callers should reflect this in the journal so users notice.
    """


def raise_for_response(
    response: requests.Response,
    *,
    host: str,
    action: str,
    host_url: str | None = None,
) -> requests.Response:
    """Convert a non-2xx Response into a HydraAPIError with an actionable hint.

    Returns the response unchanged on success (status 200/201).
    """
    if response.status_code in (200, 201):
        return response

    label = HOST_LABELS.get(host, host)
    body = _short_body(response)
    code = response.status_code

    if code == 401:
        raise HydraAPIError(
            message=f"{label} authentication failed (401) while {action}",
            host=host,
            status_code=code,
            hint=(
                f"The {label} token was rejected. "
                f"Rotate it at {_token_page(host, host_url)} and re-run "
                f"`hydra configure`, or set {ENV_VAR[host]} in your environment."
            ),
        )

    if code == 403:
        raise HydraAPIError(
            message=f"{label} returned 403 Forbidden while {action}",
            host=host,
            status_code=code,
            hint=(
                f"The {label} token authenticated but lacks permission. "
                f"Required: {SCOPE_REQUIREMENT[host]}. Mint a new token with "
                f"the right scope at {_token_page(host, host_url)}."
            ),
        )

    if code == 404:
        raise HydraAPIError(
            message=f"{label} returned 404 Not Found while {action}",
            host=host,
            status_code=code,
            hint="The host URL or resource path may be wrong — check `hydra config-path`.",
        )

    if code in (409, 422):
        raise HydraAPIError(
            message=f"{label} reported a conflict ({code}) while {action}: {body}",
            host=host,
            status_code=code,
            hint=(
                "A repo or group with this name probably already exists. "
                "Pick a different name or remove the existing one."
            ),
        )

    if 500 <= code < 600:
        raise HydraAPIError(
            message=f"{label} is having problems (HTTP {code}) while {action}",
            host=host,
            status_code=code,
            hint="Server-side issue. Wait a moment and retry; check the host's status page.",
        )

    raise HydraAPIError(
        message=f"{label} returned HTTP {code} while {action}: {body}",
        host=host,
        status_code=code,
    )


def _short_body(response: requests.Response) -> str:
    """Compact, single-line snippet of the response body for error messages."""
    try:
        payload = response.json()
    except ValueError:
        text = response.text.strip()
        return _truncate(text)

    if isinstance(payload, dict):
        for key in ("message", "error", "error_description", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
            if isinstance(value, list) and value:
                return "; ".join(str(v) for v in value)

    return _truncate(json.dumps(payload, separators=(",", ":")))


def _truncate(text: str, limit: int = BODY_SNIPPET_LIMIT) -> str:
    return text[:limit] + ("…" if len(text) > limit else "")

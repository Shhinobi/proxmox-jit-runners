"""Authentication for the mutating API.

Every `POST /api/v1/admin/*` and `PUT /api/v1/admin/policies` was reachable
without any credential: anything that could reach the port could drain the
cluster, purge build caches, rewrite repository policy, or destroy containers.
The deployed mitigation was entirely external (Authelia in front of the public
host), which is fine until the day something else reaches the port.

Proportionate to a homelab, not enterprise IAM: one shared secret, read from a
systemd credential file through the same `CachedSecret` used for the GitHub key
and the Proxmox token, compared with `hmac.compare_digest`.

CSRF is deliberately NOT implemented. The token travels in a custom header, and
there is no cookie auth, so a cross-origin form POST cannot set it and a
cross-origin `fetch` that tries is preflighted. Adding CSRF tokens would be
ceremony against a threat that cannot reach us. What *does* matter is that no
`CORSMiddleware` with a permissive origin is ever added — that single line
would convert this from structurally safe to trivially exploitable.
"""

from __future__ import annotations

import hmac
import logging
from collections.abc import Awaitable, Callable

from fastapi import Header, HTTPException, status

from .config import Settings
from .security import CachedSecret

LOGGER = logging.getLogger("runner_orchestrator")

AdminDependency = Callable[..., Awaitable[None]]


def _extract(authorization: str | None, x_admin_token: str | None) -> str | None:
    if x_admin_token:
        return x_admin_token.strip()
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


def make_admin_dependency(settings: Settings) -> AdminDependency:
    """Build the dependency guarding every admin route.

    Applied once to an `APIRouter`, not per endpoint, so a future admin route is
    protected by default rather than by remembering a decorator.
    """
    token_file = settings.api.admin_token_file
    allow_unauthenticated = settings.api.allow_unauthenticated_admin
    secret = CachedSecret(token_file) if token_file else None

    async def require_admin(
        authorization: str | None = Header(default=None),
        x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    ) -> None:
        expected: str | None = None
        if secret is not None:
            try:
                expected = secret.get()
            except (OSError, ValueError):
                expected = None

        if not expected:
            if allow_unauthenticated:
                # Loud, and surfaced as a dashboard alert, so this is never
                # silently off. Defaults to permitted for one release: config is
                # loaded at import time, so hard-failing here would brick a
                # running deployment on its next restart, and "the security fix
                # took the service down" is how security fixes get reverted.
                return
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="admin token is not configured",
            )

        presented = _extract(authorization, x_admin_token)
        if not presented or not hmac.compare_digest(presented, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="admin authentication required",
            )

    return require_admin


def admin_auth_status(settings: Settings) -> dict[str, object]:
    """What the dashboard shows about its own protection."""
    token_file = settings.api.admin_token_file
    configured = False
    if token_file:
        try:
            configured = bool(CachedSecret(token_file).get())
        except (OSError, ValueError):
            configured = False
    return {
        "enabled": configured,
        "allow_unauthenticated": settings.api.allow_unauthenticated_admin,
        "token_file": token_file or None,
    }

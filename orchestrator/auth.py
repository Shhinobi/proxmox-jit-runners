"""Authentication for the mutating API.

Every `POST`/`PUT` under `/api/v1/admin/` changes real infrastructure -- drain
the cluster, rewrite repository policy, purge a build cache, destroy a
container. A reverse proxy in front of the dashboard does not protect them:
anything that can reach the service port talks to them directly.

Setup happens **in the running application**, not in the shell. On first start
the instance is *unclaimed*: the dashboard shows a setup panel, you set a
password there, and the orchestrator stores its hash itself. There is no secret
to place on disk, no unit file to edit, and no restart.

That leaves one question: who is allowed to claim it? The pattern here follows
what self-hosted software has settled on -- a bounded first-run window. The
claim endpoint answers only while the instance is unclaimed *and* only for the
first `CLAIM_WINDOW_SECONDS` after start. Miss the window and claiming needs a
restart, which requires access to the host anyway. That converts "whoever finds
the port first, forever" into "whoever is present at start", which is a very
different exposure.

CSRF is deliberately not implemented: the token travels in a custom header and
there is no cookie auth, so a cross-origin form cannot set it and a
cross-origin `fetch` is preflighted. What does matter is that no permissive
`CORSMiddleware` is ever added -- that one line would undo this.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import secrets
import time
from collections.abc import Awaitable, Callable
from hashlib import scrypt
from pathlib import Path
from typing import Any

from fastapi import Header, HTTPException, status

from .config import Settings

LOGGER = logging.getLogger("runner_orchestrator")

AdminDependency = Callable[..., Awaitable[None]]

# How long after start an unclaimed instance will accept a claim. Long enough to
# open the dashboard and choose a password, short enough that an instance left
# running for weeks is not sitting there waiting to be adopted by anyone.
CLAIM_WINDOW_SECONDS = 30 * 60

_SCRYPT = {"n": 2**14, "r": 8, "p": 1, "dklen": 32}


class AdminCredential:
    """The stored admin password hash, owned and managed by the application."""

    def __init__(self, path: Path, *, started_at: float | None = None) -> None:
        self.path = path
        self.started_at = time.monotonic() if started_at is None else started_at

    # --- state ---------------------------------------------------------

    def _read(self) -> dict[str, Any] | None:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) and data.get("hash") else None

    @property
    def is_claimed(self) -> bool:
        return self._read() is not None

    @property
    def claim_window_remaining(self) -> int:
        remaining = CLAIM_WINDOW_SECONDS - (time.monotonic() - self.started_at)
        return max(0, int(remaining))

    @property
    def can_claim(self) -> bool:
        return not self.is_claimed and self.claim_window_remaining > 0

    # --- mutation ------------------------------------------------------

    def claim(self, password: str) -> None:
        if self.is_claimed:
            raise PermissionError("this instance already has an admin password")
        if self.claim_window_remaining <= 0:
            raise PermissionError(
                "the first-run setup window has closed; restart the service to reopen it"
            )
        self._write(password)
        LOGGER.warning("admin password set through first-run setup")

    def change(self, current: str, new: str) -> None:
        if not self.verify(current):
            raise PermissionError("current password is incorrect")
        self._write(new)
        LOGGER.warning("admin password changed")

    def _write(self, password: str) -> None:
        if len(password) < 12:
            raise ValueError("admin password must be at least 12 characters")
        salt = secrets.token_bytes(16)
        digest = scrypt(password.encode("utf-8"), salt=salt, **_SCRYPT)
        payload = json.dumps(
            {
                "version": 1,
                "salt": salt.hex(),
                "hash": digest.hex(),
                "updated_at": int(time.time()),
            }
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Written 0600 via a temp file, so a reader never sees a half-written
        # credential and the file is never briefly world-readable.
        tmp = self.path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(tmp, self.path)

    # --- verification --------------------------------------------------

    def verify(self, password: str) -> bool:
        record = self._read()
        if record is None:
            return False
        try:
            salt = bytes.fromhex(str(record["salt"]))
            expected = bytes.fromhex(str(record["hash"]))
        except (KeyError, ValueError):
            return False
        candidate = scrypt(password.encode("utf-8"), salt=salt, **_SCRYPT)
        return hmac.compare_digest(candidate, expected)

    def status(self) -> dict[str, object]:
        return {
            "claimed": self.is_claimed,
            "can_claim": self.can_claim,
            "claim_window_seconds": self.claim_window_remaining,
        }


def credential_path(settings: Settings) -> Path:
    """Beside the database, which the service already owns and can write."""
    configured = settings.api.admin_token_file
    if configured:
        return Path(configured)
    return Path(settings.database_path).resolve().parent / "admin-credential.json"


def _extract(authorization: str | None, x_admin_token: str | None) -> str | None:
    if x_admin_token:
        return x_admin_token.strip()
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


def make_admin_dependency(credential: AdminCredential) -> AdminDependency:
    """Guards every admin route. Applied once to a router, not per endpoint, so
    a route added later is protected by default rather than by remembering."""

    async def require_admin(
        authorization: str | None = Header(default=None),
        x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    ) -> None:
        if not credential.is_claimed:
            # Nothing to check against yet. The dashboard shows a standing
            # alert and a setup panel until this is resolved.
            return
        presented = _extract(authorization, x_admin_token)
        if not presented or not credential.verify(presented):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="admin authentication required",
            )

    return require_admin

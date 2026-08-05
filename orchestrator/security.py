from __future__ import annotations

import hashlib
import hmac
from pathlib import Path


def read_secret(path: str) -> str:
    value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"empty secret file: {path}")
    return value


class CachedSecret:
    """A secret file re-read only when its mtime changes.

    `GitHubClient`/`ProxmoxClient` used to read their credential file fresh
    on every request (for `ProxmoxClient`, every second while polling
    `wait_task`) — a blocking read on a hot async path for a file that in
    practice almost never changes. Caching on mtime keeps a rotated secret
    picked up automatically without re-reading the file on every call.
    """

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._mtime: float | None = None
        self._value = ""

    def get(self) -> str:
        try:
            mtime = self._path.stat().st_mtime
        except OSError:
            mtime = None
        if mtime != self._mtime:
            self._value = read_secret(str(self._path))
            self._mtime = mtime
        return self._value


def verify_webhook(body: bytes, signature: str | None, secret: str) -> bool:
    if not signature or not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

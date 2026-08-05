from __future__ import annotations

import hashlib
import hmac
import os
import time
from pathlib import Path

import pytest

from orchestrator.security import CachedSecret, verify_webhook


def test_valid_signature() -> None:
    body = b'{"action":"queued"}'
    signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert verify_webhook(body, signature, "secret")


def test_invalid_and_missing_signatures() -> None:
    assert not verify_webhook(b"payload", None, "secret")
    assert not verify_webhook(b"payload", "sha256=bad", "secret")


def test_cached_secret_reads_file(tmp_path: Path) -> None:
    path = tmp_path / "secret"
    path.write_text("s3cr3t\n", encoding="utf-8")
    assert CachedSecret(str(path)).get() == "s3cr3t"


def test_cached_secret_does_not_reread_unchanged_file(tmp_path: Path) -> None:
    path = tmp_path / "secret"
    path.write_text("first", encoding="utf-8")
    cached = CachedSecret(str(path))
    assert cached.get() == "first"

    # Rewrite on disk without touching mtime — the cache must keep serving
    # the value it already read rather than re-reading on every call.
    stat = path.stat()
    path.write_text("second", encoding="utf-8")
    os.utime(path, (stat.st_atime, stat.st_mtime))

    assert cached.get() == "first"


def test_cached_secret_rereads_after_mtime_changes(tmp_path: Path) -> None:
    path = tmp_path / "secret"
    path.write_text("first", encoding="utf-8")
    cached = CachedSecret(str(path))
    assert cached.get() == "first"

    time.sleep(0.01)
    path.write_text("second", encoding="utf-8")
    os.utime(path, None)  # bump mtime to "now"

    assert cached.get() == "second"


def test_cached_secret_rejects_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "secret"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ValueError):
        CachedSecret(str(path)).get()

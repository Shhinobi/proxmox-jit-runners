from __future__ import annotations

import hashlib
import hmac

from orchestrator.security import verify_webhook


def test_valid_signature() -> None:
    body = b'{"action":"queued"}'
    signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert verify_webhook(body, signature, "secret")


def test_invalid_and_missing_signatures() -> None:
    assert not verify_webhook(b"payload", None, "secret")
    assert not verify_webhook(b"payload", "sha256=bad", "secret")

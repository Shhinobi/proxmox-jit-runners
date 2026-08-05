from __future__ import annotations

import hashlib
import hmac
import unittest

from orchestrator.security import verify_webhook


class WebhookSecurityTests(unittest.TestCase):
    def test_valid_signature(self) -> None:
        body = b'{"action":"queued"}'
        signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
        self.assertTrue(verify_webhook(body, signature, "secret"))

    def test_invalid_and_missing_signatures(self) -> None:
        self.assertFalse(verify_webhook(b"payload", None, "secret"))
        self.assertFalse(verify_webhook(b"payload", "sha256=bad", "secret"))

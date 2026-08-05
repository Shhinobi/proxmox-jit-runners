from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest import mock

# orchestrator/app.py does `app = create_app()` at import time (for uvicorn),
# which calls load_settings() with no explicit config — point it at a real,
# readable file before the import below, or the module import itself raises.
os.environ.setdefault(
    "RUNNER_ORCHESTRATOR_CONFIG",
    str(Path(__file__).resolve().parent.parent / "orchestrator" / "default-config.yaml"),
)
# configure_telemetry() imports the opentelemetry stack, which isn't
# installed in every environment this suite runs in; the module already
# supports opting out via this env var.
os.environ.setdefault("OTEL_SDK_DISABLED", "true")

from fastapi.testclient import TestClient

from orchestrator.app import create_app
from orchestrator.service import Orchestrator
from tests.helpers import settings
from tests.test_service import FakeBroker, FakeGitHub, FakeProxmox


def _build_client(tmp_dir: str, *, webhook_secret: str = "test-secret") -> tuple[TestClient, Orchestrator]:
    secret_path = Path(tmp_dir) / "webhook-secret"
    secret_path.write_text(webhook_secret, encoding="utf-8")
    db_path = str(Path(tmp_dir) / "state.db")
    base = settings(database_path=db_path, shadow_mode=True)
    cfg = replace(base, github=replace(base.github, webhook_secret_file=str(secret_path)))
    app = create_app(cfg)
    orchestrator: Orchestrator = app.state.orchestrator
    asyncio.run(orchestrator.db.initialize())
    # create_app() wires real GitHub/Proxmox/broker clients; swap in the same
    # fakes used elsewhere so admin routes never touch a real network.
    # NOT using `with TestClient(app)` — that would run the app's lifespan
    # (orchestrator.start()), launching the real reconcile/worker loops.
    orchestrator.github = cast(Any, FakeGitHub())
    orchestrator.proxmox = cast(Any, FakeProxmox())
    orchestrator.broker = cast(Any, FakeBroker())
    return TestClient(app), orchestrator


def _sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _queued_payload() -> bytes:
    return json.dumps(
        {
            "action": "queued",
            "repository": {"full_name": "MetalDevops/NinjaCrawler"},
            "workflow_job": {
                "id": 1,
                "run_id": 1,
                "labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
            },
        }
    ).encode()


class WebhookRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client, self.orchestrator = _build_client(self.temp.name)
        self.secret = "test-secret"

    def test_valid_signature_is_accepted(self) -> None:
        body = _queued_payload()
        response = self.client.post(
            "/webhooks/github",
            content=body,
            headers={
                "X-Hub-Signature-256": _sign(self.secret, body),
                "X-GitHub-Delivery": "delivery-1",
                "X-GitHub-Event": "workflow_job",
                "Content-Type": "application/json",
            },
        )
        self.assertEqual(response.status_code, 202)
        self.assertTrue(response.json()["accepted"])

    def test_invalid_signature_is_rejected(self) -> None:
        body = _queued_payload()
        response = self.client.post(
            "/webhooks/github",
            content=body,
            headers={
                "X-Hub-Signature-256": "sha256=" + "0" * 64,
                "X-GitHub-Delivery": "delivery-2",
                "X-GitHub-Event": "workflow_job",
            },
        )
        self.assertEqual(response.status_code, 401)

    def test_missing_signature_is_rejected(self) -> None:
        body = _queued_payload()
        response = self.client.post(
            "/webhooks/github",
            content=body,
            headers={"X-GitHub-Delivery": "delivery-3", "X-GitHub-Event": "workflow_job"},
        )
        self.assertEqual(response.status_code, 401)

    def test_missing_delivery_header_is_rejected(self) -> None:
        body = _queued_payload()
        response = self.client.post(
            "/webhooks/github",
            content=body,
            headers={
                "X-Hub-Signature-256": _sign(self.secret, body),
                "X-GitHub-Event": "workflow_job",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_missing_event_header_is_rejected(self) -> None:
        body = _queued_payload()
        response = self.client.post(
            "/webhooks/github",
            content=body,
            headers={
                "X-Hub-Signature-256": _sign(self.secret, body),
                "X-GitHub-Delivery": "delivery-3b",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_invalid_json_is_rejected(self) -> None:
        body = b"not-json"
        response = self.client.post(
            "/webhooks/github",
            content=body,
            headers={
                "X-Hub-Signature-256": _sign(self.secret, body),
                "X-GitHub-Delivery": "delivery-4",
                "X-GitHub-Event": "workflow_job",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_oversized_payload_is_rejected_via_content_length_header(self) -> None:
        response = self.client.post(
            "/webhooks/github",
            content=b"{}",
            headers={
                "X-GitHub-Delivery": "delivery-5",
                "X-GitHub-Event": "workflow_job",
                "Content-Length": str(3 * 1024 * 1024),
            },
        )
        self.assertEqual(response.status_code, 413)

    def test_redelivery_of_same_delivery_id_is_deduped(self) -> None:
        body = _queued_payload()
        headers = {
            "X-Hub-Signature-256": _sign(self.secret, body),
            "X-GitHub-Delivery": "delivery-dup",
            "X-GitHub-Event": "workflow_job",
        }
        first = self.client.post("/webhooks/github", content=body, headers=headers)
        second = self.client.post("/webhooks/github", content=body, headers=headers)
        self.assertEqual(first.status_code, 202)
        self.assertEqual(second.status_code, 202)
        self.assertTrue(first.json()["accepted"])
        self.assertFalse(second.json()["accepted"])  # dedup: second insert is a no-op


class HealthRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client, self.orchestrator = _build_client(self.temp.name)

    def test_liveness_is_always_ok(self) -> None:
        response = self.client.get("/health/live")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_readiness_returns_a_status(self) -> None:
        response = self.client.get("/health/ready")
        self.assertIn(response.status_code, {200, 503})
        self.assertIn("checks", response.json())

    def test_metrics_endpoint_serves_prometheus_text(self) -> None:
        response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("runner_orchestrator", response.text)


class AdminRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        # DRAIN_MARKER defaults to /var/lib/runner-orchestrator/drain, a system
        # path this test must not depend on being writable (e.g. CI runners).
        drain_marker = Path(self.temp.name) / "drain-marker"
        for patch_target in ("orchestrator.api.DRAIN_MARKER", "orchestrator.service.DRAIN_MARKER"):
            patcher = mock.patch(patch_target, drain_marker)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client, self.orchestrator = _build_client(self.temp.name)

    def test_drain_then_resume_toggles_marker(self) -> None:
        drain = self.client.post("/api/v1/admin/drain")
        self.assertEqual(drain.status_code, 200)
        self.assertTrue(drain.json()["drain"])
        self.assertTrue(self.orchestrator.draining)

        resume = self.client.post("/api/v1/admin/resume")
        self.assertEqual(resume.status_code, 200)
        self.assertFalse(resume.json()["drain"])
        self.assertFalse(self.orchestrator.draining)

    def test_reconcile_returns_before_after_summary(self) -> None:
        response = self.client.post("/api/v1/admin/reconcile")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertIn("before", body)
        self.assertIn("after", body)

    def test_recycle_ready_succeeds_when_nothing_to_recycle(self) -> None:
        response = self.client.post("/api/v1/admin/recycle-ready")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["scheduled"], 0)

    def test_recycle_ready_refuses_when_pool_or_warm_instance_not_ready(self) -> None:
        asyncio.run(
            self.orchestrator.db.create_instance(
                5000, "10.20.60.100", "warm", "provisioning", "MetalDevops/NinjaCrawler"
            )
        )
        response = self.client.post("/api/v1/admin/recycle-ready")
        self.assertEqual(response.status_code, 409)

    def test_save_policies_rejects_missing_repositories_field(self) -> None:
        response = self.client.put("/api/v1/admin/policies", json={})
        self.assertEqual(response.status_code, 400)

    def test_save_policies_rejects_invalid_installations_type(self) -> None:
        response = self.client.put(
            "/api/v1/admin/policies",
            json={"repositories": {}, "installations": "not-an-object"},
        )
        self.assertEqual(response.status_code, 400)

    def test_cache_purge_rejects_missing_repository(self) -> None:
        response = self.client.post("/api/v1/admin/cache/purge", json={})
        self.assertEqual(response.status_code, 400)

    def test_instances_and_jobs_routes_return_lists(self) -> None:
        self.assertEqual(self.client.get("/api/v1/instances").json(), [])
        self.assertEqual(self.client.get("/api/v1/jobs").json(), [])

    def test_snapshot_route_returns_dashboard_payload(self) -> None:
        response = self.client.get("/api/v1/snapshot")
        self.assertEqual(response.status_code, 200)
        self.assertIn("instances", response.json())

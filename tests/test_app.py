from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
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

import pytest
from fastapi.testclient import TestClient

from orchestrator.app import create_app
from orchestrator.service import Orchestrator
from tests.fakes import FakeBroker, FakeGitHub, FakeProxmox
from tests.helpers import settings


def _build_client(
    tmp_dir: str, *, webhook_secret: str = "test-secret"
) -> tuple[TestClient, Orchestrator]:
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
            "repository": {"full_name": "OldOwner/example-app"},
            "workflow_job": {
                "id": 1,
                "run_id": 1,
                "labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
            },
        }
    ).encode()


@pytest.fixture
def built_client(tmp_path: Path) -> tuple[TestClient, Orchestrator]:
    return _build_client(str(tmp_path))


@pytest.fixture
def client(built_client: tuple[TestClient, Orchestrator]) -> TestClient:
    return built_client[0]


@pytest.fixture
def app_orchestrator(built_client: tuple[TestClient, Orchestrator]) -> Orchestrator:
    return built_client[1]


WEBHOOK_SECRET = "test-secret"


def test_valid_signature_is_accepted(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Delivery": "delivery-1",
            "X-GitHub-Event": "workflow_job",
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 202
    assert response.json()["accepted"]


def test_invalid_signature_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": "sha256=" + "0" * 64,
            "X-GitHub-Delivery": "delivery-2",
            "X-GitHub-Event": "workflow_job",
        },
    )
    assert response.status_code == 401


def test_missing_signature_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={"X-GitHub-Delivery": "delivery-3", "X-GitHub-Event": "workflow_job"},
    )
    assert response.status_code == 401


def test_missing_delivery_header_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Event": "workflow_job",
        },
    )
    assert response.status_code == 400


def test_missing_event_header_is_rejected(client: TestClient) -> None:
    body = _queued_payload()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Delivery": "delivery-3b",
        },
    )
    assert response.status_code == 400


def test_invalid_json_is_rejected(client: TestClient) -> None:
    body = b"not-json"
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
            "X-GitHub-Delivery": "delivery-4",
            "X-GitHub-Event": "workflow_job",
        },
    )
    assert response.status_code == 400


def test_oversized_payload_is_rejected_via_content_length_header(client: TestClient) -> None:
    response = client.post(
        "/webhooks/github",
        content=b"{}",
        headers={
            "X-GitHub-Delivery": "delivery-5",
            "X-GitHub-Event": "workflow_job",
            "Content-Length": str(3 * 1024 * 1024),
        },
    )
    assert response.status_code == 413


def test_redelivery_of_same_delivery_id_is_deduped(client: TestClient) -> None:
    body = _queued_payload()
    headers = {
        "X-Hub-Signature-256": _sign(WEBHOOK_SECRET, body),
        "X-GitHub-Delivery": "delivery-dup",
        "X-GitHub-Event": "workflow_job",
    }
    first = client.post("/webhooks/github", content=body, headers=headers)
    second = client.post("/webhooks/github", content=body, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["accepted"]
    assert not second.json()["accepted"]  # dedup: second insert is a no-op


def test_liveness_is_always_ok(client: TestClient) -> None:
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_returns_a_status(client: TestClient) -> None:
    response = client.get("/health/ready")
    assert response.status_code in {200, 503}
    assert "checks" in response.json()


def test_metrics_endpoint_serves_prometheus_text(client: TestClient) -> None:
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "runner_orchestrator" in response.text


@pytest.fixture
def admin_client(tmp_path: Path):
    # DRAIN_MARKER defaults to /var/lib/runner-orchestrator/drain, a system
    # path this test must not depend on being writable (e.g. CI runners).
    drain_marker = tmp_path / "drain-marker"
    with (
        mock.patch("orchestrator.api.DRAIN_MARKER", drain_marker),
        mock.patch("orchestrator.service.DRAIN_MARKER", drain_marker),
    ):
        yield _build_client(str(tmp_path))


@pytest.fixture
def admin_test_client(admin_client: tuple[TestClient, Orchestrator]) -> TestClient:
    return admin_client[0]


@pytest.fixture
def admin_orchestrator(admin_client: tuple[TestClient, Orchestrator]) -> Orchestrator:
    return admin_client[1]


def test_drain_then_resume_toggles_marker(
    admin_test_client: TestClient, admin_orchestrator: Orchestrator
) -> None:
    drain = admin_test_client.post("/api/v1/admin/drain")
    assert drain.status_code == 200
    assert drain.json()["drain"]
    assert admin_orchestrator.draining

    resume = admin_test_client.post("/api/v1/admin/resume")
    assert resume.status_code == 200
    assert not resume.json()["drain"]
    assert not admin_orchestrator.draining


def test_reconcile_returns_before_after_summary(admin_test_client: TestClient) -> None:
    response = admin_test_client.post("/api/v1/admin/reconcile")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"]
    assert "before" in body
    assert "after" in body


def test_recycle_ready_succeeds_when_nothing_to_recycle(admin_test_client: TestClient) -> None:
    response = admin_test_client.post("/api/v1/admin/recycle-ready")
    assert response.status_code == 200
    assert response.json()["scheduled"] == 0


async def test_recycle_ready_refuses_when_pool_or_warm_instance_not_ready(
    admin_test_client: TestClient, admin_orchestrator: Orchestrator
) -> None:
    await admin_orchestrator.db.create_instance(
        5000, "10.0.0.100", "warm", "provisioning", "OldOwner/example-app"
    )
    response = admin_test_client.post("/api/v1/admin/recycle-ready")
    assert response.status_code == 409


def test_save_policies_rejects_missing_repositories_field(admin_test_client: TestClient) -> None:
    response = admin_test_client.put("/api/v1/admin/policies", json={})
    assert response.status_code == 400


def test_save_policies_rejects_invalid_installations_type(admin_test_client: TestClient) -> None:
    response = admin_test_client.put(
        "/api/v1/admin/policies",
        json={"repositories": {}, "installations": "not-an-object"},
    )
    assert response.status_code == 400


def test_cache_purge_rejects_missing_repository(admin_test_client: TestClient) -> None:
    response = admin_test_client.post("/api/v1/admin/cache/purge", json={})
    assert response.status_code == 400


def test_instances_and_jobs_routes_return_lists(admin_test_client: TestClient) -> None:
    assert admin_test_client.get("/api/v1/instances").json() == []
    assert admin_test_client.get("/api/v1/jobs").json() == []


def test_snapshot_route_returns_dashboard_payload(admin_test_client: TestClient) -> None:
    response = admin_test_client.get("/api/v1/snapshot")
    assert response.status_code == 200
    assert "instances" in response.json()

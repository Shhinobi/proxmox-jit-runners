from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .broker import BrokerClient
from .config import Settings, load_settings
from .db import Database
from .github import GitHubClient
from .proxmox import ProxmoxClient
from .security import read_secret, verify_webhook
from .telemetry import configure_telemetry, set_current_span_attributes
from .service import Orchestrator


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or load_settings()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    database = Database(cfg.database_path)
    orchestrator = Orchestrator(
        cfg,
        database,
        GitHubClient(cfg.github),
        ProxmoxClient(cfg.proxmox),
        BrokerClient(cfg.broker, cfg.cache),
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await orchestrator.start()
        try:
            yield
        finally:
            await orchestrator.stop()

    app = FastAPI(title="Proxmox JIT Runner Orchestrator", version="0.1.0", lifespan=lifespan)
    configure_telemetry(app)
    app.state.orchestrator = orchestrator
    app.state.settings = cfg

    @app.post("/webhooks/github", status_code=status.HTTP_202_ACCEPTED)
    async def github_webhook(
        request: Request,
        x_hub_signature_256: str | None = Header(default=None),
        x_github_delivery: str | None = Header(default=None),
        x_github_event: str | None = Header(default=None),
    ) -> dict[str, object]:
        content_length = request.headers.get("content-length")
        if content_length and content_length.isdigit() and int(content_length) > 2 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="payload too large")
        body = await request.body()
        if len(body) > 2 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="payload too large")
        secret = read_secret(cfg.github.webhook_secret_file)
        if not verify_webhook(body, x_hub_signature_256, secret):
            raise HTTPException(status_code=401, detail="invalid signature")
        if not x_github_delivery or not x_github_event:
            raise HTTPException(status_code=400, detail="missing GitHub delivery headers")
        try:
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise HTTPException(status_code=400, detail="invalid JSON") from exc
        workflow_job = payload.get("workflow_job") or {}
        repository = payload.get("repository") or {}
        set_current_span_attributes(
            {
                "github.event": x_github_event,
                "github.delivery_id": x_github_delivery,
                "github.workflow_job.action": str(payload.get("action", "")),
                "github.workflow_job.id": int(workflow_job.get("id", 0) or 0),
                "github.repository": str(repository.get("full_name", "")),
            }
        )
        inserted, reason = await orchestrator.webhook(x_github_delivery, x_github_event, payload)
        return {"accepted": inserted, "reason": reason}

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def readiness() -> Response:
        ready, checks = await orchestrator.readiness()
        return Response(
            content=json.dumps({"status": "ready" if ready else "not_ready", "checks": checks}),
            media_type="application/json",
            status_code=200 if ready else 503,
        )

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()

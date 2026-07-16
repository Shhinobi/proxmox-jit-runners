from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .api import build_overview, public_policies, ui_directory
from .broker import BrokerClient
from .config import Settings, load_settings
from .db import Database
from .github import GitHubClient
from .proxmox import ProxmoxClient
from .security import read_secret, verify_webhook
from .service import Orchestrator
from .telemetry import configure_telemetry, set_current_span_attributes


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

    @app.get("/api/v1/overview")
    async def api_overview() -> dict[str, object]:
        return await build_overview(orchestrator, cfg)

    @app.get("/api/v1/instances")
    async def api_instances() -> list[dict[str, object]]:
        return await orchestrator.db.list_instances()

    @app.get("/api/v1/jobs")
    async def api_jobs(
        limit: int = Query(default=50, ge=1, le=200),
        status_filter: str | None = Query(default=None, alias="status"),
    ) -> list[dict[str, object]]:
        return await orchestrator.db.list_jobs(limit=limit, status=status_filter)

    @app.get("/api/v1/deliveries")
    async def api_deliveries(limit: int = Query(default=50, ge=1, le=200)) -> list[dict[str, object]]:
        return await orchestrator.db.list_deliveries(limit=limit)

    @app.get("/api/v1/policies")
    async def api_policies() -> dict[str, object]:
        return public_policies(cfg)

    ui_dir = ui_directory()
    if ui_dir.is_dir():
        @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
        async def root_redirect() -> RedirectResponse:
            # Dedicated dashboard hosts (runners.lan / runners.kyo) land on /.
            # Public webhook host is path-locked to /webhooks/github in Traefik.
            return RedirectResponse(url="/ui/", status_code=307)

        @app.api_route("/ui", methods=["GET", "HEAD"], include_in_schema=False)
        async def ui_redirect() -> RedirectResponse:
            return RedirectResponse(url="/ui/", status_code=307)

        # Mount last so API routes take precedence. html=True serves index.html at /ui/.
        app.mount("/ui", StaticFiles(directory=str(ui_dir), html=True), name="ui")

    return app


app = create_app()

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .api import (
    apply_repository_policies,
    build_dashboard_snapshot,
    build_overview,
    metrics_summary,
    public_policies,
    recycle_ready,
    set_drain,
    ui_directory,
)
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

    app = FastAPI(title="Proxmox JIT Runner Orchestrator", version="0.2.0", lifespan=lifespan)
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
    async def api_instances(
        status_filter: str | None = Query(default=None, alias="status"),
        repository: str | None = Query(default=None),
        mode: str | None = Query(default=None),
    ) -> list[dict[str, object]]:
        return await orchestrator.db.list_instances(
            status=status_filter, repository=repository, mode=mode
        )

    @app.get("/api/v1/jobs")
    async def api_jobs(
        limit: int = Query(default=50, ge=1, le=200),
        status_filter: str | None = Query(default=None, alias="status"),
        repository: str | None = Query(default=None),
        mode: str | None = Query(default=None),
    ) -> list[dict[str, object]]:
        return await orchestrator.db.list_jobs(
            limit=limit, status=status_filter, repository=repository, mode=mode
        )

    @app.get("/api/v1/deliveries")
    async def api_deliveries(
        limit: int = Query(default=50, ge=1, le=200),
        reason: str | None = Query(default=None),
        accepted: bool | None = Query(default=None),
    ) -> list[dict[str, object]]:
        return await orchestrator.db.list_deliveries(
            limit=limit, reason=reason, accepted=accepted
        )

    @app.get("/api/v1/policies")
    async def api_policies() -> dict[str, object]:
        return public_policies(cfg)

    @app.get("/api/v1/metrics-summary")
    async def api_metrics_summary() -> dict[str, object]:
        return metrics_summary()

    @app.get("/api/v1/job-durations")
    async def api_job_durations(
        limit: int = Query(default=30, ge=1, le=100),
    ) -> list[dict[str, object]]:
        return await orchestrator.db.recent_job_durations(limit=limit)

    @app.get("/api/v1/snapshot")
    async def api_snapshot(
        limit: int = Query(default=50, ge=1, le=200),
        status_filter: str | None = Query(default=None, alias="status"),
        repository: str | None = Query(default=None),
        mode: str | None = Query(default=None),
    ) -> dict[str, object]:
        return await build_dashboard_snapshot(
            orchestrator,
            cfg,
            job_limit=limit,
            status=status_filter,
            repository=repository,
            mode=mode,
        )

    @app.get("/api/v1/events")
    async def api_events(
        request: Request,
        interval: float = Query(default=3.0, ge=1.0, le=30.0),
        limit: int = Query(default=50, ge=1, le=200),
        status_filter: str | None = Query(default=None, alias="status"),
        repository: str | None = Query(default=None),
        mode: str | None = Query(default=None),
    ) -> StreamingResponse:
        async def event_stream():
            while True:
                if await request.is_disconnected():
                    break
                snapshot = await build_dashboard_snapshot(
                    orchestrator,
                    cfg,
                    job_limit=limit,
                    status=status_filter,
                    repository=repository,
                    mode=mode,
                )
                payload = json.dumps(snapshot, separators=(",", ":"), default=str)
                yield f"event: snapshot\ndata: {payload}\n\n"
                await asyncio.sleep(interval)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post("/api/v1/admin/drain")
    async def admin_drain() -> dict[str, object]:
        result = set_drain(True)
        return {
            **result,
            "action": "drain",
            "message": "Drain enabled. New jobs stay queued until resume.",
        }

    @app.post("/api/v1/admin/resume")
    async def admin_resume() -> dict[str, object]:
        result = set_drain(False)
        return {
            **result,
            "action": "resume",
            "message": "Drain cleared. Orchestrator accepts new jobs again.",
        }

    @app.post("/api/v1/admin/reconcile")
    async def admin_reconcile() -> dict[str, object]:
        before = await orchestrator.db.status_summary()
        await orchestrator.reconcile()
        after = await orchestrator.db.status_summary()
        return {
            "ok": True,
            "action": "reconcile",
            "drain": orchestrator.draining,
            "message": "Reconcile finished (cleanup, pool/warm replenishment).",
            "before": before,
            "after": after,
        }

    @app.post("/api/v1/admin/recycle-ready")
    async def admin_recycle_ready() -> dict[str, object]:
        try:
            return await recycle_ready(orchestrator)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.put("/api/v1/admin/policies")
    async def admin_save_policies(request: Request) -> dict[str, object]:
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="invalid JSON body") from exc
        repositories = body.get("repositories") if isinstance(body, dict) else None
        if not isinstance(repositories, dict):
            raise HTTPException(status_code=400, detail="body.repositories must be an object")
        try:
            result = apply_repository_policies(orchestrator, repositories)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        # Keep FastAPI app.state aligned for any code reading it.
        app.state.settings = orchestrator.settings
        return result

    ui_dir = ui_directory()
    asset_version = "20260716-3"
    if ui_dir.is_dir():
        @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
        async def root_redirect() -> RedirectResponse:
            return RedirectResponse(url="/ui/", status_code=307)

        @app.api_route("/ui", methods=["GET", "HEAD"], include_in_schema=False)
        async def ui_redirect() -> RedirectResponse:
            return RedirectResponse(url="/ui/", status_code=307)

        @app.get("/ui/", include_in_schema=False)
        async def ui_index() -> Response:
            html = (ui_dir / "index.html").read_text(encoding="utf-8")
            html = html.replace("styles.css", f"styles.css?v={asset_version}")
            html = html.replace("app.js", f"app.js?v={asset_version}")
            return Response(
                content=html,
                media_type="text/html; charset=utf-8",
                headers={"Cache-Control": "no-store"},
            )

        app.mount("/ui", StaticFiles(directory=str(ui_dir), html=False), name="ui")

    return app


app = create_app()

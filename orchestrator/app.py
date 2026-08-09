from __future__ import annotations

import asyncio
import json
import logging
import re
from contextlib import asynccontextmanager

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .api import (
    apply_repository_policies,
    build_dashboard_snapshot,
    build_overview,
    gc_orphan_cache,
    list_cache_inventory,
    metrics_summary,
    public_policies,
    purge_cache_repository,
    recycle_ready,
    rename_cache_repository,
    set_drain,
    ui_directory,
)
from .auth import admin_auth_status, make_admin_dependency
from .broker import BrokerClient
from .config import Settings, load_settings, projected_disk_demand_gb
from .db import Database
from .github import GitHubClient
from .models import InstanceListItem, JobListItem
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
        return await build_overview(orchestrator, orchestrator.settings)

    @app.get("/api/v1/instances")
    async def api_instances(
        status_filter: str | None = Query(default=None, alias="status"),
        repository: str | None = Query(default=None),
        mode: str | None = Query(default=None),
    ) -> list[InstanceListItem]:
        return await orchestrator.db.list_instances(
            status=status_filter, repository=repository, mode=mode
        )

    @app.get("/api/v1/jobs")
    async def api_jobs(
        limit: int = Query(default=50, ge=1, le=200),
        status_filter: str | None = Query(default=None, alias="status"),
        repository: str | None = Query(default=None),
        mode: str | None = Query(default=None),
    ) -> list[JobListItem]:
        return await orchestrator.db.list_jobs(
            limit=limit, status=status_filter, repository=repository, mode=mode
        )

    @app.get("/api/v1/deliveries")
    async def api_deliveries(
        limit: int = Query(default=50, ge=1, le=200),
        reason: str | None = Query(default=None),
        accepted: bool | None = Query(default=None),
    ) -> list[dict[str, object]]:
        return await orchestrator.db.list_deliveries(limit=limit, reason=reason, accepted=accepted)

    @app.get("/api/v1/policies")
    async def api_policies() -> dict[str, object]:
        return public_policies(orchestrator.settings)

    @app.get("/api/v1/metrics-summary")
    async def api_metrics_summary() -> dict[str, object]:
        return metrics_summary()

    @app.get("/api/v1/job-durations")
    async def api_job_durations(
        limit: int = Query(default=30, ge=1, le=100),
    ) -> list[dict[str, object]]:
        return await orchestrator.db.recent_job_durations(limit=limit)

    @app.get("/api/v1/logs")
    async def api_logs(
        limit: int = Query(default=150, ge=1, le=500),
        level: str | None = Query(default=None),
        category: str | None = Query(default=None),
        min_id: int | None = Query(default=None),
    ) -> list[dict[str, object]]:
        return orchestrator.events.list(limit=limit, level=level, category=category, min_id=min_id)

    @app.get("/api/v1/snapshot")
    async def api_snapshot(
        limit: int = Query(default=50, ge=1, le=200),
        status_filter: str | None = Query(default=None, alias="status"),
        repository: str | None = Query(default=None),
        mode: str | None = Query(default=None),
    ) -> dict[str, object]:
        return await build_dashboard_snapshot(
            orchestrator,
            orchestrator.settings,
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
                    orchestrator.settings,
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

    # Guarded as a group: adding a route here inherits the credential check.
    admin = APIRouter(dependencies=[Depends(make_admin_dependency(cfg))])

    @admin.post("/api/v1/admin/drain")
    async def admin_drain() -> dict[str, object]:
        result = set_drain(True)
        return {
            **result,
            "action": "drain",
            "message": "Drain enabled. New jobs stay queued until resume.",
        }

    @admin.post("/api/v1/admin/resume")
    async def admin_resume() -> dict[str, object]:
        result = set_drain(False)
        return {
            **result,
            "action": "resume",
            "message": "Drain cleared. Orchestrator accepts new jobs again.",
        }

    @admin.post("/api/v1/admin/reconcile")
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

    @admin.post("/api/v1/admin/recycle-ready")
    async def admin_recycle_ready() -> dict[str, object]:
        try:
            return await recycle_ready(orchestrator)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @admin.put("/api/v1/admin/policies")
    async def admin_save_policies(request: Request) -> dict[str, object]:
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="invalid JSON body") from exc
        repositories = body.get("repositories") if isinstance(body, dict) else None
        if not isinstance(repositories, dict):
            raise HTTPException(status_code=400, detail="body.repositories must be an object")
        installations = body.get("installations") if isinstance(body, dict) else None
        if installations is not None and not isinstance(installations, dict):
            raise HTTPException(
                status_code=400, detail="body.installations must be an object when provided"
            )
        cache_renames = body.get("cache_renames") if isinstance(body, dict) else None
        if cache_renames is not None and not isinstance(cache_renames, list):
            raise HTTPException(
                status_code=400, detail="body.cache_renames must be an array when provided"
            )
        purge_orphan_cache = bool(
            body.get("purge_orphan_cache") if isinstance(body, dict) else False
        )
        capacity = body.get("capacity") if isinstance(body, dict) else None
        if capacity is not None and not isinstance(capacity, dict):
            raise HTTPException(
                status_code=400, detail="body.capacity must be an object when provided"
            )
        try:
            result = await apply_repository_policies(
                orchestrator,
                repositories,
                installations=installations,
                capacity=capacity,
                cache_renames=cache_renames,
                purge_orphan_cache=purge_orphan_cache,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            # Broker/network failures after validation.
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        # Keep FastAPI app.state aligned for any code reading it.
        app.state.settings = orchestrator.settings
        return result

    @app.get("/api/v1/cache")
    async def api_cache() -> dict[str, object]:
        try:
            return await list_cache_inventory(orchestrator)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @admin.post("/api/v1/admin/cache/purge")
    async def admin_cache_purge(request: Request) -> dict[str, object]:
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="invalid JSON body") from exc
        repository = body.get("repository") if isinstance(body, dict) else None
        if not isinstance(repository, str) or not repository.strip():
            raise HTTPException(status_code=400, detail="body.repository is required")
        try:
            return await purge_cache_repository(orchestrator, repository)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @admin.post("/api/v1/admin/cache/rename")
    async def admin_cache_rename(request: Request) -> dict[str, object]:
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="invalid JSON body") from exc
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="JSON object required")
        src = body.get("from")
        dst = body.get("to")
        if not isinstance(src, str) or not isinstance(dst, str):
            raise HTTPException(status_code=400, detail="body.from and body.to are required")
        try:
            return await rename_cache_repository(orchestrator, src, dst)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @admin.post("/api/v1/admin/cache/gc")
    async def admin_cache_gc(request: Request) -> dict[str, object]:
        min_age_days: int | None = None
        try:
            body = await request.json()
            if isinstance(body, dict) and body.get("min_age_days") is not None:
                min_age_days = int(body["min_age_days"])
        except Exception:  # noqa: BLE001 - malformed body falls back to the default GC threshold
            min_age_days = None
        try:
            return await gc_orphan_cache(orchestrator, min_age_days=min_age_days)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    ui_dir = ui_directory()

    def asset_version() -> str:
        """Cache-bust from the assets' own mtimes.

        The previous version was a hand-edited constant plus a naive
        `str.replace("app.js", ...)` over the whole document — it silently
        stopped matching when the UI moved to css/ and js/ subdirectories, and
        it only worked at all if someone remembered to bump it. Deriving it
        from the files means a deploy can never serve a stale bundle.
        """
        newest = 0.0
        for path in ui_dir.rglob("*"):
            if path.suffix in {".css", ".js", ".html"} and path.is_file():
                newest = max(newest, path.stat().st_mtime)
        return str(int(newest))

    if ui_dir.is_dir():

        @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
        async def root_redirect() -> RedirectResponse:
            return RedirectResponse(url="/ui/", status_code=307)

        @app.api_route("/ui", methods=["GET", "HEAD"], include_in_schema=False)
        async def ui_redirect() -> RedirectResponse:
            return RedirectResponse(url="/ui/", status_code=307)

        @app.api_route("/ui/", methods=["GET", "HEAD"], include_in_schema=False)
        async def ui_index() -> Response:
            html = (ui_dir / "index.html").read_text(encoding="utf-8")
            version = asset_version()
            # Only href="..."/src="..." values, so the token can never land in
            # prose or a script body.
            html = re.sub(
                r'((?:href|src)=")((?:css|js|vendor)/[^"?]+)(")',
                lambda m: f"{m.group(1)}{m.group(2)}?v={version}{m.group(3)}",
                html,
            )
            return Response(
                content=html,
                media_type="text/html; charset=utf-8",
                headers={"Cache-Control": "no-store"},
            )

        app.mount("/ui", StaticFiles(directory=str(ui_dir), html=False), name="ui")

    @app.get("/api/v1/drift")
    async def drift_report() -> dict[str, object]:
        """Read-only. Always safe to call; changes nothing."""
        report = await orchestrator.detect_drift()
        return report.to_dict()

    @admin.post("/api/v1/admin/instances/{vmid}/destroy")
    async def admin_destroy_instance(vmid: int, request: Request) -> dict[str, object]:
        """Tear down one container from the dashboard.

        `force` skips the GitHub deregistration probe entirely — the escape
        hatch for exactly the situation that caused the thin-pool exhaustion incident,
        where every teardown was blocked on an unreachable API.
        """
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            # An empty or malformed body just means "no options".
            body = {}
        force = bool(body.get("force")) if isinstance(body, dict) else False
        instance = await orchestrator.db.find_instance_by_vmid(vmid)
        if instance is None:
            raise HTTPException(status_code=404, detail=f"no instance with vmid {vmid}")
        result = await orchestrator.destroy_instance_by_request(instance, force=force)
        return {"ok": True, "vmid": vmid, "force": force, "message": result}

    @admin.post("/api/v1/admin/drift/apply")
    async def drift_apply(request: Request) -> dict[str, object]:
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="invalid JSON body") from exc
        raw = body.get("confirm_vmids") if isinstance(body, dict) else None
        if not isinstance(raw, list) or not raw:
            raise HTTPException(
                status_code=400,
                detail="body.confirm_vmids must be a non-empty array from the latest scan",
            )
        try:
            confirm = [int(v) for v in raw]
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="confirm_vmids must be integers") from exc
        force = bool(body.get("force")) if isinstance(body, dict) else False
        try:
            return await orchestrator.apply_drift(confirm_vmids=confirm, force=force)
        except RuntimeError as exc:
            # 409: the world moved since the scan the operator approved.
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/v1/storage")
    async def storage_report() -> dict[str, object]:
        snapshot = await orchestrator.storage_snapshot()
        admitted, reason = await orchestrator.storage_admits(purpose="report")
        demand_gb = projected_disk_demand_gb(
            orchestrator.settings.capacity,
            orchestrator.settings.repositories,
            orchestrator.settings.profiles,
        )
        return {
            "storage": orchestrator.settings.proxmox.storage,
            "node": orchestrator.settings.proxmox.node,
            "total_bytes": snapshot.total_bytes if snapshot else None,
            "used_bytes": snapshot.used_bytes if snapshot else None,
            "avail_bytes": snapshot.avail_bytes if snapshot else None,
            "used_ratio": snapshot.used_ratio if snapshot else None,
            "floor_ratio": orchestrator.settings.storage.max_used_percent / 100.0,
            "min_free_gb": orchestrator.settings.storage.min_free_gb,
            "admits": admitted,
            "reason": reason,
            "projected_demand_bytes": demand_gb * 1024**3,
        }

    @app.get("/api/v1/dependencies")
    async def dependencies_report() -> dict[str, object]:
        return {
            # Probe results come from the loop that runs independently of
            # reconcile; the breaker state is what real traffic has observed.
            "probes": orchestrator.dependencies,
            "github_api": {
                **orchestrator.github.breaker.snapshot(),
                **(orchestrator.dependencies.get("github_api") or {}),
            },
            "pending_deregistrations": await orchestrator.db.count_pending_deregistrations(),
            "admin_auth": admin_auth_status(orchestrator.settings),
        }

    # Registered last so every admin route defined above is included, and
    # unconditionally — the UI mount below sits behind an existence check.
    app.include_router(admin)

    return app


app = create_app()

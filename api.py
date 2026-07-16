from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .config import Settings
from .db import Database
from .service import Orchestrator


def public_policies(settings: Settings) -> dict[str, Any]:
    """Return allowlisted repository policy without secret material."""
    installations = {
        name: {
            "account": installation.account,
            "installation_id": installation.installation_id,
            "scope": installation.scope,
            "runner_group_id": installation.runner_group_id,
        }
        for name, installation in settings.github.installations.items()
    }
    repositories = {
        name: {
            "github_installation": policy.github_installation,
            "default_mode": policy.default_mode,
            "warm_capacity": policy.warm_capacity,
            "allowed_modes": list(policy.allowed_modes),
            "allowed_events": list(policy.allowed_events),
            "required_labels": list(policy.required_labels),
        }
        for name, policy in settings.repositories.items()
    }
    return {"installations": installations, "repositories": repositories}


async def build_overview(orchestrator: Orchestrator, settings: Settings) -> dict[str, Any]:
    ready, checks = await orchestrator.readiness()
    summary = await orchestrator.db.status_summary()
    active = await orchestrator.db.count_active()
    pool_ready = await orchestrator.db.count_ready_pool()
    oldest_queued = await orchestrator.db.oldest_queued_job_age()
    warm_ready = 0
    for row in summary["instances"]:
        if row["mode"] == "warm" and row["status"] == "ready":
            warm_ready += int(row["total"])
    queued = next((int(row["total"]) for row in summary["jobs"] if row["status"] == "queued"), 0)
    return {
        "generated_at": int(time.time()),
        "ready": ready,
        "checks": checks,
        "shadow_mode": settings.shadow_mode,
        "drain": orchestrator.draining,
        "capacity": {
            "max_active": settings.capacity.max_active,
            "pool_size": settings.capacity.pool_size,
            "active": active,
            "queued": queued,
            "pool_ready": pool_ready,
            "warm_ready": warm_ready,
            "oldest_queued_job_age_seconds": oldest_queued,
        },
        "summary": summary,
        "proxmox": {
            "node": settings.proxmox.node,
            "template_vmid": settings.proxmox.template_vmid,
            "vmid_range": [settings.proxmox.vmid_start, settings.proxmox.vmid_end],
        },
    }


def ui_directory() -> Path:
    return Path(__file__).resolve().parent / "ui"

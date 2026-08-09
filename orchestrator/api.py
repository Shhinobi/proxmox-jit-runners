from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from prometheus_client import REGISTRY

from .config import (
    ConfigError,
    Settings,
    capacity_runtime_to_dict,
    parse_capacity_config,
    parse_repository_policies,
    projected_disk_demand_gb,
    repository_policy_to_dict,
    save_repository_policies,
    validate_capacity_budget,
    validate_repository_profiles,
)
from .models import InstanceListItem
from .policy import cache_slug
from .service import (
    DRAIN_MARKER,
    DRIFT_CTS_WITHOUT_ROW,
    DRIFT_ROWS_WITHOUT_CT,
    RECONCILE_LAST_SUCCESS,
    Orchestrator,
)


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
        name: repository_policy_to_dict(policy) for name, policy in settings.repositories.items()
    }
    profiles = {
        name: {
            "cores": profile.cores,
            "memory_mb": profile.memory_mb,
            "swap_mb": profile.swap_mb,
            "rootfs_gb": profile.rootfs_gb,
        }
        for name, profile in settings.profiles.items()
    }
    warm_target = sum(int(policy.warm_capacity) for policy in settings.repositories.values())
    return {
        "capacity": capacity_runtime_to_dict(settings.capacity),
        "installations": installations,
        "repositories": repositories,
        "profiles": profiles,
        "mode_options": ["ephemeral", "pool", "persistent"],
        "event_options": [
            "push",
            "workflow_dispatch",
            "schedule",
            "pull_request",
            "release",
        ],
        "label_options": ["self-hosted", "proxmox-lxc", "crossbuild"],
        "warm_target": warm_target,
        "broker_note": (
            "Saving policies updates the orchestrator immediately and syncs the broker "
            "allowlist on the Proxmox host. Capacity max_active is global and must cover "
            "sum(warm_capacity) + pool_size. GitHub App access must still include each "
            "repository."
        ),
    }


async def apply_repository_policies(
    orchestrator: Orchestrator,
    repositories: dict[str, Any],
    installations: dict[str, Any] | None = None,
    capacity: dict[str, Any] | None = None,
    *,
    cache_renames: list[dict[str, str]] | None = None,
    purge_orphan_cache: bool = False,
) -> dict[str, Any]:
    """Validate, sync broker allowlist, persist, and hot-reload policies."""
    try:
        if installations is None:
            effective_installations = orchestrator.settings.github.installations
        else:
            from .config import parse_github_installations

            effective_installations = parse_github_installations(installations)
        parsed = parse_repository_policies(repositories, effective_installations)
        if capacity is None:
            effective_capacity = orchestrator.settings.capacity
        else:
            effective_capacity = parse_capacity_config(
                capacity, base=orchestrator.settings.capacity
            )
        validate_capacity_budget(effective_capacity, parsed)
        validate_repository_profiles(parsed, orchestrator.settings.profiles)
    except ConfigError as exc:
        raise ValueError(str(exc)) from exc

    old_repos = set(orchestrator.settings.repositories.keys())
    new_repos = set(parsed.keys())

    cache_ops: list[str] = []
    # Explicit renames from the UI (e.g. after a GitHub handle change).
    for item in cache_renames or []:
        src = str(item.get("from") or "").strip()
        dst = str(item.get("to") or "").strip()
        if not src or not dst or src == dst:
            continue
        try:
            cache_slug(src)
            cache_slug(dst)
            result = await orchestrator.broker.cache_rename(src, dst)
            cache_ops.append(result)
            orchestrator.log(
                "info",
                "cache",
                f"renamed cache {src} → {dst}",
            )
        except Exception as exc:  # noqa: BLE001
            cache_ops.append(f"rename {src}→{dst} failed: {exc}")
            orchestrator.log(
                "warn",
                "cache",
                f"cache rename failed {src} → {dst}: {exc}",
            )

    broker_result = await orchestrator.broker.sync_allowlist(list(parsed.keys()))
    try:
        new_settings = save_repository_policies(
            repositories,
            installations=installations,
            capacity=capacity_runtime_to_dict(effective_capacity),
        )
    except ConfigError as exc:
        raise ValueError(str(exc)) from exc
    orchestrator.settings = new_settings

    removed = sorted(old_repos - new_repos, key=str.casefold)
    if purge_orphan_cache and removed:
        for repo in removed:
            try:
                cache_slug(repo)
                result = await orchestrator.broker.cache_purge(repo)
                cache_ops.append(result)
                orchestrator.log("info", "cache", f"purged removed-policy cache {repo}")
            except Exception as exc:  # noqa: BLE001
                cache_ops.append(f"purge {repo} failed: {exc}")
                orchestrator.log(
                    "warn",
                    "cache",
                    f"purge removed-policy cache failed {repo}: {exc}",
                )

    # Retire orphan warms (e.g. after handle rename) and replenish under new names.
    try:
        await orchestrator.reconcile()
    except Exception as exc:  # noqa: BLE001 — save already succeeded
        orchestrator.log(
            "warn",
            "admin",
            f"post-policy reconcile failed: {exc}",
        )
    inst_count = len(new_settings.github.installations)
    warm_target = sum(int(p.warm_capacity) for p in new_settings.repositories.values())
    return {
        "ok": True,
        "action": "save-policies",
        "message": (
            f"Saved {len(new_settings.repositories)} repository policy(ies), "
            f"{inst_count} installation(s), capacity max_active="
            f"{new_settings.capacity.max_active}/pool_size="
            f"{new_settings.capacity.pool_size} (warm target={warm_target}), "
            f"synced broker allowlist ({broker_result}), reloaded, and reconciled warm pool."
        ),
        "policies": public_policies(new_settings),
        "broker_synced": True,
        "broker_result": broker_result,
        "broker_note": (
            "Broker allowlist on the Proxmox host was updated automatically. "
            "GitHub App access must still include each repository."
        ),
        "cache_ops": cache_ops,
        "removed_repositories": removed,
    }


async def list_cache_inventory(orchestrator: Orchestrator) -> dict[str, Any]:
    data = await orchestrator.broker.cache_list()
    data["gc_orphan_days"] = int(orchestrator.settings.cache.gc_orphan_days)
    return data


async def purge_cache_repository(orchestrator: Orchestrator, repository: str) -> dict[str, Any]:
    repo = repository.strip()
    try:
        cache_slug(repo)
    except ValueError as exc:
        raise ValueError("repository must be OWNER/NAME (alphanumeric, -, _ only)") from exc
    # Refuse purge of active policy repos unless explicitly still allowed —
    # operator may purge active cache (cold rebuild); warn only.
    active = repo.casefold() in {name.casefold() for name in orchestrator.settings.repositories}
    result = await orchestrator.broker.cache_purge(repo)
    orchestrator.log(
        "warn" if active else "info",
        "cache",
        f"purged cache {repo}" + (" (active policy)" if active else " (orphan)"),
    )
    return {
        "ok": True,
        "action": "cache-purge",
        "repository": repo,
        "active_policy": active,
        "message": result,
    }


async def rename_cache_repository(
    orchestrator: Orchestrator, from_repo: str, to_repo: str
) -> dict[str, Any]:
    src = from_repo.strip()
    dst = to_repo.strip()
    if src == dst:
        raise ValueError("from and to repositories must differ")
    try:
        cache_slug(src)
        cache_slug(dst)
    except ValueError as exc:
        raise ValueError("repositories must be OWNER/NAME (alphanumeric, -, _ only)") from exc
    result = await orchestrator.broker.cache_rename(src, dst)
    orchestrator.log("info", "cache", f"renamed cache {src} → {dst}")
    return {
        "ok": True,
        "action": "cache-rename",
        "from": src,
        "to": dst,
        "message": result,
    }


async def gc_orphan_cache(
    orchestrator: Orchestrator, min_age_days: int | None = None
) -> dict[str, Any]:
    days = (
        int(orchestrator.settings.cache.gc_orphan_days)
        if min_age_days is None
        else int(min_age_days)
    )
    if days < 0:
        raise ValueError("min_age_days must be non-negative")
    result = await orchestrator.broker.cache_gc(days)
    orchestrator.log("info", "cache", f"cache-gc: {result}")
    return {
        "ok": True,
        "action": "cache-gc",
        "min_age_days": days,
        "message": result,
    }


def metrics_summary() -> dict[str, Any]:
    """Snapshot of in-process Prometheus metrics used by the dashboard chart."""
    provision: dict[str, dict[str, float]] = {}
    queue_wait: dict[str, dict[str, float]] = {}
    webhooks: dict[str, float] = {}
    provision_failures: dict[str, float] = {}
    cleanup_failures: dict[str, float] = {}
    warm_registration: dict[str, float] = {}

    for metric in REGISTRY.collect():
        name = metric.name
        for sample in metric.samples:
            labels = dict(sample.labels)
            value = float(sample.value)
            if name == "runner_orchestrator_provision_seconds" and sample.name.endswith("_sum"):
                mode = labels.get("mode", "unknown")
                provision.setdefault(mode, {})["sum"] = value
            elif name == "runner_orchestrator_provision_seconds" and sample.name.endswith("_count"):
                mode = labels.get("mode", "unknown")
                provision.setdefault(mode, {})["count"] = value
            elif name == "runner_orchestrator_job_queue_wait_seconds" and sample.name.endswith(
                "_sum"
            ):
                mode = labels.get("mode", "unknown")
                queue_wait.setdefault(mode, {})["sum"] = value
            elif name == "runner_orchestrator_job_queue_wait_seconds" and sample.name.endswith(
                "_count"
            ):
                mode = labels.get("mode", "unknown")
                queue_wait.setdefault(mode, {})["count"] = value
            elif name == "runner_orchestrator_webhooks_total" and sample.name.endswith("_total"):
                key = f"{labels.get('result', '?')}:{labels.get('reason', '?')}"
                webhooks[key] = value
            elif name == "runner_orchestrator_provision_failures_total" and sample.name.endswith(
                "_total"
            ):
                provision_failures[labels.get("mode", "unknown")] = value
            elif name == "runner_orchestrator_cleanup_failures_total" and sample.name.endswith(
                "_total"
            ):
                cleanup_failures[labels.get("mode", "unknown")] = value
            elif name == "runner_orchestrator_warm_registration_total" and sample.name.endswith(
                "_total"
            ):
                warm_registration[labels.get("outcome", "unknown")] = value

    def averages(series: dict[str, dict[str, float]]) -> dict[str, float | None]:
        out: dict[str, float | None] = {}
        for key, stats in series.items():
            count = stats.get("count", 0.0)
            total = stats.get("sum", 0.0)
            out[key] = (total / count) if count else None
        return out

    return {
        "provision_seconds_avg": averages(provision),
        "queue_wait_seconds_avg": averages(queue_wait),
        "webhooks_total": webhooks,
        "provision_failures_total": provision_failures,
        "cleanup_failures_total": cleanup_failures,
        "warm_registration_total": warm_registration,
    }


def build_alerts(
    overview: dict[str, Any],
    instances: list[InstanceListItem],
    metrics: dict[str, Any],
) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    capacity = overview.get("capacity") or {}
    if not overview.get("ready"):
        alerts.append(
            {
                "severity": "critical",
                "code": "not_ready",
                "message": f"Orchestrator not ready: {overview.get('checks')}",
            }
        )
    if overview.get("drain"):
        alerts.append(
            {
                "severity": "warning",
                "code": "drain",
                "message": "Drain is active; new jobs will not be provisioned.",
            }
        )
    if overview.get("shadow_mode"):
        alerts.append(
            {
                "severity": "info",
                "code": "shadow_mode",
                "message": "Shadow mode is on; webhooks are accepted but runners are not provisioned.",
            }
        )
    queued = int(capacity.get("queued") or 0)
    oldest = int(capacity.get("oldest_queued_job_age_seconds") or 0)
    if queued > 0 and oldest > 60:
        alerts.append(
            {
                "severity": "warning",
                "code": "queue_stale",
                "message": f"{queued} job(s) queued; oldest waiting {oldest}s.",
            }
        )
    warm_ready = int(capacity.get("warm_ready") or 0)
    warm_target = int(capacity.get("warm_target") or 0)
    warm_provisioning = int(capacity.get("warm_provisioning") or 0)
    if (
        warm_target > 0
        and warm_ready < 1
        and warm_provisioning == 0
        and not overview.get("shadow_mode")
    ):
        alerts.append(
            {
                "severity": "warning",
                "code": "warm_missing",
                "message": (
                    f"No warm runner is ready (target {warm_target}). "
                    "Check Logs for provision/destroy failures."
                ),
            }
        )
    elif (
        warm_target > 0
        and warm_ready < warm_target
        and warm_provisioning > 0
        and not overview.get("shadow_mode")
    ):
        alerts.append(
            {
                "severity": "info",
                "code": "warm_replenishing",
                "message": (
                    f"Warm pool replenishing: {warm_ready}/{warm_target} ready, "
                    f"{warm_provisioning} provisioning."
                ),
            }
        )
    delete_failed = [inst for inst in instances if str(inst.get("status")) == "delete_failed"]
    if delete_failed:
        parts = []
        for inst in delete_failed:
            err = str(inst.get("error") or "").strip()
            if err:
                parts.append(f"{inst.get('vmid')} ({err[:120]})")
            else:
                parts.append(str(inst.get("vmid")))
        alerts.append(
            {
                "severity": "warning",
                "code": "delete_failed",
                "message": (
                    f"{len(delete_failed)} instance(s) in delete_failed: "
                    f"{'; '.join(parts)}. See Logs tab."
                ),
            }
        )
    storage = overview.get("storage") or {}
    used_ratio = float(storage.get("used_ratio") or 0.0)
    if storage.get("admits") is False:
        alerts.append(
            {
                "severity": "critical",
                "code": "storage_blocked",
                "message": (
                    f"Replenishment is blocked: {storage.get('reason')}. "
                    "Queued jobs will wait rather than fail."
                ),
            }
        )
    elif used_ratio >= float(storage.get("floor_ratio") or 0.8):
        alerts.append(
            {
                "severity": "warning",
                "code": "storage_low",
                "message": f"Runner storage is {used_ratio * 100:.1f}% used.",
            }
        )

    circuit = (overview.get("dependencies") or {}).get("github_api") or {}
    if circuit.get("state") == "open":
        alerts.append(
            {
                "severity": "critical",
                "code": "github_unreachable",
                "message": (
                    "Cannot reach api.github.com; the circuit is open. Containers are "
                    "still torn down locally and deregistrations are queued."
                ),
            }
        )
    elif circuit.get("state") == "half_open":
        alerts.append(
            {
                "severity": "warning",
                "code": "github_recovering",
                "message": "GitHub connectivity is being re-probed after a failure.",
            }
        )

    pending = int(overview.get("pending_deregistrations") or 0)
    if pending:
        alerts.append(
            {
                "severity": "info",
                "code": "deregistrations_queued",
                "message": (
                    f"{pending} GitHub deregistration(s) queued from local-only teardowns; "
                    "they settle automatically once GitHub is reachable."
                ),
            }
        )

    reconcile = overview.get("reconcile") or {}
    last_success = reconcile.get("last_success_age_seconds")
    if last_success is not None and int(last_success) > 300:
        alerts.append(
            {
                "severity": "critical",
                "code": "reconcile_stalled",
                "message": f"No clean reconcile pass for {int(last_success)}s.",
            }
        )

    auth = overview.get("admin_auth") or {}
    if auth.get("claimed") is False:
        alerts.append(
            {
                "severity": "warning",
                "code": "admin_unauthenticated",
                "message": (
                    "This instance has no admin password, so drain, policy changes and "
                    "container destroy are open to anything that can reach the port. "
                    "Set one from the setup panel."
                ),
            }
        )

    cleanup = metrics.get("cleanup_failures_total") or {}
    if any(float(v) > 0 for v in cleanup.values()):
        alerts.append(
            {
                "severity": "warning",
                "code": "cleanup_failures",
                "message": f"Cleanup failures recorded: {cleanup}",
            }
        )
    return alerts


async def build_overview(
    orchestrator: Orchestrator,
    settings: Settings,
    admin_auth: dict[str, object] | None = None,
) -> dict[str, Any]:
    ready, checks = await orchestrator.readiness()
    summary = await orchestrator.db.status_summary()
    active = await orchestrator.db.count_active()
    pool_ready = await orchestrator.db.count_ready_pool()
    oldest_queued = await orchestrator.db.oldest_queued_job_age()
    warm_ready = 0
    warm_provisioning = 0
    delete_failed = 0
    for row in summary["instances"]:
        if row["mode"] == "warm" and row["status"] == "ready":
            warm_ready += int(row["total"])
        if row["mode"] == "warm" and row["status"] == "provisioning":
            warm_provisioning += int(row["total"])
        if row["status"] == "delete_failed":
            delete_failed += int(row["total"])
    warm_target = sum(int(policy.warm_capacity) for policy in settings.repositories.values())

    # The resource the incident actually ran out of. Cached inside the
    # orchestrator, so this stays cheap enough for the SSE tick.
    snapshot = await orchestrator.storage_snapshot()
    admits, admit_reason = await orchestrator.storage_admits(purpose="overview")
    demand_gb = projected_disk_demand_gb(
        settings.capacity, settings.repositories, settings.profiles
    )
    storage_block = {
        "storage": settings.proxmox.storage,
        "node": settings.proxmox.node,
        "total_bytes": snapshot.total_bytes if snapshot else None,
        "used_bytes": snapshot.used_bytes if snapshot else None,
        "avail_bytes": snapshot.avail_bytes if snapshot else None,
        "used_ratio": snapshot.used_ratio if snapshot else None,
        "floor_ratio": settings.storage.max_used_percent / 100.0,
        "min_free_gb": settings.storage.min_free_gb,
        "admits": admits,
        "reason": admit_reason,
        "projected_demand_bytes": demand_gb * 1024**3,
    }
    pending_deregistrations = await orchestrator.db.count_pending_deregistrations()
    last_success = RECONCILE_LAST_SUCCESS._value.get()
    reconcile_age = int(time.time() - last_success) if last_success else None
    queued = next((int(row["total"]) for row in summary["jobs"] if row["status"] == "queued"), 0)
    drain = orchestrator.draining
    # Operational status for the dashboard primary badge (health probes keep
    # using `ready` independently so drain does not trip /health/ready).
    if not ready:
        status = "not_ready"
    elif drain:
        status = "draining"
    elif settings.shadow_mode:
        status = "shadow"
    else:
        status = "ready"
    overview = {
        "generated_at": int(time.time()),
        "ready": ready,
        "status": status,
        "checks": checks,
        "shadow_mode": settings.shadow_mode,
        "drain": drain,
        "capacity": {
            "max_active": settings.capacity.max_active,
            "pool_size": settings.capacity.pool_size,
            "active": active,
            "queued": queued,
            "pool_ready": pool_ready,
            "warm_ready": warm_ready,
            "warm_target": warm_target,
            "warm_provisioning": warm_provisioning,
            "delete_failed": delete_failed,
            "oldest_queued_job_age_seconds": oldest_queued,
        },
        "summary": summary,
        "storage": storage_block,
        "dependencies": {
            "github_api": {
                **orchestrator.github.breaker.snapshot(),
                **(orchestrator.dependencies.get("github_api") or {}),
            },
            **{
                name: state
                for name, state in orchestrator.dependencies.items()
                if name != "github_api"
            },
        },
        "pending_deregistrations": pending_deregistrations,
        "admin_auth": admin_auth,
        "reconcile": {
            "last_success_age_seconds": reconcile_age,
            "interval_seconds": settings.capacity.reconcile_seconds,
            "budget_seconds": settings.capacity.reconcile_budget_seconds,
        },
        "proxmox": {
            "node": settings.proxmox.node,
            "template_vmid": settings.proxmox.template_vmid,
            "vmid_range": [settings.proxmox.vmid_start, settings.proxmox.vmid_end],
        },
    }
    return overview


async def build_dashboard_snapshot(
    orchestrator: Orchestrator,
    settings: Settings,
    *,
    admin_auth: dict[str, object] | None = None,
    job_limit: int = 50,
    delivery_limit: int = 50,
    status: str | None = None,
    repository: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    overview = await build_overview(orchestrator, settings, admin_auth=admin_auth)
    all_instances = await orchestrator.db.list_instances()
    instances = await orchestrator.db.list_instances(repository=repository, mode=mode)
    jobs = await orchestrator.db.list_jobs(
        limit=job_limit, status=status, repository=repository, mode=mode
    )
    deliveries = await orchestrator.db.list_deliveries(limit=delivery_limit)
    metrics = metrics_summary()
    durations = await orchestrator.db.recent_job_durations(limit=30)
    alerts = build_alerts(overview, all_instances, metrics)
    logs = orchestrator.events.list(limit=150)
    return {
        "overview": overview,
        # Counts only — the full drift scan hits the hypervisor and is an
        # explicit operator action, not something to run on every SSE tick.
        "drift_counts": {
            "rows_without_container": DRIFT_ROWS_WITHOUT_CT._value.get(),
            "containers_without_row": DRIFT_CTS_WITHOUT_ROW._value.get(),
        },
        "alerts": alerts,
        "instances": instances,
        "jobs": jobs,
        "deliveries": deliveries,
        "policies": public_policies(settings),
        "metrics": metrics,
        "job_durations": durations,
        "logs": logs,
    }


def set_drain(enabled: bool) -> dict[str, Any]:
    DRAIN_MARKER.parent.mkdir(parents=True, exist_ok=True)
    if enabled:
        DRAIN_MARKER.touch()
    else:
        DRAIN_MARKER.unlink(missing_ok=True)
    return {"drain": DRAIN_MARKER.exists(), "marker": str(DRAIN_MARKER)}


async def recycle_ready(orchestrator: Orchestrator) -> dict[str, Any]:
    """Schedule ready pool/warm clones for recycle and run reconcile immediately."""
    before = await orchestrator.db.status_summary()
    result = await orchestrator.db.schedule_ready_recycle()
    await orchestrator.reconcile()
    after = await orchestrator.db.status_summary()
    return {
        "ok": True,
        "action": "recycle-ready",
        "scheduled": result["scheduled"],
        "message": (
            f"Scheduled {result['scheduled']} ready pool/warm instance(s) for recycle "
            "and ran reconcile."
        ),
        "before": before,
        "after": after,
        "drain": orchestrator.draining,
    }


def ui_directory() -> Path:
    return Path(__file__).resolve().parent / "ui"

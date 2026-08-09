from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import re
import time
from collections.abc import Awaitable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from prometheus_client import Counter, Gauge, Histogram

from .broker import BrokerClient
from .config import (
    DEFAULT_PROFILE_NAME,
    GitHubInstallation,
    RepositoryPolicy,
    Settings,
    projected_disk_demand_gb,
)
from .db import Database
from .drift import DriftReport, is_managed
from .eventlog import EventLog
from .github import GitHubClient, GitHubUnavailable
from .policy import Decision, cache_slug, decide, repository_policy
from .proxmox import ProxmoxClient
from .security import CachedSecret
from .telemetry import span

LOGGER = logging.getLogger("runner_orchestrator")
DRAIN_MARKER = Path("/var/lib/runner-orchestrator/drain")

# How long to wait before re-probing a runner GitHub reported as online+busy.
# Short, because the job really may be finishing; but not zero, which is what
# an unconditional `return` amounted to — a full list_runners every 60s pass.
_DESTROY_DEFER_SECONDS = 30

# How long a queued GitHub deregistration is worth retrying. GitHub reaps
# offline ephemeral registrations on its own, so an obligation this old is
# noise rather than work.
_DEREGISTRATION_TTL_SECONDS = 7 * 86400

# A row is created before its container is cloned, and /cluster/resources is
# pvestatd-cached, so "absent" is only meaningful after a grace period...
_CT_MISSING_GRACE_SECONDS = 120
# ...and only after a second, later observation agrees. Persisted rather than
# in-memory so a restart does not reset the count.
_CT_MISSING_STRIKE_SECONDS = 120
# A row stuck mid-teardown means the process died between destroying the
# container and deleting the row. It holds its UNIQUE vmid/ip forever otherwise.
_DELETING_STUCK_SECONDS = 600

# Readiness changes on the order of minutes; recomputing it per client per
# SSE tick was pure overhead and a source of load on the broker.
_READINESS_TTL_SECONDS = 10.0


@dataclass(frozen=True)
class StorageSnapshot:
    total_bytes: int
    used_bytes: int
    avail_bytes: int

    @property
    def used_ratio(self) -> float:
        return (self.used_bytes / self.total_bytes) if self.total_bytes else 0.0

    @property
    def used_percent(self) -> float:
        return self.used_ratio * 100.0


@dataclass
class ReconcileContext:
    """Per-pass state, so phases agree with each other and the pass is bounded."""

    now: int
    started: float
    budget_seconds: float
    github_available: bool = True
    live_vmids: set[int] | None = None
    """The hypervisor's view, fetched once. None means UNKNOWN — never act on it."""
    phase_errors: int = 0
    instance_errors: int = 0
    instances_seen: int = 0
    total_instances: int = 0
    truncated: bool = False

    def expired(self) -> bool:
        return (time.monotonic() - self.started) >= self.budget_seconds


def _subnet_prefix(gateway: str) -> str:
    """First three octets of the runner subnet, taken from the gateway.

    This used to be a hardcoded literal while `proxmox.gateway` was
    configurable, so the two could disagree and a container would come up with
    an address that had no route to its own gateway. Deriving one from the
    other makes that impossible to misconfigure.
    """
    octets = gateway.split(".")
    if len(octets) != 4 or not all(o.isdigit() for o in octets):
        raise ValueError(f"proxmox.gateway is not an IPv4 address: {gateway!r}")
    return ".".join(octets[:3]) + "."


def _parse_avail_mib(broker_output: str) -> int | None:
    """Pull `avail_mib=N` out of the broker's clean-work reply."""
    match = re.search(r"avail_mib=(\d+)", broker_output or "")
    return int(match.group(1)) if match else None


def _events_persist_path(database_path: str) -> Path | None:
    """Sibling file next to the SQLite DB; None for :memory: (nothing to survive)."""
    if database_path == ":memory:":
        return None
    return Path(database_path).resolve().parent / "events.jsonl"


WEBHOOKS = Counter("runner_orchestrator_webhooks_total", "Webhook deliveries", ["result", "reason"])
PROVISION = Histogram(
    "runner_orchestrator_provision_seconds", "Runner provisioning duration", ["mode"]
)
PROVISION_FAILURES = Counter(
    "runner_orchestrator_provision_failures_total", "Provisioning failures", ["mode"]
)
JOBS_FAILED = Counter(
    "runner_orchestrator_jobs_failed_total",
    "Jobs that exhausted retries and landed in terminal status=failed",
    ["mode"],
)
CLEANUP_FAILURES = Counter(
    "runner_orchestrator_cleanup_failures_total", "Cleanup failures", ["mode"]
)
ACTIVE = Gauge(
    "runner_orchestrator_active_instances", "Active runner instances", ["mode", "status"]
)
QUEUE = Gauge("runner_orchestrator_queue_depth", "Queued jobs")
OLDEST_QUEUED = Gauge(
    "runner_orchestrator_oldest_queued_job_age_seconds",
    "Age of the oldest queued workflow job",
)
JOB_QUEUE_WAIT = Histogram(
    "runner_orchestrator_job_queue_wait_seconds",
    "Time from workflow_job queued to in_progress",
    ["mode"],
)
WARM_REGISTRATION = Counter(
    "runner_orchestrator_warm_registration_total",
    "Warm runner registration outcomes",
    ["outcome"],
)

RECONCILE_DURATION = Histogram(
    "runner_orchestrator_reconcile_duration_seconds", "Duration of a reconcile pass"
)
RECONCILE_LAST_SUCCESS = Gauge(
    "runner_orchestrator_reconcile_last_success_timestamp_seconds",
    "Epoch of the last clean, untruncated reconcile pass. Staleness here is the "
    "primary liveness signal: a frozen gauge is indistinguishable from a healthy one.",
)
RECONCILE_TRUNCATED = Counter(
    "runner_orchestrator_reconcile_truncated_total",
    "Reconcile passes that hit their budget and deferred work",
)
RECONCILE_PHASE_FAILURES = Counter(
    "runner_orchestrator_reconcile_phase_failures_total",
    "Reconcile phases that raised",
    ["phase"],
)
INSTANCES_REAPED = Counter(
    "runner_orchestrator_instances_reaped_total",
    "Instance rows removed without a container to destroy",
    ["mode", "reason"],
)
PENDING_DEREGISTRATIONS = Gauge(
    "runner_orchestrator_pending_deregistrations",
    "GitHub registrations queued for removal after a local-only teardown",
)
GITHUB_CIRCUIT_STATE = Gauge(
    "runner_orchestrator_github_circuit_state",
    "0=closed, 1=half-open, 2=open",
)
DELETE_BACKLOG = Gauge(
    "runner_orchestrator_delete_backlog",
    "Instances awaiting or retrying teardown",
    ["status"],
)
DELETE_BACKLOG_OLDEST = Gauge(
    "runner_orchestrator_delete_backlog_oldest_age_seconds",
    "Age of the oldest instance stuck awaiting teardown",
)
STORAGE_FREE_BYTES = Gauge(
    "runner_orchestrator_storage_free_bytes", "Free bytes on the clone target storage"
)
STORAGE_TOTAL_BYTES = Gauge(
    "runner_orchestrator_storage_total_bytes", "Total bytes on the clone target storage"
)
STORAGE_USED_RATIO = Gauge(
    "runner_orchestrator_storage_used_ratio", "Used fraction of the clone target storage"
)
STORAGE_ADMISSION_BLOCKED = Gauge(
    "runner_orchestrator_storage_admission_blocked",
    "1 while replenishment is refused for lack of storage headroom",
)
STORAGE_ADMISSION_DENIED = Counter(
    "runner_orchestrator_storage_admission_denied_total",
    "Clone requests refused for lack of storage headroom",
    ["phase"],
)
PROJECTED_DISK_DEMAND = Gauge(
    "runner_orchestrator_projected_disk_demand_bytes",
    "Worst-case disk the current configuration is allowed to consume",
)
DRIFT_ROWS_WITHOUT_CT = Gauge(
    "runner_orchestrator_drift_rows_without_container",
    "Database rows whose container does not exist on the node",
)
DRIFT_CTS_WITHOUT_ROW = Gauge(
    "runner_orchestrator_drift_containers_without_row",
    "Managed containers on the node with no database row",
)
DEPENDENCY_UP = Gauge(
    "runner_orchestrator_dependency_up",
    "1 when the last out-of-band probe of this dependency succeeded",
    ["dependency"],
)
DEPENDENCY_LATENCY = Histogram(
    "runner_orchestrator_dependency_probe_latency_seconds",
    "Latency of the out-of-band dependency probe",
    ["dependency"],
)


# Toolchain + persistent cache layout injected into every runner process.
# Cargo paths serve Tauri builds; Go paths serve Wails builds.
_RUNNER_PATH = (
    "/home/actions/go/bin:/usr/local/go/bin:/home/actions/.cargo/bin:"
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)


def runner_job_environment(*, cargo_target_dir: str) -> dict[str, str]:
    """Shared env for JIT/ephemeral/pool/persistent job runners."""
    return {
        "PATH": _RUNNER_PATH,
        "RUSTUP_HOME": "/home/actions/.rustup",
        "CARGO_HOME": "/cache/cargo",
        "XWIN_CACHE_DIR": "/cache/cargo-xwin",
        "npm_config_cache": "/cache/npm",
        "CARGO_TARGET_DIR": cargo_target_dir,
        "GOMODCACHE": "/cache/gomod",
        "GOCACHE": "/cache/gocache",
        "GOPATH": "/home/actions/go",
    }


class Orchestrator:
    def __init__(
        self,
        settings: Settings,
        database: Database,
        github: GitHubClient,
        proxmox: ProxmoxClient,
        broker: BrokerClient,
    ):
        self.settings = settings
        self.db = database
        self.github = github
        self.proxmox = proxmox
        self.broker = broker
        self.events = EventLog(maxlen=500, persist_path=_events_persist_path(database.path))
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._background_tasks: set[asyncio.Task[None]] = set()
        self.dependencies: dict[str, dict[str, Any]] = {}
        # mtime-cached, so readiness costs no file I/O once warm.
        self._required_secrets = [
            CachedSecret(settings.github.private_key_file),
            CachedSecret(settings.github.webhook_secret_file),
            CachedSecret(settings.proxmox.api_token_file),
            CachedSecret(settings.broker.key_file),
        ]
        self._readiness_cache: tuple[float, bool, dict[str, object]] | None = None
        self._readiness_lock = asyncio.Lock()
        self._storage_snapshot: StorageSnapshot | None = None
        self._storage_checked_at: float = 0.0
        self._warm_lock = asyncio.Lock()
        self._pool_lock = asyncio.Lock()
        self._allocation_lock = asyncio.Lock()

    def log(
        self,
        level: str,
        category: str,
        message: str,
        *,
        detail: str | None = None,
        **context: Any,
    ) -> None:
        event = self.events.emit(level, category, message, detail=detail, **context)
        log_line = f"[{category}] {message}"
        if detail:
            log_line = f"{log_line} | {detail[:300]}"
        if level == "error":
            LOGGER.error(log_line, extra=event.get("context") or {})
        elif level == "warn":
            LOGGER.warning(log_line, extra=event.get("context") or {})
        else:
            LOGGER.info(log_line, extra=event.get("context") or {})

    async def start(self) -> None:
        await self.db.initialize()
        self.log("info", "system", "orchestrator started")
        self._tasks = [
            asyncio.create_task(self._worker_loop(), name="runner-worker"),
            asyncio.create_task(self._reconcile_loop(), name="runner-reconciler"),
            asyncio.create_task(self._probe_loop(), name="runner-probes"),
        ]

    async def stop(self, *, drain_seconds: float = 5.0) -> None:
        """Shut down, giving in-flight work a chance to finish first.

        Cancelling immediately used to abandon detached tasks (warm recycles,
        replenishment) part-way through a database call, leaving an aiosqlite
        worker thread to report into an event loop that had already closed. In
        production that is only noise on the way out, but it also means a
        teardown interrupted at exactly the wrong moment leaves a row stranded
        in `deleting` for reconcile to recover later.

        So: signal, let the loops notice and background work settle, and only
        then cancel whatever is genuinely stuck.
        """
        self._stop.set()

        # The long-lived loops watch `_stop` and exit on their own; the detached
        # tasks are short. Wait briefly rather than interrupting them.
        pending = [t for t in (*self._tasks, *self._background_tasks) if not t.done()]
        if pending and drain_seconds > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True), timeout=drain_seconds
                )

        stuck = [t for t in (*self._tasks, *self._background_tasks) if not t.done()]
        for task in stuck:
            task.cancel()
        if stuck:
            LOGGER.warning("cancelled %d task(s) still running at shutdown", len(stuck))
            await asyncio.gather(*stuck, return_exceptions=True)
            # A cancelled task may have been inside an aiosqlite call. That
            # query still completes on a worker thread, which then posts the
            # result back with call_soon_threadsafe. Yield so the post lands
            # while the loop is still open instead of raising into a closed one.
            for _ in range(3):
                await asyncio.sleep(0)

        if self.github is not None:
            await self.github.aclose()
        if self.proxmox is not None:
            await self.proxmox.aclose()

    async def webhook(
        self, delivery_id: str, event: str, payload: dict[str, Any]
    ) -> tuple[bool, str]:
        action = str(payload.get("action", ""))
        repository = str(payload.get("repository", {}).get("full_name", ""))
        if event != "workflow_job":
            inserted = await self.db.record_delivery(
                delivery_id, event, action, False, "event_ignored"
            )
            WEBHOOKS.labels("duplicate" if not inserted else "ignored", "event_ignored").inc()
            return inserted, "event_ignored"
        resolved_repository = repository_policy(self.settings, repository)
        if resolved_repository is None:
            inserted = await self.db.record_delivery(
                delivery_id, event, action, False, "repository_not_allowed"
            )
            WEBHOOKS.labels(
                "duplicate" if not inserted else "rejected", "repository_not_allowed"
            ).inc()
            return inserted, "repository_not_allowed"
        installation = self._installation(resolved_repository[0])
        try:
            payload_installation_id = int((payload.get("installation") or {}).get("id", 0))
        except (TypeError, ValueError):
            payload_installation_id = 0
        if payload_installation_id != installation.installation_id:
            inserted = await self.db.record_delivery(
                delivery_id, event, action, False, "installation_mismatch"
            )
            WEBHOOKS.labels(
                "duplicate" if not inserted else "rejected", "installation_mismatch"
            ).inc()
            return inserted, "installation_mismatch"

        if action == "queued":
            decision = decide(self.settings, payload)
            reason = (
                "shadow_mode"
                if decision.accepted and self.settings.shadow_mode
                else decision.reason
            )
            inserted = await self.db.record_delivery(
                delivery_id, event, action, decision.accepted, reason
            )
            if not inserted:
                WEBHOOKS.labels("duplicate", reason).inc()
                return False, "duplicate"
            WEBHOOKS.labels("accepted" if decision.accepted else "rejected", reason).inc()
            if decision.accepted and not self.settings.shadow_mode:
                await self.db.enqueue_job(delivery_id, payload, decision.mode)
            return True, reason

        # Record delivery for observability, but ALWAYS apply lifecycle handlers.
        # If we skip processing on duplicate delivery_id, a crash after INSERT and
        # before _complete_job permanently leaves jobs stuck in running.
        inserted = await self.db.record_delivery(
            delivery_id, event, action, True, "lifecycle_update"
        )
        job_id = int(payload.get("workflow_job", {}).get("id", 0) or 0)
        if action == "in_progress" and job_id:
            runner_name = payload.get("workflow_job", {}).get("runner_name")
            job = await self.db.get_job(job_id)
            if job and str(job.get("status")) not in self.db.JOB_TERMINAL_STATUSES:
                JOB_QUEUE_WAIT.labels(str(job.get("mode") or "unknown")).observe(
                    max(0, int(time.time()) - int(job["created_at"]))
                )
            await self.db.set_job_status(job_id, "running", runner_name=runner_name, error=None)
            await self.db.mark_job_started(job_id)
            # Prefer GitHub's actual runner_name — never trust a pre-claim, and never
            # overwrite a live warm CT's name with a different runner (that produces
            # zombie binds like vmid=5004 / runner_name=pve-lxc-warm-5011).
            instance = None
            if runner_name:
                instance = await self.db.find_instance_by_runner(
                    resolved_repository[0], str(runner_name)
                )
            if not instance:
                by_job = await self.db.find_instance_by_job(job_id)
                if by_job is not None:
                    bound_name = str(runner_name or "")
                    expected_warm = self._warm_runner_name(int(by_job["vmid"]))
                    if by_job["mode"] == "warm" and bound_name and bound_name != expected_warm:
                        self.log(
                            "warn",
                            "job",
                            "GitHub assigned a different warm than the reserved CT; "
                            "releasing reservation (no name clobber)",
                            job_id=job_id,
                            vmid=by_job.get("vmid"),
                            runner_name=bound_name,
                            expected=expected_warm,
                        )
                        # Free the wrongly reserved CT so it can take another job.
                        if str(by_job.get("status")) in {"reserved", "ready", "running"}:
                            await self.db.transition_instance(
                                by_job["id"],
                                {"reserved", "ready", "running"},
                                "ready",
                                job_id=None,
                                last_used_at=int(time.time()),
                            )
                        instance = None
                    else:
                        instance = by_job
            if instance:
                bound_name = str(runner_name or instance.get("runner_name") or "")
                expected_warm = self._warm_runner_name(int(instance["vmid"]))
                # Safety: refuse mismatched warm names even if found by runner lookup race.
                if instance["mode"] == "warm" and bound_name and bound_name != expected_warm:
                    self.log(
                        "warn",
                        "job",
                        "refusing warm bind with runner_name≠vmid",
                        job_id=job_id,
                        vmid=instance.get("vmid"),
                        runner_name=bound_name,
                        expected=expected_warm,
                    )
                else:
                    transitioned = await self.db.transition_instance(
                        instance["id"],
                        {"provisioning", "ready", "reserved", "running"},
                        "running",
                        job_id=job_id,
                        runner_name=bound_name or instance.get("runner_name"),
                    )
                    if transitioned and instance["mode"] == "warm":
                        # A warm became busy; replenish idle supply for the next job.
                        self._schedule_warm_replenishment()
            elif runner_name:
                self.log(
                    "warn",
                    "job",
                    f"in_progress for unknown runner {runner_name}",
                    job_id=job_id,
                    repository=resolved_repository[0],
                    runner_name=str(runner_name),
                )
        elif action == "completed" and job_id:
            workflow_job = payload.get("workflow_job") or {}
            runner_name = workflow_job.get("runner_name")
            conclusion = str(workflow_job.get("conclusion") or "").lower()
            await self._complete_job(
                job_id,
                repository=resolved_repository[0],
                runner_name=str(runner_name) if runner_name else None,
                conclusion=conclusion,
            )
        if not inserted:
            WEBHOOKS.labels("duplicate", "lifecycle_update").inc()
            return False, "duplicate"
        WEBHOOKS.labels("accepted", "lifecycle_update").inc()
        return True, "accepted"

    async def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                job = await self.db.claim_next_job()
                if job is None:
                    await asyncio.sleep(1)
                    continue
                await self._handle_queued(job)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("worker loop failed")
                await asyncio.sleep(2)

    async def _handle_queued(self, job: dict[str, Any]) -> None:
        with span(
            "runner.job.provision",
            {
                "github.workflow_job.id": int(job["job_id"]),
                "github.repository": str(job["repository"]),
                "runner.mode": str(job["mode"]),
            },
        ):
            await self._handle_queued_inner(job)

    async def _handle_queued_inner(self, job: dict[str, Any]) -> None:
        job_id = int(job["job_id"])
        mode = str(job["mode"])
        try:
            installation = self._installation(job["repository"])
            run = await self.github.get_workflow_run(
                installation, job["repository"], int(job["run_id"])
            )
            decision = decide(self.settings, job["payload"], str(run.get("event", "")))
            if not decision.accepted:
                await self.db.set_job_status(job_id, "rejected", error=decision.reason)
                return
            if self.draining:
                await self.db.set_job_status(job_id, "queued", error="orchestrator_draining")
                await asyncio.sleep(5)
                return

            if mode == "persistent":
                existing = await self.db.find_persistent(job["repository"])
                if existing:
                    await self.db.update_instance(
                        existing["id"], "ready", job_id=job_id, last_used_at=int(time.time())
                    )
                    await self.db.set_job_status(
                        job_id,
                        "waiting_for_runner",
                        instance_id=existing["id"],
                        runner_name=existing["runner_name"],
                        error=None,
                    )
                    await self.db.set_job_start_context(
                        job_id, source="persistent", provisioned=True
                    )
                    return

            resolved = repository_policy(self.settings, decision.repository)
            policy = resolved[1] if resolved else None
            # Do NOT pre-claim a specific warm on queued. All warms share the same
            # labels, so GitHub may assign the job to a *different* idle warm than
            # the one we reserved — leaving a zombie busy registration and a stuck
            # job. Binding happens on in_progress by runner_name.
            if (
                mode == "ephemeral"
                and policy is not None
                and policy.warm_capacity > 0
                and self._warm_compatible(job, policy)
            ):
                ready_warms = await self.db.count_warm_ready(decision.repository)
                if ready_warms > 0:
                    self.log(
                        "info",
                        "warm",
                        f"warm supply available ({ready_warms}); waiting for GitHub assignment",
                        job_id=job_id,
                        repository=decision.repository,
                        mode=mode,
                    )
                    await self.db.set_job_status(
                        job_id,
                        "waiting_for_runner",
                        error=None,
                    )
                    # Provisioning is already done — the container exists and is
                    # registered; only GitHub's assignment is outstanding.
                    await self.db.set_job_start_context(job_id, source="warm", provisioned=True)
                    return
                self.log(
                    "info",
                    "warm",
                    "no warm ready; falling back to provision",
                    job_id=job_id,
                    repository=decision.repository,
                    mode=mode,
                )

            if await self.db.count_active() >= self.settings.capacity.max_active:
                await self.db.set_job_status(job_id, "queued", error="capacity_exhausted")
                self.log(
                    "warn",
                    "job",
                    "capacity exhausted; requeue",
                    job_id=job_id,
                    repository=decision.repository,
                    mode=mode,
                )
                await asyncio.sleep(5)
                return

            # A real queued job is REQUEUED, never failed: waiting is
            # recoverable, a burnt attempt is not. During the incident clones
            # died on an opaque Proxmox error and consumed all three attempts.
            admitted, reason = await self.storage_admits(purpose="provision")
            if not admitted:
                STORAGE_ADMISSION_DENIED.labels("provision").inc()
                await self.db.set_job_status(job_id, "queued", error="storage_exhausted")
                self.log(
                    "warn",
                    "job",
                    f"storage exhausted; requeue ({reason})",
                    job_id=job_id,
                    repository=decision.repository,
                    mode=mode,
                )
                await asyncio.sleep(5)
                return

            if policy is not None and policy.max_concurrent > 0:
                repo_active = await self.db.count_active_for_repo(decision.repository)
                if repo_active >= policy.max_concurrent:
                    await self.db.set_job_status(job_id, "queued", error="repo_capacity_exhausted")
                    self.log(
                        "warn",
                        "job",
                        "repository capacity exhausted; requeue",
                        job_id=job_id,
                        repository=decision.repository,
                        mode=mode,
                        active=repo_active,
                        max_concurrent=policy.max_concurrent,
                    )
                    await asyncio.sleep(5)
                    return

            await self._provision(job, decision)
        except Exception as exc:
            PROVISION_FAILURES.labels(mode).inc()
            self.events.exception(
                "job",
                f"job provisioning failed job_id={job_id}",
                exc,
                job_id=job_id,
                mode=mode,
                repository=job.get("repository"),
            )
            LOGGER.exception("job provisioning failed", extra={"job_id": job_id, "mode": mode})
            instance = await self.db.find_instance_by_job(job_id)
            if instance:
                await self._destroy_instance(instance)
            attempts = int(job["attempts"])
            if attempts < 3:
                await self.db.set_job_status(job_id, "queued", error=str(exc)[:500])
                # Back off before the worker loop can claim this job again, so a
                # systemic outage (e.g. Proxmox/GitHub down) doesn't burn all 3
                # attempts almost instantly.
                await asyncio.sleep(min(5 * attempts, 30))
            else:
                await self.db.set_job_status(job_id, "failed", error=str(exc)[:500])
                JOBS_FAILED.labels(mode).inc()

    async def _provision(self, job: dict[str, Any], decision: Decision) -> None:
        started = time.monotonic()
        job_id = int(job["job_id"])
        repository = decision.repository
        installation = self._installation(repository)
        mode = decision.mode
        resolved_policy = repository_policy(self.settings, repository)
        profile_name = resolved_policy[1].profile if resolved_policy else DEFAULT_PROFILE_NAME
        profile = self.settings.profiles[profile_name]

        instance: dict[str, Any] | None = None
        runner_name = self._runner_name(mode, job_id)
        if mode in {"ephemeral", "pool"}:
            instance = await self.db.claim_ready_pool(repository, job_id, runner_name, mode=mode)
        if instance:
            instance_id = int(instance["id"])
            vmid = int(instance["vmid"])
            ip_address = str(instance["ip_address"])
            await self.db.set_job_start_context(job_id, source="pool")
            self._schedule_pool_replenishment()
        else:
            reservation = await self._reserve_instance(
                mode, repository, job_id, runner_name, enforce_capacity=True
            )
            if reservation is None:
                await self.db.set_job_status(job_id, "queued", error="capacity_exhausted")
                await asyncio.sleep(5)
                return
            instance_id, vmid, ip_address = reservation
            # No pre-staged container was available: this job pays for a clone.
            await self.db.set_job_start_context(job_id, source="cold")
            await self.proxmox.clone(vmid, runner_name, ip_address, profile, mode, job_id)

        await self.broker.attach_cache(vmid, cache_slug(repository))
        labels = sorted({str(label).lower() for label in decision.labels})
        credential: dict[str, object] = {
            "mode": mode,
            "runner_name": runner_name,
            "repository": repository,
            "job_id": job_id,
            "labels": labels,
            "runner_url": self.github.runner_url(installation, repository),
            "environment": runner_job_environment(
                cargo_target_dir=f"/cache/target/{int(job['run_id'])}",
            ),
        }
        if mode == "persistent":
            credential["registration_token"] = await self.github.registration_token(
                installation, repository
            )
        else:
            jit = await self.github.generate_jit(installation, repository, runner_name, labels)
            credential["encoded_jit_config"] = str(jit["encoded_jit_config"])

        await self.proxmox.start(vmid)
        # /run is a tmpfs created at boot; inject only after the CT is running.
        await self.broker.inject(vmid, credential)
        final_status = "ready" if mode == "persistent" else "running"
        await self.db.update_instance(
            instance_id,
            final_status,
            repository=repository,
            job_id=job_id,
            runner_name=runner_name,
            last_used_at=int(time.time()),
        )
        await self.db.set_job_status(
            job_id,
            "waiting_for_runner",
            instance_id=instance_id,
            runner_name=runner_name,
            error=None,
        )
        await self.db.set_job_start_context(job_id, provisioned=True)
        PROVISION.labels(mode).observe(time.monotonic() - started)

    async def _allocate(self) -> tuple[int, str]:
        live = await self.proxmox.cluster_vmids()
        records = await self.db.list_instances()
        reserved = live | {int(row["vmid"]) for row in records}
        pve = self.settings.proxmox
        for vmid in range(pve.vmid_start, pve.vmid_end + 1):
            if vmid in reserved:
                continue
            ip_octet = pve.ip_start + (vmid - pve.vmid_start)
            if ip_octet > pve.ip_end:
                break
            return vmid, f"{_subnet_prefix(pve.gateway)}{ip_octet}"
        raise RuntimeError("runner VMID/IP range exhausted")

    async def _reserve_instance(
        self,
        mode: str,
        repository: str | None,
        job_id: int | None,
        runner_name: str | None,
        *,
        enforce_capacity: bool,
    ) -> tuple[int, int, str] | None:
        async with self._allocation_lock:
            if (
                enforce_capacity
                and await self.db.count_active() >= self.settings.capacity.max_active
            ):
                return None
            vmid, ip_address = await self._allocate()
            instance_id = await self.db.create_instance(
                vmid,
                ip_address,
                mode,
                "provisioning",
                repository,
                job_id,
                runner_name,
            )
            return instance_id, vmid, ip_address

    @staticmethod
    def _runner_name(mode: str, job_id: int) -> str:
        return f"pve-lxc-{mode}-{job_id}"

    @staticmethod
    def _warm_runner_name(vmid: int) -> str:
        return f"pve-lxc-warm-{vmid}"

    @staticmethod
    def _pool_runner_name(vmid: int) -> str:
        return f"pve-lxc-pool-{vmid}"

    async def _complete_job(
        self,
        job_id: int,
        *,
        repository: str | None = None,
        runner_name: str | None = None,
        conclusion: str = "",
    ) -> None:
        job = await self.db.get_job(job_id)
        now = int(time.time())
        if job:
            await self.db.set_job_status(job_id, "completed", completed_at=now, error=None)
        instance = await self.db.find_instance_by_job(job_id)
        if not instance and repository and runner_name:
            candidate = await self.db.find_instance_by_runner(repository, runner_name)
            # A late completion must never tear down a newer job that reused the
            # deterministic warm runner name.
            if candidate and candidate.get("job_id") in {None, job_id}:
                instance = candidate
        if not instance:
            return
        if not job:
            LOGGER.warning(
                "completion received without queued job; cleaning matched instance",
                extra={
                    "job_id": job_id,
                    "runner_name": runner_name,
                    "vmid": instance["vmid"],
                },
            )
        if instance["mode"] == "persistent":
            await self.db.update_instance(instance["id"], "ready", job_id=None, last_used_at=now)
            return
        if instance["mode"] == "warm":
            use_count = int(instance.get("use_count") or 0) + 1
            recycle_ok, recycle_reason = self._warm_recycle_allowed(
                instance, use_count=use_count, conclusion=conclusion, now=now
            )
            if recycle_ok:
                transitioned = await self.db.transition_instance(
                    instance["id"],
                    {"provisioning", "ready", "reserved", "running"},
                    "provisioning",
                    job_id=None,
                    last_used_at=now,
                    use_count=use_count,
                    error=None,
                )
                if transitioned:
                    self._schedule_warm_recycle(int(instance["id"]))
                    return
            else:
                self.log(
                    "info",
                    "warm",
                    f"skipping warm recycle ({recycle_reason}); destroying CT",
                    vmid=instance.get("vmid"),
                    repository=instance.get("repository"),
                    conclusion=conclusion or None,
                    use_count=use_count,
                )
        # GitHub expects a fast webhook acknowledgement. Reconciliation performs
        # the potentially slow Proxmox stop/delete task out of band.
        await self.db.transition_instance(
            instance["id"],
            {"provisioning", "ready", "reserved", "running"},
            "delete_pending",
        )

    def _warm_recycle_allowed(
        self,
        instance: dict[str, Any],
        *,
        use_count: int,
        conclusion: str,
        now: int,
    ) -> tuple[bool, str]:
        """Decide whether a warm CT should re-JIT in place after a job."""
        cap = self.settings.capacity
        conclusion = (conclusion or "").lower().strip()
        if cap.warm_destroy_on_failure and conclusion in {
            "failure",
            "cancelled",
            "timed_out",
            "action_required",
            "startup_failure",
        }:
            return False, f"conclusion={conclusion or 'unknown'}"
        if use_count > int(cap.warm_max_uses):
            return False, f"use_count={use_count}>{cap.warm_max_uses}"
        created_at = int(instance.get("created_at") or now)
        age = max(0, now - created_at)
        if age > int(cap.warm_max_age_seconds):
            return False, f"age_seconds={age}>{cap.warm_max_age_seconds}"
        return True, "ok"

    @staticmethod
    def _destroy_retry_backoff(attempts: int) -> int:
        """Seconds to wait after a failed destroy before retrying again.

        Exponential, capped at 1h, so a persistently-failing CT doesn't get
        hammered against Proxmox/GitHub on every 60s reconcile pass.
        """
        return min(60 * (2**attempts), 3600)

    def _destroy_retry_deadline(self, attempts: int, *, now: int) -> int:
        """Absolute epoch second before which teardown must not be retried.

        Jittered so a batch of instances that all failed in the same pass does
        not come back in lockstep and re-create the same thundering herd
        against a dependency that is still down.
        """
        base = self._destroy_retry_backoff(attempts)
        return now + max(1, int(base * random.uniform(0.8, 1.2)))

    def _destroy_reason(self, instance: dict[str, Any], *, now: int) -> str | None:
        """Why this instance should be torn down on this pass, or None.

        Replaces a five-term `or` chain whose operator precedence was load
        bearing and easy to break. Teardown eligibility for delete_pending /
        delete_failed is now an absolute deadline (`delete_not_before`) rather
        than an age measured against `updated_at`, which any unrelated write
        could reset.

        `delete_pending` still qualifies immediately when no deadline is set:
        it is the hot path that returns capacity after every job, and delaying
        its first attempt would regress normal operation.
        """
        status = str(instance["status"])
        cap = self.settings.capacity

        if status in {"delete_pending", "delete_failed"}:
            not_before = instance.get("delete_not_before")
            if not_before is not None and now < int(not_before):
                return None
            return status

        age = now - int(instance["updated_at"])
        if status in {"provisioning", "reserved"} and age > cap.ready_timeout_seconds:
            return f"{status}_timeout"

        idle = now - int(instance.get("last_used_at") or instance["updated_at"])
        if (
            instance["mode"] == "persistent"
            and status == "ready"
            and idle > cap.persistent_idle_seconds
        ):
            return "persistent_idle"
        return None

    async def _destroy_instance(self, instance: dict[str, Any]) -> None:
        with span(
            "runner.instance.destroy",
            {
                "lxc.ctid": int(instance["vmid"]),
                "runner.mode": str(instance["mode"]),
            },
        ):
            await self._destroy_instance_inner(instance)

    async def _destroy_instance_inner(self, instance: dict[str, Any]) -> None:
        vmid = int(instance["vmid"])
        # Captured before anything can move the row: the recovery path below
        # needs the status this attempt STARTED from, not the stale snapshot's
        # idea of it after a partial transition.
        entry_status = str(instance["status"])
        try:
            # GitHub registration must leave the pool *before* the CT dies whenever
            # the agent is still online. Destroying first leaves a TOCTOU window
            # where GH assigns a new job to a runner that is about to vanish →
            # offline+busy forever (stuck Set up job).
            #
            # That guarantee holds only while GitHub *answers*. When it does
            # not, the choice is between one offline+busy zombie registration
            # (which `_reconcile_zombie_github_runners` already detects and
            # cleans) and an unbounded local disk leak that took nine unrelated
            # production workloads to the brink on the incident. Local teardown
            # wins; the registration is queued for later.
            registration_purged = False
            github_unreachable = False
            if (
                instance["status"] in {"delete_pending", "delete_failed"}
                and instance.get("repository")
                and instance.get("runner_name")
            ):
                installation = self._installation_for_cleanup(str(instance["repository"]))
                if installation is not None:
                    try:
                        runner = await self.github.find_runner(
                            installation,
                            str(instance["repository"]),
                            str(instance["runner_name"]),
                        )
                    except GitHubUnavailable as exc:
                        # No answer. Proceed with the local destroy and record
                        # the deregistration we still owe.
                        github_unreachable = True
                        runner = None
                        self.log(
                            "warn",
                            "destroy",
                            "GitHub unreachable; destroying container locally "
                            "and queueing the deregistration",
                            vmid=vmid,
                            runner_name=instance.get("runner_name"),
                            repository=instance.get("repository"),
                            error=str(exc)[:200],
                        )
                    if runner:
                        status = str(runner.get("status") or "").lower()
                        busy = bool(runner.get("busy", False))
                        age = int(time.time()) - int(instance.get("updated_at") or time.time())
                        # Truly online+busy: job may still be executing — defer briefly.
                        if status == "online" and busy:
                            if age < 180:
                                # A real answer from a healthy GitHub, so honour
                                # it — but push the deadline out. Returning with
                                # no state change meant this row was re-probed
                                # (a full list_runners) on every 60s pass.
                                await self.db.set_delete_deadline(
                                    int(instance["id"]), int(time.time()) + _DESTROY_DEFER_SECONDS
                                )
                                self.log(
                                    "warn",
                                    "destroy",
                                    "cleanup deferred; GitHub runner still online/busy",
                                    vmid=vmid,
                                    runner_name=instance.get("runner_name"),
                                    repository=instance.get("repository"),
                                    age_seconds=age,
                                    retry_in_seconds=_DESTROY_DEFER_SECONDS,
                                )
                                return
                            self.log(
                                "warn",
                                "destroy",
                                "forcing destroy of long online/busy runner",
                                vmid=vmid,
                                runner_name=instance.get("runner_name"),
                                repository=instance.get("repository"),
                                age_seconds=age,
                            )
                        elif status == "online" and not busy:
                            # Idle online: registration delete is mandatory before CT
                            # teardown. If GH refuses, abort so we don't create a zombie.
                            deleted = await self.github.try_delete_runner(
                                installation,
                                str(instance["repository"]),
                                int(runner["id"]),
                            )
                            if not deleted:
                                self.log(
                                    "warn",
                                    "destroy",
                                    "cleanup deferred; could not delete idle online runner",
                                    vmid=vmid,
                                    runner_name=instance.get("runner_name"),
                                    repository=instance.get("repository"),
                                    runner_id=runner.get("id"),
                                )
                                await self.db.bump_delete_failure(
                                    int(instance["id"]),
                                    expected_statuses={entry_status},
                                    error="github_runner_delete_failed_still_online",
                                    not_before=self._destroy_retry_deadline(
                                        int(instance.get("delete_attempts") or 0) + 1,
                                        now=int(time.time()),
                                    ),
                                )
                                return
                            registration_purged = True
                            self.log(
                                "info",
                                "destroy",
                                f"deleted GitHub runner {instance.get('runner_name')} "
                                "before CT destroy",
                                vmid=vmid,
                                runner_id=runner.get("id"),
                                repository=instance.get("repository"),
                            )
            transitioned = await self.db.transition_instance(
                instance["id"], {str(instance["status"])}, "deleting", error=None
            )
            if not transitioned:
                self.log(
                    "info",
                    "destroy",
                    "cleanup skipped after concurrent state change",
                    vmid=vmid,
                    status=instance.get("status"),
                )
                return
            if (
                not registration_purged
                and instance.get("repository")
                and instance.get("runner_name")
            ):
                # Queued BEFORE the destroy on purpose: if the process dies
                # between destroying the container and deleting the row, the
                # obligation must not be lost. A redundant row is harmless —
                # the drain finds no such runner and drops it.
                if github_unreachable:
                    await self.db.enqueue_deregistration(
                        str(instance["repository"]),
                        str(instance["runner_name"]),
                        vmid=vmid,
                        reason="github_unreachable_at_destroy",
                    )
                else:
                    await self._purge_github_runner_registration(
                        str(instance["repository"]),
                        str(instance["runner_name"]),
                        vmid=vmid,
                    )
            await self.proxmox.destroy(vmid)
            await self.db.delete_instance(instance["id"])
            self.log(
                "info",
                "destroy",
                f"destroyed instance vmid={vmid}",
                vmid=vmid,
                mode=instance.get("mode"),
                repository=instance.get("repository"),
            )
        except Exception as exc:
            CLEANUP_FAILURES.labels(str(instance["mode"])).inc()
            # The expected set must contain the entry status as well as
            # "deleting". Matching only "deleting" meant that any failure
            # raised BEFORE the transition — above all the GitHub
            # deregistration probe — updated zero rows and silently did
            # nothing: delete_attempts never advanced, the backoff never
            # engaged, and the row was retried at full cadence forever. That is
            # the mechanism behind the thin-pool exhaustion incident.
            #
            # It stays narrow on purpose: if another task concurrently claimed
            # this instance, nothing matches and we log instead of stomping it.
            attempts = await self.db.bump_delete_failure(
                int(instance["id"]),
                expected_statuses={entry_status, "deleting"},
                error=str(exc)[:500],
                not_before=self._destroy_retry_deadline(
                    int(instance.get("delete_attempts") or 0) + 1, now=int(time.time())
                ),
            )
            if attempts == 0:
                self.log(
                    "info",
                    "destroy",
                    "cleanup failed but the instance had already moved on; leaving it alone",
                    vmid=vmid,
                    status=entry_status,
                )
            self.events.exception(
                "destroy",
                f"instance cleanup failed vmid={vmid}",
                exc,
                vmid=vmid,
                mode=instance.get("mode"),
                runner_name=instance.get("runner_name"),
                repository=instance.get("repository"),
            )
            LOGGER.exception("instance cleanup failed", extra={"vmid": vmid})

    async def _reconcile_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.events.exception("reconcile", "reconciliation failed", exc)
                LOGGER.exception("reconciliation failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.settings.capacity.reconcile_seconds
                )

    async def _reconcile_stale_jobs(self, now: int) -> None:
        """Repair job rows left open by lost/racy lifecycle webhooks."""
        jobs = await self.db.list_jobs(limit=200)
        for job in jobs:
            job_id = int(job["job_id"])
            status = str(job.get("status") or "")
            # completed_at written but status later clobbered by late provision/in_progress.
            completed_at = job.get("completed_at")
            if completed_at is not None and status != "completed":
                repaired = await self.db.set_job_status(
                    job_id,
                    "completed",
                    completed_at=int(completed_at),
                    error=job.get("error"),
                )
                if repaired:
                    self.log(
                        "warn",
                        "job",
                        f"repaired job {job_id} {status}→completed (completed_at already set)",
                        job_id=job_id,
                        repository=job.get("repository"),
                    )
                continue
            if status not in {"running", "waiting_for_runner", "provisioning"}:
                continue
            age = now - int(job.get("updated_at") or job.get("created_at") or now)
            instance = await self.db.find_instance_by_job(job_id)
            # Instance released or moved on, but job row never closed.
            if instance is None or int(instance.get("job_id") or 0) != job_id:
                if age < 90:
                    continue
                await self.db.set_job_status(
                    job_id,
                    "completed",
                    completed_at=now,
                    error="stale_job_reconciled_no_instance",
                )
                self.log(
                    "warn",
                    "job",
                    f"closed stale {status} job {job_id} (no bound instance)",
                    job_id=job_id,
                    repository=job.get("repository"),
                )
                continue
            # Instance still bound but already idle/ready after job finished.
            if (
                instance["status"] in {"ready", "delete_pending", "deleting", "delete_failed"}
                and age >= 90
            ):
                await self.db.set_job_status(
                    job_id,
                    "completed",
                    completed_at=now,
                    error="stale_job_reconciled_instance_released",
                )
                if instance["status"] == "ready" and int(instance.get("job_id") or 0) == job_id:
                    await self.db.update_instance(
                        int(instance["id"]), "ready", job_id=None, last_used_at=now
                    )
                self.log(
                    "warn",
                    "job",
                    f"closed stale {status} job {job_id} (instance {instance['status']})",
                    job_id=job_id,
                    vmid=instance.get("vmid"),
                    repository=job.get("repository"),
                )
                continue
            # Job+instance still "running" but GitHub runner is online/idle → complete
            # webhook was lost (e.g. crash after delivery insert, before handler).
            if (
                status == "running"
                and instance["status"] == "running"
                and age >= 120
                and instance.get("runner_name")
                and job.get("repository")
            ):
                try:
                    installation = self._installation(str(job["repository"]))
                    runner = await self.github.find_runner(
                        installation,
                        str(job["repository"]),
                        str(instance["runner_name"]),
                    )
                except Exception as exc:  # noqa: BLE001
                    self.log(
                        "warn",
                        "job",
                        f"stale running check failed for job {job_id}: {exc}",
                        job_id=job_id,
                    )
                    continue
                # Idle online → job finished, complete webhook lost.
                # Missing/offline runner (JIT exited) → same, especially after poweroff.
                runner_done = runner is None or (
                    runner.get("status") == "online" and not runner.get("busy", False)
                )
                if runner_done:
                    reason = (
                        "stale_job_reconciled_runner_gone"
                        if runner is None
                        else "stale_job_reconciled_runner_idle"
                    )
                    await self.db.set_job_status(
                        job_id,
                        "completed",
                        completed_at=now,
                        error=reason,
                    )
                    mode = str(instance.get("mode") or "")
                    if mode == "persistent":
                        await self.db.update_instance(
                            int(instance["id"]), "ready", job_id=None, last_used_at=now
                        )
                    elif mode == "warm":
                        await self.db.transition_instance(
                            int(instance["id"]),
                            {"running"},
                            "provisioning",
                            job_id=None,
                            last_used_at=now,
                            error=None,
                        )
                        self._schedule_warm_recycle(int(instance["id"]))
                    else:
                        await self.db.transition_instance(
                            int(instance["id"]),
                            {"running"},
                            "delete_pending",
                            error=reason,
                        )
                    self.log(
                        "warn",
                        "job",
                        f"closed stale running job {job_id} ({reason})",
                        job_id=job_id,
                        vmid=instance.get("vmid"),
                        runner_name=instance.get("runner_name"),
                    )

        # Instances stuck "running" for jobs that are already terminal (or gone).
        for running_instance in await self.db.list_instances():
            running_job_id = running_instance.get("job_id")
            if running_instance.get("status") != "running" or not running_job_id:
                continue
            bound = int(running_job_id)
            bound_job = await self.db.get_job(bound)
            if bound_job is None or str(bound_job.get("status")) in self.db.JOB_TERMINAL_STATUSES:
                age = now - int(running_instance.get("updated_at") or now)
                if age < 60:
                    continue
                mode = str(running_instance.get("mode") or "")
                if mode == "persistent":
                    await self.db.update_instance(
                        int(running_instance["id"]), "ready", job_id=None, last_used_at=now
                    )
                elif mode == "warm":
                    await self.db.transition_instance(
                        int(running_instance["id"]),
                        {"running"},
                        "provisioning",
                        job_id=None,
                        last_used_at=now,
                        error=None,
                    )
                    self._schedule_warm_recycle(int(running_instance["id"]))
                else:
                    await self.db.transition_instance(
                        int(running_instance["id"]),
                        {"running"},
                        "delete_pending",
                        error="stale_running_instance_job_terminal",
                    )
                self.log(
                    "warn",
                    "job",
                    f"released instance vmid={running_instance.get('vmid')} "
                    f"stuck running for terminal job {bound}",
                    job_id=bound,
                    vmid=running_instance.get("vmid"),
                    mode=mode,
                )

    async def _purge_github_runner_registration(
        self,
        repository: str,
        runner_name: str,
        *,
        vmid: int | None = None,
    ) -> None:
        """Best-effort remove of a GH self-hosted registration (handles zombies).

        "Best-effort" has to include the case where GitHub does not answer:
        this runs on the teardown path, so raising here would block the local
        destroy — the exact coupling that caused the incident.
        """
        installation = self._installation_for_cleanup(repository)
        if installation is None:
            return
        # The fallback across other repositories exists to survive a repo/handle
        # rename, where the registration still lives under the old path. That is
        # only meaningful for a repository-scoped installation, which has a
        # separate runner list per repository. An organization-scoped one serves
        # every repository from ONE list, so walking them all was N identical
        # listings per teardown for no possible gain.
        candidates = [repository]
        if installation.scope != "organization":
            for configured in self.settings.repositories:
                if configured not in candidates:
                    candidates.append(configured)
        for candidate in candidates:
            try:
                runner = await self.github.find_runner(installation, candidate, runner_name)
            except GitHubUnavailable as exc:
                await self.db.enqueue_deregistration(
                    repository, runner_name, vmid=vmid, reason="github_unreachable_at_purge"
                )
                self.log(
                    "warn",
                    "destroy",
                    "GitHub unreachable during registration purge; queued for retry",
                    vmid=vmid,
                    runner_name=runner_name,
                    repository=repository,
                    error=str(exc)[:200],
                )
                return
            if not runner:
                continue
            deleted = await self.github.try_delete_runner(
                installation, candidate, int(runner["id"])
            )
            self.log(
                "info" if deleted else "warn",
                "destroy",
                (
                    f"deleted GitHub runner {runner_name}"
                    if deleted
                    else f"could not delete GitHub runner {runner_name} (busy zombie?)"
                ),
                vmid=vmid,
                runner_id=runner.get("id"),
                repository=candidate,
                status=runner.get("status"),
                busy=runner.get("busy"),
            )
            return

    async def _reconcile_zombie_github_runners(self) -> None:
        """Drop offline/orphan GH registrations that no longer map to a live CT."""
        instances = await self.db.list_instances()
        live_names = {
            str(i.get("runner_name"))
            for i in instances
            if i.get("runner_name")
            and i.get("status") not in {"delete_pending", "delete_failed", "deleting"}
        }
        for repository, _policy in self.settings.repositories.items():
            try:
                installation = self._installation(repository)
            except Exception as exc:  # noqa: BLE001
                self.log(
                    "warn",
                    "reconcile",
                    f"skipping zombie-runner reconcile for {repository}: {exc}",
                    repository=repository,
                )
                continue
            try:
                runners = await self.github.list_runners(installation, repository)
            except Exception as exc:  # noqa: BLE001
                self.log(
                    "warn",
                    "reconcile",
                    f"list runners failed for {repository}: {exc}",
                    repository=repository,
                )
                continue
            for runner in runners:
                name = str(runner.get("name") or "")
                if not name.startswith("pve-lxc-"):
                    continue
                status = str(runner.get("status") or "").lower()
                busy = bool(runner.get("busy", False))
                orphan = name not in live_names
                zombie = status == "offline" and busy
                if not (orphan or zombie):
                    continue
                deleted = await self.github.try_delete_runner(
                    installation, repository, int(runner["id"])
                )
                self.log(
                    "warn",
                    "reconcile",
                    (
                        f"purged zombie GitHub runner {name}"
                        if deleted
                        else f"zombie GitHub runner {name} still locked by a job"
                    ),
                    repository=repository,
                    runner_id=runner.get("id"),
                    status=status,
                    busy=busy,
                    orphan=orphan,
                )
                # Jobs stuck on offline+busy never get complete webhooks.
                if zombie or (orphan and status == "offline"):
                    for job in await self.db.list_jobs(limit=100, status="running"):
                        if str(job.get("runner_name") or "") != name:
                            continue
                        if str(job.get("repository") or "").lower() != repository.lower():
                            continue
                        await self.db.set_job_status(
                            int(job["job_id"]),
                            "failed",
                            completed_at=int(time.time()),
                            error="zombie_offline_runner",
                        )
                        self.log(
                            "warn",
                            "job",
                            f"failed job {job['job_id']} stuck on zombie runner {name}",
                            job_id=int(job["job_id"]),
                            repository=repository,
                            runner_name=name,
                        )

    async def _begin_pass(self) -> ReconcileContext:
        ctx = ReconcileContext(
            now=int(time.time()),
            started=time.monotonic(),
            budget_seconds=float(self.settings.capacity.reconcile_budget_seconds),
            github_available=self.github.available,
        )
        # Fetched once per pass rather than per instance. Several phases need
        # the hypervisor's view, and asking N times was both wasteful and a
        # source of disagreement between phases within one pass.
        try:
            live = await self.proxmox.cluster_vmids()
        except Exception as exc:  # noqa: BLE001 — unknown is a valid, safe answer
            self.log("warn", "reconcile", f"cluster_vmids failed: {exc}")
            ctx.live_vmids = None
        else:
            ctx.live_vmids = live
        return ctx

    async def _finish_pass(self, ctx: ReconcileContext) -> None:
        """Always runs. Metrics in particular must not be skipped by a failure.

        Gauges that only refresh at the end of a *successful* pass freeze at
        their last healthy values exactly when something is wrong — which is
        why monitoring showed nothing during the thin-pool exhaustion incident.
        """
        RECONCILE_DURATION.observe(time.monotonic() - ctx.started)
        if ctx.truncated:
            RECONCILE_TRUNCATED.inc()
        try:
            await self._update_metrics()
        except Exception as exc:  # noqa: BLE001
            self.events.exception("reconcile", "metrics update failed", exc)
        if not ctx.truncated and ctx.phase_errors == 0:
            RECONCILE_LAST_SUCCESS.set(time.time())

    async def _run_phase(self, name: str, phase: Awaitable[None], ctx: ReconcileContext) -> None:
        try:
            await phase
        except Exception as exc:  # noqa: BLE001 — one phase must not abort the pass
            ctx.phase_errors += 1
            RECONCILE_PHASE_FAILURES.labels(name).inc()
            self.events.exception("reconcile", f"reconcile phase {name!r} failed", exc)

    async def reconcile(self) -> None:
        ctx = await self._begin_pass()
        try:
            await self._run_phase("stale_jobs", self._reconcile_stale_jobs(ctx.now), ctx)
            await self._run_phase("zombie_runners", self._reconcile_zombie_github_runners(), ctx)
            await self._run_phase(
                "deregistrations", self._drain_pending_deregistrations(now=ctx.now), ctx
            )
            await self._run_phase("instances", self._sweep_instances(ctx), ctx)
            if not self.settings.shadow_mode and not self.draining and not ctx.expired():
                await self._run_phase("warm_replenish", self._replenish_warm_pool(), ctx)
                await self._run_phase("pool_replenish", self._replenish_pool(), ctx)
            await self._run_phase("cache_gc", self._maybe_gc_orphan_cache(), ctx)
        finally:
            await self._finish_pass(ctx)

    async def _sweep_instances(self, ctx: ReconcileContext) -> None:
        # _destroy_instance/_reconcile_warm_instance/_reconcile_running_instance are
        # shared across call sites with heterogeneous dict sources (get_instance,
        # find_instance_by_job, ...); widen back rather than typing them to the
        # stricter InstanceListItem shape that's specific to list_instances().
        instances = cast(list[dict[str, Any]], await self.db.list_instances())
        ctx.total_instances = len(instances)
        for instance in instances:
            if ctx.expired():
                ctx.truncated = True
                self.log(
                    "warn",
                    "reconcile",
                    "pass truncated at budget; remaining instances deferred",
                    budget_seconds=ctx.budget_seconds,
                    handled=ctx.instances_seen,
                    total=len(instances),
                )
                return
            ctx.instances_seen += 1
            try:
                await self._sweep_one_instance(instance, ctx)
            except Exception as exc:  # noqa: BLE001 — one bad row must not abort the sweep
                ctx.instance_errors += 1
                self.events.exception(
                    "reconcile",
                    f"instance reconcile failed vmid={instance.get('vmid')}",
                    exc,
                    vmid=instance.get("vmid"),
                    status=instance.get("status"),
                )

    async def _sweep_one_instance(self, instance: dict[str, Any], ctx: ReconcileContext) -> None:
        now = ctx.now
        age = now - int(instance["updated_at"])
        if await self._reap_if_container_missing(instance, ctx):
            return
        if self._destroy_reason(instance, now=now) is not None:
            await self._destroy_instance(instance)
        elif instance["status"] == "deleting":
            await self._reconcile_stuck_deleting(instance, ctx)
        elif instance["mode"] == "warm" and instance["status"] in {
            "ready",
            "provisioning",
            "reserved",
        }:
            await self._reconcile_warm_instance(instance)
        elif instance["mode"] == "pool" and instance["status"] == "ready":
            await self._reconcile_pool_instance(instance, ctx)
        elif instance["status"] == "running":
            await self._reconcile_running_instance(instance, now=now, age=age)

    async def storage_snapshot(self, *, force: bool = False) -> StorageSnapshot | None:
        """Cached free/used view of the clone target.

        Cached because this gates every clone and the value moves slowly; a
        query per clone would be pure overhead on an already-serialized path.
        Returns None when the reading is unavailable.
        """
        cfg = self.settings.storage
        now = time.monotonic()
        if (
            not force
            and self._storage_snapshot is not None
            and now - self._storage_checked_at < cfg.poll_seconds
        ):
            return self._storage_snapshot
        try:
            data = await self.proxmox.storage_status()
        except Exception as exc:  # noqa: BLE001
            self._storage_checked_at = now
            self._storage_snapshot = None
            self.log("warn", "storage", f"storage status unavailable: {exc}")
            return None
        # A shape mismatch must be loud. Silently reading zeros would look
        # exactly like "reading unavailable", the guard would fail open forever,
        # and nobody would learn that the admission floor was never armed.
        if not any(key in data for key in ("total", "used", "avail")):
            self._storage_checked_at = now
            self._storage_snapshot = None
            self.log(
                "error",
                "storage",
                "storage status response has none of total/used/avail; "
                "the admission guard cannot arm",
                storage=self.settings.proxmox.storage,
                keys=sorted(data)[:12],
            )
            return None
        total = int(data.get("total") or 0)
        used = int(data.get("used") or 0)
        avail = int(data.get("avail") or max(0, total - used))
        if total <= 0:
            self._storage_checked_at = now
            self._storage_snapshot = None
            self.log(
                "error",
                "storage",
                f"storage reports total={total}; the admission guard cannot arm",
                storage=self.settings.proxmox.storage,
            )
            return None
        snapshot = StorageSnapshot(total_bytes=total, used_bytes=used, avail_bytes=avail)
        self._storage_snapshot = snapshot
        self._storage_checked_at = now
        STORAGE_FREE_BYTES.set(avail)
        STORAGE_TOTAL_BYTES.set(total)
        STORAGE_USED_RATIO.set(snapshot.used_ratio)
        return snapshot

    async def storage_admits(self, *, purpose: str) -> tuple[bool, str]:
        """Whether a new clone may be started, and why not if it may not.

        The floor exists because this orchestrator is a guest on a thin pool
        shared with nine production services: it has to stop before it takes the
        node down, not after.
        """
        cfg = self.settings.storage
        if not cfg.guard_enabled:
            return True, "guard disabled"
        snapshot = await self.storage_snapshot()
        if snapshot is None:
            # Fail open: a homelab must not halt CI because a status endpoint
            # hiccuped. If the storage API is down the clone API probably is
            # too, and that failure is handled where it happens.
            return (cfg.fail_open, "storage reading unavailable")
        free_gb = snapshot.avail_bytes / 1024**3
        if free_gb < cfg.min_free_gb:
            return False, f"only {free_gb:.1f} GiB free, floor is {cfg.min_free_gb} GiB"
        if snapshot.used_percent > cfg.max_used_percent:
            return False, (
                f"{snapshot.used_percent:.1f}% used, ceiling is {cfg.max_used_percent:.0f}%"
            )
        if free_gb < cfg.warn_free_gb:
            self.log(
                "warn",
                "storage",
                f"storage low: {free_gb:.1f} GiB free (warn below {cfg.warn_free_gb} GiB)",
                purpose=purpose,
            )
        return True, "ok"

    async def _refuse_replenish_on_storage(self, phase: str) -> bool:
        """Speculative capacity is refused hard; the caller skips the phase."""
        admitted, reason = await self.storage_admits(purpose=phase)
        if admitted:
            STORAGE_ADMISSION_BLOCKED.set(0)
            return False
        STORAGE_ADMISSION_BLOCKED.set(1)
        STORAGE_ADMISSION_DENIED.labels(phase).inc()
        self.log(
            "warn",
            "storage",
            f"{phase} refused: {reason}",
            storage=self.settings.proxmox.storage,
            node=self.settings.proxmox.node,
        )
        return True

    def _container_absent(self, instance: dict[str, Any], ctx: ReconcileContext) -> bool:
        """Whether the hypervisor genuinely has no container for this row.

        Three guards, all load-bearing, because this path deletes state:

        1. `live_vmids is None` means the query failed — unknown, never act.
        2. An empty set while several rows exist is treated as unknown too. A
           healthy API returning nothing is indistinguishable from "the node is
           gone", and a quorum event must not trigger a fleet-wide reap.
        3. A grace period, because `_reserve_instance` inserts the row *before*
           `proxmox.clone()` and /cluster/resources is pvestatd-cached.
        """
        if ctx.live_vmids is None:
            return False
        if not ctx.live_vmids and ctx.total_instances > 2:
            return False
        if int(instance["vmid"]) in ctx.live_vmids:
            return False
        return (ctx.now - int(instance["updated_at"])) >= _CT_MISSING_GRACE_SECONDS

    async def _reap_if_container_missing(
        self, instance: dict[str, Any], ctx: ReconcileContext
    ) -> bool:
        """Two-strike removal of a row whose container no longer exists.

        This is the gap that made the incident's manual `DELETE FROM instances`
        necessary: nothing ever compared the database against the hypervisor, so
        rows for containers destroyed by hand stayed `ready` forever, holding
        their UNIQUE vmid/ip and their share of capacity.

        Deleting the row directly rather than routing through `delete_pending`
        is deliberate — there is nothing to destroy, so a teardown would be
        several wasted API calls and a pointless GitHub probe.
        """
        if not self._container_absent(instance, ctx):
            if instance.get("ct_missing_since") is not None:
                await self.db.set_ct_missing_since(int(instance["id"]), None)
            return False

        first_seen = instance.get("ct_missing_since")
        if first_seen is None:
            await self.db.set_ct_missing_since(int(instance["id"]), ctx.now)
            return False
        if ctx.now - int(first_seen) < _CT_MISSING_STRIKE_SECONDS:
            return False

        vmid = int(instance["vmid"])
        if instance.get("repository") and instance.get("runner_name"):
            await self.db.enqueue_deregistration(
                str(instance["repository"]),
                str(instance["runner_name"]),
                vmid=vmid,
                reason="container_missing",
            )
        await self.db.delete_instance(int(instance["id"]))
        INSTANCES_REAPED.labels(str(instance["mode"]), "ct_missing").inc()
        self.log(
            "warn",
            "reconcile",
            f"reaped instance row vmid={vmid}; no such container on the node",
            vmid=vmid,
            mode=instance.get("mode"),
            status=instance.get("status"),
            repository=instance.get("repository"),
        )
        return True

    async def _reconcile_stuck_deleting(
        self, instance: dict[str, Any], ctx: ReconcileContext
    ) -> None:
        """Recover a row abandoned mid-teardown.

        `deleting` had no reconcile branch at all: a crash between
        `proxmox.destroy` and `delete_instance` stranded the row forever, and
        because `_allocate()` unions every row's vmid, it also permanently
        consumed a slot in the VMID/IP range.
        """
        if ctx.now - int(instance["updated_at"]) < _DELETING_STUCK_SECONDS:
            return
        vmid = int(instance["vmid"])
        if ctx.live_vmids is not None and vmid not in ctx.live_vmids:
            await self.db.delete_instance(int(instance["id"]))
            INSTANCES_REAPED.labels(str(instance["mode"]), "stuck_deleting").inc()
            self.log("warn", "reconcile", f"cleared stranded deleting row vmid={vmid}", vmid=vmid)
            return
        await self.db.transition_instance(
            int(instance["id"]), {"deleting"}, "delete_pending", error="teardown_interrupted"
        )
        self.log(
            "warn",
            "reconcile",
            f"requeued interrupted teardown vmid={vmid}",
            vmid=vmid,
            age_seconds=ctx.now - int(instance["updated_at"]),
        )

    async def _reconcile_pool_instance(
        self, instance: dict[str, Any], ctx: ReconcileContext
    ) -> None:
        """Health/expiry for `mode="pool"`, which previously had neither.

        `_replenish_pool` only ever *created* pool instances; nothing revalidated
        or retired them. That is why three `pool`/`ready` rows survived in the
        database long after their containers were gone, and why only a manual
        DELETE removed them.

        Container existence is handled generically by `_reap_if_container_missing`
        before this runs; what is left is age, and drift between the row and the
        container's actual power state.
        """
        vmid = int(instance["vmid"])
        max_age = int(self.settings.capacity.pool_max_age_seconds or 0)
        if max_age > 0 and (ctx.now - int(instance["created_at"])) > max_age:
            await self.db.transition_instance(
                int(instance["id"]), {"ready"}, "delete_pending", error="pool_max_age"
            )
            self.log(
                "info",
                "reconcile",
                f"retiring pool container vmid={vmid} past max age",
                vmid=vmid,
                age_seconds=ctx.now - int(instance["created_at"]),
            )
            return

        # Pool containers are cloned and left stopped until a job claims them.
        # One that is running means a provision crashed part-way through.
        try:
            current = await self.proxmox.status(vmid)
        except Exception as exc:  # noqa: BLE001 — a status blip is not a verdict
            self.log("warn", "reconcile", f"pool status check failed vmid={vmid}: {exc}", vmid=vmid)
            return
        if str(current.get("status", "")).lower() == "running":
            await self.db.transition_instance(
                int(instance["id"]), {"ready"}, "delete_pending", error="pool_unexpectedly_running"
            )
            self.log(
                "warn",
                "reconcile",
                f"retiring pool container vmid={vmid}; running while marked ready",
                vmid=vmid,
            )

    async def destroy_instance_by_request(
        self, instance: dict[str, Any], *, force: bool = False
    ) -> str:
        """Operator-initiated teardown of a single container.

        `force` marks the row `delete_pending` with its deadline cleared and
        skips the GitHub probe, so a container can always be reclaimed by hand
        even while the dependency that normally gates teardown is down.
        """
        instance_id = int(instance["id"])
        vmid = int(instance["vmid"])
        await self.db.update_instance(instance_id, "delete_pending", error="operator_requested")
        await self.db.set_delete_deadline(instance_id, None)
        fresh = await self.db.get_instance(instance_id)
        if fresh is None:
            return f"instance {vmid} disappeared before teardown"
        if force:
            if fresh.get("repository") and fresh.get("runner_name"):
                await self.db.enqueue_deregistration(
                    str(fresh["repository"]),
                    str(fresh["runner_name"]),
                    vmid=vmid,
                    reason="operator_force_destroy",
                )
            await self.proxmox.destroy(vmid)
            await self.db.delete_instance(instance_id)
            self.log(
                "warn",
                "admin",
                f"force-destroyed container {vmid}; deregistration queued",
                vmid=vmid,
            )
            return f"container {vmid} destroyed; GitHub deregistration queued"
        await self._destroy_instance(fresh)
        remaining = await self.db.get_instance(instance_id)
        if remaining is None:
            return f"container {vmid} destroyed"
        return (
            f"teardown of {vmid} did not complete: {remaining.get('error') or remaining['status']}"
        )

    async def detect_drift(self) -> DriftReport:
        """Read-only comparison of the database against the hypervisor."""
        now = int(time.time())
        pve = self.settings.proxmox
        report = DriftReport(checked_at=now, node=pve.node)

        instances = cast(list[dict[str, Any]], await self.db.list_instances())
        try:
            containers = await self.proxmox.list_node_cts()
        except Exception as exc:  # noqa: BLE001
            report.scan_incomplete = True
            self.log("warn", "reconcile", f"drift scan could not read the node: {exc}")
            return report

        live = {int(c["vmid"]) for c in containers if c.get("vmid") is not None}
        known = {int(row["vmid"]) for row in instances}

        for row in instances:
            vmid = int(row["vmid"])
            if row["status"] == "deleting" and (now - int(row["updated_at"])) >= (
                _DELETING_STUCK_SECONDS
            ):
                report.stuck_deleting.append(
                    {
                        "id": int(row["id"]),
                        "vmid": vmid,
                        "mode": row["mode"],
                        "status": row["status"],
                        "age_seconds": now - int(row["updated_at"]),
                    }
                )
                continue
            if vmid not in live:
                report.rows_without_container.append(
                    {
                        "id": int(row["id"]),
                        "vmid": vmid,
                        "mode": row["mode"],
                        "status": row["status"],
                        "runner_name": row.get("runner_name"),
                        "repository": row.get("repository"),
                        "age_seconds": now - int(row["updated_at"]),
                        "missing_since": row.get("ct_missing_since"),
                    }
                )

        for container in containers:
            vmid = int(container.get("vmid") or 0)
            if vmid in known:
                continue
            if not is_managed(container, vmid_start=pve.vmid_start, vmid_end=pve.vmid_end):
                continue
            report.containers_without_row.append(
                {
                    "vmid": vmid,
                    "name": container.get("name"),
                    "status": container.get("status"),
                    "tags": container.get("tags"),
                }
            )

        DRIFT_ROWS_WITHOUT_CT.set(len(report.rows_without_container))
        DRIFT_CTS_WITHOUT_ROW.set(len(report.containers_without_row))
        return report

    async def apply_drift(self, *, confirm_vmids: list[int], force: bool = False) -> dict[str, Any]:
        """Repair exactly the set the caller confirmed, or nothing at all.

        Refusing on any mismatch — rather than repairing the intersection — is
        the point: the operator approved a specific list, and a list that has
        since changed is a different decision.
        """
        report = await self.detect_drift()
        if report.scan_incomplete:
            raise RuntimeError("drift scan incomplete; refusing to apply")
        requested = set(confirm_vmids)
        available = report.selectable_vmids()
        unknown = requested - available
        if unknown:
            raise RuntimeError(
                f"drift set changed since the scan; unknown vmids {sorted(unknown)}. Re-scan."
            )

        rows_deleted = 0
        containers_destroyed = 0
        requeued = 0

        for item in report.rows_without_container:
            if int(item["vmid"]) not in requested:
                continue
            if item.get("repository") and item.get("runner_name"):
                await self.db.enqueue_deregistration(
                    str(item["repository"]),
                    str(item["runner_name"]),
                    vmid=int(item["vmid"]),
                    reason="drift_repair",
                )
            await self.db.delete_instance(int(item["id"]))
            INSTANCES_REAPED.labels(str(item["mode"]), "drift_apply").inc()
            rows_deleted += 1

        for item in report.stuck_deleting:
            if int(item["vmid"]) not in requested:
                continue
            await self.db.transition_instance(
                int(item["id"]), {"deleting"}, "delete_pending", error="drift_repair"
            )
            requeued += 1

        for item in report.containers_without_row:
            vmid = int(item["vmid"])
            if vmid not in requested:
                continue
            if str(item.get("status") or "").lower() == "running" and not force:
                self.log(
                    "warn",
                    "reconcile",
                    f"skipping running orphan container {vmid}; pass force to destroy it",
                    vmid=vmid,
                )
                continue
            await self.proxmox.destroy(vmid)
            containers_destroyed += 1

        self.log(
            "warn",
            "admin",
            f"drift repair applied: {rows_deleted} row(s) deleted, "
            f"{containers_destroyed} container(s) destroyed, {requeued} requeued",
            confirmed=len(requested),
        )
        return {
            "rows_deleted": rows_deleted,
            "containers_destroyed": containers_destroyed,
            "requeued": requeued,
        }

    async def _maybe_gc_orphan_cache(self) -> None:
        """Phase 3: purge allowlist orphans older than cache.gc_orphan_days."""
        days = int(self.settings.cache.gc_orphan_days or 0)
        if days <= 0:
            return
        try:
            result = await self.broker.cache_gc(days)
            if result and "purged=0" not in result:
                self.log("info", "cache", f"auto GC: {result}", min_age_days=days)
        except Exception as exc:  # noqa: BLE001 — reconcile must continue
            self.log("warn", "cache", f"auto GC failed: {exc}", min_age_days=days)

    async def _drain_pending_deregistrations(self, *, now: int) -> None:
        """Settle GitHub registrations we owe from destroys done while it was down.

        Skipped wholesale when the circuit is not closed: this queue exists
        precisely because GitHub was unreachable, so retrying into an open
        circuit would burn the pass budget for nothing.
        """
        if not self.github.available:
            return

        expired = await self.db.purge_expired_deregistrations(
            older_than=now - _DEREGISTRATION_TTL_SECONDS
        )
        if expired:
            # GitHub reaps offline ephemeral registrations itself, so a row this
            # old is noise rather than a task. Say so instead of retrying forever.
            self.log(
                "warn",
                "destroy",
                f"dropped {expired} deregistration(s) older than "
                f"{_DEREGISTRATION_TTL_SECONDS // 3600}h",
            )

        for row in await self.db.list_due_deregistrations(now=now):
            repository = str(row["repository"])
            runner_name = str(row["runner_name"])
            installation = self._installation_for_cleanup(repository)
            if installation is None:
                await self.db.delete_deregistration(int(row["id"]))
                continue
            try:
                runner = await self.github.find_runner(installation, repository, runner_name)
                if runner is None:
                    # Already gone — the obligation is discharged.
                    await self.db.delete_deregistration(int(row["id"]))
                    continue
                deleted = await self.github.try_delete_runner(
                    installation, repository, int(runner["id"])
                )
            except Exception as exc:  # noqa: BLE001 — one bad row must not stop the drain
                await self.db.bump_deregistration(
                    int(row["id"]),
                    error=str(exc),
                    not_before=now + self._destroy_retry_backoff(int(row["attempts"] or 0) + 1),
                )
                continue
            if deleted:
                await self.db.delete_deregistration(int(row["id"]))
                self.log(
                    "info",
                    "destroy",
                    f"deferred deregistration completed for {runner_name}",
                    repository=repository,
                    vmid=row.get("vmid"),
                )
            else:
                await self.db.bump_deregistration(
                    int(row["id"]),
                    error="github refused delete (busy)",
                    not_before=now + self._destroy_retry_backoff(int(row["attempts"] or 0) + 1),
                )

    async def _fail_and_retire(
        self, instance: dict[str, Any], bound_job: Any, reason: str, *, now: int
    ) -> None:
        """Fail the bound job (if any) and schedule the running instance for teardown."""
        if bound_job:
            await self.db.set_job_status(int(bound_job), "failed", completed_at=now, error=reason)
        await self.db.transition_instance(
            instance["id"], {"running"}, "delete_pending", error=reason
        )

    async def _reconcile_running_instance(
        self, instance: dict[str, Any], *, now: int, age: int
    ) -> None:
        """Free capacity when a 'running' row can no longer execute work.

        Classic failure: GH assigned job to offline warm, in_progress bound the
        wrong CT (or none), and capacity stays full for max_job_seconds.
        """
        vmid = int(instance["vmid"])
        runner_name = str(instance.get("runner_name") or "")
        bound_job = instance.get("job_id")

        # Warm name must match VMID. Mismatch means a bad in_progress bind.
        if instance["mode"] == "warm" and runner_name:
            expected = self._warm_runner_name(vmid)
            if runner_name != expected:
                self.log(
                    "warn",
                    "reconcile",
                    "running warm has runner_name≠vmid; scheduling destroy",
                    vmid=vmid,
                    runner_name=runner_name,
                    expected=expected,
                    job_id=bound_job,
                )
                await self._fail_and_retire(
                    instance, bound_job, "runner_name_vmid_mismatch", now=now
                )
                return

        # CT gone from Proxmox while still "running" in DB.
        if age >= 60:
            try:
                live = await self.proxmox.cluster_vmids()
            except Exception as exc:  # noqa: BLE001
                self.log(
                    "warn",
                    "reconcile",
                    f"cluster_vmids failed while checking running vmid={vmid}: {exc}",
                    vmid=vmid,
                )
                live = None
            if live is not None and vmid not in live:
                self.log(
                    "warn",
                    "reconcile",
                    f"running instance CT {vmid} missing from Proxmox; scheduling destroy",
                    vmid=vmid,
                    runner_name=runner_name or None,
                    job_id=bound_job,
                )
                await self._fail_and_retire(
                    instance, bound_job, "ct_missing_while_running", now=now
                )
                return

        # GH registration offline+busy with no agent → stuck Set up job.
        if age >= 90 and runner_name and instance.get("repository"):
            try:
                installation = self._installation(str(instance["repository"]))
                runner = await self.github.find_runner(
                    installation,
                    str(instance["repository"]),
                    runner_name,
                )
            except Exception as exc:  # noqa: BLE001
                self.log(
                    "warn",
                    "reconcile",
                    f"running-instance runner check failed: {exc}",
                    vmid=vmid,
                    runner_name=runner_name,
                )
                runner = None
            if (
                runner is not None
                and str(runner.get("status") or "").lower() == "offline"
                and bool(runner.get("busy", False))
            ):
                self.log(
                    "warn",
                    "reconcile",
                    f"running instance bound to offline+busy zombie {runner_name}",
                    vmid=vmid,
                    runner_name=runner_name,
                    job_id=bound_job,
                )
                await self._fail_and_retire(instance, bound_job, "zombie_offline_runner", now=now)
                return

        if age > self.settings.capacity.max_job_seconds:
            await self._destroy_instance(instance)
            if bound_job:
                await self.db.set_job_status(
                    int(bound_job),
                    "failed",
                    completed_at=now,
                    error="max_job_seconds_exceeded",
                )

    async def _reconcile_warm_instance(self, instance: dict[str, Any]) -> None:
        """Health-check warm runners and retire orphans after policy renames."""
        repo = str(instance.get("repository") or "")
        resolved = repository_policy(self.settings, repo) if repo else None
        if resolved is None:
            # Handle an owner/repo rename: old warms still count
            # against max_active and block replenishment for the new policy name.
            self.log(
                "warn",
                "warm",
                "retiring orphan warm (repository no longer in policy)",
                vmid=instance.get("vmid"),
                repository=repo or None,
                status=instance.get("status"),
            )
            await self._destroy_instance(instance)
            return
        canonical_repo, policy = resolved
        if repo != canonical_repo:
            await self.db.update_instance(
                int(instance["id"]),
                str(instance["status"]),
                repository=canonical_repo,
            )
            instance = {**instance, "repository": canonical_repo}
            self.log(
                "info",
                "warm",
                f"rebound warm repository {repo!r} → {canonical_repo!r}",
                vmid=instance.get("vmid"),
            )
        if instance["status"] != "ready":
            return
        try:
            installation = self._installation(canonical_repo)
            runner = await self.github.find_runner(
                installation,
                canonical_repo,
                str(instance["runner_name"]),
            )
            if runner is None or runner.get("status") != "online":
                await self._destroy_instance(instance)
                return
            if not set(self._warm_labels(policy)).issubset(
                {
                    str(label.get("name", "")).lower()
                    for label in runner.get("labels", [])
                    if isinstance(label, dict)
                }
            ):
                await self._destroy_instance(instance)
        except Exception as exc:
            self.events.exception(
                "warm",
                f"warm runner health check failed vmid={instance.get('vmid')}",
                exc,
                vmid=instance.get("vmid"),
                repository=canonical_repo,
            )
            LOGGER.exception("warm runner health check failed", extra={"vmid": instance["vmid"]})

    async def _replenish_pool(self) -> None:
        if await self._refuse_replenish_on_storage("pool_replenish"):
            return
        async with self._pool_lock:
            missing = self.settings.capacity.pool_size - await self.db.count_ready_pool()
            for _ in range(max(0, missing)):
                # validate_capacity_budget already guarantees pool_size fits under
                # max_active at config time; this is defense-in-depth against a
                # runtime spike in active ephemeral/warm usage piling pool clones
                # on top of an already-saturated host.
                reservation = await self._reserve_instance(
                    "pool", None, None, None, enforce_capacity=True
                )
                if reservation is None:
                    return
                instance_id, vmid, ip_address = reservation
                try:
                    await self.proxmox.clone(
                        vmid,
                        self._pool_runner_name(vmid),
                        ip_address,
                        # Pool CTs are pre-staged before any repo claims them,
                        # so there's no per-repo profile preference to use yet.
                        self.settings.profiles[DEFAULT_PROFILE_NAME],
                        "pool",
                        None,
                    )
                    await self.db.update_instance(instance_id, "ready")
                except Exception:
                    PROVISION_FAILURES.labels("pool").inc()
                    await self.db.update_instance(instance_id, "delete_failed")
                    raise

    async def _replenish_warm_pool(self, *, blocking: bool = False) -> None:
        """Bring warm supply up to target.

        By default the actual creation is scheduled, not awaited. Each
        `_create_warm_runner` blocks on `_wait_for_warm_runner` for up to
        `ready_timeout_seconds` (300s by default) while holding `_warm_lock`,
        which — called inline from `reconcile()` — meant one slow registration
        could stall the whole pass well past its budget.

        `blocking=True` is for callers that need the work finished before they
        return, notably tests and the admin reconcile endpoint.
        """
        if await self._refuse_replenish_on_storage("warm_replenish"):
            return
        deficits: list[tuple[str, RepositoryPolicy]] = []
        async with self._warm_lock:
            for repository, policy in self.settings.repositories.items():
                target = await self._warm_target(repository, policy)
                # Scaling DOWN is database-only and instant, so it stays inline:
                # it is the one part of replenishment that cannot stall a pass.
                await self._scale_down_idle_warm(repository, target)
                missing = target - await self.db.count_warm_supply(repository)
                if missing > 0:
                    deficits.append((repository, policy))
        if not deficits:
            return
        if blocking:
            await self._create_missing_warm(deficits)
            return
        self._spawn_background(self._create_missing_warm(deficits), "warm-runner-replenishment")

    async def _create_missing_warm(self, deficits: list[tuple[str, RepositoryPolicy]]) -> None:
        async with self._warm_lock:
            for repository, policy in deficits:
                target = await self._warm_target(repository, policy)
                # Re-read under the lock: a concurrent scheduled run may already
                # have closed part of the gap.
                missing = target - await self.db.count_warm_supply(repository)
                for _ in range(max(0, missing)):
                    created = await self._create_warm_runner(repository, policy)
                    if not created:
                        return

    async def _warm_target(self, repository: str, policy: RepositoryPolicy) -> int:
        """warm_capacity normally; scaled down to warm_idle_capacity once the
        repo has gone warm_idle_after_seconds without a job request."""
        if policy.warm_idle_capacity < 0:
            return policy.warm_capacity
        last_activity = await self.db.last_job_activity(repository)
        if last_activity is None:
            # No activity recorded yet (e.g. fresh install) — nothing to prove
            # idleness against, so start at the configured steady-state target.
            return policy.warm_capacity
        idle_seconds = int(time.time()) - last_activity
        if idle_seconds >= policy.warm_idle_after_seconds:
            return policy.warm_idle_capacity
        return policy.warm_capacity

    async def _scale_down_idle_warm(self, repository: str, target: int) -> None:
        """Retire ready (idle) warm CTs down to target; reconcile()/_destroy_instance
        takes it from delete_pending, including the existing GitHub online/busy
        safety check — no new teardown logic needed here."""
        excess = await self.db.count_warm_supply(repository) - target
        if excess <= 0:
            return
        for instance in (await self.db.list_ready_warm(repository))[:excess]:
            transitioned = await self.db.transition_instance(
                int(instance["id"]), {"ready"}, "delete_pending"
            )
            if transitioned:
                self.log(
                    "info",
                    "warm",
                    f"scaling down idle warm {instance.get('runner_name')} (target={target})",
                    vmid=instance.get("vmid"),
                    repository=repository,
                )

    async def _create_warm_runner(self, repository: str, policy: RepositoryPolicy) -> bool:
        started = time.monotonic()
        active = await self.db.count_active()
        max_active = self.settings.capacity.max_active
        if active >= max_active:
            self.log(
                "warn",
                "warm",
                "warm reserve failed: capacity full "
                f"(active={active}/{max_active}; free capacity or recycle orphan warms)",
                repository=repository,
                active=active,
                max_active=max_active,
            )
            return False
        reservation = await self._reserve_instance(
            "warm", repository, None, None, enforce_capacity=True
        )
        if reservation is None:
            self.log(
                "warn",
                "warm",
                "warm reserve failed (VMID/IP range exhausted)",
                repository=repository,
                active=active,
                max_active=max_active,
            )
            return False
        instance_id, vmid, ip_address = reservation
        runner_name = self._warm_runner_name(vmid)
        self.log(
            "info",
            "warm",
            f"provisioning warm runner {runner_name}",
            vmid=vmid,
            repository=repository,
            ip_address=ip_address,
        )
        await self.db.update_instance(
            instance_id, "provisioning", runner_name=runner_name, error=None
        )
        try:
            profile = self.settings.profiles[policy.profile]
            await self.proxmox.clone(vmid, runner_name, ip_address, profile, "warm", None)
            await self.broker.attach_cache(vmid, cache_slug(repository))
            installation = self._installation(repository)
            # Stale GitHub runner registrations (e.g. after failed destroy) cause
            # generate-jitconfig 409 Already exists. Remove offline/idle leftovers.
            stale = await self.github.find_runner(installation, repository, runner_name)
            if stale:
                if stale.get("busy", False):
                    raise RuntimeError(f"GitHub runner {runner_name} still busy; cannot recreate")
                await self.github.delete_runner(installation, repository, int(stale["id"]))
                self.log(
                    "warn",
                    "warm",
                    f"removed stale GitHub runner {runner_name} before JIT",
                    vmid=vmid,
                    runner_id=stale.get("id"),
                    repository=repository,
                )
            credential = await self._build_warm_credential(
                installation, repository, runner_name, policy
            )
            await self.proxmox.start(vmid)
            await self.broker.inject(vmid, credential)
            outcome = await self._wait_for_warm_runner(
                installation, repository, runner_name, instance_id
            )
            target_status = await self._finalize_warm_transition(
                instance_id, runner_name, outcome, clear_job=False
            )
            WARM_REGISTRATION.labels(outcome).inc()
            PROVISION.labels("warm").observe(time.monotonic() - started)
            self.log(
                "info",
                "warm",
                f"warm runner {runner_name} is {target_status}",
                vmid=vmid,
                repository=repository,
                outcome=outcome,
                duration_seconds=round(time.monotonic() - started, 1),
            )
            return True
        except Exception as exc:
            PROVISION_FAILURES.labels("warm").inc()
            failed = await self.db.transition_instance(
                instance_id,
                {"provisioning"},
                "delete_failed",
                error=str(exc)[:500],
            )
            if failed:
                WARM_REGISTRATION.labels("failed").inc()
                self.events.exception(
                    "warm",
                    f"warm provisioning failed vmid={vmid}",
                    exc,
                    vmid=vmid,
                    repository=repository,
                    runner_name=runner_name,
                )
                raise
            current = await self.db.get_instance(instance_id)
            self.log(
                "warn",
                "warm",
                "warm provisioning error ignored after runner was claimed",
                vmid=vmid,
                status=current and current.get("status"),
                error=str(exc)[:300],
            )
            return True

    async def _build_warm_credential(
        self,
        installation: GitHubInstallation,
        repository: str,
        runner_name: str,
        policy: RepositoryPolicy,
    ) -> dict[str, object]:
        """JIT-register `runner_name` and build its injectable credential payload.

        Shared by _create_warm_runner (fresh clone) and _recycle_warm_instance
        (re-JIT in place) — identical from here on regardless of how the CT
        itself got to a running state.
        """
        labels = self._warm_labels(policy)
        jit = await self.github.generate_jit(installation, repository, runner_name, labels)
        return {
            "mode": "warm",
            "runner_name": runner_name,
            "repository": repository,
            "labels": labels,
            "runner_url": self.github.runner_url(installation, repository),
            "encoded_jit_config": str(jit["encoded_jit_config"]),
            "environment": runner_job_environment(
                cargo_target_dir=(
                    f"/cache/target/{cache_slug(repository).replace('/', '-').lower()}-warm"
                ),
            ),
        }

    async def _finalize_warm_transition(
        self, instance_id: int, runner_name: str, outcome: str, *, clear_job: bool
    ) -> str:
        """Move a provisioning warm CT to ready/running after registration.

        Returns the resolved target status. Raises if the instance moved to
        an unexpected state concurrently (anything but the known-safe ones
        below), instead of silently accepting a state we don't understand.
        """
        target_status = "ready" if outcome == "ready" else "running"
        fields: dict[str, Any] = {"last_used_at": int(time.time()), "error": None}
        if clear_job:
            fields["job_id"] = None
        transitioned = await self.db.transition_instance(
            instance_id, {"provisioning"}, target_status, **fields
        )
        if not transitioned:
            current = await self.db.get_instance(instance_id)
            if current is None or current["status"] not in {
                "ready",
                "reserved",
                "running",
                "delete_pending",
            }:
                raise RuntimeError(
                    f"warm runner {runner_name} changed to unexpected state: {current}"
                )
        return target_status

    async def _wait_for_warm_runner(
        self,
        installation: GitHubInstallation,
        repository: str,
        runner_name: str,
        instance_id: int,
    ) -> str:
        deadline = time.monotonic() + self.settings.capacity.ready_timeout_seconds
        while time.monotonic() < deadline:
            instance = await self.db.get_instance(instance_id)
            if instance is None:
                raise RuntimeError(f"warm runner {runner_name} disappeared from state")
            if instance["status"] in {"reserved", "running", "delete_pending"} or instance.get(
                "job_id"
            ):
                return "claimed"
            # Polling for a state change: a cached list would never show it.
            runner = await self.github.find_runner(
                installation, repository, runner_name, fresh=True
            )
            if runner and runner.get("status") == "online":
                return "claimed" if runner.get("busy", False) else "ready"
            await asyncio.sleep(1)
        raise TimeoutError(f"warm runner {runner_name} did not become online and idle")

    @staticmethod
    def _warm_labels(policy: RepositoryPolicy) -> list[str]:
        return sorted({str(label).lower() for label in policy.required_labels} | {"mode-ephemeral"})

    @classmethod
    def _warm_compatible(cls, job: dict[str, Any], policy: RepositoryPolicy) -> bool:
        requested = {str(label).lower() for label in job.get("labels", [])}
        return requested.issubset(set(cls._warm_labels(policy)))

    def _schedule_warm_replenishment(self) -> None:
        task = asyncio.create_task(self._replenish_warm_pool(), name="warm-runner-replenishment")
        self._background_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            self._background_tasks.discard(completed)
            if not completed.cancelled() and (error := completed.exception()) is not None:
                LOGGER.error(
                    "warm runner replenishment failed",
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(finished)

    def _schedule_warm_recycle(self, instance_id: int) -> None:
        task = asyncio.create_task(
            self._recycle_warm_instance(instance_id),
            name=f"warm-runner-recycle-{instance_id}",
        )
        self._background_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            self._background_tasks.discard(completed)
            if not completed.cancelled() and (error := completed.exception()) is not None:
                LOGGER.error(
                    "warm runner recycle failed",
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(finished)

    async def _recycle_warm_instance(self, instance_id: int) -> None:
        """Re-JIT an existing warm CT after a job without cloning the golden."""
        instance = await self.db.get_instance(instance_id)
        if instance is None:
            return
        if instance["mode"] != "warm" or instance["status"] != "provisioning":
            return
        vmid = int(instance["vmid"])
        repository = str(instance.get("repository") or "")
        runner_name = str(instance.get("runner_name") or self._warm_runner_name(vmid))
        if not repository:
            await self._fail_warm_recycle(
                instance_id, "recycle_missing_repository", vmid=vmid, repository=repository
            )
            return
        resolved = repository_policy(self.settings, repository)
        if resolved is None:
            await self._fail_warm_recycle(
                instance_id, "recycle_repo_not_allowed", vmid=vmid, repository=repository
            )
            return
        repository, policy = resolved
        started = time.monotonic()
        use_count = int(instance.get("use_count") or 0)
        self.log(
            "info",
            "warm",
            f"recycling warm runner {runner_name}",
            vmid=vmid,
            repository=repository,
            use_count=use_count,
        )
        try:
            current = await self.proxmox.status(vmid)
            if str(current.get("status", "")).lower() != "running":
                await self.proxmox.start(vmid)
            clean_out = await self.broker.clean_work(vmid)
            if "disk_low" in clean_out.lower():
                raise RuntimeError(f"rootfs low on free space after clean: {clean_out}")
            # `warm_min_root_mib` was validated and serialized but never read by
            # any runtime path. The broker already reports `avail_mib`, so the
            # threshold the field name promises can simply be applied.
            avail_mib = _parse_avail_mib(clean_out)
            floor_mib = int(self.settings.capacity.warm_min_root_mib)
            if avail_mib is not None and avail_mib < floor_mib:
                raise RuntimeError(
                    f"rootfs free {avail_mib} MiB below warm_min_root_mib {floor_mib} MiB"
                )
            installation = self._installation(repository)
            await self._wait_github_runner_idle(
                installation, repository, runner_name, timeout_seconds=90
            )
            stale = await self.github.find_runner(installation, repository, runner_name)
            if stale:
                await self.github.delete_runner(installation, repository, int(stale["id"]))
            credential = await self._build_warm_credential(
                installation, repository, runner_name, policy
            )
            await self.broker.inject(vmid, credential)
            outcome = await self._wait_for_warm_runner(
                installation, repository, runner_name, instance_id
            )
            target_status = await self._finalize_warm_transition(
                instance_id, runner_name, outcome, clear_job=True
            )
            WARM_REGISTRATION.labels(f"recycle_{outcome}").inc()
            self.log(
                "info",
                "warm",
                f"recycled warm runner {runner_name} → {target_status}",
                vmid=vmid,
                repository=repository,
                outcome=outcome,
                use_count=use_count,
                duration_seconds=round(time.monotonic() - started, 1),
                clean=clean_out[:120],
            )
        except Exception as exc:  # noqa: BLE001 - recorded below, caller falls back to destroy
            PROVISION_FAILURES.labels("warm_recycle").inc()
            self.events.exception(
                "warm",
                f"warm recycle failed vmid={vmid}",
                exc,
                vmid=vmid,
                repository=repository,
                runner_name=runner_name,
            )
            await self._fail_warm_recycle(
                instance_id,
                f"recycle_failed: {str(exc)[:400]}",
                vmid=vmid,
                repository=repository,
                runner_name=runner_name,
            )

    async def _wait_github_runner_idle(
        self,
        installation: GitHubInstallation,
        repository: str,
        runner_name: str,
        *,
        timeout_seconds: int = 90,
    ) -> None:
        """Wait until the previous JIT registration is gone or idle (not busy)."""
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            # Polling for a state change: a cached list would never show it.
            runner = await self.github.find_runner(
                installation, repository, runner_name, fresh=True
            )
            if runner is None:
                return
            if not runner.get("busy", False):
                return
            await asyncio.sleep(2)
        raise TimeoutError(f"GitHub runner {runner_name} still busy after {timeout_seconds}s")

    async def _fail_warm_recycle(
        self,
        instance_id: int,
        error: str,
        *,
        vmid: int | None = None,
        repository: str | None = None,
        runner_name: str | None = None,
    ) -> None:
        await self.db.transition_instance(
            instance_id,
            {"provisioning", "ready", "reserved", "running"},
            "delete_pending",
            error=error[:500],
        )
        failed = await self.db.get_instance(instance_id)
        if failed is not None:
            await self._destroy_instance(failed)
        self._schedule_warm_replenishment()
        self.log(
            "warn",
            "warm",
            f"warm recycle aborted; destroy scheduled ({error[:160]})",
            vmid=vmid,
            repository=repository,
            runner_name=runner_name,
        )

    def _schedule_pool_replenishment(self) -> None:
        task = asyncio.create_task(self._replenish_pool(), name="global-pool-replenishment")
        self._background_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            self._background_tasks.discard(completed)
            if not completed.cancelled() and (error := completed.exception()) is not None:
                LOGGER.error(
                    "global pool replenishment failed",
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(finished)

    async def _update_metrics(self) -> None:
        summary = await self.db.status_summary()
        QUEUE.set(next((row["total"] for row in summary["jobs"] if row["status"] == "queued"), 0))
        OLDEST_QUEUED.set(await self.db.oldest_queued_job_age())
        ACTIVE.clear()
        for row in summary["instances"]:
            ACTIVE.labels(row["mode"], row["status"]).set(row["total"])

        # The teardown backlog is the leading indicator the incident lacked:
        # the existing thin-pool alerts only fired once disk was already full.
        backlog = await self.db.delete_backlog()
        now = int(time.time())
        oldest = 0
        DELETE_BACKLOG.clear()
        for status in ("delete_pending", "delete_failed", "deleting"):
            row = backlog.get(status)
            DELETE_BACKLOG.labels(status).set(int(row["total"]) if row else 0)
            if row and row.get("oldest") is not None:
                oldest = max(oldest, now - int(row["oldest"]))
        DELETE_BACKLOG_OLDEST.set(oldest)

        PENDING_DEREGISTRATIONS.set(await self.db.count_pending_deregistrations())
        PROJECTED_DISK_DEMAND.set(
            projected_disk_demand_gb(
                self.settings.capacity, self.settings.repositories, self.settings.profiles
            )
            * 1024**3
        )
        GITHUB_CIRCUIT_STATE.set(
            {"closed": 0, "half_open": 1, "open": 2}.get(
                str(self.github.breaker.snapshot()["state"]), 0
            )
        )

    def _spawn_background(self, coro: Awaitable[None], name: str) -> None:
        """Run work off the critical path, tracked so shutdown can drain it."""
        task = asyncio.create_task(cast("Coroutine[Any, Any, None]", coro), name=name)
        self._background_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            self._background_tasks.discard(completed)
            if not completed.cancelled() and (error := completed.exception()) is not None:
                LOGGER.error(
                    "background task %s failed",
                    name,
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(finished)

    async def _probe_loop(self) -> None:
        """Watch external dependencies on their own schedule.

        Deliberately independent of `reconcile()`. During the incident that
        motivated this, the reconcile loop was the only thing touching GitHub,
        so when
        it stalled the system also stopped learning anything about *why*. A
        probe that shares the loop it is meant to diagnose is not a probe.
        """
        interval = float(self.settings.capacity.dependency_probe_seconds)
        round_number = 0
        while not self._stop.is_set():
            round_number += 1
            await self._probe_once(round_number)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=interval)

    async def _probe_once(self, round_number: int = 1) -> None:
        async def record(name: str, coro: Awaitable[float]) -> None:
            try:
                latency = await coro
            except Exception as exc:  # noqa: BLE001 — a probe reports, it never raises
                self.dependencies[name] = {
                    "up": False,
                    "latency_seconds": None,
                    "last_error": str(exc)[:200],
                    "checked_at": int(time.time()),
                }
                DEPENDENCY_UP.labels(name).set(0)
            else:
                self.dependencies[name] = {
                    "up": True,
                    "latency_seconds": round(latency, 4),
                    "last_error": None,
                    "checked_at": int(time.time()),
                    "last_success_at": int(time.time()),
                }
                DEPENDENCY_UP.labels(name).set(1)
                DEPENDENCY_LATENCY.labels(name).observe(latency)

        await record("github_api", self.github.probe())
        await record("proxmox_api", self.proxmox.probe())
        # Forks an SSH process, so it runs less often than the HTTP probes.
        if round_number % 4 == 1 and self.settings.repositories:
            repository = next(iter(self.settings.repositories))

            async def broker_probe() -> float:
                started = time.monotonic()
                if not await self.broker.cache_ready(repository):
                    raise RuntimeError(f"cache not ready for {repository}")
                return time.monotonic() - started

            await record("broker_ssh", broker_probe())

    async def readiness(self) -> tuple[bool, dict[str, object]]:
        """Health for the probes, memoized.

        Every connected dashboard used to trigger a full recomputation on every
        3-second SSE tick: four blocking secret reads plus one SSH round-trip
        per configured repository. With a handful of tabs open that was a
        steady stream of SSH connections to pve for information that changes
        on the order of minutes.

        Now: secrets are mtime-cached, the broker answer comes from the probe
        loop, and the whole thing is computed at most once per TTL no matter how
        many clients ask.
        """
        now = time.monotonic()
        cached = self._readiness_cache
        if cached and (now - cached[0]) < _READINESS_TTL_SECONDS:
            return cached[1], cached[2]

        async with self._readiness_lock:
            cached = self._readiness_cache
            if cached and (time.monotonic() - cached[0]) < _READINESS_TTL_SECONDS:
                return cached[1], cached[2]

            checks: dict[str, object] = {"database": False, "secrets": False, "cache": False}
            try:
                await self.db.status_summary()
                checks["database"] = True
                checks["secrets"] = all(bool(secret.get()) for secret in self._required_secrets)
                broker = self.dependencies.get("broker_ssh")
                if broker is None:
                    # No probe result yet (first seconds after start): ask once
                    # rather than reporting a failure we have not observed.
                    await self._probe_once()
                    broker = self.dependencies.get("broker_ssh")
                checks["cache"] = bool(broker and broker.get("up")) if broker else True
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("readiness check failed")
                checks["error"] = type(exc).__name__

            ready = all(checks.get(name) is True for name in ("database", "secrets", "cache"))
            self._readiness_cache = (time.monotonic(), ready, checks)
            return ready, checks

    @property
    def draining(self) -> bool:
        return self.settings.drain or DRAIN_MARKER.exists()

    def _installation(self, repository: str) -> GitHubInstallation:
        resolved = repository_policy(self.settings, repository)
        if resolved is None:
            raise KeyError(f"repository policy not found: {repository}")
        _, policy = resolved
        return self.settings.github.installations[policy.github_installation]

    def _installation_for_cleanup(self, repository: str) -> GitHubInstallation | None:
        """Resolve installation for destroy; fall back when policy was removed/renamed."""
        try:
            return self._installation(repository)
        except KeyError:
            if self.settings.github.installations:
                return next(iter(self.settings.github.installations.values()))
            return None

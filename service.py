from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

from .broker import BrokerClient
from .config import GitHubInstallation, RepositoryPolicy, Settings
from .db import Database
from .eventlog import EventLog
from .github import GitHubClient
from .policy import Decision, cache_slug, decide, repository_policy
from .proxmox import ProxmoxClient
from .telemetry import span

LOGGER = logging.getLogger("runner_orchestrator")
DRAIN_MARKER = Path("/var/lib/runner-orchestrator/drain")

WEBHOOKS = Counter("runner_orchestrator_webhooks_total", "Webhook deliveries", ["result", "reason"])
PROVISION = Histogram("runner_orchestrator_provision_seconds", "Runner provisioning duration", ["mode"])
PROVISION_FAILURES = Counter("runner_orchestrator_provision_failures_total", "Provisioning failures", ["mode"])
CLEANUP_FAILURES = Counter("runner_orchestrator_cleanup_failures_total", "Cleanup failures", ["mode"])
ACTIVE = Gauge("runner_orchestrator_active_instances", "Active runner instances", ["mode", "status"])
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


# Toolchain + persistent cache layout injected into every runner process.
# Cargo paths serve Tauri (example-app); Go paths serve Wails (example-service).
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
        self.events = EventLog(maxlen=500)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._background_tasks: set[asyncio.Task[None]] = set()
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
        event = self.events.emit(
            level, category, message, detail=detail, **context
        )
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
        ]

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, *self._background_tasks, return_exceptions=True)

    async def webhook(
        self, delivery_id: str, event: str, payload: dict[str, Any]
    ) -> tuple[bool, str]:
        action = str(payload.get("action", ""))
        repository = str(payload.get("repository", {}).get("full_name", ""))
        if event != "workflow_job":
            inserted = await self.db.record_delivery(delivery_id, event, action, False, "event_ignored")
            WEBHOOKS.labels("duplicate" if not inserted else "ignored", "event_ignored").inc()
            return inserted, "event_ignored"
        resolved_repository = repository_policy(self.settings, repository)
        if resolved_repository is None:
            inserted = await self.db.record_delivery(delivery_id, event, action, False, "repository_not_allowed")
            WEBHOOKS.labels("duplicate" if not inserted else "rejected", "repository_not_allowed").inc()
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
            reason = "shadow_mode" if decision.accepted and self.settings.shadow_mode else decision.reason
            inserted = await self.db.record_delivery(delivery_id, event, action, decision.accepted, reason)
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
                    expected_warm = f"pve-lxc-warm-{by_job['vmid']}"
                    if (
                        by_job["mode"] == "warm"
                        and bound_name
                        and bound_name != expected_warm
                    ):
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
                expected_warm = f"pve-lxc-warm-{instance['vmid']}"
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
                    await self.db.update_instance(existing["id"], "ready", job_id=job_id, last_used_at=int(time.time()))
                    await self.db.set_job_status(
                        job_id,
                        "waiting_for_runner",
                        instance_id=existing["id"],
                        runner_name=existing["runner_name"],
                        error=None,
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
            if int(job["attempts"]) < 3:
                await self.db.set_job_status(job_id, "queued", error=str(exc)[:500])
            else:
                await self.db.set_job_status(job_id, "failed", error=str(exc)[:500])

    async def _provision(self, job: dict[str, Any], decision: Decision) -> None:
        started = time.monotonic()
        job_id = int(job["job_id"])
        repository = decision.repository
        installation = self._installation(repository)
        mode = decision.mode
        profile = self.settings.profiles["crossbuild-small"]

        instance: dict[str, Any] | None = None
        runner_name = self._runner_name(mode, job_id)
        if mode in {"ephemeral", "pool"}:
            instance = await self.db.claim_ready_pool(
                repository, job_id, runner_name, mode=mode
            )
        if instance:
            instance_id = int(instance["id"])
            vmid = int(instance["vmid"])
            ip_address = str(instance["ip_address"])
            self._schedule_pool_replenishment()
        else:
            reservation = await self._reserve_instance(
                mode, repository, job_id, runner_name, enforce_capacity=True
            )
            if reservation is None:
                await self.db.set_job_status(
                    job_id, "queued", error="capacity_exhausted"
                )
                await asyncio.sleep(5)
                return
            instance_id, vmid, ip_address = reservation
            await self.proxmox.clone(vmid, runner_name, ip_address, profile, mode, job_id)

        await self.broker.attach_cache(vmid, cache_slug(repository))
        labels = sorted(set(str(label).lower() for label in decision.labels))
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
            jit = await self.github.generate_jit(
                installation, repository, runner_name, labels
            )
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
            return vmid, f"10.0.0.{ip_octet}"
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
            if enforce_capacity and await self.db.count_active() >= self.settings.capacity.max_active:
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
            await self.db.set_job_status(
                job_id, "completed", completed_at=now, error=None
            )
        instance = await self.db.find_instance_by_job(job_id)
        if not instance and repository and runner_name:
            candidate = await self.db.find_instance_by_runner(
                repository, runner_name
            )
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
        try:
            # GitHub registration must leave the pool *before* the CT dies whenever
            # the agent is still online. Destroying first leaves a TOCTOU window
            # where GH assigns a new job to a runner that is about to vanish →
            # offline+busy forever (stuck Set up job).
            registration_purged = False
            if (
                instance["status"] in {"delete_pending", "delete_failed"}
                and instance.get("repository")
                and instance.get("runner_name")
            ):
                installation = self._installation_for_cleanup(str(instance["repository"]))
                if installation is not None:
                    runner = await self.github.find_runner(
                        installation,
                        str(instance["repository"]),
                        str(instance["runner_name"]),
                    )
                    if runner:
                        status = str(runner.get("status") or "").lower()
                        busy = bool(runner.get("busy", False))
                        age = int(time.time()) - int(
                            instance.get("updated_at") or time.time()
                        )
                        # Truly online+busy: job may still be executing — defer briefly.
                        if status == "online" and busy:
                            if age < 180:
                                self.log(
                                    "warn",
                                    "destroy",
                                    "cleanup deferred; GitHub runner still online/busy",
                                    vmid=vmid,
                                    runner_name=instance.get("runner_name"),
                                    repository=instance.get("repository"),
                                    age_seconds=age,
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
                                await self.db.transition_instance(
                                    instance["id"],
                                    {str(instance["status"])},
                                    "delete_failed",
                                    error="github_runner_delete_failed_still_online",
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
            await self.db.transition_instance(
                instance["id"],
                {"deleting"},
                "delete_failed",
                error=str(exc)[:500],
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
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.settings.capacity.reconcile_seconds
                )
            except TimeoutError:
                pass

    async def _reconcile_stale_jobs(self, now: int) -> None:
        """Repair job rows left open by lost/racy lifecycle webhooks."""
        jobs = await self.db.list_jobs(limit=200)
        for job in jobs:
            job_id = int(job["job_id"])
            status = str(job.get("status") or "")
            # completed_at written but status later clobbered by late provision/in_progress.
            if job.get("completed_at") is not None and status != "completed":
                repaired = await self.db.set_job_status(
                    job_id,
                    "completed",
                    completed_at=int(job["completed_at"]),
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
        for instance in await self.db.list_instances():
            if instance.get("status") != "running" or not instance.get("job_id"):
                continue
            bound = int(instance["job_id"])
            job = await self.db.get_job(bound)
            if job is None or str(job.get("status")) in self.db.JOB_TERMINAL_STATUSES:
                age = now - int(instance.get("updated_at") or now)
                if age < 60:
                    continue
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
                        error="stale_running_instance_job_terminal",
                    )
                self.log(
                    "warn",
                    "job",
                    f"released instance vmid={instance.get('vmid')} stuck running for terminal job {bound}",
                    job_id=bound,
                    vmid=instance.get("vmid"),
                    mode=mode,
                )

    async def _purge_github_runner_registration(
        self,
        repository: str,
        runner_name: str,
        *,
        vmid: int | None = None,
    ) -> None:
        """Best-effort remove of a GH self-hosted registration (handles zombies)."""
        installation = self._installation_for_cleanup(repository)
        if installation is None:
            return
        candidates = [repository]
        for configured in self.settings.repositories:
            if configured not in candidates:
                candidates.append(configured)
        for candidate in candidates:
            runner = await self.github.find_runner(installation, candidate, runner_name)
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
        for repository, policy in self.settings.repositories.items():
            try:
                installation = self._installation(repository)
            except Exception:
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

    async def reconcile(self) -> None:
        now = int(time.time())
        await self._reconcile_stale_jobs(now)
        await self._reconcile_zombie_github_runners()
        instances = await self.db.list_instances()
        for instance in instances:
            age = now - int(instance["updated_at"])
            idle = now - int(instance.get("last_used_at") or instance["updated_at"])
            if instance["status"] in {"delete_pending", "delete_failed"}:
                await self._destroy_instance(instance)
            elif instance["status"] == "provisioning" and age > self.settings.capacity.ready_timeout_seconds:
                await self._destroy_instance(instance)
            elif instance["status"] == "reserved" and age > self.settings.capacity.ready_timeout_seconds:
                await self._destroy_instance(instance)
            elif instance["mode"] == "persistent" and instance["status"] == "ready" and idle > self.settings.capacity.persistent_idle_seconds:
                await self._destroy_instance(instance)
            elif instance["mode"] == "warm" and instance["status"] in {
                "ready",
                "provisioning",
                "reserved",
            }:
                await self._reconcile_warm_instance(instance)
            elif instance["status"] == "running":
                await self._reconcile_running_instance(instance, now=now, age=age)

        if not self.settings.shadow_mode and not self.draining:
            await self._replenish_warm_pool()
            await self._replenish_pool()
        await self._maybe_gc_orphan_cache()
        await self._update_metrics()

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
            expected = f"pve-lxc-warm-{vmid}"
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
                if bound_job:
                    await self.db.set_job_status(
                        int(bound_job),
                        "failed",
                        completed_at=now,
                        error="runner_name_vmid_mismatch",
                    )
                await self.db.transition_instance(
                    instance["id"],
                    {"running"},
                    "delete_pending",
                    error="runner_name_vmid_mismatch",
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
                if bound_job:
                    await self.db.set_job_status(
                        int(bound_job),
                        "failed",
                        completed_at=now,
                        error="ct_missing_while_running",
                    )
                await self.db.transition_instance(
                    instance["id"],
                    {"running"},
                    "delete_pending",
                    error="ct_missing_while_running",
                )
                return

        # GH registration offline+busy with no agent → stuck Set up job.
        if (
            age >= 90
            and runner_name
            and instance.get("repository")
        ):
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
                if bound_job:
                    await self.db.set_job_status(
                        int(bound_job),
                        "failed",
                        completed_at=now,
                        error="zombie_offline_runner",
                    )
                await self.db.transition_instance(
                    instance["id"],
                    {"running"},
                    "delete_pending",
                    error="zombie_offline_runner",
                )
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
            # Handle rename (OldOwner → acme): old warms still count
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
            LOGGER.exception(
                "warm runner health check failed", extra={"vmid": instance["vmid"]}
            )

    async def _replenish_pool(self) -> None:
        async with self._pool_lock:
            missing = self.settings.capacity.pool_size - await self.db.count_ready_pool()
            for _ in range(max(0, missing)):
                reservation = await self._reserve_instance(
                    "pool", None, None, None, enforce_capacity=False
                )
                if reservation is None:
                    return
                instance_id, vmid, ip_address = reservation
                try:
                    await self.proxmox.clone(
                        vmid,
                        f"pve-lxc-pool-{vmid}",
                        ip_address,
                        self.settings.profiles["crossbuild-small"],
                        "pool",
                        None,
                    )
                    await self.db.update_instance(instance_id, "ready")
                except Exception:
                    PROVISION_FAILURES.labels("pool").inc()
                    await self.db.update_instance(instance_id, "delete_failed")
                    raise

    async def _replenish_warm_pool(self) -> None:
        async with self._warm_lock:
            for repository, policy in self.settings.repositories.items():
                missing = policy.warm_capacity - await self.db.count_warm_supply(repository)
                for _ in range(max(0, missing)):
                    created = await self._create_warm_runner(repository, policy)
                    if not created:
                        return

    async def _create_warm_runner(
        self, repository: str, policy: RepositoryPolicy
    ) -> bool:
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
        runner_name = f"pve-lxc-warm-{vmid}"
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
            profile = self.settings.profiles["crossbuild-small"]
            await self.proxmox.clone(
                vmid, runner_name, ip_address, profile, "warm", None
            )
            await self.broker.attach_cache(vmid, cache_slug(repository))
            installation = self._installation(repository)
            labels = self._warm_labels(policy)
            # Stale GitHub runner registrations (e.g. after failed destroy) cause
            # generate-jitconfig 409 Already exists. Remove offline/idle leftovers.
            stale = await self.github.find_runner(
                installation, repository, runner_name
            )
            if stale:
                if stale.get("busy", False):
                    raise RuntimeError(
                        f"GitHub runner {runner_name} still busy; cannot recreate"
                    )
                await self.github.delete_runner(
                    installation, repository, int(stale["id"])
                )
                self.log(
                    "warn",
                    "warm",
                    f"removed stale GitHub runner {runner_name} before JIT",
                    vmid=vmid,
                    runner_id=stale.get("id"),
                    repository=repository,
                )
            jit = await self.github.generate_jit(
                installation, repository, runner_name, labels
            )
            credential: dict[str, object] = {
                "mode": "warm",
                "runner_name": runner_name,
                "repository": repository,
                "labels": labels,
                "runner_url": self.github.runner_url(installation, repository),
                "encoded_jit_config": str(jit["encoded_jit_config"]),
                "environment": runner_job_environment(
                    cargo_target_dir=(
                        f"/cache/target/"
                        f"{cache_slug(repository).replace('/', '-').lower()}-warm"
                    ),
                ),
            }
            await self.proxmox.start(vmid)
            await self.broker.inject(vmid, credential)
            outcome = await self._wait_for_warm_runner(
                installation, repository, runner_name, instance_id
            )
            target_status = "ready" if outcome == "ready" else "running"
            transitioned = await self.db.transition_instance(
                instance_id,
                {"provisioning"},
                target_status,
                last_used_at=int(time.time()),
                error=None,
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
            if instance["status"] in {"reserved", "running", "delete_pending"} or instance.get("job_id"):
                return "claimed"
            runner = await self.github.find_runner(installation, repository, runner_name)
            if runner and runner.get("status") == "online":
                return "claimed" if runner.get("busy", False) else "ready"
            await asyncio.sleep(1)
        raise TimeoutError(f"warm runner {runner_name} did not become online and idle")

    @staticmethod
    def _warm_labels(policy: RepositoryPolicy) -> list[str]:
        return sorted(
            set(str(label).lower() for label in policy.required_labels)
            | {"mode-ephemeral"}
        )

    @classmethod
    def _warm_compatible(
        cls, job: dict[str, Any], policy: RepositoryPolicy
    ) -> bool:
        requested = set(str(label).lower() for label in job.get("labels", []))
        return requested.issubset(set(cls._warm_labels(policy)))

    def _schedule_warm_replenishment(self) -> None:
        task = asyncio.create_task(
            self._replenish_warm_pool(), name="warm-runner-replenishment"
        )
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
        runner_name = str(instance.get("runner_name") or f"pve-lxc-warm-{vmid}")
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
            installation = self._installation(repository)
            await self._wait_github_runner_idle(
                installation, repository, runner_name, timeout_seconds=90
            )
            stale = await self.github.find_runner(installation, repository, runner_name)
            if stale:
                await self.github.delete_runner(
                    installation, repository, int(stale["id"])
                )
            labels = self._warm_labels(policy)
            jit = await self.github.generate_jit(
                installation, repository, runner_name, labels
            )
            credential: dict[str, object] = {
                "mode": "warm",
                "runner_name": runner_name,
                "repository": repository,
                "labels": labels,
                "runner_url": self.github.runner_url(installation, repository),
                "encoded_jit_config": str(jit["encoded_jit_config"]),
                "environment": runner_job_environment(
                    cargo_target_dir=(
                        f"/cache/target/"
                        f"{cache_slug(repository).replace('/', '-').lower()}-warm"
                    ),
                ),
            }
            await self.broker.inject(vmid, credential)
            outcome = await self._wait_for_warm_runner(
                installation, repository, runner_name, instance_id
            )
            target_status = "ready" if outcome == "ready" else "running"
            transitioned = await self.db.transition_instance(
                instance_id,
                {"provisioning"},
                target_status,
                job_id=None,
                last_used_at=int(time.time()),
                error=None,
            )
            if not transitioned:
                current_row = await self.db.get_instance(instance_id)
                if current_row is None or current_row["status"] not in {
                    "ready",
                    "reserved",
                    "running",
                    "delete_pending",
                }:
                    raise RuntimeError(
                        f"warm runner {runner_name} changed to unexpected state: {current_row}"
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
        except Exception as exc:
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
            runner = await self.github.find_runner(installation, repository, runner_name)
            if runner is None:
                return
            if not runner.get("busy", False):
                return
            await asyncio.sleep(2)
        raise TimeoutError(
            f"GitHub runner {runner_name} still busy after {timeout_seconds}s"
        )

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
        task = asyncio.create_task(
            self._replenish_pool(), name="global-pool-replenishment"
        )
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

    async def readiness(self) -> tuple[bool, dict[str, object]]:
        checks: dict[str, object] = {"database": False, "secrets": False, "cache": False}
        try:
            await self.db.status_summary()
            checks["database"] = True
            secret_paths = [
                self.settings.github.private_key_file,
                self.settings.github.webhook_secret_file,
                self.settings.proxmox.api_token_file,
                self.settings.broker.key_file,
            ]
            checks["secrets"] = all(bool(Path(path).read_text(encoding="utf-8").strip()) for path in secret_paths)
            cache_checks = await asyncio.gather(
                *(self.broker.cache_ready(repository) for repository in self.settings.repositories)
            )
            checks["cache"] = all(cache_checks)
        except Exception as exc:
            checks["error"] = type(exc).__name__
        return all(checks.get(name) is True for name in ("database", "secrets", "cache")), checks

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

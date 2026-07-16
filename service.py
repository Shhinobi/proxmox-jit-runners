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
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._warm_lock = asyncio.Lock()
        self._pool_lock = asyncio.Lock()
        self._allocation_lock = asyncio.Lock()

    async def start(self) -> None:
        await self.db.initialize()
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

        inserted = await self.db.record_delivery(delivery_id, event, action, True, "lifecycle_update")
        if not inserted:
            WEBHOOKS.labels("duplicate", "lifecycle_update").inc()
            return False, "duplicate"
        job_id = int(payload.get("workflow_job", {}).get("id", 0))
        if action == "in_progress" and job_id:
            runner_name = payload.get("workflow_job", {}).get("runner_name")
            job = await self.db.get_job(job_id)
            if job:
                JOB_QUEUE_WAIT.labels(str(job.get("mode") or "unknown")).observe(
                    max(0, int(time.time()) - int(job["created_at"]))
                )
            await self.db.set_job_status(job_id, "running", runner_name=runner_name, error=None)
            instance = await self.db.find_instance_by_job(job_id)
            if not instance and runner_name:
                instance = await self.db.find_instance_by_runner(
                    resolved_repository[0], str(runner_name)
                )
            if instance:
                transitioned = await self.db.transition_instance(
                    instance["id"],
                    {"provisioning", "ready", "reserved", "running"},
                    "running",
                    job_id=job_id,
                    runner_name=runner_name,
                )
                if transitioned and instance["mode"] == "warm":
                    self._schedule_warm_replenishment()
        elif action == "completed" and job_id:
            runner_name = payload.get("workflow_job", {}).get("runner_name")
            await self._complete_job(
                job_id,
                repository=resolved_repository[0],
                runner_name=str(runner_name) if runner_name else None,
            )
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
            if (
                mode == "ephemeral"
                and policy is not None
                and policy.warm_capacity > 0
                and self._warm_compatible(job, policy)
            ):
                instance = await self.db.claim_ready_warm(decision.repository, job_id)
                if instance:
                    await self.db.set_job_status(
                        job_id,
                        "waiting_for_runner",
                        instance_id=instance["id"],
                        runner_name=instance["runner_name"],
                        error=None,
                    )
                    self._schedule_warm_replenishment()
                    return

            if await self.db.count_active() >= self.settings.capacity.max_active:
                await self.db.set_job_status(job_id, "queued", error="capacity_exhausted")
                await asyncio.sleep(5)
                return

            await self._provision(job, decision)
        except Exception as exc:
            PROVISION_FAILURES.labels(mode).inc()
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
            "environment": {
                "PATH": "/home/actions/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "RUSTUP_HOME": "/home/actions/.rustup",
                "CARGO_HOME": "/cache/cargo",
                "XWIN_CACHE_DIR": "/cache/cargo-xwin",
                "npm_config_cache": "/cache/npm",
                "CARGO_TARGET_DIR": f"/cache/target/{int(job['run_id'])}",
            },
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
            return vmid, f"10.20.60.{ip_octet}"
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
        # GitHub expects a fast webhook acknowledgement. Reconciliation performs
        # the potentially slow Proxmox stop/delete task out of band.
        await self.db.transition_instance(
            instance["id"],
            {"provisioning", "ready", "reserved", "running"},
            "delete_pending",
        )

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
        try:
            if (
                instance["status"] in {"delete_pending", "delete_failed"}
                and instance.get("repository")
                and instance.get("runner_name")
            ):
                installation = self._installation(str(instance["repository"]))
                runner = await self.github.find_runner(
                    installation,
                    str(instance["repository"]),
                    str(instance["runner_name"]),
                )
                if runner and runner.get("busy", False):
                    LOGGER.warning(
                        "instance cleanup deferred because GitHub runner is busy",
                        extra={"vmid": instance["vmid"]},
                    )
                    return
            transitioned = await self.db.transition_instance(
                instance["id"], {str(instance["status"])}, "deleting"
            )
            if not transitioned:
                LOGGER.info(
                    "instance cleanup skipped after concurrent state change",
                    extra={"vmid": instance["vmid"]},
                )
                return
            if instance["mode"] == "warm" and instance.get("repository") and instance.get("runner_name"):
                installation = self._installation(str(instance["repository"]))
                runner = await self.github.find_runner(
                    installation, str(instance["repository"]), str(instance["runner_name"])
                )
                if runner:
                    await self.github.delete_runner(
                        installation, str(instance["repository"]), int(runner["id"])
                    )
            await self.proxmox.destroy(int(instance["vmid"]))
            await self.db.delete_instance(instance["id"])
        except Exception:
            CLEANUP_FAILURES.labels(str(instance["mode"])).inc()
            await self.db.transition_instance(
                instance["id"], {"deleting"}, "delete_failed"
            )
            LOGGER.exception("instance cleanup failed", extra={"vmid": instance["vmid"]})

    async def _reconcile_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("reconciliation failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.settings.capacity.reconcile_seconds
                )
            except TimeoutError:
                pass

    async def reconcile(self) -> None:
        now = int(time.time())
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
            elif instance["mode"] == "warm" and instance["status"] == "ready":
                try:
                    installation = self._installation(str(instance["repository"]))
                    resolved = repository_policy(
                        self.settings, str(instance["repository"])
                    )
                    runner = await self.github.find_runner(
                        installation,
                        str(instance["repository"]),
                        str(instance["runner_name"]),
                    )
                    if runner is None or runner.get("status") != "online":
                        await self._destroy_instance(instance)
                    elif resolved is None or not set(
                        self._warm_labels(resolved[1])
                    ).issubset({
                        str(label.get("name", "")).lower()
                        for label in runner.get("labels", [])
                        if isinstance(label, dict)
                    }):
                        await self._destroy_instance(instance)
                except Exception:
                    LOGGER.exception(
                        "warm runner health check failed", extra={"vmid": instance["vmid"]}
                    )
            elif instance["status"] == "running" and age > self.settings.capacity.max_job_seconds:
                await self._destroy_instance(instance)

        if not self.settings.shadow_mode and not self.draining:
            await self._replenish_warm_pool()
            await self._replenish_pool()
        await self._update_metrics()

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
        reservation = await self._reserve_instance(
            "warm", repository, None, None, enforce_capacity=True
        )
        if reservation is None:
            return False
        instance_id, vmid, ip_address = reservation
        runner_name = f"pve-lxc-warm-{vmid}"
        await self.db.update_instance(
            instance_id, "provisioning", runner_name=runner_name
        )
        try:
            profile = self.settings.profiles["crossbuild-small"]
            await self.proxmox.clone(
                vmid, runner_name, ip_address, profile, "warm", None
            )
            await self.broker.attach_cache(vmid, cache_slug(repository))
            installation = self._installation(repository)
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
                "environment": {
                    "PATH": "/home/actions/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                    "RUSTUP_HOME": "/home/actions/.rustup",
                    "CARGO_HOME": "/cache/cargo",
                    "XWIN_CACHE_DIR": "/cache/cargo-xwin",
                    "npm_config_cache": "/cache/npm",
                    "CARGO_TARGET_DIR": f"/cache/target/{cache_slug(repository).replace('/', '-').lower()}-warm",
                },
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
            return True
        except Exception:
            PROVISION_FAILURES.labels("warm").inc()
            failed = await self.db.transition_instance(
                instance_id, {"provisioning"}, "delete_failed"
            )
            if failed:
                WARM_REGISTRATION.labels("failed").inc()
                raise
            current = await self.db.get_instance(instance_id)
            LOGGER.warning(
                "warm provisioning error ignored after runner was claimed",
                extra={"vmid": vmid, "status": current and current.get("status")},
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

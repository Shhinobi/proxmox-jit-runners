from __future__ import annotations

import asyncio
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest import mock

from orchestrator.config import RunnerProfile
from orchestrator.db import Database
from orchestrator.service import JOBS_FAILED, Orchestrator, _events_persist_path
from tests.helpers import settings


def webhook_payload(installation_id: int) -> dict[str, Any]:
    return {
        "action": "queued",
        "installation": {"id": installation_id},
        "repository": {"full_name": "MetalDevOps/NinjaCrawler"},
        "workflow_job": {
            "id": 42,
            "run_id": 7,
            "labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
        },
    }


class EventsPersistPathTests(unittest.TestCase):
    def test_memory_database_disables_persistence(self) -> None:
        self.assertIsNone(_events_persist_path(":memory:"))

    def test_file_database_gets_sibling_events_file(self) -> None:
        path = _events_persist_path("/var/lib/runner-orchestrator/state.db")
        assert path is not None
        self.assertEqual(path.name, "events.jsonl")
        self.assertEqual(path.parent, Path("/var/lib/runner-orchestrator"))


class VmidAllocationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        base = settings()
        self.settings = replace(
            base,
            proxmox=replace(base.proxmox, vmid_start=5000, vmid_end=5001, ip_start=100, ip_end=101),
        )
        self.github = FakeGitHub()
        self.proxmox = FakeProxmox()
        self.broker = FakeBroker()
        self.orchestrator = Orchestrator(
            self.settings,
            self.db,
            cast(Any, self.github),
            cast(Any, self.proxmox),
            cast(Any, self.broker),
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def test_allocates_first_free_vmid_in_range(self) -> None:
        await self.db.create_instance(5000, "10.20.60.100", "warm", "ready", "MetalDevops/NinjaCrawler")

        vmid, ip_address = await self.orchestrator._allocate()

        self.assertEqual(vmid, 5001)
        self.assertEqual(ip_address, "10.20.60.101")

    async def test_raises_when_range_is_exhausted(self) -> None:
        await self.db.create_instance(5000, "10.20.60.100", "warm", "ready", "MetalDevops/NinjaCrawler")
        await self.db.create_instance(5001, "10.20.60.101", "warm", "ready", "MetalDevops/NinjaCrawler")

        with self.assertRaisesRegex(RuntimeError, "VMID/IP range exhausted"):
            await self.orchestrator._allocate()

    async def test_reserve_instance_surfaces_exhaustion_to_caller(self) -> None:
        await self.db.create_instance(5000, "10.20.60.100", "warm", "ready", "MetalDevops/NinjaCrawler")
        await self.db.create_instance(5001, "10.20.60.101", "warm", "ready", "MetalDevops/NinjaCrawler")

        with self.assertRaises(RuntimeError):
            await self.orchestrator._reserve_instance(
                "ephemeral", "MetalDevops/NinjaCrawler", 1, "runner-1", enforce_capacity=False
            )

    async def test_accounts_for_vmids_live_on_proxmox_but_not_in_db(self) -> None:
        """A VMID that exists on Proxmox (e.g. manually created) but isn't
        tracked in our DB yet must still not be double-allocated."""

        async def fake_cluster_vmids() -> set[int]:
            return {5000}

        self.proxmox.cluster_vmids = fake_cluster_vmids  # type: ignore[method-assign]

        vmid, ip_address = await self.orchestrator._allocate()

        self.assertEqual(vmid, 5001)
        self.assertEqual(ip_address, "10.20.60.101")


class WebhookInstallationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        database = Database(str(Path(self.temp.name) / "state.db"))
        await database.initialize()
        self.orchestrator = Orchestrator(
            settings(shadow_mode=True),
            database,
            cast(Any, None),
            cast(Any, None),
            cast(Any, None),
        )

    async def asyncTearDown(self) -> None:
        self.temp.cleanup()

    async def test_expected_installation_is_accepted_in_shadow_mode(self) -> None:
        accepted, reason = await self.orchestrator.webhook(
            "delivery-ok", "workflow_job", webhook_payload(146433491)
        )
        self.assertTrue(accepted)
        self.assertEqual(reason, "shadow_mode")

    async def test_other_installation_is_rejected(self) -> None:
        accepted, reason = await self.orchestrator.webhook(
            "delivery-wrong", "workflow_job", webhook_payload(999)
        )
        self.assertTrue(accepted)
        self.assertEqual(reason, "installation_mismatch")


class FakeGitHub:
    def __init__(self, *, busy: bool = False) -> None:
        self.busy = busy
        self.runners: dict[str, int] = {}
        self._next_id = 10
        self.deleted: list[int] = []

    def register(self, name: str, runner_id: int | None = None) -> int:
        rid = runner_id if runner_id is not None else self._next_id
        self.runners[name] = rid
        self._next_id = max(self._next_id, rid + 1)
        return rid

    async def get_workflow_run(self, *_: Any) -> dict[str, str]:
        return {"event": "workflow_dispatch"}

    async def generate_jit(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
        # Positional: installation, repository, name, labels
        name = _args[2] if len(_args) >= 3 else str(_kwargs.get("name") or "runner")
        self.register(str(name))
        return {"encoded_jit_config": "jit"}

    async def list_runners(self, *_args: Any, **_kwargs: Any) -> list[dict[str, object]]:
        return [
            {
                "id": rid,
                "name": name,
                "status": "online",
                "busy": self.busy,
                "labels": [
                    {"name": "self-hosted"},
                    {"name": "proxmox-lxc"},
                    {"name": "crossbuild"},
                    {"name": "mode-ephemeral"},
                ],
            }
            for name, rid in self.runners.items()
        ]

    async def find_runner(self, *_args: Any, **_kwargs: Any) -> dict[str, object] | None:
        name = str(_args[2] if len(_args) >= 3 else _kwargs.get("name") or "")
        runner_id = self.runners.get(name)
        if runner_id is None:
            return None
        return {
            "id": runner_id,
            "status": "online",
            "busy": self.busy,
            "labels": [
                {"name": "self-hosted"},
                {"name": "proxmox-lxc"},
                {"name": "crossbuild"},
                {"name": "mode-ephemeral"},
            ],
        }

    async def delete_runner(self, *_args: Any, **_kwargs: Any) -> None:
        runner_id = _args[2] if len(_args) >= 3 else _kwargs.get("runner_id")
        self.deleted.append(int(runner_id))
        for name, rid in list(self.runners.items()):
            if rid == int(runner_id):
                del self.runners[name]

    async def try_delete_runner(self, *_args: Any, **_kwargs: Any) -> bool:
        if self.busy:
            return False
        await self.delete_runner(*_args, **_kwargs)
        return True

    @staticmethod
    def runner_url(*_: Any) -> str:
        return "https://github.com/MetalDevops/NinjaCrawler"


class FakeProxmox:
    def __init__(self) -> None:
        self.clones: list[int] = []
        self.clone_calls: list[tuple[Any, ...]] = []
        self.starts: list[int] = []
        self.destroyed: list[int] = []
        self.running: set[int] = set()

    async def cluster_vmids(self) -> set[int]:
        return set()

    async def clone(self, vmid: int, *rest: Any) -> None:
        self.clones.append(vmid)
        self.clone_calls.append((vmid, *rest))
        self.running.add(vmid)

    async def start(self, vmid: int) -> None:
        self.starts.append(vmid)
        self.running.add(vmid)

    async def status(self, vmid: int) -> dict[str, str]:
        return {"status": "running" if vmid in self.running else "stopped"}

    async def destroy(self, vmid: int) -> None:
        self.destroyed.append(vmid)
        self.running.discard(vmid)


class FakeBroker:
    def __init__(self) -> None:
        self.attached: list[int] = []
        self.credentials: list[dict[str, object]] = []
        self.cleaned: list[int] = []

    async def attach_cache(self, vmid: int, _: str) -> None:
        self.attached.append(vmid)

    async def inject(self, _: int, credential: dict[str, object]) -> None:
        self.credentials.append(credential)

    async def clean_work(self, vmid: int) -> str:
        self.cleaned.append(vmid)
        return "cleaned avail_mib=9000"

    async def cache_gc(self, min_age_days: int = 0) -> str:
        return f"gc purged=0 skipped=0 bytes=0 min_age_days={min_age_days}"


class WarmRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        base = settings()
        policy = replace(
            base.repositories["MetalDevops/NinjaCrawler"], warm_capacity=1
        )
        self.settings = replace(
            base,
            repositories={"MetalDevops/NinjaCrawler": policy},
            capacity=replace(base.capacity, max_active=2, pool_size=0),
        )
        self.github = FakeGitHub()
        self.proxmox = FakeProxmox()
        self.broker = FakeBroker()
        self.orchestrator = Orchestrator(
            self.settings,
            self.db,
            cast(Any, self.github),
            cast(Any, self.proxmox),
            cast(Any, self.broker),
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def test_warm_runner_is_registered_online_before_ready(self) -> None:
        policy = self.settings.repositories["MetalDevops/NinjaCrawler"]
        await self.orchestrator._create_warm_runner(
            "MetalDevops/NinjaCrawler", policy
        )
        instances = await self.db.list_instances()
        self.assertEqual(instances[0]["status"], "ready")
        self.assertEqual(self.proxmox.clones, [5000])
        self.assertEqual(self.proxmox.starts, [5000])
        self.assertEqual(self.broker.attached, [5000])
        self.assertEqual(self.broker.credentials[0]["mode"], "warm")
        self.assertEqual(
            self.broker.credentials[0]["labels"],
            ["crossbuild", "mode-ephemeral", "proxmox-lxc", "self-hosted"],
        )

    async def test_warm_runner_claimed_during_registration_is_running(self) -> None:
        self.github.busy = True
        policy = self.settings.repositories["MetalDevops/NinjaCrawler"]

        created = await self.orchestrator._create_warm_runner(
            "MetalDevops/NinjaCrawler", policy
        )

        instances = await self.db.list_instances()
        self.assertTrue(created)
        self.assertEqual(instances[0]["status"], "running")
        self.assertNotEqual(instances[0]["status"], "delete_failed")

    async def test_ephemeral_job_falls_back_to_ready_global_pool(self) -> None:
        await self.db.create_instance(
            5000, "10.20.60.100", "pool", "ready"
        )
        payload = webhook_payload(146433491)
        await self.db.enqueue_job("delivery-pool-fallback", payload, "ephemeral")
        job = await self.db.claim_next_job()

        await self.orchestrator._handle_queued(job)
        if self.orchestrator._background_tasks:
            await asyncio.gather(*self.orchestrator._background_tasks)

        instance = (await self.db.list_instances())[0]
        saved_job = await self.db.get_job(42)
        self.assertEqual(instance["vmid"], 5000)
        self.assertEqual(instance["mode"], "ephemeral")
        self.assertEqual(instance["status"], "running")
        self.assertEqual(instance["job_id"], 42)
        self.assertEqual(saved_job["status"], "waiting_for_runner")
        self.assertEqual(self.proxmox.clones, [])
        self.assertEqual(self.proxmox.starts, [5000])

    async def test_cleanup_does_not_destroy_instance_after_concurrent_claim(self) -> None:
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "delete_pending",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        stale = await self.db.get_instance(instance_id)
        await self.db.transition_instance(
            instance_id, {"delete_pending"}, "running", job_id=42
        )

        await self.orchestrator._destroy_instance(stale)

        current = await self.db.get_instance(instance_id)
        self.assertEqual(current["status"], "running")
        self.assertEqual(current["job_id"], 42)
        self.assertEqual(self.proxmox.destroyed, [])

    async def test_cleanup_is_deferred_while_github_runner_is_busy(self) -> None:
        self.github.busy = True
        self.github.register("pve-lxc-warm-5000")
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "delete_failed",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        instance = await self.db.get_instance(instance_id)

        await self.orchestrator._destroy_instance(instance)

        current = await self.db.get_instance(instance_id)
        # Fresh online+busy still defers destroy.
        self.assertEqual(current["status"], "delete_failed")
        self.assertEqual(self.proxmox.destroyed, [])

    async def test_cleanup_forces_destroy_for_offline_busy_zombie(self) -> None:
        """offline+busy must not block CT teardown forever."""
        self.github.busy = True
        self.github.register("pve-lxc-warm-5000")
        # Force offline status via list/find by patching find return.
        async def offline_busy(*_a: Any, **_k: Any) -> dict[str, object]:
            return {
                "id": 10,
                "status": "offline",
                "busy": True,
                "name": "pve-lxc-warm-5000",
            }

        self.github.find_runner = offline_busy  # type: ignore[method-assign]

        async def refuse_delete(*_a: Any, **_k: Any) -> bool:
            return False

        self.github.try_delete_runner = refuse_delete  # type: ignore[method-assign]
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "delete_pending",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        instance = await self.db.get_instance(instance_id)
        await self.orchestrator._destroy_instance(instance)
        self.assertIsNone(await self.db.get_instance(instance_id))
        self.assertIn(5000, self.proxmox.destroyed)

    async def test_cleanup_aborts_if_idle_online_runner_cannot_be_deleted(self) -> None:
        """Never destroy a CT while GH still has an online idle registration."""
        self.github.register("pve-lxc-warm-5000")

        async def refuse_delete(*_a: Any, **_k: Any) -> bool:
            return False

        self.github.try_delete_runner = refuse_delete  # type: ignore[method-assign]
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "delete_pending",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        instance = await self.db.get_instance(instance_id)
        await self.orchestrator._destroy_instance(instance)
        current = await self.db.get_instance(instance_id)
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current["status"], "delete_failed")
        self.assertIn("github_runner_delete_failed", str(current.get("error") or ""))
        self.assertEqual(self.proxmox.destroyed, [])

    async def test_in_progress_does_not_clobber_warm_name_on_mismatch(self) -> None:
        """GH assigned 5011 must not rewrite reserved warm-5004's runner_name."""
        payload = webhook_payload(146433491)
        await self.db.enqueue_job("delivery-queued-mismatch", payload, "ephemeral")
        # Pre-reserved wrong CT (historical pre-claim race).
        instance_id = await self.db.create_instance(
            5004,
            "10.20.60.104",
            "warm",
            "reserved",
            "MetalDevops/NinjaCrawler",
            job_id=42,
            runner_name="pve-lxc-warm-5004",
        )
        payload["action"] = "in_progress"
        payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5011"

        accepted, reason = await self.orchestrator.webhook(
            "delivery-in-progress-mismatch", "workflow_job", payload
        )

        instance = await self.db.get_instance(instance_id)
        job = await self.db.get_job(42)
        self.assertTrue(accepted)
        self.assertEqual(reason, "accepted")
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["runner_name"], "pve-lxc-warm-5011")
        # Reserved CT released, name NOT clobbered to 5011.
        assert instance is not None
        self.assertEqual(instance["status"], "ready")
        self.assertIsNone(instance["job_id"])
        self.assertEqual(instance["runner_name"], "pve-lxc-warm-5004")

    async def test_reconcile_running_releases_runner_name_mismatch(self) -> None:
        await self.db.enqueue_job(
            "delivery-mismatch-running",
            {
                "action": "queued",
                "installation": {"id": 146433491},
                "repository": {"full_name": "MetalDevops/NinjaCrawler"},
                "workflow_job": {
                    "id": 99,
                    "run_id": 1,
                    "labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
                },
            },
            "ephemeral",
        )
        await self.db.set_job_status(99, "running", runner_name="pve-lxc-warm-5011")
        instance_id = await self.db.create_instance(
            5004,
            "10.20.60.104",
            "warm",
            "running",
            "MetalDevops/NinjaCrawler",
            job_id=99,
            runner_name="pve-lxc-warm-5011",
        )
        instance = await self.db.get_instance(instance_id)
        assert instance is not None
        await self.orchestrator._reconcile_running_instance(
            instance, now=int(__import__("time").time()), age=10
        )
        current = await self.db.get_instance(instance_id)
        assert current is not None
        self.assertEqual(current["status"], "delete_pending")
        job = await self.db.get_job(99)
        assert job is not None
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["error"], "runner_name_vmid_mismatch")

    async def test_in_progress_event_claims_provisioning_warm(self) -> None:
        payload = webhook_payload(146433491)
        await self.db.enqueue_job("delivery-queued", payload, "ephemeral")
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "provisioning",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        payload["action"] = "in_progress"
        payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"

        accepted, reason = await self.orchestrator.webhook(
            "delivery-in-progress", "workflow_job", payload
        )

        instance = await self.db.get_instance(instance_id)
        job = await self.db.get_job(42)
        self.assertTrue(accepted)
        self.assertEqual(reason, "accepted")
        self.assertEqual(instance["status"], "running")
        self.assertEqual(instance["job_id"], 42)
        self.assertEqual(job["status"], "running")

    async def test_completed_event_recycles_warm_when_queued_event_was_lost(self) -> None:
        self.proxmox.running.add(5000)
        self.github.register("pve-lxc-warm-5000")
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "running",
            "MetalDevops/NinjaCrawler",
            job_id=42,
            runner_name="pve-lxc-warm-5000",
        )
        payload = webhook_payload(146433491)
        payload["action"] = "completed"
        payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"
        payload["workflow_job"]["conclusion"] = "success"

        accepted, reason = await self.orchestrator.webhook(
            "delivery-completed-without-queued", "workflow_job", payload
        )
        if self.orchestrator._background_tasks:
            await asyncio.gather(*self.orchestrator._background_tasks)

        instance = await self.db.get_instance(instance_id)
        self.assertTrue(accepted)
        self.assertEqual(reason, "accepted")
        self.assertIsNone(await self.db.get_job(42))
        # Warm is re-JITed on the same CT; no Proxmox destroy/clone.
        self.assertEqual(instance["status"], "ready")
        self.assertIsNone(instance["job_id"])
        self.assertEqual(int(instance.get("use_count") or 0), 1)
        self.assertEqual(self.proxmox.destroyed, [])
        self.assertEqual(self.proxmox.clones, [])
        self.assertIn(5000, self.broker.cleaned)
        self.assertTrue(self.broker.credentials)

    async def test_failed_job_destroys_warm_instead_of_recycle(self) -> None:
        self.proxmox.running.add(5000)
        self.github.register("pve-lxc-warm-5000")
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "running",
            "MetalDevops/NinjaCrawler",
            job_id=42,
            runner_name="pve-lxc-warm-5000",
        )
        payload = webhook_payload(146433491)
        payload["action"] = "completed"
        payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"
        payload["workflow_job"]["conclusion"] = "failure"

        accepted, reason = await self.orchestrator.webhook(
            "delivery-failed-job", "workflow_job", payload
        )
        if self.orchestrator._background_tasks:
            await asyncio.gather(*self.orchestrator._background_tasks)

        instance = await self.db.get_instance(instance_id)
        self.assertTrue(accepted)
        self.assertEqual(reason, "accepted")
        # Failure refuses recycle and schedules async destroy via delete_pending.
        self.assertIsNotNone(instance)
        self.assertEqual(instance["status"], "delete_pending")
        self.assertEqual(self.broker.cleaned, [])
        self.assertEqual(self.proxmox.destroyed, [])

    async def test_late_completed_event_does_not_clean_reused_warm_runner(self) -> None:
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "running",
            "MetalDevops/NinjaCrawler",
            job_id=99,
            runner_name="pve-lxc-warm-5000",
        )
        payload = webhook_payload(146433491)
        payload["action"] = "completed"
        payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"

        await self.orchestrator.webhook(
            "delivery-late-completed", "workflow_job", payload
        )

        instance = await self.db.get_instance(instance_id)
        self.assertEqual(instance["status"], "running")
        self.assertEqual(instance["job_id"], 99)

    async def test_reconcile_retires_orphan_warm_after_repo_rename(self) -> None:
        """Warms for repos no longer in policy must free max_active capacity."""
        await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "ready",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        await self.db.create_instance(
            5002,
            "10.20.60.102",
            "warm",
            "ready",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5002",
        )
        # Simulate policy rename: only JustShinobi remains.
        from dataclasses import replace as dc_replace

        new_policy = dc_replace(
            self.settings.repositories["MetalDevops/NinjaCrawler"],
            warm_capacity=2,
        )
        self.orchestrator.settings = dc_replace(
            self.settings,
            repositories={"JustShinobi/NinjaCrawler": new_policy},
        )
        self.github.register("pve-lxc-warm-5000")
        self.github.register("pve-lxc-warm-5002")

        await self.orchestrator.reconcile()

        # Old MetalDevops warms destroyed; new JustShinobi warms replenished.
        self.assertIn(5000, self.proxmox.destroyed)
        self.assertIn(5002, self.proxmox.destroyed)
        remaining = await self.db.list_instances()
        for row in remaining:
            if row["mode"] == "warm":
                self.assertEqual(row["repository"], "JustShinobi/NinjaCrawler")
                self.assertIn(row["status"], {"ready", "provisioning", "running"})

    async def test_queued_job_waits_for_warm_without_preclaim(self) -> None:
        """Do not bind a specific warm on queued — GitHub picks among equal labels."""
        policy = self.settings.repositories["MetalDevops/NinjaCrawler"]
        await self.orchestrator._create_warm_runner(
            "MetalDevops/NinjaCrawler", policy
        )
        payload = webhook_payload(146433491)
        await self.db.enqueue_job("delivery-warm", payload, "ephemeral")
        job = await self.db.claim_next_job()
        await self.orchestrator._handle_queued(job)
        if self.orchestrator._background_tasks:
            await asyncio.gather(*self.orchestrator._background_tasks)

        saved_job = await self.db.get_job(42)
        instances = await self.db.list_instances()
        ready = [i for i in instances if i["status"] == "ready" and i["mode"] == "warm"]
        self.assertEqual(saved_job["status"], "waiting_for_runner")
        self.assertIsNone(saved_job.get("instance_id") or saved_job.get("runner_name"))
        self.assertEqual(len(ready), 1)
        self.assertIsNone(ready[0]["job_id"])


class DestroyBackoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        base = settings()
        # No warm/pool replenishment noise — these tests only care about the
        # delete_pending/delete_failed retry path.
        self.settings = replace(
            base,
            capacity=replace(base.capacity, pool_size=0),
            repositories={
                "MetalDevops/NinjaCrawler": replace(
                    base.repositories["MetalDevops/NinjaCrawler"], warm_capacity=0
                )
            },
        )
        self.github = FakeGitHub()
        self.proxmox = FakeProxmox()
        self.broker = FakeBroker()
        self.orchestrator = Orchestrator(
            self.settings,
            self.db,
            cast(Any, self.github),
            cast(Any, self.proxmox),
            cast(Any, self.broker),
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def test_destroy_failure_increments_delete_attempts(self) -> None:
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "delete_pending",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        instance = await self.db.get_instance(instance_id)

        async def failing_destroy(_vmid: int) -> None:
            raise RuntimeError("proxmox unavailable")

        self.proxmox.destroy = failing_destroy  # type: ignore[method-assign]

        await self.orchestrator._destroy_instance(instance)

        current = await self.db.get_instance(instance_id)
        self.assertEqual(current["status"], "delete_failed")
        self.assertEqual(current["delete_attempts"], 1)

    async def test_reconcile_backs_off_before_retrying_failed_destroy(self) -> None:
        instance_id = await self.db.create_instance(
            5000,
            "10.20.60.100",
            "warm",
            "delete_failed",
            "MetalDevops/NinjaCrawler",
            runner_name="pve-lxc-warm-5000",
        )
        await self.db.transition_instance(
            instance_id, {"delete_failed"}, "delete_failed", delete_attempts=1
        )

        # backoff(1) = 120s and updated_at was just set — reconcile must not retry yet.
        await self.orchestrator.reconcile()
        self.assertEqual(self.proxmox.destroyed, [])

        async with self.db.connect() as conn:
            await conn.execute("UPDATE instances SET updated_at=0 WHERE id=?", (instance_id,))
            await conn.commit()

        await self.orchestrator.reconcile()
        self.assertIn(5000, self.proxmox.destroyed)


class JobRetryBackoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        self.github = FakeGitHub()
        self.orchestrator = Orchestrator(
            settings(), self.db, cast(Any, self.github), cast(Any, None), cast(Any, None)
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def test_backoff_grows_between_requeue_attempts_then_fails_terminally(self) -> None:
        payload = webhook_payload(146433491)
        await self.db.enqueue_job("delivery-retry-backoff", payload, "ephemeral")
        failed_before = JOBS_FAILED.labels("ephemeral")._value.get()

        async def failing_provision(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("boom")

        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)

        with mock.patch.object(Orchestrator, "_provision", failing_provision), mock.patch(
            "orchestrator.service.asyncio.sleep", fake_sleep
        ):
            for _ in range(3):
                job = await self.db.claim_next_job()
                await self.orchestrator._handle_queued(job)

        self.assertEqual(sleeps, [5, 10])
        final_job = await self.db.get_job(42)
        self.assertEqual(final_job["status"], "failed")
        # The terminal-failure metric (backs the RunnerOrchestratorJobsFailedTerminal
        # Prometheus alert) fires exactly once, only on the final exhausted attempt.
        self.assertEqual(JOBS_FAILED.labels("ephemeral")._value.get(), failed_before + 1)


class RepoCapacityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        base = settings()
        # Global max_active is generous — only the per-repo ceiling should bind.
        self.settings = replace(
            base,
            capacity=replace(base.capacity, max_active=5, pool_size=0),
            repositories={
                "MetalDevops/NinjaCrawler": replace(
                    base.repositories["MetalDevops/NinjaCrawler"], max_concurrent=1
                )
            },
        )
        self.github = FakeGitHub()
        self.proxmox = FakeProxmox()
        self.broker = FakeBroker()
        self.orchestrator = Orchestrator(
            self.settings,
            self.db,
            cast(Any, self.github),
            cast(Any, self.proxmox),
            cast(Any, self.broker),
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def test_second_job_for_same_repo_is_throttled(self) -> None:
        def payload_for(job_id: int) -> dict[str, Any]:
            data = webhook_payload(146433491)
            data["workflow_job"]["id"] = job_id
            return data

        await self.db.enqueue_job("delivery-repo-cap-1", payload_for(101), "ephemeral")
        await self.db.enqueue_job("delivery-repo-cap-2", payload_for(102), "ephemeral")

        with mock.patch("orchestrator.service.asyncio.sleep", mock.AsyncMock()):
            job1 = await self.db.claim_next_job()
            await self.orchestrator._handle_queued(job1)

            job2 = await self.db.claim_next_job()
            await self.orchestrator._handle_queued(job2)

        job1_row = await self.db.get_job(101)
        job2_row = await self.db.get_job(102)
        self.assertEqual(job1_row["status"], "waiting_for_runner")
        self.assertEqual(job2_row["status"], "queued")
        self.assertEqual(job2_row["error"], "repo_capacity_exhausted")


class PoolCapacityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        base = settings()
        self.settings = replace(base, capacity=replace(base.capacity, max_active=1, pool_size=2))
        self.github = FakeGitHub()
        self.proxmox = FakeProxmox()
        self.broker = FakeBroker()
        self.orchestrator = Orchestrator(
            self.settings,
            self.db,
            cast(Any, self.github),
            cast(Any, self.proxmox),
            cast(Any, self.broker),
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def test_pool_replenishment_respects_already_saturated_active_budget(self) -> None:
        # Occupy the single max_active slot with a running ephemeral instance first.
        await self.db.create_instance(
            5000, "10.20.60.100", "ephemeral", "running", "MetalDevops/NinjaCrawler", 1
        )

        await self.orchestrator._replenish_pool()

        instances = await self.db.list_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]["vmid"], 5000)


class WarmIdleScalingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        base = settings()
        policy = replace(
            base.repositories["MetalDevops/NinjaCrawler"],
            warm_capacity=4,
            warm_idle_capacity=1,
            warm_idle_after_seconds=3600,
        )
        self.settings = replace(
            base,
            repositories={"MetalDevops/NinjaCrawler": policy},
            capacity=replace(base.capacity, max_active=5, pool_size=0),
        )
        self.github = FakeGitHub()
        self.proxmox = FakeProxmox()
        self.broker = FakeBroker()
        self.orchestrator = Orchestrator(
            self.settings,
            self.db,
            cast(Any, self.github),
            cast(Any, self.proxmox),
            cast(Any, self.broker),
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def _seed_ready_warms(self, count: int) -> None:
        for i in range(count):
            vmid = 5000 + i
            await self.db.create_instance(
                vmid,
                f"10.20.60.{100 + i}",
                "warm",
                "ready",
                "MetalDevops/NinjaCrawler",
                runner_name=f"pve-lxc-warm-{vmid}",
            )

    async def _record_job_activity(self, job_id: int, *, created_at: int | None = None) -> None:
        data = webhook_payload(146433491)
        data["workflow_job"]["id"] = job_id
        await self.db.enqueue_job(f"delivery-activity-{job_id}", data, "ephemeral")
        if created_at is not None:
            async with self.db.connect() as conn:
                await conn.execute(
                    "UPDATE jobs SET created_at=? WHERE job_id=?", (created_at, job_id)
                )
                await conn.commit()

    async def test_scales_down_after_idle_period(self) -> None:
        await self._seed_ready_warms(4)
        await self._record_job_activity(201, created_at=0)  # far in the past -> idle

        await self.orchestrator._replenish_warm_pool()

        instances = await self.db.list_instances()
        self.assertEqual(
            sum(1 for i in instances if i["status"] == "delete_pending"), 3
        )
        self.assertEqual(sum(1 for i in instances if i["status"] == "ready"), 1)

    async def test_stays_at_full_capacity_within_idle_window(self) -> None:
        await self._seed_ready_warms(4)
        await self._record_job_activity(202)  # created_at defaults to now

        await self.orchestrator._replenish_warm_pool()

        instances = await self.db.list_instances()
        self.assertEqual(sum(1 for i in instances if i["status"] == "delete_pending"), 0)
        self.assertEqual(sum(1 for i in instances if i["status"] == "ready"), 4)

    async def test_scales_back_up_after_new_activity(self) -> None:
        # Start already scaled down to the floor, as if idle for a while.
        await self._seed_ready_warms(1)
        await self._record_job_activity(203, created_at=0)

        await self.orchestrator._replenish_warm_pool()
        self.assertEqual(len(await self.db.list_instances()), 1)  # still idle, no change

        # A fresh job request resets the idle clock.
        await self._record_job_activity(204)
        await self.orchestrator._replenish_warm_pool()

        warm_rows = [i for i in await self.db.list_instances() if i["mode"] == "warm"]
        self.assertEqual(len(warm_rows), 4)
        self.assertTrue(all(row["status"] in {"ready", "running"} for row in warm_rows))

    async def test_idle_scaling_disabled_by_default_keeps_fixed_capacity(self) -> None:
        always_on_policy = replace(
            self.settings.repositories["MetalDevops/NinjaCrawler"],
            warm_idle_capacity=-1,
            warm_idle_after_seconds=0,
        )
        self.orchestrator.settings = replace(
            self.settings, repositories={"MetalDevops/NinjaCrawler": always_on_policy}
        )
        await self._seed_ready_warms(4)
        await self._record_job_activity(205, created_at=0)  # would be "idle" if enabled

        await self.orchestrator._replenish_warm_pool()

        instances = await self.db.list_instances()
        self.assertEqual(sum(1 for i in instances if i["status"] == "delete_pending"), 0)
        self.assertEqual(sum(1 for i in instances if i["status"] == "ready"), 4)


class RunnerProfileSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()
        base = settings()
        self.large_profile = RunnerProfile(cores=12, memory_mb=16384, swap_mb=4096, rootfs_gb=60)
        policy = replace(
            base.repositories["MetalDevops/NinjaCrawler"],
            profile="crossbuild-large",
            warm_capacity=1,
        )
        self.settings = replace(
            base,
            repositories={"MetalDevops/NinjaCrawler": policy},
            profiles={**base.profiles, "crossbuild-large": self.large_profile},
            capacity=replace(base.capacity, max_active=5, pool_size=1),
        )
        self.github = FakeGitHub()
        self.proxmox = FakeProxmox()
        self.broker = FakeBroker()
        self.orchestrator = Orchestrator(
            self.settings,
            self.db,
            cast(Any, self.github),
            cast(Any, self.proxmox),
            cast(Any, self.broker),
        )

    async def asyncTearDown(self) -> None:
        await self.orchestrator.stop()
        self.temp.cleanup()

    async def test_ephemeral_provisioning_uses_repo_profile(self) -> None:
        payload = webhook_payload(146433491)
        await self.db.enqueue_job("delivery-profile", payload, "ephemeral")
        job = await self.db.claim_next_job()

        await self.orchestrator._handle_queued(job)

        self.assertEqual(self.proxmox.clone_calls[-1][3], self.large_profile)

    async def test_warm_creation_uses_repo_profile(self) -> None:
        policy = self.settings.repositories["MetalDevops/NinjaCrawler"]
        await self.orchestrator._create_warm_runner("MetalDevops/NinjaCrawler", policy)

        self.assertEqual(self.proxmox.clone_calls[-1][3], self.large_profile)

    async def test_pool_creation_always_uses_default_profile(self) -> None:
        await self.orchestrator._replenish_pool()

        self.assertTrue(self.proxmox.clone_calls)
        default_profile = self.settings.profiles["crossbuild-small"]
        for call in self.proxmox.clone_calls:
            self.assertEqual(call[3], default_profile)

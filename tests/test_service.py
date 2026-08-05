from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest import mock

import pytest

from orchestrator.config import RunnerProfile
from orchestrator.db import Database
from orchestrator.service import JOBS_FAILED, Orchestrator, _events_persist_path
from tests.fakes import FakeBroker, FakeGitHub, FakeProxmox
from tests.helpers import settings as make_settings


def webhook_payload(installation_id: int) -> dict[str, Any]:
    return {
        "action": "queued",
        "installation": {"id": installation_id},
        "repository": {"full_name": "OldOwner/example-app"},
        "workflow_job": {
            "id": 42,
            "run_id": 7,
            "labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
        },
    }


def test_memory_database_disables_persistence() -> None:
    assert _events_persist_path(":memory:") is None


def test_file_database_gets_sibling_events_file() -> None:
    path = _events_persist_path("/var/lib/runner-orchestrator/state.db")
    assert path is not None
    assert path.name == "events.jsonl"
    assert path.parent == Path("/var/lib/runner-orchestrator")


# --- VMID/IP allocation -----------------------------------------------------


@pytest.fixture
async def vmid_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    custom_settings = replace(
        base,
        proxmox=replace(base.proxmox, vmid_start=5000, vmid_end=5001, ip_start=100, ip_end=101),
    )
    orch = Orchestrator(
        custom_settings, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop()


async def test_allocates_first_free_vmid_in_range(
    vmid_orchestrator: Orchestrator, db: Database
) -> None:
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")

    vmid, ip_address = await vmid_orchestrator._allocate()

    assert vmid == 5001
    assert ip_address == "10.0.0.101"


async def test_raises_when_range_is_exhausted(
    vmid_orchestrator: Orchestrator, db: Database
) -> None:
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")
    await db.create_instance(5001, "10.0.0.101", "warm", "ready", "OldOwner/example-app")

    with pytest.raises(RuntimeError, match="VMID/IP range exhausted"):
        await vmid_orchestrator._allocate()


async def test_reserve_instance_surfaces_exhaustion_to_caller(
    vmid_orchestrator: Orchestrator, db: Database
) -> None:
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")
    await db.create_instance(5001, "10.0.0.101", "warm", "ready", "OldOwner/example-app")

    with pytest.raises(RuntimeError):
        await vmid_orchestrator._reserve_instance(
            "ephemeral", "OldOwner/example-app", 1, "runner-1", enforce_capacity=False
        )


async def test_accounts_for_vmids_live_on_proxmox_but_not_in_db(
    vmid_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    """A VMID that exists on Proxmox (e.g. manually created) but isn't
    tracked in our DB yet must still not be double-allocated."""

    async def fake_cluster_vmids() -> set[int]:
        return {5000}

    fake_proxmox.cluster_vmids = fake_cluster_vmids  # type: ignore[method-assign]

    vmid, ip_address = await vmid_orchestrator._allocate()

    assert vmid == 5001
    assert ip_address == "10.0.0.101"


# --- Webhook installation matching ------------------------------------------


@pytest.fixture
async def webhook_orchestrator(db: Database):
    orch = Orchestrator(
        make_settings(shadow_mode=True), db, cast(Any, None), cast(Any, None), cast(Any, None)
    )
    yield orch
    await orch.stop()


async def test_expected_installation_is_accepted_in_shadow_mode(
    webhook_orchestrator: Orchestrator,
) -> None:
    accepted, reason = await webhook_orchestrator.webhook(
        "delivery-ok", "workflow_job", webhook_payload(1234567)
    )
    assert accepted
    assert reason == "shadow_mode"


async def test_other_installation_is_rejected(webhook_orchestrator: Orchestrator) -> None:
    accepted, reason = await webhook_orchestrator.webhook(
        "delivery-wrong", "workflow_job", webhook_payload(999)
    )
    assert accepted
    assert reason == "installation_mismatch"


# --- Warm runner lifecycle ---------------------------------------------------


@pytest.fixture
async def warm_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    policy = replace(base.repositories["OldOwner/example-app"], warm_capacity=1)
    custom_settings = replace(
        base,
        repositories={"OldOwner/example-app": policy},
        capacity=replace(base.capacity, max_active=2, pool_size=0),
    )
    orch = Orchestrator(
        custom_settings, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop()


async def test_warm_runner_is_registered_online_before_ready(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_proxmox: FakeProxmox,
    fake_broker: FakeBroker,
) -> None:
    policy = warm_orchestrator.settings.repositories["OldOwner/example-app"]
    await warm_orchestrator._create_warm_runner("OldOwner/example-app", policy)
    instances = await db.list_instances()
    assert instances[0]["status"] == "ready"
    assert fake_proxmox.clones == [5000]
    assert fake_proxmox.starts == [5000]
    assert fake_broker.attached == [5000]
    assert fake_broker.credentials[0]["mode"] == "warm"
    assert fake_broker.credentials[0]["labels"] == [
        "crossbuild",
        "mode-ephemeral",
        "proxmox-lxc",
        "self-hosted",
    ]


async def test_warm_runner_claimed_during_registration_is_running(
    warm_orchestrator: Orchestrator, db: Database, fake_github: FakeGitHub
) -> None:
    fake_github.busy = True
    policy = warm_orchestrator.settings.repositories["OldOwner/example-app"]

    created = await warm_orchestrator._create_warm_runner("OldOwner/example-app", policy)

    instances = await db.list_instances()
    assert created
    assert instances[0]["status"] == "running"
    assert instances[0]["status"] != "delete_failed"


async def test_ephemeral_job_falls_back_to_ready_global_pool(
    warm_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    payload = webhook_payload(1234567)
    await db.enqueue_job("delivery-pool-fallback", payload, "ephemeral")
    job = await db.claim_next_job()

    await warm_orchestrator._handle_queued(job)
    if warm_orchestrator._background_tasks:
        await asyncio.gather(*warm_orchestrator._background_tasks)

    instance = (await db.list_instances())[0]
    saved_job = await db.get_job(42)
    assert instance["vmid"] == 5000
    assert instance["mode"] == "ephemeral"
    assert instance["status"] == "running"
    assert instance["job_id"] == 42
    assert saved_job["status"] == "waiting_for_runner"
    assert fake_proxmox.clones == []
    assert fake_proxmox.starts == [5000]


async def test_cleanup_does_not_destroy_instance_after_concurrent_claim(
    warm_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    stale = await db.get_instance(instance_id)
    await db.transition_instance(instance_id, {"delete_pending"}, "running", job_id=42)

    await warm_orchestrator._destroy_instance(stale)

    current = await db.get_instance(instance_id)
    assert current["status"] == "running"
    assert current["job_id"] == 42
    assert fake_proxmox.destroyed == []


async def test_cleanup_is_deferred_while_github_runner_is_busy(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
) -> None:
    fake_github.busy = True
    fake_github.register("pve-lxc-warm-5000")
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_failed",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    instance = await db.get_instance(instance_id)

    await warm_orchestrator._destroy_instance(instance)

    current = await db.get_instance(instance_id)
    # Fresh online+busy still defers destroy.
    assert current["status"] == "delete_failed"
    assert fake_proxmox.destroyed == []


async def test_cleanup_forces_destroy_for_offline_busy_zombie(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
) -> None:
    """offline+busy must not block CT teardown forever."""
    fake_github.busy = True
    fake_github.register("pve-lxc-warm-5000")

    # Force offline status via list/find by patching find return.
    async def offline_busy(*_a: Any, **_k: Any) -> dict[str, object]:
        return {
            "id": 10,
            "status": "offline",
            "busy": True,
            "name": "pve-lxc-warm-5000",
        }

    fake_github.find_runner = offline_busy  # type: ignore[method-assign]

    async def refuse_delete(*_a: Any, **_k: Any) -> bool:
        return False

    fake_github.try_delete_runner = refuse_delete  # type: ignore[method-assign]
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    instance = await db.get_instance(instance_id)
    await warm_orchestrator._destroy_instance(instance)
    assert await db.get_instance(instance_id) is None
    assert 5000 in fake_proxmox.destroyed


async def test_cleanup_aborts_if_idle_online_runner_cannot_be_deleted(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
) -> None:
    """Never destroy a CT while GH still has an online idle registration."""
    fake_github.register("pve-lxc-warm-5000")

    async def refuse_delete(*_a: Any, **_k: Any) -> bool:
        return False

    fake_github.try_delete_runner = refuse_delete  # type: ignore[method-assign]
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    instance = await db.get_instance(instance_id)
    await warm_orchestrator._destroy_instance(instance)
    current = await db.get_instance(instance_id)
    assert current is not None
    assert current["status"] == "delete_failed"
    assert "github_runner_delete_failed" in str(current.get("error") or "")
    assert fake_proxmox.destroyed == []


async def test_in_progress_does_not_clobber_warm_name_on_mismatch(
    warm_orchestrator: Orchestrator, db: Database
) -> None:
    """GH assigned 5011 must not rewrite reserved warm-5004's runner_name."""
    payload = webhook_payload(1234567)
    await db.enqueue_job("delivery-queued-mismatch", payload, "ephemeral")
    # Pre-reserved wrong CT (historical pre-claim race).
    instance_id = await db.create_instance(
        5004,
        "10.0.0.104",
        "warm",
        "reserved",
        "OldOwner/example-app",
        job_id=42,
        runner_name="pve-lxc-warm-5004",
    )
    payload["action"] = "in_progress"
    payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5011"

    accepted, reason = await warm_orchestrator.webhook(
        "delivery-in-progress-mismatch", "workflow_job", payload
    )

    instance = await db.get_instance(instance_id)
    job = await db.get_job(42)
    assert accepted
    assert reason == "accepted"
    assert job["status"] == "running"
    assert job["runner_name"] == "pve-lxc-warm-5011"
    # Reserved CT released, name NOT clobbered to 5011.
    assert instance is not None
    assert instance["status"] == "ready"
    assert instance["job_id"] is None
    assert instance["runner_name"] == "pve-lxc-warm-5004"


async def test_reconcile_running_releases_runner_name_mismatch(
    warm_orchestrator: Orchestrator, db: Database
) -> None:
    await db.enqueue_job(
        "delivery-mismatch-running",
        {
            "action": "queued",
            "installation": {"id": 1234567},
            "repository": {"full_name": "OldOwner/example-app"},
            "workflow_job": {
                "id": 99,
                "run_id": 1,
                "labels": ["self-hosted", "proxmox-lxc", "crossbuild"],
            },
        },
        "ephemeral",
    )
    await db.set_job_status(99, "running", runner_name="pve-lxc-warm-5011")
    instance_id = await db.create_instance(
        5004,
        "10.0.0.104",
        "warm",
        "running",
        "OldOwner/example-app",
        job_id=99,
        runner_name="pve-lxc-warm-5011",
    )
    instance = await db.get_instance(instance_id)
    assert instance is not None
    await warm_orchestrator._reconcile_running_instance(instance, now=int(time.time()), age=10)
    current = await db.get_instance(instance_id)
    assert current is not None
    assert current["status"] == "delete_pending"
    job = await db.get_job(99)
    assert job is not None
    assert job["status"] == "failed"
    assert job["error"] == "runner_name_vmid_mismatch"


async def test_in_progress_event_claims_provisioning_warm(
    warm_orchestrator: Orchestrator, db: Database
) -> None:
    payload = webhook_payload(1234567)
    await db.enqueue_job("delivery-queued", payload, "ephemeral")
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "provisioning",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    payload["action"] = "in_progress"
    payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"

    accepted, reason = await warm_orchestrator.webhook(
        "delivery-in-progress", "workflow_job", payload
    )

    instance = await db.get_instance(instance_id)
    job = await db.get_job(42)
    assert accepted
    assert reason == "accepted"
    assert instance["status"] == "running"
    assert instance["job_id"] == 42
    assert job["status"] == "running"


async def test_completed_event_recycles_warm_when_queued_event_was_lost(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
    fake_broker: FakeBroker,
) -> None:
    fake_proxmox.running.add(5000)
    fake_github.register("pve-lxc-warm-5000")
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "running",
        "OldOwner/example-app",
        job_id=42,
        runner_name="pve-lxc-warm-5000",
    )
    payload = webhook_payload(1234567)
    payload["action"] = "completed"
    payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"
    payload["workflow_job"]["conclusion"] = "success"

    accepted, reason = await warm_orchestrator.webhook(
        "delivery-completed-without-queued", "workflow_job", payload
    )
    if warm_orchestrator._background_tasks:
        await asyncio.gather(*warm_orchestrator._background_tasks)

    instance = await db.get_instance(instance_id)
    assert accepted
    assert reason == "accepted"
    assert await db.get_job(42) is None
    # Warm is re-JITed on the same CT; no Proxmox destroy/clone.
    assert instance["status"] == "ready"
    assert instance["job_id"] is None
    assert int(instance.get("use_count") or 0) == 1
    assert fake_proxmox.destroyed == []
    assert fake_proxmox.clones == []
    assert 5000 in fake_broker.cleaned
    assert fake_broker.credentials


async def test_failed_job_destroys_warm_instead_of_recycle(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
    fake_broker: FakeBroker,
) -> None:
    fake_proxmox.running.add(5000)
    fake_github.register("pve-lxc-warm-5000")
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "running",
        "OldOwner/example-app",
        job_id=42,
        runner_name="pve-lxc-warm-5000",
    )
    payload = webhook_payload(1234567)
    payload["action"] = "completed"
    payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"
    payload["workflow_job"]["conclusion"] = "failure"

    accepted, reason = await warm_orchestrator.webhook(
        "delivery-failed-job", "workflow_job", payload
    )
    if warm_orchestrator._background_tasks:
        await asyncio.gather(*warm_orchestrator._background_tasks)

    instance = await db.get_instance(instance_id)
    assert accepted
    assert reason == "accepted"
    # Failure refuses recycle and schedules async destroy via delete_pending.
    assert instance is not None
    assert instance["status"] == "delete_pending"
    assert fake_broker.cleaned == []
    assert fake_proxmox.destroyed == []


async def test_late_completed_event_does_not_clean_reused_warm_runner(
    warm_orchestrator: Orchestrator, db: Database
) -> None:
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "running",
        "OldOwner/example-app",
        job_id=99,
        runner_name="pve-lxc-warm-5000",
    )
    payload = webhook_payload(1234567)
    payload["action"] = "completed"
    payload["workflow_job"]["runner_name"] = "pve-lxc-warm-5000"

    await warm_orchestrator.webhook("delivery-late-completed", "workflow_job", payload)

    instance = await db.get_instance(instance_id)
    assert instance["status"] == "running"
    assert instance["job_id"] == 99


async def test_reconcile_retires_orphan_warm_after_repo_rename(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
) -> None:
    """Warms for repos no longer in policy must free max_active capacity."""
    await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "ready",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    await db.create_instance(
        5002,
        "10.0.0.102",
        "warm",
        "ready",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5002",
    )
    # Simulate policy rename: only acme remains.
    new_policy = replace(
        warm_orchestrator.settings.repositories["OldOwner/example-app"], warm_capacity=2
    )
    warm_orchestrator.settings = replace(
        warm_orchestrator.settings,
        repositories={"acme/example-app": new_policy},
    )
    fake_github.register("pve-lxc-warm-5000")
    fake_github.register("pve-lxc-warm-5002")

    await warm_orchestrator.reconcile()

    # Old OldOwner warms destroyed; new acme warms replenished.
    assert 5000 in fake_proxmox.destroyed
    assert 5002 in fake_proxmox.destroyed
    remaining = await db.list_instances()
    for row in remaining:
        if row["mode"] == "warm":
            assert row["repository"] == "acme/example-app"
            assert row["status"] in {"ready", "provisioning", "running"}


async def test_queued_job_waits_for_warm_without_preclaim(
    warm_orchestrator: Orchestrator, db: Database
) -> None:
    """Do not bind a specific warm on queued — GitHub picks among equal labels."""
    policy = warm_orchestrator.settings.repositories["OldOwner/example-app"]
    await warm_orchestrator._create_warm_runner("OldOwner/example-app", policy)
    payload = webhook_payload(1234567)
    await db.enqueue_job("delivery-warm", payload, "ephemeral")
    job = await db.claim_next_job()
    await warm_orchestrator._handle_queued(job)
    if warm_orchestrator._background_tasks:
        await asyncio.gather(*warm_orchestrator._background_tasks)

    saved_job = await db.get_job(42)
    instances = await db.list_instances()
    ready = [i for i in instances if i["status"] == "ready" and i["mode"] == "warm"]
    assert saved_job["status"] == "waiting_for_runner"
    assert not (saved_job.get("instance_id") or saved_job.get("runner_name"))
    assert len(ready) == 1
    assert ready[0]["job_id"] is None


# --- Destroy backoff ----------------------------------------------------------


@pytest.fixture
async def destroy_backoff_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    # No warm/pool replenishment noise — these tests only care about the
    # delete_pending/delete_failed retry path.
    custom_settings = replace(
        base,
        capacity=replace(base.capacity, pool_size=0),
        repositories={
            "OldOwner/example-app": replace(
                base.repositories["OldOwner/example-app"], warm_capacity=0
            )
        },
    )
    orch = Orchestrator(
        custom_settings, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop()


async def test_destroy_failure_increments_delete_attempts(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    instance = await db.get_instance(instance_id)

    async def failing_destroy(_vmid: int) -> None:
        raise RuntimeError("proxmox unavailable")

    fake_proxmox.destroy = failing_destroy  # type: ignore[method-assign]

    await destroy_backoff_orchestrator._destroy_instance(instance)

    current = await db.get_instance(instance_id)
    assert current["status"] == "delete_failed"
    assert current["delete_attempts"] == 1


async def test_reconcile_backs_off_before_retrying_failed_destroy(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_failed",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    await db.transition_instance(instance_id, {"delete_failed"}, "delete_failed", delete_attempts=1)

    # backoff(1) = 120s and updated_at was just set — reconcile must not retry yet.
    await destroy_backoff_orchestrator.reconcile()
    assert fake_proxmox.destroyed == []

    async with db.connect() as conn:
        await conn.execute("UPDATE instances SET updated_at=0 WHERE id=?", (instance_id,))
        await conn.commit()

    await destroy_backoff_orchestrator.reconcile()
    assert 5000 in fake_proxmox.destroyed


# --- Job retry backoff --------------------------------------------------------


@pytest.fixture
async def job_retry_orchestrator(db: Database, fake_github: FakeGitHub):
    orch = Orchestrator(
        make_settings(), db, cast(Any, fake_github), cast(Any, None), cast(Any, None)
    )
    yield orch
    await orch.stop()


async def test_backoff_grows_between_requeue_attempts_then_fails_terminally(
    job_retry_orchestrator: Orchestrator, db: Database
) -> None:
    payload = webhook_payload(1234567)
    await db.enqueue_job("delivery-retry-backoff", payload, "ephemeral")
    failed_before = JOBS_FAILED.labels("ephemeral")._value.get()

    async def failing_provision(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("boom")

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    with (
        mock.patch.object(Orchestrator, "_provision", failing_provision),
        mock.patch("orchestrator.service.asyncio.sleep", fake_sleep),
    ):
        for _ in range(3):
            job = await db.claim_next_job()
            await job_retry_orchestrator._handle_queued(job)

    assert sleeps == [5, 10]
    final_job = await db.get_job(42)
    assert final_job["status"] == "failed"
    # The terminal-failure metric (backs the RunnerOrchestratorJobsFailedTerminal
    # Prometheus alert) fires exactly once, only on the final exhausted attempt.
    assert JOBS_FAILED.labels("ephemeral")._value.get() == failed_before + 1


# --- Per-repository concurrency ceiling ---------------------------------------


@pytest.fixture
async def repo_capacity_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    # Global max_active is generous — only the per-repo ceiling should bind.
    custom_settings = replace(
        base,
        capacity=replace(base.capacity, max_active=5, pool_size=0),
        repositories={
            "OldOwner/example-app": replace(
                base.repositories["OldOwner/example-app"], max_concurrent=1
            )
        },
    )
    orch = Orchestrator(
        custom_settings, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop()


async def test_second_job_for_same_repo_is_throttled(
    repo_capacity_orchestrator: Orchestrator, db: Database
) -> None:
    def payload_for(job_id: int) -> dict[str, Any]:
        data = webhook_payload(1234567)
        data["workflow_job"]["id"] = job_id
        return data

    await db.enqueue_job("delivery-repo-cap-1", payload_for(101), "ephemeral")
    await db.enqueue_job("delivery-repo-cap-2", payload_for(102), "ephemeral")

    with mock.patch("orchestrator.service.asyncio.sleep", mock.AsyncMock()):
        job1 = await db.claim_next_job()
        await repo_capacity_orchestrator._handle_queued(job1)

        job2 = await db.claim_next_job()
        await repo_capacity_orchestrator._handle_queued(job2)

    job1_row = await db.get_job(101)
    job2_row = await db.get_job(102)
    assert job1_row["status"] == "waiting_for_runner"
    assert job2_row["status"] == "queued"
    assert job2_row["error"] == "repo_capacity_exhausted"


# --- Global pool capacity ------------------------------------------------------


@pytest.fixture
async def pool_capacity_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    custom_settings = replace(base, capacity=replace(base.capacity, max_active=1, pool_size=2))
    orch = Orchestrator(
        custom_settings, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop()


async def test_pool_replenishment_respects_already_saturated_active_budget(
    pool_capacity_orchestrator: Orchestrator, db: Database
) -> None:
    # Occupy the single max_active slot with a running ephemeral instance first.
    await db.create_instance(
        5000, "10.0.0.100", "ephemeral", "running", "OldOwner/example-app", 1
    )

    await pool_capacity_orchestrator._replenish_pool()

    instances = await db.list_instances()
    assert len(instances) == 1
    assert instances[0]["vmid"] == 5000


# --- Warm idle scaling ---------------------------------------------------------


@pytest.fixture
async def warm_idle_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    policy = replace(
        base.repositories["OldOwner/example-app"],
        warm_capacity=4,
        warm_idle_capacity=1,
        warm_idle_after_seconds=3600,
    )
    custom_settings = replace(
        base,
        repositories={"OldOwner/example-app": policy},
        capacity=replace(base.capacity, max_active=5, pool_size=0),
    )
    orch = Orchestrator(
        custom_settings, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop()


async def _seed_ready_warms(db: Database, count: int) -> None:
    for i in range(count):
        vmid = 5000 + i
        await db.create_instance(
            vmid,
            f"10.0.0.{100 + i}",
            "warm",
            "ready",
            "OldOwner/example-app",
            runner_name=f"pve-lxc-warm-{vmid}",
        )


async def _record_job_activity(db: Database, job_id: int, *, created_at: int | None = None) -> None:
    data = webhook_payload(1234567)
    data["workflow_job"]["id"] = job_id
    await db.enqueue_job(f"delivery-activity-{job_id}", data, "ephemeral")
    if created_at is not None:
        async with db.connect() as conn:
            await conn.execute("UPDATE jobs SET created_at=? WHERE job_id=?", (created_at, job_id))
            await conn.commit()


async def test_scales_down_after_idle_period(
    warm_idle_orchestrator: Orchestrator, db: Database
) -> None:
    await _seed_ready_warms(db, 4)
    await _record_job_activity(db, 201, created_at=0)  # far in the past -> idle

    await warm_idle_orchestrator._replenish_warm_pool()

    instances = await db.list_instances()
    assert sum(1 for i in instances if i["status"] == "delete_pending") == 3
    assert sum(1 for i in instances if i["status"] == "ready") == 1


async def test_stays_at_full_capacity_within_idle_window(
    warm_idle_orchestrator: Orchestrator, db: Database
) -> None:
    await _seed_ready_warms(db, 4)
    await _record_job_activity(db, 202)  # created_at defaults to now

    await warm_idle_orchestrator._replenish_warm_pool()

    instances = await db.list_instances()
    assert sum(1 for i in instances if i["status"] == "delete_pending") == 0
    assert sum(1 for i in instances if i["status"] == "ready") == 4


async def test_scales_back_up_after_new_activity(
    warm_idle_orchestrator: Orchestrator, db: Database
) -> None:
    # Start already scaled down to the floor, as if idle for a while.
    await _seed_ready_warms(db, 1)
    await _record_job_activity(db, 203, created_at=0)

    await warm_idle_orchestrator._replenish_warm_pool()
    assert len(await db.list_instances()) == 1  # still idle, no change

    # A fresh job request resets the idle clock.
    await _record_job_activity(db, 204)
    await warm_idle_orchestrator._replenish_warm_pool()

    warm_rows = [i for i in await db.list_instances() if i["mode"] == "warm"]
    assert len(warm_rows) == 4
    assert all(row["status"] in {"ready", "running"} for row in warm_rows)


async def test_idle_scaling_disabled_by_default_keeps_fixed_capacity(
    warm_idle_orchestrator: Orchestrator, db: Database
) -> None:
    always_on_policy = replace(
        warm_idle_orchestrator.settings.repositories["OldOwner/example-app"],
        warm_idle_capacity=-1,
        warm_idle_after_seconds=0,
    )
    warm_idle_orchestrator.settings = replace(
        warm_idle_orchestrator.settings,
        repositories={"OldOwner/example-app": always_on_policy},
    )
    await _seed_ready_warms(db, 4)
    await _record_job_activity(db, 205, created_at=0)  # would be "idle" if enabled

    await warm_idle_orchestrator._replenish_warm_pool()

    instances = await db.list_instances()
    assert sum(1 for i in instances if i["status"] == "delete_pending") == 0
    assert sum(1 for i in instances if i["status"] == "ready") == 4


# --- Runner profile selection --------------------------------------------------


@pytest.fixture
async def profile_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    large_profile = RunnerProfile(cores=12, memory_mb=16384, swap_mb=4096, rootfs_gb=60)
    policy = replace(
        base.repositories["OldOwner/example-app"],
        profile="crossbuild-large",
        warm_capacity=1,
    )
    custom_settings = replace(
        base,
        repositories={"OldOwner/example-app": policy},
        profiles={**base.profiles, "crossbuild-large": large_profile},
        capacity=replace(base.capacity, max_active=5, pool_size=1),
    )
    orch = Orchestrator(
        custom_settings, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch, large_profile
    await orch.stop()


async def test_ephemeral_provisioning_uses_repo_profile(
    profile_orchestrator: tuple[Orchestrator, RunnerProfile],
    db: Database,
    fake_proxmox: FakeProxmox,
) -> None:
    orch, large_profile = profile_orchestrator
    payload = webhook_payload(1234567)
    await db.enqueue_job("delivery-profile", payload, "ephemeral")
    job = await db.claim_next_job()

    await orch._handle_queued(job)

    assert fake_proxmox.clone_calls[-1][3] == large_profile


async def test_warm_creation_uses_repo_profile(
    profile_orchestrator: tuple[Orchestrator, RunnerProfile], fake_proxmox: FakeProxmox
) -> None:
    orch, large_profile = profile_orchestrator
    policy = orch.settings.repositories["OldOwner/example-app"]
    await orch._create_warm_runner("OldOwner/example-app", policy)

    assert fake_proxmox.clone_calls[-1][3] == large_profile


async def test_pool_creation_always_uses_default_profile(
    profile_orchestrator: tuple[Orchestrator, RunnerProfile], fake_proxmox: FakeProxmox
) -> None:
    orch, _large_profile = profile_orchestrator
    await orch._replenish_pool()

    assert fake_proxmox.clone_calls
    default_profile = orch.settings.profiles["crossbuild-small"]
    for call in fake_proxmox.clone_calls:
        assert call[3] == default_profile

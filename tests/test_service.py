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
from orchestrator.drift import MANAGED_TAG, is_managed
from orchestrator.github import GitHubError
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
    await orch.stop(drain_seconds=0.5)


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
    await orch.stop(drain_seconds=0.5)


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
    await orch.stop(drain_seconds=0.5)


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
    await orch.stop(drain_seconds=0.5)


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
    """A failed teardown must not be retried until its deadline passes.

    Driven through a real failing destroy rather than by hand-setting
    `delete_attempts`, so the deadline is the one production would write.
    """
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    fake_proxmox.fail("destroy", RuntimeError("proxmox unavailable"), times=1)

    await destroy_backoff_orchestrator.reconcile()
    assert fake_proxmox.destroyed == []  # the attempt raised before recording one

    failed = await db.get_instance(instance_id)
    assert failed["status"] == "delete_failed"
    assert failed["delete_attempts"] == 1
    # backoff(1) = 120s ±20% jitter, measured from an absolute deadline that an
    # unrelated `updated_at` bump cannot reset.
    deadline = int(failed["delete_not_before"])
    assert 96 <= deadline - int(time.time()) <= 144

    # Still inside the window: reconcile must leave it alone.
    await destroy_backoff_orchestrator.reconcile()
    assert fake_proxmox.destroyed == []

    # An unrelated write bumps updated_at; the deadline must survive it.
    await db.update_instance(instance_id, "delete_failed", error="unrelated touch")
    await destroy_backoff_orchestrator.reconcile()
    assert fake_proxmox.destroyed == []

    async with db.connect() as conn:
        await conn.execute("UPDATE instances SET delete_not_before=0 WHERE id=?", (instance_id,))
        await conn.commit()

    await destroy_backoff_orchestrator.reconcile()
    assert 5000 in fake_proxmox.destroyed


async def test_destroy_probe_failure_still_increments_delete_attempts(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_github: FakeGitHub
) -> None:
    """The the incident regression, in one test.

    When GitHub is unreachable the exception is raised inside the
    deregistration probe, i.e. BEFORE the row moves to `deleting`. The old
    recovery path only matched `status='deleting'`, so it updated zero rows:
    `delete_attempts` stayed at 0 forever, the exponential backoff never
    engaged, and the instance was re-probed at full cadence on every pass while
    its container leaked disk. Live proof at the time: vmid 5000 sat at
    `delete_attempts: 0` after more than a day of failing.
    """
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    fake_github.register("pve-lxc-warm-5000")
    # A 5xx is an *answer* from GitHub, not an availability failure, so it is
    # not absorbed by the teardown decoupling — it propagates from inside the
    # probe, before the row reaches `deleting`, which is the shape that used to
    # be swallowed.
    fake_github.fail("find_runner", GitHubError("GitHub API 500: boom", status_code=500))

    instance = await db.get_instance(instance_id)
    await destroy_backoff_orchestrator._destroy_instance(instance)

    current = await db.get_instance(instance_id)
    assert current["status"] == "delete_failed"
    assert current["delete_attempts"] == 1, "backoff never engages if the attempt isn't counted"
    assert current["delete_not_before"] is not None


async def test_repeated_probe_failures_keep_growing_the_backoff(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_github: FakeGitHub
) -> None:
    """Attempts must accumulate across passes, so the window actually widens."""
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    fake_github.register("pve-lxc-warm-5000")
    fake_github.fail("find_runner", GitHubError("GitHub API 500: boom", status_code=500))

    windows: list[int] = []
    for _ in range(3):
        # Expire the deadline so the next pass is eligible, then fail again.
        async with db.connect() as conn:
            await conn.execute(
                "UPDATE instances SET delete_not_before=0 WHERE id=?", (instance_id,)
            )
            await conn.commit()
        await destroy_backoff_orchestrator.reconcile()
        row = await db.get_instance(instance_id)
        windows.append(int(row["delete_not_before"]) - int(time.time()))

    assert (await db.get_instance(instance_id))["delete_attempts"] == 3
    # 60/120/240 base with ±20% jitter — strictly growing even at the extremes.
    assert windows[0] < windows[1] < windows[2]


async def test_delete_pending_is_eligible_on_its_first_pass(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """Teardown is the hot path that returns capacity — it must not be delayed.

    Only a failure or a deferral may set a deadline; a fresh `delete_pending`
    has none and is collected immediately.
    """
    instance_id = await db.create_instance(
        5000, "10.0.0.100", "warm", "delete_pending", "OldOwner/example-app"
    )
    assert (await db.get_instance(instance_id))["delete_not_before"] is None

    await destroy_backoff_orchestrator.reconcile()
    assert 5000 in fake_proxmox.destroyed


async def test_cleanup_failure_does_not_stomp_a_concurrent_claim(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """Widening the expected set must not make the recovery path destructive.

    If another task claimed the instance while teardown was in flight, the
    failure handler must match nothing and leave the claim intact.
    """
    instance_id = await db.create_instance(
        5000, "10.0.0.100", "warm", "delete_pending", "OldOwner/example-app"
    )
    stale = await db.get_instance(instance_id)

    async def claim_then_fail(_vmid: int) -> None:
        await db.update_instance(instance_id, "running", job_id=42)
        raise RuntimeError("proxmox unavailable")

    fake_proxmox.destroy = claim_then_fail  # type: ignore[method-assign]

    await destroy_backoff_orchestrator._destroy_instance(stale)

    current = await db.get_instance(instance_id)
    assert current["status"] == "running"
    assert current["job_id"] == 42
    assert current["delete_attempts"] == 0


# --- Job retry backoff --------------------------------------------------------


@pytest.fixture
async def job_retry_orchestrator(db: Database, fake_github: FakeGitHub):
    orch = Orchestrator(
        make_settings(), db, cast(Any, fake_github), cast(Any, None), cast(Any, None)
    )
    yield orch
    await orch.stop(drain_seconds=0.5)


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
    await orch.stop(drain_seconds=0.5)


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
    await orch.stop(drain_seconds=0.5)


async def test_pool_replenishment_respects_already_saturated_active_budget(
    pool_capacity_orchestrator: Orchestrator, db: Database
) -> None:
    # Occupy the single max_active slot with a running ephemeral instance first.
    await db.create_instance(5000, "10.0.0.100", "ephemeral", "running", "OldOwner/example-app", 1)

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
    await orch.stop(drain_seconds=0.5)


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

    await warm_idle_orchestrator._replenish_warm_pool(blocking=True)

    instances = await db.list_instances()
    assert sum(1 for i in instances if i["status"] == "delete_pending") == 3
    assert sum(1 for i in instances if i["status"] == "ready") == 1


async def test_stays_at_full_capacity_within_idle_window(
    warm_idle_orchestrator: Orchestrator, db: Database
) -> None:
    await _seed_ready_warms(db, 4)
    await _record_job_activity(db, 202)  # created_at defaults to now

    await warm_idle_orchestrator._replenish_warm_pool(blocking=True)

    instances = await db.list_instances()
    assert sum(1 for i in instances if i["status"] == "delete_pending") == 0
    assert sum(1 for i in instances if i["status"] == "ready") == 4


async def test_scales_back_up_after_new_activity(
    warm_idle_orchestrator: Orchestrator, db: Database
) -> None:
    # Start already scaled down to the floor, as if idle for a while.
    await _seed_ready_warms(db, 1)
    await _record_job_activity(db, 203, created_at=0)

    await warm_idle_orchestrator._replenish_warm_pool(blocking=True)
    assert len(await db.list_instances()) == 1  # still idle, no change

    # A fresh job request resets the idle clock.
    await _record_job_activity(db, 204)
    await warm_idle_orchestrator._replenish_warm_pool(blocking=True)

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

    await warm_idle_orchestrator._replenish_warm_pool(blocking=True)

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
    await orch.stop(drain_seconds=0.5)


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


# --- Teardown must not depend on GitHub (RJIT-001) ----------------------------


async def test_destroy_proceeds_locally_when_github_is_unreachable(
    destroy_backoff_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
) -> None:
    """The headline fix.

    Destroying a container is a local operation. Coupling it to an external API
    turned a GitHub outage into an unbounded disk leak that filled a thin pool
    shared with nine production services. The container must go now; the
    registration is an obligation we record and settle later.
    """
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    fake_github.register("pve-lxc-warm-5000")
    fake_github.unreachable = True

    instance = await db.get_instance(instance_id)
    await destroy_backoff_orchestrator._destroy_instance(instance)

    assert 5000 in fake_proxmox.destroyed, "the container must be released regardless"
    assert await db.get_instance(instance_id) is None, "and its row with it"

    queued = await db.list_due_deregistrations(now=int(time.time()) + 1)
    assert [(r["repository"], r["runner_name"]) for r in queued] == [
        ("OldOwner/example-app", "pve-lxc-warm-5000")
    ]


async def test_destroy_still_defers_when_github_says_the_runner_is_busy(
    destroy_backoff_orchestrator: Orchestrator,
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
) -> None:
    """An answer may defer a destroy; only the absence of one may not.

    Guards the TOCTOU protection that the decoupling must not trade away:
    destroying a runner GitHub still considers online lets it hand out a job to
    a container that is about to vanish.
    """
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "delete_pending",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    fake_github.register("pve-lxc-warm-5000")
    fake_github.set_runner_state("pve-lxc-warm-5000", status="online", busy=True)

    instance = await db.get_instance(instance_id)
    await destroy_backoff_orchestrator._destroy_instance(instance)

    assert fake_proxmox.destroyed == []
    current = await db.get_instance(instance_id)
    assert current["status"] == "delete_pending"
    # Deferred, not failed: this is not our error, so it must not burn an attempt.
    assert current["delete_attempts"] == 0
    assert current["delete_not_before"] > int(time.time())


async def test_queued_deregistration_is_settled_when_github_returns(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_github: FakeGitHub
) -> None:
    """The obligation is discharged on the first pass after recovery."""
    fake_github.register("pve-lxc-warm-5000", runner_id=77)
    await db.enqueue_deregistration(
        "OldOwner/example-app", "pve-lxc-warm-5000", vmid=5000, reason="test"
    )
    assert await db.count_pending_deregistrations() == 1

    await destroy_backoff_orchestrator.reconcile()

    assert 77 in fake_github.deleted
    assert await db.count_pending_deregistrations() == 0


async def test_drain_is_skipped_entirely_while_the_circuit_is_open(
    destroy_backoff_orchestrator: Orchestrator, db: Database, fake_github: FakeGitHub
) -> None:
    """Retrying into an open circuit burns the pass budget for nothing."""
    await db.enqueue_deregistration(
        "OldOwner/example-app", "pve-lxc-warm-5000", vmid=5000, reason="test"
    )
    fake_github.unreachable = True

    await destroy_backoff_orchestrator.reconcile()

    assert fake_github.calls["find_runner"] == 0
    assert await db.count_pending_deregistrations() == 1


async def test_queued_deregistration_for_an_already_gone_runner_is_dropped(
    destroy_backoff_orchestrator: Orchestrator, db: Database
) -> None:
    """A redundant obligation must not be retried forever."""
    await db.enqueue_deregistration(
        "OldOwner/example-app", "pve-lxc-warm-9999", vmid=9999, reason="test"
    )
    await destroy_backoff_orchestrator.reconcile()
    assert await db.count_pending_deregistrations() == 0


async def test_enqueue_deregistration_is_idempotent(db: Database) -> None:
    """Re-queuing must not reset the backoff of an obligation already retrying."""
    await db.enqueue_deregistration("Acme/Repo", "runner-1", vmid=1, reason="first")
    rows = await db.list_due_deregistrations(now=int(time.time()) + 1)
    await db.bump_deregistration(int(rows[0]["id"]), error="boom", not_before=2**31)

    await db.enqueue_deregistration("Acme/Repo", "runner-1", vmid=1, reason="second")

    assert await db.count_pending_deregistrations() == 1
    assert await db.list_due_deregistrations(now=int(time.time())) == []


# --- Reality check: the DB vs the hypervisor (RJIT-003) -----------------------


@pytest.fixture
async def sweep_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    custom = replace(
        base,
        capacity=replace(base.capacity, pool_size=0),
        repositories={
            "OldOwner/example-app": replace(
                base.repositories["OldOwner/example-app"], warm_capacity=0
            )
        },
    )
    orch = Orchestrator(
        custom, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop(drain_seconds=0.5)


async def _age_instance(db: Database, instance_id: int, seconds: int) -> None:
    async with db.connect() as conn:
        await conn.execute(
            "UPDATE instances SET updated_at=updated_at-? WHERE id=?", (seconds, instance_id)
        )
        await conn.commit()


async def test_row_without_a_container_is_reaped_after_two_strikes(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """The gap that forced hand-written SQL against production.

    Nothing ever compared the database against the hypervisor, so rows for
    containers destroyed by hand stayed `ready` forever, holding their UNIQUE
    vmid/ip and a share of capacity.
    """
    instance_id = await db.create_instance(
        5000, "10.0.0.100", "pool", "ready", runner_name="pve-lxc-pool-5000"
    )
    fake_proxmox.existing = {9999}  # something else exists, so the set isn't empty
    await _age_instance(db, instance_id, 300)

    await sweep_orchestrator.reconcile()
    row = await db.get_instance(instance_id)
    assert row is not None, "one observation must not be enough"
    assert row["ct_missing_since"] is not None

    await _age_instance(db, instance_id, 300)
    async with db.connect() as conn:
        await conn.execute(
            "UPDATE instances SET ct_missing_since=ct_missing_since-300 WHERE id=?", (instance_id,)
        )
        await conn.commit()

    await sweep_orchestrator.reconcile()
    assert await db.get_instance(instance_id) is None
    assert fake_proxmox.destroyed == [], "there was nothing to destroy"


async def test_reappearing_container_clears_the_strike(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    instance_id = await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    fake_proxmox.existing = {9999}
    await _age_instance(db, instance_id, 300)
    await sweep_orchestrator.reconcile()
    assert (await db.get_instance(instance_id))["ct_missing_since"] is not None

    fake_proxmox.existing = {5000, 9999}
    await sweep_orchestrator.reconcile()
    assert (await db.get_instance(instance_id))["ct_missing_since"] is None


async def test_no_reaping_when_the_hypervisor_view_is_unavailable(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """Unknown is not the same as absent."""
    instance_id = await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    await _age_instance(db, instance_id, 600)
    fake_proxmox.fail("cluster_vmids", RuntimeError("proxmox unreachable"))

    await sweep_orchestrator.reconcile()
    await sweep_orchestrator.reconcile()

    row = await db.get_instance(instance_id)
    assert row is not None
    assert row["ct_missing_since"] is None


async def test_no_fleet_wide_reap_when_the_node_reports_nothing(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """An empty set from a healthy API is indistinguishable from a lost node.

    A quorum event must not cascade into deleting every row we have.
    """
    ids = []
    for offset in range(4):
        ids.append(
            await db.create_instance(5000 + offset, f"10.0.0.{100 + offset}", "pool", "ready")
        )
    for instance_id in ids:
        await _age_instance(db, instance_id, 600)
    fake_proxmox.existing = set()

    await sweep_orchestrator.reconcile()
    await sweep_orchestrator.reconcile()

    for instance_id in ids:
        assert await db.get_instance(instance_id) is not None


async def test_stranded_deleting_row_is_requeued(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """`deleting` had no reconcile branch at all; the row stranded forever."""
    instance_id = await db.create_instance(5000, "10.0.0.100", "warm", "deleting")
    fake_proxmox.existing = {5000}
    await _age_instance(db, instance_id, 700)

    await sweep_orchestrator.reconcile()

    row = await db.get_instance(instance_id)
    assert row["status"] == "delete_pending"
    assert row["error"] == "teardown_interrupted"


async def test_stranded_deleting_row_with_no_container_is_dropped(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    instance_id = await db.create_instance(5000, "10.0.0.100", "warm", "deleting")
    fake_proxmox.existing = {9999}
    await _age_instance(db, instance_id, 700)

    # Strike one records; strike two removes.
    await sweep_orchestrator.reconcile()
    async with db.connect() as conn:
        await conn.execute(
            "UPDATE instances SET ct_missing_since=ct_missing_since-300 WHERE id=?", (instance_id,)
        )
        await conn.commit()
    await sweep_orchestrator.reconcile()

    assert await db.get_instance(instance_id) is None


async def test_ready_pool_container_running_unexpectedly_is_retired(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """Pool containers stay stopped until claimed; running means a crashed provision."""
    instance_id = await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    fake_proxmox.existing = {5000}
    fake_proxmox.running = {5000}

    await sweep_orchestrator.reconcile()

    row = await db.get_instance(instance_id)
    assert row["status"] == "delete_pending"
    assert row["error"] == "pool_unexpectedly_running"


async def test_pool_container_past_max_age_is_retired(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    instance_id = await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    fake_proxmox.existing = {5000}
    async with db.connect() as conn:
        await conn.execute(
            "UPDATE instances SET created_at=created_at-? WHERE id=?", (200_000, instance_id)
        )
        await conn.commit()

    await sweep_orchestrator.reconcile()

    assert (await db.get_instance(instance_id))["error"] == "pool_max_age"


async def test_one_bad_instance_does_not_abort_the_sweep(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """A single failing row used to abort the whole pass, metrics included."""
    first = await db.create_instance(5000, "10.0.0.100", "warm", "delete_pending")
    second = await db.create_instance(5001, "10.0.0.101", "warm", "delete_pending")
    fake_proxmox.existing = {5000, 5001}
    fake_proxmox.fail("destroy", RuntimeError("boom"), times=1)

    await sweep_orchestrator.reconcile()

    assert 5001 in fake_proxmox.destroyed, "the second row must still be processed"
    assert (await db.get_instance(first))["status"] == "delete_failed"
    assert await db.get_instance(second) is None


async def test_metrics_are_updated_even_when_a_phase_raises(
    sweep_orchestrator: Orchestrator, db: Database
) -> None:
    """Gauges frozen at healthy values are why monitoring saw nothing."""
    from orchestrator.service import RECONCILE_LAST_SUCCESS

    await db.create_instance(5000, "10.0.0.100", "warm", "delete_pending")
    before = RECONCILE_LAST_SUCCESS._value.get()

    async def boom(_now: int) -> None:
        raise RuntimeError("phase exploded")

    sweep_orchestrator._reconcile_stale_jobs = boom  # type: ignore[method-assign]
    await sweep_orchestrator.reconcile()

    # The pass did not abort, and the failure did not silently advance liveness.
    assert RECONCILE_LAST_SUCCESS._value.get() == before


# --- Storage admission control (RJIT-005) ------------------------------------


GIB = 1024**3


def _storage(total_gb: int, used_gb: int) -> dict[str, int]:
    return {
        "total": total_gb * GIB,
        "used": used_gb * GIB,
        "avail": (total_gb - used_gb) * GIB,
    }


@pytest.fixture
async def storage_orchestrator(
    db: Database, fake_github: FakeGitHub, fake_proxmox: FakeProxmox, fake_broker: FakeBroker
):
    base = make_settings()
    custom = replace(
        base,
        capacity=replace(base.capacity, pool_size=2, max_active=4),
        repositories={
            "OldOwner/example-app": replace(
                base.repositories["OldOwner/example-app"], warm_capacity=0
            )
        },
    )
    orch = Orchestrator(
        custom, db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    await orch.stop(drain_seconds=0.5)


async def test_replenish_is_refused_below_the_storage_floor(
    storage_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    """Speculative capacity must stop before it takes the shared pool down.

    This is the 91.2%-of-thin-pool state from the incident.
    """
    fake_proxmox.storage = _storage(350, 319)

    await storage_orchestrator._replenish_pool()

    assert fake_proxmox.clones == []


async def test_replenish_is_allowed_with_headroom(
    storage_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    fake_proxmox.storage = _storage(350, 120)

    await storage_orchestrator._replenish_pool()

    assert len(fake_proxmox.clones) == 2


async def test_storage_reading_is_cached_across_clones(
    storage_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    """Gating every clone must not mean querying on every clone."""
    fake_proxmox.storage = _storage(350, 120)

    await storage_orchestrator._replenish_pool()

    assert len(fake_proxmox.clones) == 2
    assert fake_proxmox.calls["storage_status"] == 1


async def test_storage_guard_fails_open_when_the_reading_is_unavailable(
    storage_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    """A metrics endpoint hiccup must not halt CI."""
    fake_proxmox.fail("storage_status", RuntimeError("storage API down"))

    admitted, _ = await storage_orchestrator.storage_admits(purpose="test")

    assert admitted is True


async def test_storage_guard_can_fail_closed_when_configured(
    storage_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    from orchestrator.config import StorageConfig

    storage_orchestrator.settings = replace(
        storage_orchestrator.settings, storage=StorageConfig(fail_open=False)
    )
    fake_proxmox.fail("storage_status", RuntimeError("storage API down"))

    admitted, _ = await storage_orchestrator.storage_admits(purpose="test")

    assert admitted is False


async def test_queued_job_is_requeued_not_failed_when_storage_is_exhausted(
    storage_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """Waiting is recoverable; a burnt attempt is not.

    During the incident clones died on an opaque Proxmox error and consumed all
    three job attempts before the job failed terminally.
    """
    fake_proxmox.storage = _storage(350, 340)
    payload = webhook_payload(1234567)
    await db.enqueue_job("delivery-storage", payload, "ephemeral")
    job = await db.claim_next_job()

    await storage_orchestrator._handle_queued(job)

    saved = await db.get_job(42)
    assert saved["status"] == "queued"
    assert saved["error"] == "storage_exhausted"
    assert fake_proxmox.clones == []


def test_projected_disk_demand_multiplies_slots_by_rootfs() -> None:
    """`rootfs_gb` existed but was never connected to `max_active`."""
    from orchestrator.config import projected_disk_demand_gb

    base = make_settings()
    capacity = replace(base.capacity, max_active=11, pool_size=3)
    repositories = {
        "OldOwner/example-app": replace(base.repositories["OldOwner/example-app"], warm_capacity=2)
    }
    # 2 warm + 3 pool + 6 spare slots, all on the 25 GiB default profile.
    assert projected_disk_demand_gb(capacity, repositories, base.profiles) == 11 * 25


def test_warm_min_root_mib_is_actually_enforced() -> None:
    """The field was validated and serialized but never read by any code path."""
    from orchestrator.service import _parse_avail_mib

    assert _parse_avail_mib("cleaned avail_mib=9000") == 9000
    assert _parse_avail_mib("cleaned avail_mib=512") == 512
    assert _parse_avail_mib("cleaned") is None


async def test_warm_recycle_aborts_when_rootfs_is_below_the_floor(
    warm_orchestrator: Orchestrator,
    db: Database,
    fake_broker: FakeBroker,
    fake_proxmox: FakeProxmox,
) -> None:
    instance_id = await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "provisioning",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )

    async def nearly_full(vmid: int) -> str:
        fake_broker.cleaned.append(vmid)
        return "cleaned avail_mib=100"

    fake_broker.clean_work = nearly_full  # type: ignore[method-assign]

    await warm_orchestrator._recycle_warm_instance(instance_id)

    # Recycling in place is refused, so the container is retired instead of
    # being handed to a job with no room to build. `_fail_warm_recycle`
    # schedules the teardown immediately, so the row is already gone.
    row = await db.get_instance(instance_id)
    assert row is None or row["status"] in {"delete_pending", "delete_failed"}
    assert 5000 in fake_proxmox.destroyed


# --- Drift: database vs hypervisor (RJIT-007) --------------------------------


async def test_drift_reports_both_directions(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    await db.create_instance(5001, "10.0.0.101", "warm", "ready")

    async def node_cts() -> list[dict[str, Any]]:
        return [
            {"vmid": 5001, "name": "pve-lxc-warm-5001", "status": "running", "tags": "jit-runner"},
            # Managed and in range, but unknown to the database.
            {
                "vmid": 5022,
                "name": "pve-lxc-warm-5022",
                "status": "stopped",
                "tags": "jit-runner;mode-warm;managed-orchestrator",
            },
            # In range but not ours: must never be a candidate.
            {"vmid": 5030, "name": "someone-elses-ct", "status": "running", "tags": "prod"},
            # Ours by tag but outside the configured range.
            {"vmid": 4000, "name": "out-of-range", "status": "stopped", "tags": MANAGED_TAG},
        ]

    fake_proxmox.list_node_cts = node_cts  # type: ignore[method-assign]

    report = await sweep_orchestrator.detect_drift()

    assert [r["vmid"] for r in report.rows_without_container] == [5000]
    assert [c["vmid"] for c in report.containers_without_row] == [5022]


async def test_drift_apply_refuses_a_set_that_changed_since_the_scan(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """The operator approved a specific list; a different list is a new decision."""
    await db.create_instance(5000, "10.0.0.100", "pool", "ready")

    async def node_cts() -> list[dict[str, Any]]:
        return []

    fake_proxmox.list_node_cts = node_cts  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="changed since the scan"):
        await sweep_orchestrator.apply_drift(confirm_vmids=[5000, 9999])


async def test_drift_apply_removes_only_the_confirmed_rows(
    sweep_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    keep = await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    drop = await db.create_instance(5001, "10.0.0.101", "pool", "ready")

    async def node_cts() -> list[dict[str, Any]]:
        return []

    fake_proxmox.list_node_cts = node_cts  # type: ignore[method-assign]

    result = await sweep_orchestrator.apply_drift(confirm_vmids=[5001])

    assert result["rows_deleted"] == 1
    assert await db.get_instance(keep) is not None
    assert await db.get_instance(drop) is None


async def test_drift_apply_will_not_destroy_a_running_orphan_without_force(
    sweep_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    async def node_cts() -> list[dict[str, Any]]:
        return [
            {
                "vmid": 5022,
                "name": "pve-lxc-warm-5022",
                "status": "running",
                "tags": MANAGED_TAG,
            }
        ]

    fake_proxmox.list_node_cts = node_cts  # type: ignore[method-assign]

    result = await sweep_orchestrator.apply_drift(confirm_vmids=[5022])
    assert result["containers_destroyed"] == 0
    assert fake_proxmox.destroyed == []

    result = await sweep_orchestrator.apply_drift(confirm_vmids=[5022], force=True)
    assert result["containers_destroyed"] == 1
    assert 5022 in fake_proxmox.destroyed


async def test_drift_refuses_to_apply_when_the_scan_is_incomplete(
    sweep_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    """A partial view of the hypervisor is not a basis for deleting anything."""
    fake_proxmox.fail("list_node_cts", RuntimeError("node unreachable"))

    report = await sweep_orchestrator.detect_drift()
    assert report.scan_incomplete is True

    with pytest.raises(RuntimeError, match="scan incomplete"):
        await sweep_orchestrator.apply_drift(confirm_vmids=[5000])


def test_managed_requires_both_range_and_tag() -> None:
    """Both conditions, never either: this is a shared hypervisor."""
    tagged_in_range = {"vmid": 5010, "tags": "jit-runner;managed-orchestrator"}
    tagged_out_of_range = {"vmid": 4000, "tags": MANAGED_TAG}
    untagged_in_range = {"vmid": 5010, "tags": "prod;postgres"}

    assert is_managed(tagged_in_range, vmid_start=5000, vmid_end=5099)
    assert not is_managed(tagged_out_of_range, vmid_start=5000, vmid_end=5099)
    assert not is_managed(untagged_in_range, vmid_start=5000, vmid_end=5099)


# --- Collapsing redundant GitHub calls ----------------------------------------


async def test_registration_purge_does_not_walk_one_scope_twice(
    destroy_backoff_orchestrator: Orchestrator, fake_github: FakeGitHub
) -> None:
    """Repository-scoped installations resolve to distinct scopes; org ones do not.

    The rename fallback used to issue one full listing per configured
    repository on every teardown, even when they all shared a runner list.
    """
    settings = destroy_backoff_orchestrator.settings
    base = settings.repositories["OldOwner/example-app"]
    destroy_backoff_orchestrator.settings = replace(
        settings,
        github=replace(
            settings.github,
            installations={
                "personal": replace(settings.github.installations["personal"], scope="organization")
            },
        ),
        repositories={
            "OldOwner/example-app": base,
            "OldOwner/example-other": replace(base, warm_capacity=0),
            "OldOwner/example-third": replace(base, warm_capacity=0),
        },
    )

    await destroy_backoff_orchestrator._purge_github_runner_registration(
        "OldOwner/example-app", "pve-lxc-warm-5000", vmid=5000
    )

    # Three repositories, one organization runner list, so one lookup.
    assert fake_github.calls["find_runner"] == 1


async def test_warm_creation_does_not_block_the_reconcile_pass(
    warm_orchestrator: Orchestrator, db: Database, fake_proxmox: FakeProxmox
) -> None:
    """Creation is scheduled, not awaited.

    `_create_warm_runner` waits up to `ready_timeout_seconds` for a
    registration to come online while holding `_warm_lock`. Called inline from
    `reconcile()` that let one slow registration stall the entire pass — and
    everything else it was supposed to do — well past its budget.
    """
    fake_proxmox.slow("clone", 0.3)

    started = time.monotonic()
    await warm_orchestrator.reconcile()
    elapsed = time.monotonic() - started

    assert elapsed < 0.2, "the pass returned before the clone finished"
    assert warm_orchestrator._background_tasks, "creation must still be scheduled"

    await asyncio.gather(*warm_orchestrator._background_tasks, return_exceptions=True)
    assert fake_proxmox.clones, "the scheduled work does run"


async def test_warm_scale_down_still_happens_inline(
    warm_idle_orchestrator: Orchestrator, db: Database
) -> None:
    """Scaling down is database-only, so it must not be deferred.

    Retiring surplus containers frees capacity; postponing it to a background
    task would leave the fleet over-provisioned for longer than intended.
    """
    await _seed_ready_warms(db, 4)
    await _record_job_activity(db, 301, created_at=0)

    await warm_idle_orchestrator._replenish_warm_pool()

    rows = await db.list_instances()
    assert sum(1 for r in rows if r["status"] == "delete_pending") == 3


async def test_unexpected_storage_response_shape_is_loud(
    storage_orchestrator: Orchestrator, fake_proxmox: FakeProxmox
) -> None:
    """A wrong field name must not look like 'reading unavailable'.

    Both paths fail open, so without this the admission floor would silently
    never arm and nobody would find out until the pool filled again.
    """
    fake_proxmox.storage = {"unexpected": 1}

    snapshot = await storage_orchestrator.storage_snapshot()

    assert snapshot is None
    errors = [
        e
        for e in storage_orchestrator.events.list(limit=50)
        if e["level"] == "error" and e["category"] == "storage"
    ]
    assert errors, "a shape mismatch must be reported, not swallowed"

from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite
import pytest

from orchestrator.db import MIGRATIONS, Database


async def test_schema_version_reaches_latest_after_initialize(db: Database) -> None:
    row = await db._fetchone("SELECT version FROM schema_version")
    assert row is not None
    assert row["version"] == len(MIGRATIONS)


async def test_initialize_is_idempotent(db: Database) -> None:
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")
    await db.initialize()  # re-running must not fail or reset data
    instances = await db.list_instances()
    assert len(instances) == 1
    row = await db._fetchone("SELECT version FROM schema_version")
    assert row is not None
    assert row["version"] == len(MIGRATIONS)


async def test_legacy_database_catches_up_missing_columns(tmp_path: Path) -> None:
    """A pre-existing DB with schema_version=1 (the old hardcoded seed) but
    missing later ad-hoc columns must catch up without manual intervention."""
    path = str(tmp_path / "legacy.db")
    async with aiosqlite.connect(path) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        await conn.execute("INSERT INTO schema_version(version) VALUES (1)")
        await conn.execute(
            """CREATE TABLE instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vmid INTEGER NOT NULL UNIQUE,
                ip_address TEXT NOT NULL UNIQUE,
                repository TEXT,
                mode TEXT NOT NULL,
                runner_name TEXT,
                status TEXT NOT NULL,
                job_id INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_used_at INTEGER,
                error TEXT
            )"""
        )
        await conn.commit()

    legacy_db = Database(path)
    await legacy_db.initialize()

    row = await legacy_db._fetchone("SELECT version FROM schema_version")
    assert row is not None
    assert row["version"] == len(MIGRATIONS)
    columns = {c["name"] for c in await legacy_db._fetchall("PRAGMA table_info(instances)")}
    assert "use_count" in columns
    assert "delete_attempts" in columns


async def test_delivery_is_idempotent(db: Database) -> None:
    assert await db.record_delivery("delivery-1", "workflow_job", "queued", True, "accepted")
    assert not await db.record_delivery("delivery-1", "workflow_job", "queued", True, "accepted")


async def test_list_jobs_omits_payload_and_parses_labels(db: Database) -> None:
    payload = {
        "action": "queued",
        "repository": {"full_name": "OldOwner/example-app"},
        "workflow_job": {"id": 99, "run_id": 8, "labels": ["self-hosted", "crossbuild"]},
    }
    await db.enqueue_job("delivery-list", payload, "ephemeral")
    jobs = await db.list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0]["job_id"] == 99
    assert jobs[0]["labels"] == ["self-hosted", "crossbuild"]
    assert "payload_json" not in jobs[0]
    assert "payload" not in jobs[0]
    assert (
        jobs[0]["github_job_url"]
        == "https://github.com/OldOwner/example-app/actions/runs/8/job/99"
    )
    filtered = await db.list_jobs(limit=10, repository="OldOwner/example-app", mode="ephemeral")
    assert len(filtered) == 1
    empty = await db.list_jobs(limit=10, repository="OldOwner/Other")
    assert empty == []


async def test_list_instances_includes_github_job_links(db: Database) -> None:
    payload = {
        "action": "queued",
        "repository": {"full_name": "OldOwner/example-app"},
        "workflow_job": {"id": 77, "run_id": 12, "labels": ["self-hosted"]},
    }
    await db.enqueue_job("delivery-inst-link", payload, "ephemeral")
    await db.create_instance(
        5100, "10.0.0.150", "ephemeral", "running", "OldOwner/example-app", 77
    )
    await db.create_instance(5101, "10.0.0.151", "warm", "ready")
    rows = await db.list_instances()
    by_vmid = {row["vmid"]: row for row in rows}
    busy = by_vmid[5100]
    idle = by_vmid[5101]
    assert (
        busy["github_job_url"]
        == "https://github.com/OldOwner/example-app/actions/runs/12/job/77"
    )
    assert busy["github_run_url"] == "https://github.com/OldOwner/example-app/actions/runs/12"
    assert "github_job_url" not in idle
    assert "job_run_id" not in busy
    filtered = await db.list_instances(status="running", mode="ephemeral")
    assert len(filtered) == 1
    assert filtered[0]["vmid"] == 5100


async def test_schedule_ready_recycle_marks_pool_and_warm(db: Database) -> None:
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")
    await db.create_instance(5001, "10.0.0.101", "pool", "ready")
    result = await db.schedule_ready_recycle()
    assert result["scheduled"] == 2
    statuses = {(row["vmid"], row["status"]) for row in await db.list_instances()}
    assert statuses == {(5000, "delete_pending"), (5001, "delete_pending")}


async def test_schedule_ready_recycle_refuses_busy_pool_or_warm_instances(db: Database) -> None:
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")
    await db.create_instance(
        5001, "10.0.0.101", "warm", "provisioning", "OldOwner/example-app"
    )
    with pytest.raises(RuntimeError):
        await db.schedule_ready_recycle()


async def test_schedule_ready_recycle_ignores_busy_ephemeral_or_persistent(db: Database) -> None:
    """A busy job elsewhere in the fleet must not block recycling pool/warm supply."""
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")
    await db.create_instance(
        5001, "10.0.0.101", "ephemeral", "running", "OldOwner/example-app", 9
    )
    await db.create_instance(
        5002, "10.0.0.102", "persistent", "ready", "OldOwner/example-app"
    )

    result = await db.schedule_ready_recycle()

    assert result["scheduled"] == 1
    statuses = {row["vmid"]: row["status"] for row in await db.list_instances()}
    assert statuses[5000] == "delete_pending"
    assert statuses[5001] == "running"
    # persistent is ready but not pool/warm — schedule_ready_recycle never touches it.
    assert statuses[5002] == "ready"


async def test_list_deliveries_returns_newest_first(db: Database) -> None:
    await db.record_delivery("d-old", "workflow_job", "queued", False, "repository_not_allowed")
    await db.record_delivery("d-new", "workflow_job", "queued", True, "accepted")
    deliveries = await db.list_deliveries(limit=10)
    assert len(deliveries) >= 2
    ids = {row["delivery_id"] for row in deliveries}
    assert {"d-old", "d-new"}.issubset(ids)
    timestamps = [int(row["received_at"]) for row in deliveries]
    assert timestamps == sorted(timestamps, reverse=True)


async def test_job_can_only_be_claimed_once(db: Database) -> None:
    payload = {
        "action": "queued",
        "repository": {"full_name": "OldOwner/example-app"},
        "workflow_job": {"id": 42, "run_id": 7, "labels": ["self-hosted"]},
    }
    await db.enqueue_job("delivery-2", payload, "ephemeral")
    claimed = await db.claim_next_job()
    assert claimed is not None
    assert claimed["attempts"] == 1
    assert await db.claim_next_job() is None


async def test_busy_persistent_instance_is_not_reused(db: Database) -> None:
    instance_id = await db.create_instance(
        5000, "10.0.0.100", "persistent", "running", "OldOwner/example-app", 42
    )
    assert await db.find_persistent("OldOwner/example-app") is None
    await db.update_instance(instance_id, "ready", job_id=None)
    assert (await db.find_persistent("OldOwner/example-app"))["id"] == instance_id


async def test_instance_transition_requires_expected_status(db: Database) -> None:
    instance_id = await db.create_instance(5000, "10.0.0.100", "warm", "provisioning")

    changed = await db.transition_instance(instance_id, {"provisioning"}, "running", job_id=42)
    stale_change = await db.transition_instance(instance_id, {"provisioning"}, "delete_failed")

    instance = await db.get_instance(instance_id)
    assert changed
    assert not stale_change
    assert instance["status"] == "running"
    assert instance["job_id"] == 42


async def test_pool_claim_can_adopt_ephemeral_lifecycle(db: Database) -> None:
    await db.create_instance(5000, "10.0.0.100", "pool", "ready")

    claimed = await db.claim_ready_pool(
        "OldOwner/example-app", 42, "pve-lxc-ephemeral-42", mode="ephemeral"
    )

    assert claimed["mode"] == "ephemeral"
    assert claimed["status"] == "provisioning"
    assert claimed["job_id"] == 42


async def test_warm_instance_can_only_be_claimed_once(db: Database) -> None:
    await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "ready",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    claims = await asyncio.gather(
        db.claim_ready_warm("OldOwner/example-app", 42),
        db.claim_ready_warm("OldOwner/example-app", 43),
    )
    claimed = [claim for claim in claims if claim is not None]
    assert len(claimed) == 1
    assert claimed[0]["status"] == "reserved"
    assert claimed[0]["job_id"] in {42, 43}
    assert await db.count_warm_supply("OldOwner/example-app") == 0


async def test_warm_claim_is_case_insensitive_and_rebounds_repo(db: Database) -> None:
    await db.create_instance(
        5000,
        "10.0.0.100",
        "warm",
        "ready",
        "OldOwner/example-app",
        runner_name="pve-lxc-warm-5000",
    )
    claimed = await db.claim_ready_warm("oldowner/example-app", 99)
    assert claimed is not None
    assert claimed["job_id"] == 99
    assert claimed["repository"] == "oldowner/example-app"
    assert await db.count_warm_supply("OldOwner/example-app") == 0


async def test_global_pool_instance_can_only_be_claimed_once(db: Database) -> None:
    await db.create_instance(5000, "10.0.0.100", "pool", "ready")
    claims = await asyncio.gather(
        db.claim_ready_pool("OldOwner/example-app", 42, "pve-lxc-pool-42"),
        db.claim_ready_pool("AcmeOrg/OtherProject", 43, "pve-lxc-pool-43"),
    )
    claimed = [claim for claim in claims if claim is not None]
    assert len(claimed) == 1
    assert claimed[0]["status"] == "provisioning"
    assert claimed[0]["repository"] in {"OldOwner/example-app", "AcmeOrg/OtherProject"}
    assert await db.count_ready_pool() == 0


async def test_last_job_activity_returns_none_without_jobs(db: Database) -> None:
    assert await db.last_job_activity("OldOwner/example-app") is None


async def test_last_job_activity_returns_most_recent_timestamp(db: Database) -> None:
    payload = {
        "action": "queued",
        "repository": {"full_name": "OldOwner/example-app"},
        "workflow_job": {"id": 301, "run_id": 1, "labels": ["self-hosted"]},
    }
    await db.enqueue_job("delivery-activity-1", payload, "ephemeral")
    async with db.connect() as conn:
        await conn.execute("UPDATE jobs SET created_at=100 WHERE job_id=301")
        await conn.commit()

    payload2 = dict(payload)
    payload2["workflow_job"] = {"id": 302, "run_id": 2, "labels": ["self-hosted"]}
    await db.enqueue_job("delivery-activity-2", payload2, "ephemeral")
    async with db.connect() as conn:
        await conn.execute("UPDATE jobs SET created_at=200 WHERE job_id=302")
        await conn.commit()

    assert await db.last_job_activity("OldOwner/example-app") == 200
    assert await db.last_job_activity("oldowner/example-app") == 200  # case-insensitive
    assert await db.last_job_activity("Other/Repo") is None


async def test_list_ready_warm_only_returns_ready_rows_oldest_first(db: Database) -> None:
    await db.create_instance(5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app")
    await db.create_instance(
        5001, "10.0.0.101", "warm", "provisioning", "OldOwner/example-app"
    )
    await db.create_instance(5002, "10.0.0.102", "warm", "ready", "OldOwner/example-app")
    await db.create_instance(5003, "10.0.0.103", "warm", "ready", "Other/Repo")

    rows = await db.list_ready_warm("OldOwner/example-app")

    assert [row["vmid"] for row in rows] == [5000, 5002]

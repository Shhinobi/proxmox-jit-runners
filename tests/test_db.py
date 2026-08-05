from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

import aiosqlite

from orchestrator.db import MIGRATIONS, Database


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp.name) / "state.db"))
        await self.db.initialize()

    async def asyncTearDown(self) -> None:
        self.temp.cleanup()

    async def test_schema_version_reaches_latest_after_initialize(self) -> None:
        row = await self.db._fetchone("SELECT version FROM schema_version")
        assert row is not None
        self.assertEqual(row["version"], len(MIGRATIONS))

    async def test_initialize_is_idempotent(self) -> None:
        await self.db.create_instance(
            5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app"
        )
        await self.db.initialize()  # re-running must not fail or reset data
        instances = await self.db.list_instances()
        self.assertEqual(len(instances), 1)
        row = await self.db._fetchone("SELECT version FROM schema_version")
        assert row is not None
        self.assertEqual(row["version"], len(MIGRATIONS))

    async def test_legacy_database_catches_up_missing_columns(self) -> None:
        """A pre-existing DB with schema_version=1 (the old hardcoded seed) but
        missing later ad-hoc columns must catch up without manual intervention."""
        path = str(Path(self.temp.name) / "legacy.db")
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
        self.assertEqual(row["version"], len(MIGRATIONS))
        columns = {c["name"] for c in await legacy_db._fetchall("PRAGMA table_info(instances)")}
        self.assertIn("use_count", columns)
        self.assertIn("delete_attempts", columns)

    async def test_delivery_is_idempotent(self) -> None:
        self.assertTrue(
            await self.db.record_delivery("delivery-1", "workflow_job", "queued", True, "accepted")
        )
        self.assertFalse(
            await self.db.record_delivery("delivery-1", "workflow_job", "queued", True, "accepted")
        )

    async def test_list_jobs_omits_payload_and_parses_labels(self) -> None:
        payload = {
            "action": "queued",
            "repository": {"full_name": "OldOwner/example-app"},
            "workflow_job": {"id": 99, "run_id": 8, "labels": ["self-hosted", "crossbuild"]},
        }
        await self.db.enqueue_job("delivery-list", payload, "ephemeral")
        jobs = await self.db.list_jobs(limit=10)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["job_id"], 99)
        self.assertEqual(jobs[0]["labels"], ["self-hosted", "crossbuild"])
        self.assertNotIn("payload_json", jobs[0])
        self.assertNotIn("payload", jobs[0])
        self.assertEqual(
            jobs[0]["github_job_url"],
            "https://github.com/OldOwner/example-app/actions/runs/8/job/99",
        )
        filtered = await self.db.list_jobs(
            limit=10, repository="OldOwner/example-app", mode="ephemeral"
        )
        self.assertEqual(len(filtered), 1)
        empty = await self.db.list_jobs(limit=10, repository="OldOwner/Other")
        self.assertEqual(empty, [])

    async def test_list_instances_includes_github_job_links(self) -> None:
        payload = {
            "action": "queued",
            "repository": {"full_name": "OldOwner/example-app"},
            "workflow_job": {"id": 77, "run_id": 12, "labels": ["self-hosted"]},
        }
        await self.db.enqueue_job("delivery-inst-link", payload, "ephemeral")
        await self.db.create_instance(
            5100, "10.0.0.150", "ephemeral", "running", "OldOwner/example-app", 77
        )
        await self.db.create_instance(5101, "10.0.0.151", "warm", "ready")
        rows = await self.db.list_instances()
        by_vmid = {row["vmid"]: row for row in rows}
        busy = by_vmid[5100]
        idle = by_vmid[5101]
        self.assertEqual(
            busy["github_job_url"],
            "https://github.com/OldOwner/example-app/actions/runs/12/job/77",
        )
        self.assertEqual(
            busy["github_run_url"],
            "https://github.com/OldOwner/example-app/actions/runs/12",
        )
        self.assertNotIn("github_job_url", idle)
        self.assertNotIn("job_run_id", busy)
        filtered = await self.db.list_instances(status="running", mode="ephemeral")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["vmid"], 5100)

    async def test_schedule_ready_recycle_marks_pool_and_warm(self) -> None:
        await self.db.create_instance(
            5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app"
        )
        await self.db.create_instance(5001, "10.0.0.101", "pool", "ready")
        result = await self.db.schedule_ready_recycle()
        self.assertEqual(result["scheduled"], 2)
        statuses = {(row["vmid"], row["status"]) for row in await self.db.list_instances()}
        self.assertEqual(statuses, {(5000, "delete_pending"), (5001, "delete_pending")})

    async def test_schedule_ready_recycle_refuses_busy_pool_or_warm_instances(self) -> None:
        await self.db.create_instance(
            5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app"
        )
        await self.db.create_instance(
            5001, "10.0.0.101", "warm", "provisioning", "OldOwner/example-app"
        )
        with self.assertRaises(RuntimeError):
            await self.db.schedule_ready_recycle()

    async def test_schedule_ready_recycle_ignores_busy_ephemeral_or_persistent(self) -> None:
        """A busy job elsewhere in the fleet must not block recycling pool/warm supply."""
        await self.db.create_instance(
            5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app"
        )
        await self.db.create_instance(
            5001, "10.0.0.101", "ephemeral", "running", "OldOwner/example-app", 9
        )
        await self.db.create_instance(
            5002, "10.0.0.102", "persistent", "ready", "OldOwner/example-app"
        )

        result = await self.db.schedule_ready_recycle()

        self.assertEqual(result["scheduled"], 1)
        statuses = {row["vmid"]: row["status"] for row in await self.db.list_instances()}
        self.assertEqual(statuses[5000], "delete_pending")
        self.assertEqual(statuses[5001], "running")
        # persistent is ready but not pool/warm — schedule_ready_recycle never touches it.
        self.assertEqual(statuses[5002], "ready")

    async def test_list_deliveries_returns_newest_first(self) -> None:
        await self.db.record_delivery(
            "d-old", "workflow_job", "queued", False, "repository_not_allowed"
        )
        await self.db.record_delivery("d-new", "workflow_job", "queued", True, "accepted")
        deliveries = await self.db.list_deliveries(limit=10)
        self.assertGreaterEqual(len(deliveries), 2)
        ids = {row["delivery_id"] for row in deliveries}
        self.assertTrue({"d-old", "d-new"}.issubset(ids))
        timestamps = [int(row["received_at"]) for row in deliveries]
        self.assertEqual(timestamps, sorted(timestamps, reverse=True))

    async def test_job_can_only_be_claimed_once(self) -> None:
        payload = {
            "action": "queued",
            "repository": {"full_name": "OldOwner/example-app"},
            "workflow_job": {"id": 42, "run_id": 7, "labels": ["self-hosted"]},
        }
        await self.db.enqueue_job("delivery-2", payload, "ephemeral")
        claimed = await self.db.claim_next_job()
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["attempts"], 1)
        self.assertIsNone(await self.db.claim_next_job())

    async def test_busy_persistent_instance_is_not_reused(self) -> None:
        instance_id = await self.db.create_instance(
            5000, "10.0.0.100", "persistent", "running", "OldOwner/example-app", 42
        )
        self.assertIsNone(await self.db.find_persistent("OldOwner/example-app"))
        await self.db.update_instance(instance_id, "ready", job_id=None)
        self.assertEqual(
            (await self.db.find_persistent("OldOwner/example-app"))["id"], instance_id
        )

    async def test_instance_transition_requires_expected_status(self) -> None:
        instance_id = await self.db.create_instance(5000, "10.0.0.100", "warm", "provisioning")

        changed = await self.db.transition_instance(
            instance_id, {"provisioning"}, "running", job_id=42
        )
        stale_change = await self.db.transition_instance(
            instance_id, {"provisioning"}, "delete_failed"
        )

        instance = await self.db.get_instance(instance_id)
        self.assertTrue(changed)
        self.assertFalse(stale_change)
        self.assertEqual(instance["status"], "running")
        self.assertEqual(instance["job_id"], 42)

    async def test_pool_claim_can_adopt_ephemeral_lifecycle(self) -> None:
        await self.db.create_instance(5000, "10.0.0.100", "pool", "ready")

        claimed = await self.db.claim_ready_pool(
            "OldOwner/example-app", 42, "pve-lxc-ephemeral-42", mode="ephemeral"
        )

        self.assertEqual(claimed["mode"], "ephemeral")
        self.assertEqual(claimed["status"], "provisioning")
        self.assertEqual(claimed["job_id"], 42)

    async def test_warm_instance_can_only_be_claimed_once(self) -> None:
        await self.db.create_instance(
            5000,
            "10.0.0.100",
            "warm",
            "ready",
            "OldOwner/example-app",
            runner_name="pve-lxc-warm-5000",
        )
        claims = await asyncio.gather(
            self.db.claim_ready_warm("OldOwner/example-app", 42),
            self.db.claim_ready_warm("OldOwner/example-app", 43),
        )
        claimed = [claim for claim in claims if claim is not None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0]["status"], "reserved")
        self.assertIn(claimed[0]["job_id"], {42, 43})
        self.assertEqual(await self.db.count_warm_supply("OldOwner/example-app"), 0)

    async def test_warm_claim_is_case_insensitive_and_rebounds_repo(self) -> None:
        await self.db.create_instance(
            5000,
            "10.0.0.100",
            "warm",
            "ready",
            "OldOwner/example-app",
            runner_name="pve-lxc-warm-5000",
        )
        claimed = await self.db.claim_ready_warm("oldowner/example-app", 99)
        self.assertIsNotNone(claimed)
        assert claimed is not None
        self.assertEqual(claimed["job_id"], 99)
        self.assertEqual(claimed["repository"], "oldowner/example-app")
        self.assertEqual(await self.db.count_warm_supply("OldOwner/example-app"), 0)

    async def test_global_pool_instance_can_only_be_claimed_once(self) -> None:
        await self.db.create_instance(5000, "10.0.0.100", "pool", "ready")
        claims = await asyncio.gather(
            self.db.claim_ready_pool("OldOwner/example-app", 42, "pve-lxc-pool-42"),
            self.db.claim_ready_pool("AcmeOrg/OtherProject", 43, "pve-lxc-pool-43"),
        )
        claimed = [claim for claim in claims if claim is not None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0]["status"], "provisioning")
        self.assertIn(
            claimed[0]["repository"],
            {"OldOwner/example-app", "AcmeOrg/OtherProject"},
        )
        self.assertEqual(await self.db.count_ready_pool(), 0)

    async def test_last_job_activity_returns_none_without_jobs(self) -> None:
        self.assertIsNone(await self.db.last_job_activity("OldOwner/example-app"))

    async def test_last_job_activity_returns_most_recent_timestamp(self) -> None:
        payload = {
            "action": "queued",
            "repository": {"full_name": "OldOwner/example-app"},
            "workflow_job": {"id": 301, "run_id": 1, "labels": ["self-hosted"]},
        }
        await self.db.enqueue_job("delivery-activity-1", payload, "ephemeral")
        async with self.db.connect() as conn:
            await conn.execute("UPDATE jobs SET created_at=100 WHERE job_id=301")
            await conn.commit()

        payload2 = dict(payload)
        payload2["workflow_job"] = {"id": 302, "run_id": 2, "labels": ["self-hosted"]}
        await self.db.enqueue_job("delivery-activity-2", payload2, "ephemeral")
        async with self.db.connect() as conn:
            await conn.execute("UPDATE jobs SET created_at=200 WHERE job_id=302")
            await conn.commit()

        self.assertEqual(await self.db.last_job_activity("OldOwner/example-app"), 200)
        self.assertEqual(
            await self.db.last_job_activity("oldowner/example-app"), 200
        )  # case-insensitive
        self.assertIsNone(await self.db.last_job_activity("Other/Repo"))

    async def test_list_ready_warm_only_returns_ready_rows_oldest_first(self) -> None:
        await self.db.create_instance(
            5000, "10.0.0.100", "warm", "ready", "OldOwner/example-app"
        )
        await self.db.create_instance(
            5001, "10.0.0.101", "warm", "provisioning", "OldOwner/example-app"
        )
        await self.db.create_instance(
            5002, "10.0.0.102", "warm", "ready", "OldOwner/example-app"
        )
        await self.db.create_instance(5003, "10.0.0.103", "warm", "ready", "Other/Repo")

        rows = await self.db.list_ready_warm("OldOwner/example-app")

        self.assertEqual([row["vmid"] for row in rows], [5000, 5002])

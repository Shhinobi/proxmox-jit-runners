from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Collection

import aiosqlite


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER NOT NULL
);
INSERT INTO schema_version(version)
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM schema_version);

CREATE TABLE IF NOT EXISTS deliveries (
  delivery_id TEXT PRIMARY KEY,
  event TEXT NOT NULL,
  action TEXT,
  accepted INTEGER NOT NULL,
  reason TEXT NOT NULL,
  received_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  job_id INTEGER PRIMARY KEY,
  delivery_id TEXT NOT NULL,
  run_id INTEGER NOT NULL,
  repository TEXT NOT NULL,
  mode TEXT,
  labels_json TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  runner_name TEXT,
  instance_id INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER
);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status, created_at);

CREATE TABLE IF NOT EXISTS instances (
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
  last_used_at INTEGER
);
CREATE INDEX IF NOT EXISTS instances_status_idx ON instances(status, mode);
"""


class Database:
    def __init__(self, path: str):
        self.path = path

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(self.path)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            yield db
        finally:
            await db.close()

    async def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        async with self.connect() as db:
            await db.executescript(SCHEMA)
            await db.commit()

    async def record_delivery(
        self, delivery_id: str, event: str, action: str, accepted: bool, reason: str
    ) -> bool:
        async with self.connect() as db:
            cursor = await db.execute(
                "INSERT OR IGNORE INTO deliveries VALUES (?, ?, ?, ?, ?, ?)",
                (delivery_id, event, action, int(accepted), reason, int(time.time())),
            )
            await db.commit()
            return cursor.rowcount == 1

    async def enqueue_job(self, delivery_id: str, payload: dict[str, Any], mode: str) -> None:
        now = int(time.time())
        job = payload["workflow_job"]
        await self._execute(
            """
            INSERT INTO jobs(job_id, delivery_id, run_id, repository, mode, labels_json,
                             payload_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
              delivery_id=excluded.delivery_id,
              payload_json=excluded.payload_json,
              labels_json=excluded.labels_json,
              updated_at=excluded.updated_at
            """,
            (
                int(job["id"]),
                delivery_id,
                int(job["run_id"]),
                str(payload["repository"]["full_name"]),
                mode,
                json.dumps(job.get("labels") or []),
                json.dumps(payload, separators=(",", ":")),
                now,
                now,
            ),
        )

    async def claim_next_job(self) -> dict[str, Any] | None:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (
                await db.execute(
                    "SELECT * FROM jobs WHERE status='queued' ORDER BY created_at, job_id LIMIT 1"
                )
            ).fetchone()
            if row is None:
                await db.rollback()
                return None
            now = int(time.time())
            await db.execute(
                "UPDATE jobs SET status='provisioning', attempts=attempts+1, updated_at=? WHERE job_id=?",
                (now, row["job_id"]),
            )
            await db.commit()
            result = dict(row)
            result["status"] = "provisioning"
            result["attempts"] = int(result["attempts"]) + 1
            result["payload"] = json.loads(result.pop("payload_json"))
            result["labels"] = json.loads(result.pop("labels_json"))
            return result

    async def set_job_status(self, job_id: int, status: str, **fields: Any) -> None:
        allowed = {"mode", "runner_name", "instance_id", "error", "completed_at"}
        invalid = set(fields) - allowed
        if invalid:
            raise ValueError(f"invalid job fields: {sorted(invalid)}")
        fields["updated_at"] = int(time.time())
        assignments = ", ".join(f"{name}=?" for name in fields)
        values = [*fields.values(), job_id]
        await self._execute(f"UPDATE jobs SET status=?, {assignments} WHERE job_id=?", (status, *values))

    async def get_job(self, job_id: int) -> dict[str, Any] | None:
        return await self._fetchone("SELECT * FROM jobs WHERE job_id=?", (job_id,))

    async def create_instance(
        self,
        vmid: int,
        ip_address: str,
        mode: str,
        status: str,
        repository: str | None = None,
        job_id: int | None = None,
        runner_name: str | None = None,
    ) -> int:
        now = int(time.time())
        async with self.connect() as db:
            cursor = await db.execute(
                """
                INSERT INTO instances(vmid, ip_address, repository, mode, runner_name, status,
                                      job_id, created_at, updated_at, last_used_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (vmid, ip_address, repository, mode, runner_name, status, job_id, now, now, now),
            )
            await db.commit()
            return int(cursor.lastrowid)

    async def update_instance(self, instance_id: int, status: str, **fields: Any) -> None:
        allowed = {"repository", "job_id", "runner_name", "last_used_at", "mode"}
        invalid = set(fields) - allowed
        if invalid:
            raise ValueError(f"invalid instance fields: {sorted(invalid)}")
        fields["updated_at"] = int(time.time())
        assignments = ", ".join(f"{name}=?" for name in fields)
        await self._execute(
            f"UPDATE instances SET status=?, {assignments} WHERE id=?",
            (status, *fields.values(), instance_id),
        )

    async def transition_instance(
        self,
        instance_id: int,
        expected_statuses: Collection[str],
        status: str,
        **fields: Any,
    ) -> bool:
        """Atomically update an instance only while it is in an expected state."""
        allowed = {"repository", "job_id", "runner_name", "last_used_at", "mode"}
        invalid = set(fields) - allowed
        if invalid:
            raise ValueError(f"invalid instance fields: {sorted(invalid)}")
        expected = tuple(expected_statuses)
        if not expected:
            raise ValueError("expected_statuses must not be empty")
        fields["updated_at"] = int(time.time())
        assignments = ", ".join(f"{name}=?" for name in fields)
        placeholders = ",".join("?" for _ in expected)
        async with self.connect() as db:
            cursor = await db.execute(
                f"UPDATE instances SET status=?, {assignments} "
                f"WHERE id=? AND status IN ({placeholders})",
                (status, *fields.values(), instance_id, *expected),
            )
            await db.commit()
            return cursor.rowcount == 1

    async def find_ready_pool(self) -> dict[str, Any] | None:
        return await self._fetchone(
            "SELECT * FROM instances WHERE mode='pool' AND status='ready' ORDER BY created_at LIMIT 1"
        )

    async def claim_ready_pool(
        self, repository: str, job_id: int, runner_name: str, mode: str = "pool"
    ) -> dict[str, Any] | None:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (
                await db.execute(
                    """SELECT * FROM instances
                       WHERE mode='pool' AND status='ready'
                       ORDER BY created_at LIMIT 1"""
                )
            ).fetchone()
            if row is None:
                await db.rollback()
                return None
            now = int(time.time())
            await db.execute(
                """UPDATE instances
                   SET status='provisioning', repository=?, job_id=?, runner_name=?, mode=?,
                       updated_at=?, last_used_at=?
                   WHERE id=?""",
                (repository, job_id, runner_name, mode, now, now, row["id"]),
            )
            await db.commit()
            result = dict(row)
            result.update(
                status="provisioning",
                repository=repository,
                job_id=job_id,
                runner_name=runner_name,
                mode=mode,
                updated_at=now,
                last_used_at=now,
            )
            return result

    async def claim_ready_warm(self, repository: str, job_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (
                await db.execute(
                    """SELECT * FROM instances
                       WHERE mode='warm' AND repository=? AND status='ready'
                       ORDER BY created_at LIMIT 1""",
                    (repository,),
                )
            ).fetchone()
            if row is None:
                await db.rollback()
                return None
            now = int(time.time())
            await db.execute(
                """UPDATE instances
                   SET status='reserved', job_id=?, updated_at=?, last_used_at=?
                   WHERE id=?""",
                (job_id, now, now, row["id"]),
            )
            await db.commit()
            result = dict(row)
            result.update(status="reserved", job_id=job_id, updated_at=now, last_used_at=now)
            return result

    async def count_warm_supply(self, repository: str) -> int:
        row = await self._fetchone(
            """SELECT COUNT(*) AS total FROM instances
               WHERE mode='warm' AND repository=? AND status IN ('provisioning','ready')""",
            (repository,),
        )
        return int((row or {"total": 0})["total"])

    async def find_persistent(self, repository: str) -> dict[str, Any] | None:
        return await self._fetchone(
            """SELECT * FROM instances
               WHERE mode='persistent' AND repository=? AND status='ready'
               ORDER BY last_used_at DESC LIMIT 1""",
            (repository,),
        )

    async def get_instance(self, instance_id: int) -> dict[str, Any] | None:
        return await self._fetchone("SELECT * FROM instances WHERE id=?", (instance_id,))

    async def find_instance_by_job(self, job_id: int) -> dict[str, Any] | None:
        return await self._fetchone("SELECT * FROM instances WHERE job_id=?", (job_id,))

    async def find_instance_by_runner(
        self, repository: str, runner_name: str
    ) -> dict[str, Any] | None:
        return await self._fetchone(
            "SELECT * FROM instances WHERE repository=? AND runner_name=?",
            (repository, runner_name),
        )

    async def list_instances(self) -> list[dict[str, Any]]:
        return await self._fetchall("SELECT * FROM instances ORDER BY vmid")

    async def count_active(self) -> int:
        row = await self._fetchone(
            "SELECT COUNT(*) AS total FROM instances WHERE status IN ('provisioning','ready','reserved','running') AND mode != 'pool'"
        )
        pool_running = await self._fetchone(
            "SELECT COUNT(*) AS total FROM instances WHERE status IN ('provisioning','running') AND mode = 'pool'"
        )
        return int((row or {"total": 0})["total"]) + int((pool_running or {"total": 0})["total"])

    async def count_ready_pool(self) -> int:
        row = await self._fetchone("SELECT COUNT(*) AS total FROM instances WHERE mode='pool' AND status='ready'")
        return int((row or {"total": 0})["total"])

    async def oldest_queued_job_age(self) -> int:
        row = await self._fetchone(
            "SELECT MIN(created_at) AS created_at FROM jobs WHERE status='queued'"
        )
        created_at = (row or {}).get("created_at")
        return max(0, int(time.time()) - int(created_at)) if created_at is not None else 0

    async def delete_instance(self, instance_id: int) -> None:
        await self._execute("DELETE FROM instances WHERE id=?", (instance_id,))

    async def status_summary(self) -> dict[str, Any]:
        jobs = await self._fetchall("SELECT status, COUNT(*) AS total FROM jobs GROUP BY status")
        instances = await self._fetchall("SELECT mode, status, COUNT(*) AS total FROM instances GROUP BY mode, status")
        return {"jobs": jobs, "instances": instances}

    async def _execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> None:
        async with self.connect() as db:
            await db.execute(sql, parameters)
            await db.commit()

    async def _fetchone(self, sql: str, parameters: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        async with self.connect() as db:
            row = await (await db.execute(sql, parameters)).fetchone()
            return dict(row) if row else None

    async def _fetchall(self, sql: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with self.connect() as db:
            rows = await (await db.execute(sql, parameters)).fetchall()
            return [dict(row) for row in rows]

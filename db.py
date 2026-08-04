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
  last_used_at INTEGER,
  error TEXT
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
            # Lightweight forward-compatible migrations for existing DBs.
            columns = {
                str(row[1])
                for row in await (
                    await db.execute("PRAGMA table_info(instances)")
                ).fetchall()
            }
            if "error" not in columns:
                await db.execute("ALTER TABLE instances ADD COLUMN error TEXT")
            if "use_count" not in columns:
                await db.execute(
                    "ALTER TABLE instances ADD COLUMN use_count INTEGER NOT NULL DEFAULT 0"
                )
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

    # Terminal job states must not be overwritten by late webhooks/provision.
    JOB_TERMINAL_STATUSES = frozenset({"completed", "failed", "rejected"})
    # Allowed previous statuses when moving into each non-terminal status.
    JOB_STATUS_FROM: dict[str, frozenset[str]] = {
        # capacity/drain failures requeue from provisioning; never from terminal.
        "queued": frozenset({"queued", "provisioning"}),
        "provisioning": frozenset({"queued", "provisioning"}),
        "waiting_for_runner": frozenset({"queued", "provisioning", "waiting_for_runner"}),
        "running": frozenset({"queued", "provisioning", "waiting_for_runner", "running"}),
        "completed": frozenset({
            "queued",
            "provisioning",
            "waiting_for_runner",
            "running",
            "completed",
        }),
        "failed": frozenset({
            "queued",
            "provisioning",
            "waiting_for_runner",
            "running",
            "failed",
        }),
        "rejected": frozenset({"queued", "rejected"}),
    }

    async def set_job_status(self, job_id: int, status: str, **fields: Any) -> bool:
        """Update job status with monotonic lifecycle guards.

        Returns True if a row was updated. Late in_progress/provision must not
        revive a job that already completed/failed.
        """
        allowed = {"mode", "runner_name", "instance_id", "error", "completed_at"}
        invalid = set(fields) - allowed
        if invalid:
            raise ValueError(f"invalid job fields: {sorted(invalid)}")
        fields["updated_at"] = int(time.time())
        assignments = ", ".join(f"{name}=?" for name in fields)
        allowed_from = self.JOB_STATUS_FROM.get(status)
        async with self.connect() as db:
            if allowed_from is None:
                cursor = await db.execute(
                    f"UPDATE jobs SET status=?, {assignments} WHERE job_id=?",
                    (status, *fields.values(), job_id),
                )
            else:
                placeholders = ",".join("?" for _ in allowed_from)
                cursor = await db.execute(
                    f"UPDATE jobs SET status=?, {assignments} "
                    f"WHERE job_id=? AND status IN ({placeholders})",
                    (status, *fields.values(), job_id, *allowed_from),
                )
            await db.commit()
            return cursor.rowcount == 1

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
        allowed = {
            "repository",
            "job_id",
            "runner_name",
            "last_used_at",
            "mode",
            "error",
            "use_count",
        }
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
        allowed = {
            "repository",
            "job_id",
            "runner_name",
            "last_used_at",
            "mode",
            "error",
            "use_count",
        }
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
                       WHERE mode='warm' AND LOWER(repository)=LOWER(?) AND status='ready'
                       ORDER BY created_at LIMIT 1""",
                    (repository,),
                )
            ).fetchone()
            if row is None:
                await db.rollback()
                return None
            now = int(time.time())
            # Keep repository field aligned with the policy's canonical casing.
            await db.execute(
                """UPDATE instances
                   SET status='reserved', job_id=?, repository=?, updated_at=?, last_used_at=?
                   WHERE id=?""",
                (job_id, repository, now, now, row["id"]),
            )
            await db.commit()
            result = dict(row)
            result.update(
                status="reserved",
                job_id=job_id,
                repository=repository,
                updated_at=now,
                last_used_at=now,
            )
            return result

    async def count_warm_supply(self, repository: str) -> int:
        row = await self._fetchone(
            """SELECT COUNT(*) AS total FROM instances
               WHERE mode='warm' AND LOWER(repository)=LOWER(?)
                 AND status IN ('provisioning','ready')""",
            (repository,),
        )
        return int((row or {"total": 0})["total"])

    async def count_warm_ready(self, repository: str) -> int:
        row = await self._fetchone(
            """SELECT COUNT(*) AS total FROM instances
               WHERE mode='warm' AND LOWER(repository)=LOWER(?)
                 AND status='ready'""",
            (repository,),
        )
        return int((row or {"total": 0})["total"])

    async def rebind_warm_repository(self, from_repository: str, to_repository: str) -> int:
        """Point warm instances at a renamed policy repository (exact match)."""
        if from_repository == to_repository:
            return 0
        async with self.connect() as db:
            cursor = await db.execute(
                """UPDATE instances
                   SET repository=?, updated_at=?
                   WHERE mode='warm' AND LOWER(repository)=LOWER(?)
                     AND status IN ('provisioning','ready','reserved','running')""",
                (to_repository, int(time.time()), from_repository),
            )
            await db.commit()
            return int(cursor.rowcount or 0)

    async def find_persistent(self, repository: str) -> dict[str, Any] | None:
        return await self._fetchone(
            """SELECT * FROM instances
               WHERE mode='persistent' AND LOWER(repository)=LOWER(?) AND status='ready'
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

    async def list_jobs(
        self,
        limit: int = 50,
        status: str | None = None,
        repository: str | None = None,
        mode: str | None = None,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 200))
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if repository:
            clauses.append("LOWER(repository) = LOWER(?)")
            params.append(repository)
        if mode:
            clauses.append("mode = ?")
            params.append(mode)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = await self._fetchall(
            f"""
            SELECT job_id, delivery_id, run_id, repository, mode, labels_json, status,
                   runner_name, instance_id, attempts, error, created_at, updated_at, completed_at
            FROM jobs
            {where}
            ORDER BY created_at DESC, job_id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        now = int(time.time())
        for row in rows:
            raw_labels = row.pop("labels_json", "[]")
            try:
                row["labels"] = json.loads(raw_labels) if raw_labels else []
            except (TypeError, json.JSONDecodeError):
                row["labels"] = []
            repo = str(row.get("repository") or "")
            run_id = row.get("run_id")
            job_id = row.get("job_id")
            if repo and run_id:
                row["github_run_url"] = f"https://github.com/{repo}/actions/runs/{run_id}"
                if job_id:
                    row["github_job_url"] = (
                        f"https://github.com/{repo}/actions/runs/{run_id}/job/{job_id}"
                    )
            created = row.get("created_at")
            completed = row.get("completed_at")
            if created is not None:
                end = int(completed) if completed is not None else now
                row["duration_seconds"] = max(0, end - int(created))
            else:
                row["duration_seconds"] = None
        return rows

    async def list_instances(
        self,
        status: str | None = None,
        repository: str | None = None,
        mode: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("i.status = ?")
            params.append(status)
        if repository:
            clauses.append("LOWER(i.repository) = LOWER(?)")
            params.append(repository)
        if mode:
            clauses.append("i.mode = ?")
            params.append(mode)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = await self._fetchall(
            f"""
            SELECT
              i.id, i.vmid, i.ip_address, i.repository, i.mode, i.runner_name,
              i.status, i.job_id, i.created_at, i.updated_at, i.last_used_at,
              i.error,
              j.run_id AS job_run_id,
              j.repository AS job_repository
            FROM instances i
            LEFT JOIN jobs j ON j.job_id = i.job_id
            {where}
            ORDER BY i.vmid
            """,
            tuple(params),
        )
        for row in rows:
            job_id = row.get("job_id")
            run_id = row.pop("job_run_id", None)
            job_repo = row.pop("job_repository", None)
            repo = str(job_repo or row.get("repository") or "")
            if repo and run_id and job_id:
                row["github_run_url"] = f"https://github.com/{repo}/actions/runs/{run_id}"
                row["github_job_url"] = (
                    f"https://github.com/{repo}/actions/runs/{run_id}/job/{job_id}"
                )
        return rows

    async def list_deliveries(
        self,
        limit: int = 50,
        reason: str | None = None,
        accepted: bool | None = None,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 200))
        clauses: list[str] = []
        params: list[Any] = []
        if reason:
            clauses.append("reason = ?")
            params.append(reason)
        if accepted is not None:
            clauses.append("accepted = ?")
            params.append(int(accepted))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        return await self._fetchall(
            f"""
            SELECT delivery_id, event, action, accepted, reason, received_at
            FROM deliveries
            {where}
            ORDER BY received_at DESC, delivery_id DESC
            LIMIT ?
            """,
            tuple(params),
        )

    async def recent_job_durations(self, limit: int = 30) -> list[dict[str, Any]]:
        """Completed jobs with duration for dashboard charts."""
        limit = max(1, min(int(limit), 100))
        rows = await self._fetchall(
            """
            SELECT job_id, repository, mode, status, created_at, completed_at
            FROM jobs
            WHERE completed_at IS NOT NULL AND created_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        for row in rows:
            row["duration_seconds"] = max(0, int(row["completed_at"]) - int(row["created_at"]))
        rows.reverse()  # chronological for charts
        return rows

    async def schedule_ready_recycle(self) -> dict[str, Any]:
        """Mark ready pool/warm instances for deletion so reconcile can rebuild them.

        Refuses when any non-ready instance exists (busy provision/run path).
        """
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            busy = await (
                await db.execute(
                    "SELECT vmid, mode, status FROM instances WHERE status NOT IN ('ready')"
                )
            ).fetchall()
            if busy:
                await db.rollback()
                details = [
                    {"vmid": int(row["vmid"]), "mode": row["mode"], "status": row["status"]}
                    for row in busy
                ]
                raise RuntimeError(f"refusing recycle with non-ready instances: {details}")
            cursor = await db.execute(
                """
                UPDATE instances
                SET status='delete_pending', updated_at=?
                WHERE status='ready' AND mode IN ('pool', 'warm')
                """,
                (int(time.time()),),
            )
            await db.commit()
            return {"scheduled": int(cursor.rowcount)}

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

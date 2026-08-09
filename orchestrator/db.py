from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Collection
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import aiosqlite

from .models import InstanceListItem, JobListItem

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER NOT NULL
);
INSERT INTO schema_version(version)
SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM schema_version);

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


async def _instance_columns(db: aiosqlite.Connection) -> set[str]:
    return {
        str(row[1]) for row in await (await db.execute("PRAGMA table_info(instances)")).fetchall()
    }


async def _migration_001_instances_error(db: aiosqlite.Connection) -> None:
    """Safety net for databases older than `error` joining the base CREATE TABLE."""
    if "error" not in await _instance_columns(db):
        await db.execute("ALTER TABLE instances ADD COLUMN error TEXT")


async def _migration_002_instances_use_count(db: aiosqlite.Connection) -> None:
    if "use_count" not in await _instance_columns(db):
        await db.execute("ALTER TABLE instances ADD COLUMN use_count INTEGER NOT NULL DEFAULT 0")


async def _migration_003_instances_delete_attempts(db: aiosqlite.Connection) -> None:
    if "delete_attempts" not in await _instance_columns(db):
        await db.execute(
            "ALTER TABLE instances ADD COLUMN delete_attempts INTEGER NOT NULL DEFAULT 0"
        )


async def _migration_004_instances_delete_not_before(db: aiosqlite.Connection) -> None:
    """Absolute retry deadline for teardown, replacing an age-vs-`updated_at` check.

    The old backoff compared `now - updated_at` against an exponential window,
    but *any* `update_instance` call bumps `updated_at` — so an unrelated write
    silently reset the backoff clock. An absolute deadline cannot be reset by
    accident. NULL means "eligible now", which is what a fresh `delete_pending`
    wants: teardown is the hot path that frees capacity after every job and
    must not be delayed on its first attempt.
    """
    if "delete_not_before" not in await _instance_columns(db):
        await db.execute("ALTER TABLE instances ADD COLUMN delete_not_before INTEGER")


async def _migration_005_pending_deregistrations(db: aiosqlite.Connection) -> None:
    """Work queue for GitHub registrations we could not remove at teardown time.

    Teardown of a local container must never be blocked on an external API, but
    the registration still has to go, or GitHub keeps assigning jobs to a runner
    that no longer exists. So the destroy proceeds and the obligation is
    recorded here, drained once connectivity returns.
    """
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS pending_deregistrations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          repository TEXT NOT NULL,
          runner_name TEXT NOT NULL,
          vmid INTEGER,
          reason TEXT,
          attempts INTEGER NOT NULL DEFAULT 0,
          not_before INTEGER,
          last_error TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(repository, runner_name)
        )
        """
    )


async def _migration_006_instances_ct_missing_since(db: aiosqlite.Connection) -> None:
    """First time a pass observed no container for this row.

    Persisted rather than held in memory so a restart does not reset the
    two-strike confirmation and let a transient API blip reap live state.
    """
    if "ct_missing_since" not in await _instance_columns(db):
        await db.execute("ALTER TABLE instances ADD COLUMN ct_missing_since INTEGER")


async def _job_columns(db: aiosqlite.Connection) -> set[str]:
    return {str(row[1]) for row in await (await db.execute("PRAGMA table_info(jobs)")).fetchall()}


async def _migration_007_job_phase_timestamps(db: aiosqlite.Connection) -> None:
    """Enough timestamps to show where a job's wall time actually went.

    Previously only `created_at`/`completed_at` existed, so the dashboard could
    plot total duration and nothing else — a 200s job that spent 180s waiting
    for a cold clone looked identical to one that spent 180s building. These
    three columns split it into queue / provision / run without inventing data.
    """
    columns = await _job_columns(db)
    if "provisioned_at" not in columns:
        await db.execute("ALTER TABLE jobs ADD COLUMN provisioned_at INTEGER")
    if "started_at" not in columns:
        await db.execute("ALTER TABLE jobs ADD COLUMN started_at INTEGER")
    if "start_source" not in columns:
        await db.execute("ALTER TABLE jobs ADD COLUMN start_source TEXT")


# Applied in order, once each, tracked by schema_version.version. Every
# migration must stay idempotent (checks before it alters) since an existing
# database's on-disk shape may already be ahead of what its stored version
# claims — these ran unconditionally before schema_version was wired up.
#
# NOTE: every new instances column must also be added to the explicit SELECT in
# `list_instances`, or reconcile silently reads it as missing.
MIGRATIONS: tuple[Callable[[aiosqlite.Connection], Awaitable[None]], ...] = (
    _migration_001_instances_error,
    _migration_002_instances_use_count,
    _migration_003_instances_delete_attempts,
    _migration_004_instances_delete_not_before,
    _migration_005_pending_deregistrations,
    _migration_006_instances_ct_missing_since,
    _migration_007_job_phase_timestamps,
)


def _job_phases(row: dict[str, Any], *, now: int) -> dict[str, int]:
    """Split a job's wall time into the segments we actually recorded.

    Only boundaries we observed are used; anything unknown collapses to zero
    rather than being estimated, so a bar is never wider than the truth.
    """
    created = row.get("created_at")
    if created is None:
        return {}
    end = int(row.get("completed_at") or now)
    provisioned = row.get("provisioned_at")
    started = row.get("started_at")

    phases: dict[str, int] = {}
    cursor = int(created)
    if provisioned is not None:
        phases["queued"] = max(0, int(provisioned) - cursor)
        cursor = int(provisioned)
    if started is not None:
        phases["provisioning"] = max(0, int(started) - cursor)
        cursor = int(started)
    label = "failed" if row.get("status") == "failed" else "running"
    phases[label] = max(0, end - cursor)
    return {k: v for k, v in phases.items() if v > 0}


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
            row = await (await db.execute("SELECT version FROM schema_version")).fetchone()
            current_version = int(row[0]) if row else 0
            for index, migration in enumerate(MIGRATIONS, start=1):
                if index <= current_version:
                    continue
                await migration(db)
                await db.execute("UPDATE schema_version SET version=?", (index,))
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
        "completed": frozenset(
            {
                "queued",
                "provisioning",
                "waiting_for_runner",
                "running",
                "completed",
            }
        ),
        "failed": frozenset(
            {
                "queued",
                "provisioning",
                "waiting_for_runner",
                "running",
                "failed",
            }
        ),
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
            row_id = cursor.lastrowid
            if row_id is None:
                raise RuntimeError("insert did not return a row id")
            return row_id

    async def update_instance(self, instance_id: int, status: str, **fields: Any) -> None:
        allowed = {
            "repository",
            "job_id",
            "runner_name",
            "last_used_at",
            "mode",
            "error",
            "use_count",
            "delete_attempts",
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
            "delete_attempts",
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

    async def bump_delete_failure(
        self,
        instance_id: int,
        *,
        expected_statuses: Collection[str],
        error: str,
        not_before: int,
    ) -> int:
        """Record a failed teardown: mark delete_failed, count the attempt, set the deadline.

        This exists because the previous recovery path used
        `transition_instance({"deleting"}, ...)`, which only matched when the
        exception happened *after* the row had already moved to `deleting`. Any
        failure raised earlier — notably the GitHub deregistration probe, i.e.
        the exact shape of the thin-pool exhaustion incident — matched zero rows and
        silently did nothing, so `delete_attempts` never advanced and the
        exponential backoff never engaged even once.

        `expected_statuses` must therefore include the row's status on *entry*
        as well as `deleting`. It deliberately stays narrow: if another task
        concurrently claimed the instance (e.g. moved it to `running`), this
        matches nothing and returns 0 rather than stomping the claim.

        Returns the new attempt count, or 0 if no row matched.
        """
        expected = tuple(expected_statuses)
        if not expected:
            raise ValueError("expected_statuses must not be empty")
        placeholders = ",".join("?" for _ in expected)
        async with self.connect() as db:
            cursor = await db.execute(
                f"""
                UPDATE instances
                   SET status='delete_failed',
                       error=?,
                       updated_at=?,
                       delete_attempts=delete_attempts+1,
                       delete_not_before=?
                 WHERE id=? AND status IN ({placeholders})
                """,
                (error, int(time.time()), int(not_before), instance_id, *expected),
            )
            if cursor.rowcount != 1:
                await db.commit()
                return 0
            row = await (
                await db.execute("SELECT delete_attempts FROM instances WHERE id=?", (instance_id,))
            ).fetchone()
            await db.commit()
            return int(row[0]) if row else 0

    async def set_delete_deadline(self, instance_id: int, not_before: int | None) -> None:
        """Defer (or clear) the next teardown attempt without counting a failure.

        Used when GitHub gives a real answer of "still busy": that is not a
        failure of ours, so it must not consume the attempt budget — but it
        must still stop the row being retried on every 60s pass.
        """
        await self._execute(
            "UPDATE instances SET delete_not_before=? WHERE id=?",
            (None if not_before is None else int(not_before), instance_id),
        )

    # --- deferred GitHub deregistrations ---------------------------------

    async def enqueue_deregistration(
        self, repository: str, runner_name: str, *, vmid: int | None, reason: str
    ) -> None:
        """Record that a runner registration still needs removing from GitHub.

        Idempotent: re-queuing the same runner keeps the original row (and its
        attempt count) rather than resetting the backoff.
        """
        now = int(time.time())
        await self._execute(
            """
            INSERT INTO pending_deregistrations
                (repository, runner_name, vmid, reason, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(repository, runner_name) DO NOTHING
            """,
            (repository, runner_name, vmid, reason, now, now),
        )

    async def list_due_deregistrations(self, *, now: int, limit: int = 25) -> list[dict[str, Any]]:
        return await self._fetchall(
            """
            SELECT * FROM pending_deregistrations
             WHERE not_before IS NULL OR not_before <= ?
             ORDER BY created_at
             LIMIT ?
            """,
            (now, limit),
        )

    async def bump_deregistration(self, row_id: int, *, error: str, not_before: int) -> None:
        await self._execute(
            """
            UPDATE pending_deregistrations
               SET attempts=attempts+1, last_error=?, not_before=?, updated_at=?
             WHERE id=?
            """,
            (error[:500], not_before, int(time.time()), row_id),
        )

    async def delete_deregistration(self, row_id: int) -> None:
        await self._execute("DELETE FROM pending_deregistrations WHERE id=?", (row_id,))

    async def delete_deregistration_by_runner(self, repository: str, runner_name: str) -> None:
        """Clear the obligation when something else already removed the runner."""
        await self._execute(
            "DELETE FROM pending_deregistrations WHERE repository=? AND runner_name=?",
            (repository, runner_name),
        )

    async def count_pending_deregistrations(self) -> int:
        row = await self._fetchone("SELECT COUNT(*) AS total FROM pending_deregistrations")
        return int((row or {"total": 0})["total"])

    async def purge_expired_deregistrations(self, *, older_than: int) -> int:
        """Drop obligations too old to be worth retrying.

        GitHub garbage-collects offline ephemeral registrations on its own, so a
        row that has been unsatisfiable for days is noise, not a task.
        """
        async with self.connect() as db:
            cursor = await db.execute(
                "DELETE FROM pending_deregistrations WHERE created_at < ?", (older_than,)
            )
            await db.commit()
            return int(cursor.rowcount or 0)

    async def set_ct_missing_since(self, instance_id: int, when: int | None) -> None:
        """Record (or clear) the first pass that saw no container for this row."""
        await self._execute(
            "UPDATE instances SET ct_missing_since=? WHERE id=?", (when, instance_id)
        )

    async def delete_backlog(self) -> dict[str, Any]:
        """Counts and oldest age for instances awaiting teardown."""
        rows = await self._fetchall(
            """
            SELECT status, COUNT(*) AS total, MIN(updated_at) AS oldest
              FROM instances
             WHERE status IN ('delete_pending','delete_failed','deleting')
             GROUP BY status
            """
        )
        return {str(r["status"]): r for r in rows}

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

    async def list_ready_warm(self, repository: str) -> list[dict[str, Any]]:
        """Ready (idle, safe to retire) warm instances for a repo, oldest first."""
        return await self._fetchall(
            """SELECT * FROM instances
               WHERE mode='warm' AND LOWER(repository)=LOWER(?) AND status='ready'
               ORDER BY created_at""",
            (repository,),
        )

    async def last_job_activity(self, repository: str) -> int | None:
        """Timestamp of the most recent job request for this repo, any mode/outcome."""
        row = await self._fetchone(
            "SELECT MAX(created_at) AS latest FROM jobs WHERE LOWER(repository)=LOWER(?)",
            (repository,),
        )
        latest = (row or {}).get("latest")
        return int(latest) if latest is not None else None

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

    async def count_active_for_repo(self, repository: str) -> int:
        """Mirrors count_active() but scoped to one repository's non-pool instances.

        Pool instances are generic/unclaimed at this point (no repository yet),
        so they're excluded the same way count_active() excludes idle ones.
        """
        row = await self._fetchone(
            """SELECT COUNT(*) AS total FROM instances
               WHERE status IN ('provisioning','ready','reserved','running')
                 AND mode != 'pool' AND LOWER(repository) = LOWER(?)""",
            (repository,),
        )
        return int((row or {"total": 0})["total"])

    async def count_ready_pool(self) -> int:
        row = await self._fetchone(
            "SELECT COUNT(*) AS total FROM instances WHERE mode='pool' AND status='ready'"
        )
        return int((row or {"total": 0})["total"])

    async def oldest_queued_job_age(self) -> int:
        row = await self._fetchone(
            "SELECT MIN(created_at) AS created_at FROM jobs WHERE status='queued'"
        )
        created_at = (row or {}).get("created_at")
        return max(0, int(time.time()) - int(created_at)) if created_at is not None else 0

    async def find_instance_by_vmid(self, vmid: int) -> dict[str, Any] | None:
        return await self._fetchone("SELECT * FROM instances WHERE vmid=?", (vmid,))

    async def delete_instance(self, instance_id: int) -> None:
        await self._execute("DELETE FROM instances WHERE id=?", (instance_id,))

    async def status_summary(self) -> dict[str, Any]:
        jobs = await self._fetchall("SELECT status, COUNT(*) AS total FROM jobs GROUP BY status")
        instances = await self._fetchall(
            "SELECT mode, status, COUNT(*) AS total FROM instances GROUP BY mode, status"
        )
        return {"jobs": jobs, "instances": instances}

    async def set_job_start_context(
        self, job_id: int, *, source: str | None = None, provisioned: bool = False
    ) -> None:
        """Record how a job got its runner, and when provisioning finished.

        Written once and never overwritten: a requeue must not make a job that
        originally waited for a cold clone look like it landed on a warm runner.
        """
        sets: list[str] = []
        params: list[Any] = []
        if source is not None:
            sets.append("start_source=COALESCE(start_source, ?)")
            params.append(source)
        if provisioned:
            sets.append("provisioned_at=COALESCE(provisioned_at, ?)")
            params.append(int(time.time()))
        if not sets:
            return
        params.append(job_id)
        await self._execute(f"UPDATE jobs SET {', '.join(sets)} WHERE job_id=?", tuple(params))

    async def mark_job_started(self, job_id: int) -> None:
        await self._execute(
            "UPDATE jobs SET started_at=COALESCE(started_at, ?) WHERE job_id=?",
            (int(time.time()), job_id),
        )

    async def list_jobs(
        self,
        limit: int = 50,
        status: str | None = None,
        repository: str | None = None,
        mode: str | None = None,
    ) -> list[JobListItem]:
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
                   runner_name, instance_id, attempts, error, created_at, updated_at,
                   completed_at, provisioned_at, started_at, start_source
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
            row["phases"] = _job_phases(row, now=now)
        return cast(list[JobListItem], rows)

    async def list_instances(
        self,
        status: str | None = None,
        repository: str | None = None,
        mode: str | None = None,
    ) -> list[InstanceListItem]:
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
              i.error, i.delete_attempts, i.delete_not_before, i.use_count,
              i.ct_missing_since,
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
        return cast(list[InstanceListItem], rows)

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

        Refuses when a pool/warm instance is not ready (busy provision/run
        path) — those are the only rows this recycles, so a busy ephemeral or
        persistent instance elsewhere in the fleet must not block it.
        """
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            busy = await (
                await db.execute(
                    """SELECT vmid, mode, status FROM instances
                       WHERE status != 'ready' AND mode IN ('pool', 'warm')"""
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

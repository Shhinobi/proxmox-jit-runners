"""Typed shapes for the dashboard-facing read paths in `db.py`.

`Database.list_jobs`/`list_instances` build enriched, dashboard-ready rows
(computed URLs, durations, parsed labels) with a stable, well-defined shape —
unlike the raw-row methods (`get_job`, `claim_next_job`, etc.), which return
whatever columns the underlying `SELECT *` happens to include and don't share
one consistent shape. These two get typed first; the rest stay `dict[str,
Any]` until they're worth modeling individually.
"""

from __future__ import annotations

from typing import TypedDict


class JobListItem(TypedDict, total=False):
    job_id: int
    delivery_id: str
    run_id: int
    repository: str
    mode: str | None
    status: str
    runner_name: str | None
    instance_id: int | None
    attempts: int
    error: str | None
    created_at: int
    updated_at: int
    completed_at: int | None
    labels: list[str]
    github_run_url: str
    github_job_url: str
    duration_seconds: int | None


class InstanceListItem(TypedDict, total=False):
    id: int
    vmid: int
    ip_address: str
    repository: str | None
    mode: str
    runner_name: str | None
    status: str
    job_id: int | None
    created_at: int
    updated_at: int
    last_used_at: int | None
    error: str | None
    delete_attempts: int
    github_run_url: str
    github_job_url: str

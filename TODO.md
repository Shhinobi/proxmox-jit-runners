# TODO / Future Work

## Done in the resilience pass

Following the thin-pool exhaustion of the incident: teardown decoupled from
GitHub with a deferred-deregistration queue; an absolute retry deadline (the
old backoff was resettable by any unrelated write, and never engaged at all
when the failure came from the GitHub probe); a circuit breaker; layered HTTP
timeouts; a runner-list cache that collapses the repeated `list_runners` calls
a pass used to make; CT-existence reaping; pool health-checks; storage
admission control; per-phase reconcile isolation with a pass budget; an
independent dependency probe loop; cached readiness; graceful shutdown; warm
creation moved off the reconcile critical path; drift detection/repair; a token
on the mutating API; and a full dashboard rewrite.

## Known, low-priority

### Intermittent aiosqlite teardown warning in the test suite

Roughly one run in three, some test emits
`PytestUnhandledThreadExceptionWarning: RuntimeError: Event loop is closed`
from an `aiosqlite` worker thread. It is a teardown artifact, not a product
fault: a task cancelled inside a database call still completes its query on a
worker thread, which then posts the result back with `call_soon_threadsafe`
after pytest-asyncio has closed the loop.

`Orchestrator.stop()` was reworked to drain before cancelling and to yield the
loop afterwards, which reduced the frequency (it used to be about two runs in
three) but cannot eliminate it: a cancel can always land mid-call, and the
completion is posted from a thread we do not own. The production impact is nil
— the process is exiting.

If this ever needs to be zero (e.g. before adding `-W error` to CI), the real
fix is on the data layer: a single long-lived connection, or an explicit
`Database.close()` awaited during shutdown, rather than the connection-per-call
model. That is the "reuse a single SQLite connection" item below.

## Blocked on the operator (not code)

Deployment-specific detail — real hostnames, addresses, secret-store paths —
deliberately lives outside this repository. Operators keep their own notes;
this section states only what has to be true, not where.

Securing the admin API is **not** in this list any more: it is done from the
dashboard on first run, with no host access required.

### Confirm the storage endpoint on your Proxmox version

The admission guard reads `total`, `used` and `avail` (bytes) from
`GET /nodes/{node}/storage/{storage}/status`. Verified against PVE 9.2.6 with
an `lvmthin` storage, and pinned by
`tests/test_proxmox.py::test_storage_status_parses_a_real_pve_response`. A
mismatch is reported as an `error`-level `storage` event rather than silently
failing open, but it is worth one `pvesh get` on your own version before
trusting the floor.

Note when tuning: projected demand is `max_active x rootfs_gb`, i.e. every
container filling its whole root filesystem. Thin provisioning makes real usage
far lower — a runner measured at roughly a quarter of its provisioned rootfs in
practice — so `storage.total_budget_gb` is opt-in and off by default. The
protection that acts on reality is `min_free_gb` / `max_used_percent`.

### Host-level housekeeping this application cannot do

- **Periodic `fstrim` for LXC guests on thin-provisioned storage.** Blocks freed
  inside a guest are never returned to the pool without it, so "used" drifts
  upward indefinitely regardless of what this orchestrator does. In the incident
  that motivated this work, roughly half the exhausted space was phantom.
- **Watch egress to the GitHub API from wherever this runs.** The orchestrator
  now survives losing it — containers are still torn down locally and
  deregistrations queued — but it cannot repair the network path.

## Decided against (revisit only if circumstances change)

### Decompose `Orchestrator` into separate classes

`orchestrator/service.py`'s `Orchestrator` class is ~1800 lines with many
responsibilities (webhook ingestion, job/instance state machines, warm/pool
capacity management, GitHub reconciliation). Splitting warm/pool logic into
a `WarmPoolManager` and reconcile logic into an `InstanceReconciler` was on
the table, but mapping the actual dependencies first showed that
`_reserve_instance`, `_destroy_instance`, `_installation`, and the
`_schedule_*` methods are called **both ways** — from inside the warm/pool
code and from `webhook()`/`_provision()`/`_complete_job()` outside it. A
class split would need a back-reference to the whole `Orchestrator` anyway,
so it would move code to another file without actually reducing coupling.
Only the safe, mechanical dedup was done (see git log: runner naming,
`_fail_and_retire`, warm-runner credential/transition unification).

If this gets revisited, do the dependency mapping again first — the
untangling work (making `_reserve_instance`/`_destroy_instance` free
functions or a separate collaborator with its own narrow interface) is a
prerequisite, not a side effect of "just moving methods."

### Make `EventLog.emit()` (orchestrator/eventlog.py) non-blocking

`_persist_locked()`/`_compact_locked()` do synchronous file I/O (including a
full-file rewrite every 50 events) while holding a `threading.Lock`, called
from `emit()`, which runs on the async request-handling path. A proper fix
is either:
- making `emit()` async (cascades through every `self.log(...)` call site
  in `service.py` — dozens of them), or
- offloading to a background writer thread fed by a queue — but this makes
  `emit()` return before the write lands, and several existing tests in
  `tests/test_eventlog.py` assert "emit, then immediately read back from a
  *new* `EventLog` instance pointed at the same file" — those would become
  flaky without also rewriting them to wait/flush.

At this project's actual scale (homelab, a handful of events per minute),
the blocking cost is negligible, so this wasn't worth the risk today.
Revisit if event volume grows enough for it to show up in latency.

## Incremental follow-ups (intentionally left for a future pass)

### Extend typed domain models past `list_jobs`/`list_instances`

`orchestrator/models.py` currently has `JobListItem`/`InstanceListItem`,
used only by `Database.list_jobs`/`list_instances` — the two read methods
with one stable, well-defined output shape. The raw-row methods
(`get_job`, `get_instance`, `find_instance_by_job`, `claim_next_job`, etc.)
still return `dict[str, Any]`. Typing those next is straightforward but
touches many call sites in `service.py`; do it module by module, the same
way `list_jobs`/`list_instances` were done, and let mypy's `disallow_untyped_defs`
tighten alongside it (see `[tool.mypy]` in `pyproject.toml`).

### ~~Convert the remaining dashboard tabs to Alpine.js~~ — done

Done in the UI rework. The dashboard is now `orchestrator/ui/`
with `css/tokens.css` (the design system), `css/app.css`, and `js/app.js`
(one Alpine component plus the ported policies store). The hand-rolled
`innerHTML` renderers are gone, along with the manual `asset_version`
constant — cache-busting now derives from asset mtimes.

The Policies store was carried over verbatim rather than rewritten: it was
already the most carefully validated part of the old UI. It is re-skinned
by the new tokens and gained a live projected-disk readout beside the warm
budget hint, so raising `max_active` shows its storage cost before saving.

### Reuse a single SQLite connection instead of one per call

`Database` (orchestrator/db.py) opens and closes a new `aiosqlite`
connection on every one of its ~35 methods. Simple and safe for SQLite's
single-writer model, but not efficient on a hot path like
`claim_next_job()`. Not a correctness issue today; worth revisiting if
connection overhead ever shows up in profiling.

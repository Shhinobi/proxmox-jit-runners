# TODO / Future Work

Notes from the 2026 modernization pass (uv/ruff/mypy, pytest migration,
typed models, dashboard rewrite) — what was deliberately left out, and why,
plus the natural next steps for continuing that work. Kept here instead of
only in commit messages/chat history so it isn't lost.

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

### Convert the remaining dashboard tabs to Alpine.js

Only the Policies tab (the most complex, highest-value one) was converted
from hand-rolled `innerHTML` rendering to Alpine.js. Instances/Jobs/
Deliveries/Cache still use the original vanilla render functions in
`orchestrator/ui/app.js` (`renderInstances`, `renderJobs`,
`renderDeliveries`, `renderCacheInventory`). They coexist fine with Alpine
today; converting them is optional cleanup, not a bug fix — do it tab by
tab, same as Policies, each as its own reviewable change.

### Reuse a single SQLite connection instead of one per call

`Database` (orchestrator/db.py) opens and closes a new `aiosqlite`
connection on every one of its ~35 methods. Simple and safe for SQLite's
single-writer model, but not efficient on a hot path like
`claim_next_job()`. Not a correctness issue today; worth revisiting if
connection overhead ever shows up in profiling.

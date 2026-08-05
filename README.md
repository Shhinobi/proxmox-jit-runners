# proxmox-jit-runners

A self-hosted GitHub Actions runner orchestrator: it receives `workflow_job`
webhooks, validates policy and signature, clones a golden LXC template on a
Proxmox node, attaches a persistent build cache, and injects a single-use
just-in-time (JIT) runner registration. State is SQLite/WAL; every webhook
delivery is deduplicated by `X-GitHub-Delivery`.

A single GitHub App key can serve multiple installations. Each repository
policy picks an installation of scope `repository` (personal account) or
`organization` (with a runner group), and incoming webhooks are validated
against that binding.

## What it does

- **Ephemeral runners on demand** — clones a Proxmox LXC template per job,
  injects a JIT-scoped credential, destroys the container after the job.
- **Warm pool** — keeps N pre-registered, idle runners per repository so a
  job can start in seconds instead of waiting for a clone. Optionally scales
  the warm target down after a configurable idle window and back up on the
  next request (`warm_idle_capacity` / `warm_idle_after_seconds`).
- **Global pool** — a shared reserve of generic, unregistered clones that any
  repository's ephemeral jobs can claim as a fallback, cutting cold-start
  latency without a dedicated warm reserve per repo.
- **Persistent mode** — a long-lived runner per repository, recycled between
  jobs instead of destroyed.
- **Per-repository policy** — required labels, allowed modes/events,
  concurrency ceiling (`max_concurrent`), a resource profile
  (`profiles:`, selectable per repo), all hot-reloadable through the API/UI
  without a restart.
- **Reconcile loop** — a continuous background pass that self-heals: retries
  provisioning/cleanup with backoff, replenishes warm/pool supply, clears
  stuck clones and zombie GitHub registrations, and enforces capacity.
- **Dashboard** — a no-build-step HTML/JS UI with live updates over SSE,
  policy editing, cache inventory, and admin actions (drain/resume/reconcile).

## Quick start

```bash
uv sync
cp orchestrator/default-config.yaml /etc/runner-orchestrator/config.yaml   # edit: GitHub App, Proxmox, broker, repositories
uv run python -m orchestrator   # or: uv run uvicorn orchestrator.app:app --host 0.0.0.0 --port 8080
```

Requires [`uv`](https://docs.astral.sh/uv/); `uv sync` creates `.venv` and
installs pinned dependencies from `uv.lock`.

Configuration is a single YAML file (`RUNNER_ORCHESTRATOR_CONFIG`, default
`/etc/runner-orchestrator/config.yaml`) covering the GitHub App, the Proxmox
API endpoint, a cache-management SSH broker, capacity limits, resource
profiles, and per-repository policy. See `orchestrator/default-config.yaml`
for every field with inline documentation. Repository policy and global
capacity can also be edited live through `PUT /api/v1/admin/policies`
(persisted to a writable override file, hot-reloaded, no restart needed).

The orchestrator only talks to two other systems it does not implement
itself:
- A **Proxmox VE** API endpoint (clones/starts/stops/destroys the LXC template).
- A restricted-command **SSH broker** running on the Proxmox host, for cache
  mount/attach/cleanup operations that shouldn't go through the general
  Proxmox API.

## Dashboard / API

- Live snapshot: `GET /api/v1/snapshot`, SSE stream: `GET /api/v1/events`
- Listings: `instances|jobs|deliveries|policies|metrics-summary|job-durations`
  with `?repository=&status=&mode=` filters
- Admin: `POST /api/v1/admin/{drain,resume,reconcile,recycle-ready}`
- Policies: `PUT /api/v1/admin/policies` (hot-reload, validates capacity
  budget and profile references before persisting)
- Cache: `GET /api/v1/cache`, `POST /api/v1/admin/cache/{purge,rename,gc}`
- Metrics: `GET /metrics` (Prometheus), `GET /health/live`, `GET /health/ready`
- Webhook: `POST /webhooks/github` (HMAC-validated `workflow_job` events)

The bundled dashboard (`orchestrator/ui/`) does not implement its own
authentication — put it behind a reverse proxy / auth layer for anything
beyond a trusted network.

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy orchestrator
```

Tests use fakes for the GitHub/Proxmox/broker clients (no live cluster
needed) plus an in-memory/temp-file SQLite database. `uv run pre-commit
install` wires ruff/mypy into a pre-commit hook.

## Origin

Extracted from a private homelab infrastructure monorepo
([`infra-cluster`](https://github.com/KyoNinja/infra-cluster)), where it's
consumed as a git submodule for a reference production deployment (Proxmox
cluster, systemd service, SSH-key-based deploy). That deployment tooling is
cluster-specific and stays in the consuming repository; this repository is
the standalone, reusable application.

## License

MIT — see [LICENSE](LICENSE).

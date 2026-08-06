# proxmox-jit-runners

Self-hosted GitHub Actions runners for a homelab Proxmox cluster — spun up
fresh for each job and thrown away when it's done, instead of sitting
around as an always-on VM.

## Why this exists

GitHub's own hosted runners are easy but limited: capped build minutes, no
custom hardware, and no shared build cache between runs. The usual fix —
register a long-lived VM as a self-hosted runner — solves that, but that
VM now sits idle most of the day, and every job that runs on it can see
whatever the previous job left behind.

This project takes a different approach. It listens for GitHub's
`workflow_job` webhook and, when a job needs a runner, clones a fresh
Proxmox LXC container from a template ("golden image"), registers it with
GitHub for exactly one job — a "just-in-time" (JIT) runner — and destroys
the container once the job finishes. Every job gets a clean environment,
and nothing sits around burning electricity between jobs. Optionally, a
small pool of pre-cloned containers can be kept ready (a "warm pool") so a
job starts in a couple of seconds instead of waiting ~30s for a clone.

A background loop constantly double-checks reality against what's
configured — replenishing the warm pool, retrying anything that failed,
and cleaning up containers or GitHub runner registrations that got stuck —
so a crash or a flaky webhook doesn't leave things in a broken state.

## What it can do

- **Ephemeral runners on demand** — clone → register → run the job →
  destroy. The default, simplest mode.
- **Warm pool** — keep a few runners per repository pre-registered and
  idle, so jobs start almost instantly. Can automatically scale down after
  a period of inactivity and back up on the next request.
- **Global pool** — a shared reserve of generic, not-yet-registered clones
  that any repository can claim as a fallback, without needing a dedicated
  warm pool for every repo.
- **Persistent mode** — for repos that want a long-lived runner recycled
  between jobs instead of destroyed each time.
- **Per-repository policy** — required labels, which modes/events are
  allowed, a concurrency ceiling, and a resource profile (CPU/RAM/disk),
  all editable live from the dashboard — no restart needed.
- **Self-healing reconcile loop** — the background pass described above.
- **A live dashboard** — see what's running, browse job/webhook history,
  edit policy, inspect the build cache, and trigger admin actions (drain,
  resume, force-reconcile), all updating live over SSE.

## Before you start

You'll need:
- A **Proxmox VE** cluster with a "golden" LXC container template already
  built (the thing that gets cloned for every job).
- A **GitHub App** (not a personal access token) with permission to manage
  self-hosted runners for the repos/orgs you want to serve.
- An SSH-reachable **cache broker** on the Proxmox host — a small
  restricted-command script (not included here) that mounts/cleans a
  shared build cache directory for each container. See `orchestrator/broker.py`
  for the exact commands it needs to support.
- [`uv`](https://docs.astral.sh/uv/) to run it.

## Quick start

```bash
uv sync
cp orchestrator/default-config.yaml /etc/runner-orchestrator/config.yaml   # edit: GitHub App, Proxmox, broker, repositories
uv run python -m orchestrator   # or: uv run uvicorn orchestrator.app:app --host 0.0.0.0 --port 8080
```

`uv sync` creates a `.venv` and installs the pinned dependencies from
`uv.lock` — no manual virtualenv/pip steps needed.

Everything is configured through one YAML file (path from the
`RUNNER_ORCHESTRATOR_CONFIG` env var, default
`/etc/runner-orchestrator/config.yaml`): the GitHub App, the Proxmox API
endpoint, the cache broker, capacity limits, resource profiles, and policy
for each repository. `orchestrator/default-config.yaml` documents every
field inline — start from a copy of that file. Repository policy and
global capacity can also be changed live from the dashboard (or
`PUT /api/v1/admin/policies`), which writes a small override file and
hot-reloads — no restart required either way.

The orchestrator talks to exactly two other systems, neither of which it
implements itself:
- The **Proxmox VE API** — to clone, start, stop, and destroy containers.
- The **SSH cache broker** on the Proxmox host — a narrow, restricted set
  of commands for mounting/cleaning the shared build cache, kept separate
  from the general Proxmox API on purpose.

## Dashboard & API

| What | Where |
|---|---|
| Live dashboard | `/ui/` |
| Live snapshot / SSE stream | `GET /api/v1/snapshot`, `GET /api/v1/events` |
| Instances, jobs, webhook deliveries | `GET /api/v1/{instances,jobs,deliveries}` (filter with `?repository=&status=&mode=`) |
| Admin actions | `POST /api/v1/admin/{drain,resume,reconcile,recycle-ready}` |
| Policy editing | `PUT /api/v1/admin/policies` (validates before saving, hot-reloads) |
| Build cache | `GET /api/v1/cache`, `POST /api/v1/admin/cache/{purge,rename,gc}` |
| Health & metrics | `GET /health/live`, `GET /health/ready`, `GET /metrics` (Prometheus) |
| GitHub webhook | `POST /webhooks/github` (HMAC-signed `workflow_job` events) |

The dashboard doesn't implement its own login — put it behind a reverse
proxy or auth layer (e.g. Authelia, Tailscale) unless it's only reachable
on a trusted network.

## Development

```bash
uv sync --dev
uv run pytest              # test suite (uses fakes for GitHub/Proxmox/broker — no live cluster needed)
uv run ruff check .        # lint
uv run ruff format .       # format
uv run mypy orchestrator   # type-check
```

`uv run pre-commit install` wires ruff and mypy into a pre-commit hook.
See [TODO.md](TODO.md) for known gaps and planned follow-ups.

## Origin

Extracted from a private homelab infrastructure monorepo
([`infra-cluster`](https://github.com/KyoNinja/infra-cluster)), where it's
consumed as a git submodule for a reference production deployment (Proxmox
cluster, systemd service, SSH-key-based deploy). That deployment tooling is
cluster-specific and stays in the consuming repository; this repository is
the standalone, reusable application.

## License

MIT — see [LICENSE](LICENSE).

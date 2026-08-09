"""Shared in-memory fakes for GitHub/Proxmox/broker clients.

Used by both test_service.py and test_app.py so the two suites exercise the
same fake behavior instead of drifting apart.

Failure injection
-----------------
Every fake method routes through `FakeClientBase._gate(name)`, so a test can
declare failures instead of monkeypatching bound methods::

    fake_github.fail("find_runner", httpx.ConnectError("boom"))
    fake_proxmox.fail("destroy", ProxmoxError("locked"), times=2)
    fake_github.slow("list_runners", 0.2)

`FakeGitHub(unreachable=True)` makes every call raise `GitHubUnavailable`,
matching the real client's contract that transport failures are wrapped before
they reach a caller. The raw `httpx` types are exercised against the real
client in `test_github.py`, which is where the classification actually lives.

The older style (assigning over a bound method) still works; nothing here
removes it.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import httpx

from orchestrator.clock import Clock
from orchestrator.github import GitHubUnavailable


@dataclass
class FailurePlan:
    """A scripted failure for one fake method."""

    exc: Exception
    after_calls: int = 0
    """Succeed this many times before the failure starts."""
    times: int | None = None
    """Fail this many times, then succeed again. None = fail forever."""
    latency: float = 0.0
    seen: int = field(default=0, init=False)

    def should_raise(self) -> bool:
        self.seen += 1
        if self.seen <= self.after_calls:
            return False
        if self.times is None:
            return True
        return self.seen <= self.after_calls + self.times


class FakeClientBase:
    """Call counting plus declarative failure/latency injection."""

    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()
        self._plans: dict[str, FailurePlan] = {}
        self._latency: dict[str, float] = {}
        self.unreachable: bool = False
        self.unreachable_exc: Exception | None = None

    def fail(
        self,
        method: str,
        exc: Exception,
        *,
        after_calls: int = 0,
        times: int | None = None,
        latency: float = 0.0,
    ) -> None:
        self._plans[method] = FailurePlan(
            exc=exc, after_calls=after_calls, times=times, latency=latency
        )

    def slow(self, method: str, latency: float) -> None:
        self._latency[method] = latency

    def clear_failures(self) -> None:
        self._plans.clear()
        self._latency.clear()
        self.unreachable = False

    async def _gate(self, method: str) -> None:
        """First line of every fake method: count, delay, maybe raise."""
        self.calls[method] += 1
        latency = self._latency.get(method, 0.0)
        if latency:
            await asyncio.sleep(latency)
        if self.unreachable:
            raise self._unreachable_error(method)
        plan = self._plans.get(method)
        if plan is None:
            return
        if plan.latency:
            await asyncio.sleep(plan.latency)
        if plan.should_raise():
            raise plan.exc

    def _unreachable_error(self, method: str) -> Exception:
        return self.unreachable_exc or httpx.ConnectError(f"simulated: {method} unreachable")


class FakeClock(Clock):
    """A clock that only moves when a test tells it to.

    `sleep()` advances instead of waiting, so code under test that backs off can
    be driven to completion without the suite actually sleeping.
    """

    def __init__(self, start: float = 1_780_000_000.0) -> None:
        self._wall = start
        self._mono = 0.0
        self.slept: list[float] = []

    def now(self) -> int:
        return int(self._wall)

    def monotonic(self) -> float:
        return self._mono

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.advance(seconds)
        await asyncio.sleep(0)  # still yield, so other tasks can interleave

    def advance(self, seconds: float) -> None:
        self._wall += seconds
        self._mono += seconds


class FakeGitHub(FakeClientBase):
    def __init__(self, *, busy: bool = False, unreachable: bool = False) -> None:
        super().__init__()
        self._busy = busy
        self.unreachable = unreachable
        self.runners: dict[str, int] = {}
        self.runner_states: dict[str, dict[str, Any]] = {}
        self._next_id = 10
        self.deleted: list[int] = []

    @property
    def available(self) -> bool:
        """Mirrors GitHubClient.available: reconcile skips GitHub-only phases."""
        return not self.unreachable

    @property
    def breaker(self) -> Any:
        """Minimal stand-in for the real CircuitBreaker, for metrics wiring."""
        state = "open" if self.unreachable else "closed"
        return SimpleNamespace(snapshot=lambda: {"state": state})

    def _unreachable_error(self, method: str) -> Exception:
        """Honour the real client's contract.

        `GitHubClient` converts every transport failure into `GitHubUnavailable`
        before it reaches a caller, so a fake that leaked raw `httpx` errors
        would be testing a shape service code never actually sees. The raw
        exception types are exercised where they belong, in `test_github.py`,
        against the real client.
        """
        return self.unreachable_exc or GitHubUnavailable(f"simulated: {method} unreachable")

    @property
    def busy(self) -> bool:
        return self._busy

    @busy.setter
    def busy(self, value: bool) -> None:
        """Write-through, so the historical `FakeGitHub(busy=True)` still means
        "every runner is busy" while per-runner overrides remain possible."""
        self._busy = value
        for state in self.runner_states.values():
            state["busy"] = value

    def register(self, name: str, runner_id: int | None = None) -> int:
        rid = runner_id if runner_id is not None else self._next_id
        self.runners[name] = rid
        self.runner_states.setdefault(name, {"status": "online", "busy": self._busy})
        self._next_id = max(self._next_id, rid + 1)
        return rid

    def set_runner_state(self, name: str, *, status: str = "online", busy: bool = False) -> None:
        self.runner_states[name] = {"status": status, "busy": busy}

    def _state(self, name: str) -> dict[str, Any]:
        return self.runner_states.get(name, {"status": "online", "busy": self._busy})

    async def aclose(self) -> None:
        pass

    async def get_workflow_run(self, *_: Any) -> dict[str, str]:
        await self._gate("get_workflow_run")
        return {"event": "workflow_dispatch"}

    async def generate_jit(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
        await self._gate("generate_jit")
        # Positional: installation, repository, name, labels
        name = _args[2] if len(_args) >= 3 else str(_kwargs.get("name") or "runner")
        self.register(str(name))
        return {"encoded_jit_config": "jit"}

    def _runner_payload(self, name: str, rid: int) -> dict[str, object]:
        state = self._state(name)
        return {
            "id": rid,
            "name": name,
            "status": state["status"],
            "busy": state["busy"],
            "labels": [
                {"name": "self-hosted"},
                {"name": "proxmox-lxc"},
                {"name": "crossbuild"},
                {"name": "mode-ephemeral"},
            ],
        }

    async def list_runners(self, *_args: Any, **_kwargs: Any) -> list[dict[str, object]]:
        await self._gate("list_runners")
        return [self._runner_payload(name, rid) for name, rid in self.runners.items()]

    async def find_runner(self, *_args: Any, **_kwargs: Any) -> dict[str, object] | None:
        await self._gate("find_runner")
        name = str(_args[2] if len(_args) >= 3 else _kwargs.get("name") or "")
        runner_id = self.runners.get(name)
        if runner_id is None:
            return None
        payload = self._runner_payload(name, runner_id)
        payload.pop("name")
        return payload

    async def delete_runner(self, *_args: Any, **_kwargs: Any) -> None:
        await self._gate("delete_runner")
        runner_id = _args[2] if len(_args) >= 3 else _kwargs.get("runner_id")
        self.deleted.append(int(runner_id))
        for name, rid in list(self.runners.items()):
            if rid == int(runner_id):
                del self.runners[name]
                self.runner_states.pop(name, None)

    async def try_delete_runner(self, *_args: Any, **_kwargs: Any) -> bool:
        runner_id = _args[2] if len(_args) >= 3 else _kwargs.get("runner_id")
        name = next((n for n, rid in self.runners.items() if rid == int(runner_id)), None)
        if name is not None and self._state(name)["busy"]:
            await self._gate("try_delete_runner")
            return False
        if name is None and self._busy:
            await self._gate("try_delete_runner")
            return False
        await self._gate("try_delete_runner")
        await self.delete_runner(*_args, **_kwargs)
        return True

    @staticmethod
    def runner_url(*_: Any) -> str:
        return "https://github.com/OldOwner/example-app"


class FakeProxmox(FakeClientBase):
    def __init__(self) -> None:
        super().__init__()
        self.clones: list[int] = []
        self.clone_calls: list[tuple[Any, ...]] = []
        self.starts: list[int] = []
        self.destroyed: list[int] = []
        self.running: set[int] = set()
        self.existing: set[int] = set()
        """VMIDs the hypervisor reports. Populated by clone(), cleared by destroy().

        Left empty by default so the historical `cluster_vmids() -> set()`
        behaviour is preserved for tests that create instance rows directly.
        """
        self.storage: dict[str, Any] = {
            "total": 349 * 1024**3,
            "used": 120 * 1024**3,
            "avail": 229 * 1024**3,
        }

    async def aclose(self) -> None:
        pass

    async def cluster_vmids(self) -> set[int]:
        await self._gate("cluster_vmids")
        return set(self.existing)

    async def list_node_cts(self) -> list[dict[str, Any]]:
        await self._gate("list_node_cts")
        return [
            {"vmid": vmid, "status": "running" if vmid in self.running else "stopped"}
            for vmid in sorted(self.existing)
        ]

    async def storage_status(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        await self._gate("storage_status")
        return dict(self.storage)

    async def clone(self, vmid: int, *rest: Any) -> None:
        await self._gate("clone")
        self.clones.append(vmid)
        self.clone_calls.append((vmid, *rest))
        self.running.add(vmid)
        self.existing.add(vmid)

    async def start(self, vmid: int) -> None:
        await self._gate("start")
        self.starts.append(vmid)
        self.running.add(vmid)

    async def status(self, vmid: int) -> dict[str, str]:
        await self._gate("status")
        return {"status": "running" if vmid in self.running else "stopped"}

    async def destroy(self, vmid: int) -> None:
        await self._gate("destroy")
        self.destroyed.append(vmid)
        self.running.discard(vmid)
        self.existing.discard(vmid)


class FakeBroker(FakeClientBase):
    def __init__(self) -> None:
        super().__init__()
        self.attached: list[int] = []
        self.credentials: list[dict[str, object]] = []
        self.cleaned: list[int] = []
        self.allowlist: list[str] = []
        self.renamed: list[tuple[str, str]] = []
        self.purged: list[str] = []
        # Set by tests to exercise apply_repository_policies' partial-failure paths.
        self.fail_rename = False
        self.fail_purge = False

    async def aclose(self) -> None:
        pass

    async def cache_ready(self, _repository: str) -> bool:
        await self._gate("cache_ready")
        return True

    async def attach_cache(self, vmid: int, _: str) -> None:
        await self._gate("attach_cache")
        self.attached.append(vmid)

    async def inject(self, _: int, credential: dict[str, object]) -> None:
        await self._gate("inject")
        self.credentials.append(credential)

    async def clean_work(self, vmid: int) -> str:
        await self._gate("clean_work")
        self.cleaned.append(vmid)
        return "cleaned avail_mib=9000"

    async def cache_gc(self, min_age_days: int = 0) -> str:
        await self._gate("cache_gc")
        return f"gc purged=0 skipped=0 bytes=0 min_age_days={min_age_days}"

    async def sync_allowlist(self, repositories: list[str]) -> str:
        await self._gate("sync_allowlist")
        self.allowlist = sorted(repositories)
        return f"synced {len(self.allowlist)}"

    async def cache_rename(self, src: str, dst: str) -> str:
        await self._gate("cache_rename")
        if self.fail_rename:
            raise RuntimeError(f"broker unreachable: rename {src} -> {dst}")
        self.renamed.append((src, dst))
        return f"renamed {src} -> {dst}"

    async def cache_purge(self, repository: str) -> str:
        await self._gate("cache_purge")
        if self.fail_purge:
            raise RuntimeError(f"broker unreachable: purge {repository}")
        self.purged.append(repository)
        return f"purged {repository}"

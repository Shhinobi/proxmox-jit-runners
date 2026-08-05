"""Shared in-memory fakes for GitHub/Proxmox/broker clients.

Used by both test_service.py and test_app.py so the two suites exercise the
same fake behavior instead of drifting apart.
"""

from __future__ import annotations

from typing import Any


class FakeGitHub:
    def __init__(self, *, busy: bool = False) -> None:
        self.busy = busy
        self.runners: dict[str, int] = {}
        self._next_id = 10
        self.deleted: list[int] = []

    def register(self, name: str, runner_id: int | None = None) -> int:
        rid = runner_id if runner_id is not None else self._next_id
        self.runners[name] = rid
        self._next_id = max(self._next_id, rid + 1)
        return rid

    async def aclose(self) -> None:
        pass

    async def get_workflow_run(self, *_: Any) -> dict[str, str]:
        return {"event": "workflow_dispatch"}

    async def generate_jit(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
        # Positional: installation, repository, name, labels
        name = _args[2] if len(_args) >= 3 else str(_kwargs.get("name") or "runner")
        self.register(str(name))
        return {"encoded_jit_config": "jit"}

    async def list_runners(self, *_args: Any, **_kwargs: Any) -> list[dict[str, object]]:
        return [
            {
                "id": rid,
                "name": name,
                "status": "online",
                "busy": self.busy,
                "labels": [
                    {"name": "self-hosted"},
                    {"name": "proxmox-lxc"},
                    {"name": "crossbuild"},
                    {"name": "mode-ephemeral"},
                ],
            }
            for name, rid in self.runners.items()
        ]

    async def find_runner(self, *_args: Any, **_kwargs: Any) -> dict[str, object] | None:
        name = str(_args[2] if len(_args) >= 3 else _kwargs.get("name") or "")
        runner_id = self.runners.get(name)
        if runner_id is None:
            return None
        return {
            "id": runner_id,
            "status": "online",
            "busy": self.busy,
            "labels": [
                {"name": "self-hosted"},
                {"name": "proxmox-lxc"},
                {"name": "crossbuild"},
                {"name": "mode-ephemeral"},
            ],
        }

    async def delete_runner(self, *_args: Any, **_kwargs: Any) -> None:
        runner_id = _args[2] if len(_args) >= 3 else _kwargs.get("runner_id")
        self.deleted.append(int(runner_id))
        for name, rid in list(self.runners.items()):
            if rid == int(runner_id):
                del self.runners[name]

    async def try_delete_runner(self, *_args: Any, **_kwargs: Any) -> bool:
        if self.busy:
            return False
        await self.delete_runner(*_args, **_kwargs)
        return True

    @staticmethod
    def runner_url(*_: Any) -> str:
        return "https://github.com/OldOwner/example-app"


class FakeProxmox:
    def __init__(self) -> None:
        self.clones: list[int] = []
        self.clone_calls: list[tuple[Any, ...]] = []
        self.starts: list[int] = []
        self.destroyed: list[int] = []
        self.running: set[int] = set()

    async def aclose(self) -> None:
        pass

    async def cluster_vmids(self) -> set[int]:
        return set()

    async def clone(self, vmid: int, *rest: Any) -> None:
        self.clones.append(vmid)
        self.clone_calls.append((vmid, *rest))
        self.running.add(vmid)

    async def start(self, vmid: int) -> None:
        self.starts.append(vmid)
        self.running.add(vmid)

    async def status(self, vmid: int) -> dict[str, str]:
        return {"status": "running" if vmid in self.running else "stopped"}

    async def destroy(self, vmid: int) -> None:
        self.destroyed.append(vmid)
        self.running.discard(vmid)


class FakeBroker:
    def __init__(self) -> None:
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

    async def attach_cache(self, vmid: int, _: str) -> None:
        self.attached.append(vmid)

    async def inject(self, _: int, credential: dict[str, object]) -> None:
        self.credentials.append(credential)

    async def clean_work(self, vmid: int) -> str:
        self.cleaned.append(vmid)
        return "cleaned avail_mib=9000"

    async def cache_gc(self, min_age_days: int = 0) -> str:
        return f"gc purged=0 skipped=0 bytes=0 min_age_days={min_age_days}"

    async def sync_allowlist(self, repositories: list[str]) -> str:
        self.allowlist = sorted(repositories)
        return f"synced {len(self.allowlist)}"

    async def cache_rename(self, src: str, dst: str) -> str:
        if self.fail_rename:
            raise RuntimeError(f"broker unreachable: rename {src} -> {dst}")
        self.renamed.append((src, dst))
        return f"renamed {src} -> {dst}"

    async def cache_purge(self, repository: str) -> str:
        if self.fail_purge:
            raise RuntimeError(f"broker unreachable: purge {repository}")
        self.purged.append(repository)
        return f"purged {repository}"

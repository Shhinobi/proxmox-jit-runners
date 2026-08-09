from __future__ import annotations

import asyncio
import json
from typing import Any

from .config import BrokerConfig, CacheConfig
from .telemetry import span


class BrokerError(RuntimeError):
    pass


class BrokerClient:
    def __init__(self, config: BrokerConfig, cache: CacheConfig):
        self.config = config
        self.cache = cache

    def _ssh(self, command: str) -> list[str]:
        return [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            f"UserKnownHostsFile={self.config.known_hosts_file}",
            "-i",
            self.config.key_file,
            f"{self.config.user}@{self.config.host}",
            command,
        ]

    # ConnectTimeout only bounds the SSH handshake; a hung remote script (or a
    # an unreachable Proxmox host) would otherwise stall the calling task forever.
    _COMMAND_TIMEOUT_SECONDS = 120.0

    async def _run(self, command: str, payload: bytes = b"") -> str:
        operation = command.split(maxsplit=1)[0]
        with span("runner.broker", {"runner.broker.operation": operation}):
            process = await asyncio.create_subprocess_exec(
                *self._ssh(command),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(payload), timeout=self._COMMAND_TIMEOUT_SECONDS
                )
            except TimeoutError as exc:
                process.kill()
                await process.wait()
                raise BrokerError(
                    f"broker command {operation!r} timed out after {self._COMMAND_TIMEOUT_SECONDS}s"
                ) from exc
            if process.returncode != 0:
                raise BrokerError(
                    f"broker command failed ({process.returncode}): {stderr.decode()[:500]}"
                )
            return stdout.decode().strip()

    async def attach_cache(self, vmid: int, repository: str) -> None:
        await self._run(f"attach-cache {vmid} {repository}")

    async def inject(self, vmid: int, credential: dict[str, object]) -> None:
        payload = json.dumps(credential, separators=(",", ":")).encode("utf-8")
        await self._run(f"inject {vmid}", payload)

    async def clean_work(self, vmid: int) -> str:
        """Clear Actions job workdirs before re-JIT recycle on a warm CT."""
        return await self._run(f"clean-work {vmid}")

    async def cache_ready(self, repository: str) -> bool:
        output = await self._run(f"cache-ready {repository}")
        return output == "ready"

    async def sync_allowlist(self, repositories: list[str]) -> str:
        """Replace the broker allowlist with the given OWNER/REPO entries."""
        if not repositories:
            raise BrokerError("sync-allowlist requires at least one repository")
        # Keep argv simple and deterministic for the forced-command broker.
        repos = " ".join(sorted({repo.strip() for repo in repositories if repo.strip()}))
        if not repos:
            raise BrokerError("sync-allowlist requires at least one repository")
        return await self._run(f"sync-allowlist {repos}")

    async def cache_list(self) -> dict[str, Any]:
        raw = await self._run("cache-list")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BrokerError(f"cache-list returned non-JSON: {raw[:300]}") from exc
        if not isinstance(data, dict):
            raise BrokerError("cache-list returned invalid payload")
        return data

    async def cache_purge(self, repository: str) -> str:
        return await self._run(f"cache-purge {repository}")

    async def cache_rename(self, from_repository: str, to_repository: str) -> str:
        return await self._run(f"cache-rename {from_repository} {to_repository}")

    async def cache_gc(self, min_age_days: int = 0) -> str:
        return await self._run(f"cache-gc {int(min_age_days)}")

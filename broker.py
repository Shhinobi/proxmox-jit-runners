from __future__ import annotations

import asyncio
import json

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
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=10",
            "-o", f"UserKnownHostsFile={self.config.known_hosts_file}",
            "-i", self.config.key_file,
            f"{self.config.user}@{self.config.host}",
            command,
        ]

    async def _run(self, command: str, payload: bytes = b"") -> str:
        operation = command.split(maxsplit=1)[0]
        with span("runner.broker", {"runner.broker.operation": operation}):
            process = await asyncio.create_subprocess_exec(
                *self._ssh(command),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate(payload)
            if process.returncode != 0:
                raise BrokerError(f"broker command failed ({process.returncode}): {stderr.decode()[:500]}")
            return stdout.decode().strip()

    async def attach_cache(self, vmid: int, repository: str) -> None:
        await self._run(f"attach-cache {vmid} {repository}")

    async def inject(self, vmid: int, credential: dict[str, object]) -> None:
        payload = json.dumps(credential, separators=(",", ":")).encode("utf-8")
        await self._run(f"inject {vmid}", payload)

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

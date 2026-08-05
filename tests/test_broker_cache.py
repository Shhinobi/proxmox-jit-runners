from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any
from unittest import mock

from orchestrator.api import gc_orphan_cache, list_cache_inventory, purge_cache_repository, rename_cache_repository
from orchestrator.broker import BrokerClient, BrokerError
from orchestrator.config import BrokerConfig, CacheConfig


class BrokerCacheClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_list_parses_json(self) -> None:
        client = BrokerClient(BrokerConfig(), CacheConfig())
        payload = {
            "root": "/srv/runner-cache",
            "total_bytes": 100,
            "used_bytes": 40,
            "free_bytes": 60,
            "usage_percent": 40,
            "max_usage_percent": 80,
            "quota_gb": 40,
            "repositories": [
                {
                    "repository": "JustShinobi/NinjaCrawler",
                    "size_bytes": 10,
                    "mtime": 1,
                    "allowed": True,
                    "orphan": False,
                }
            ],
        }

        async def fake_run(command: str, body: bytes = b"") -> str:
            self.assertEqual(command, "cache-list")
            return json.dumps(payload)

        with mock.patch.object(client, "_run", side_effect=fake_run):
            data = await client.cache_list()
        self.assertEqual(data["usage_percent"], 40)
        self.assertEqual(data["repositories"][0]["repository"], "JustShinobi/NinjaCrawler")

    async def test_cache_list_rejects_non_json(self) -> None:
        client = BrokerClient(BrokerConfig(), CacheConfig())

        async def fake_run(command: str, payload: bytes = b"") -> str:
            return "not-json"

        with mock.patch.object(client, "_run", side_effect=fake_run):
            with self.assertRaises(BrokerError):
                await client.cache_list()

    async def test_run_times_out_and_kills_hung_process(self) -> None:
        client = BrokerClient(BrokerConfig(), CacheConfig())

        class _FakeProcess:
            def __init__(self) -> None:
                self.killed = False
                self.waited = False

            async def communicate(self, _payload: bytes = b"") -> tuple[bytes, bytes]:
                await asyncio.sleep(10)
                return b"", b""  # pragma: no cover - never reached, timeout fires first

            def kill(self) -> None:
                self.killed = True

            async def wait(self) -> int:
                self.waited = True
                return -9

        fake_process = _FakeProcess()

        async def fake_create_subprocess_exec(*_args: Any, **_kwargs: Any) -> _FakeProcess:
            return fake_process

        with mock.patch(
            "orchestrator.broker.asyncio.create_subprocess_exec",
            side_effect=fake_create_subprocess_exec,
        ), mock.patch.object(BrokerClient, "_COMMAND_TIMEOUT_SECONDS", 0.05):
            with self.assertRaises(BrokerError) as ctx:
                await client._run("cache-list")

        self.assertTrue(fake_process.killed)
        self.assertTrue(fake_process.waited)
        self.assertIn("timed out", str(ctx.exception))


class CacheApiHelperTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_cache_inventory_adds_gc_days(self) -> None:
        class _Broker:
            async def cache_list(self) -> dict[str, Any]:
                return {"root": "/srv/runner-cache", "repositories": []}

        class _Orch:
            broker = _Broker()
            settings = type("S", (), {"cache": CacheConfig(gc_orphan_days=7)})()

        data = await list_cache_inventory(_Orch())  # type: ignore[arg-type]
        self.assertEqual(data["gc_orphan_days"], 7)

    async def test_purge_and_rename_and_gc(self) -> None:
        calls: list[tuple[str, tuple]] = []

        class _Broker:
            async def cache_purge(self, repository: str) -> str:
                calls.append(("purge", (repository,)))
                return f"purged {repository}"

            async def cache_rename(self, src: str, dst: str) -> str:
                calls.append(("rename", (src, dst)))
                return f"renamed {src} -> {dst}"

            async def cache_gc(self, days: int) -> str:
                calls.append(("gc", (days,)))
                return f"gc purged=1 min_age_days={days}"

        class _Orch:
            broker = _Broker()
            settings = type(
                "S",
                (),
                {
                    "cache": CacheConfig(gc_orphan_days=7),
                    "repositories": {"JustShinobi/NinjaCrawler": object()},
                },
            )()

            def log(self, *args: Any, **kwargs: Any) -> None:
                return None

        purge = await purge_cache_repository(_Orch(), "MetalDevops/NinjaCrawler")  # type: ignore[arg-type]
        self.assertTrue(purge["ok"])
        self.assertFalse(purge["active_policy"])

        rename = await rename_cache_repository(
            _Orch(), "MetalDevops/NinjaCrawler", "JustShinobi/NinjaCrawler"  # type: ignore[arg-type]
        )
        self.assertIn("renamed", rename["message"])

        gc = await gc_orphan_cache(_Orch(), min_age_days=0)  # type: ignore[arg-type]
        self.assertEqual(gc["min_age_days"], 0)
        self.assertEqual(calls[-1], ("gc", (0,)))

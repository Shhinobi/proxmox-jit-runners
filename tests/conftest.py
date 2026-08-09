from __future__ import annotations

import os

# Must be set before `orchestrator.app` is imported: `create_app()` runs at
# module import and calls `configure_telemetry()`, which builds an OTLP gRPC
# exporter pointed at 127.0.0.1:4317. With nothing listening there, the
# exporter's shutdown flush retries with backoff and the suite takes minutes
# instead of seconds (measured: 15s with this set, >5min without).
os.environ.setdefault("OTEL_SDK_DISABLED", "true")

from pathlib import Path  # noqa: E402
from typing import Any, cast  # noqa: E402

import pytest  # noqa: E402

from orchestrator.db import Database  # noqa: E402
from orchestrator.service import Orchestrator  # noqa: E402
from tests.fakes import FakeBroker, FakeGitHub, FakeProxmox  # noqa: E402
from tests.helpers import settings as make_settings  # noqa: E402


@pytest.fixture
async def db(tmp_path: Path) -> Database:
    database = Database(str(tmp_path / "state.db"))
    await database.initialize()
    return database


@pytest.fixture
def fake_github() -> FakeGitHub:
    return FakeGitHub()


@pytest.fixture
def fake_proxmox() -> FakeProxmox:
    return FakeProxmox()


@pytest.fixture
def fake_broker() -> FakeBroker:
    return FakeBroker()


@pytest.fixture
async def orchestrator(
    db: Database,
    fake_github: FakeGitHub,
    fake_proxmox: FakeProxmox,
    fake_broker: FakeBroker,
):
    """An Orchestrator wired to the default test settings and in-memory fakes.

    Tests that need different repository/capacity policy override
    `orchestrator.settings = replace(orchestrator.settings, ...)` after
    injection, same as `apply_repository_policies` does at runtime.
    """
    orch = Orchestrator(
        make_settings(), db, cast(Any, fake_github), cast(Any, fake_proxmox), cast(Any, fake_broker)
    )
    yield orch
    # Short drain: the fixtures have no long-lived loops running, and a full
    # production-length wait per test would add minutes to the suite.
    await orch.stop(drain_seconds=0.5)

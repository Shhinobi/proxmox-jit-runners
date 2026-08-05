from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from orchestrator.db import Database
from orchestrator.service import Orchestrator
from tests.fakes import FakeBroker, FakeGitHub, FakeProxmox
from tests.helpers import settings as make_settings


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
    await orch.stop()

from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.eventlog import EventLog


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return tmp_path / "events.jsonl"


def test_disabled_by_default_writes_nothing(path: Path) -> None:
    log = EventLog(maxlen=5)
    log.emit("info", "system", "hello")
    assert not path.exists()


def test_events_survive_reload_from_persist_path(path: Path) -> None:
    first = EventLog(maxlen=10, persist_path=path)
    first.emit("info", "system", "startup")
    first.emit("warn", "warm", "low supply", vmid=5000)

    # Simulate a process restart: a fresh EventLog reading the same file.
    second = EventLog(maxlen=10, persist_path=path)
    items = second.list(limit=10)

    assert len(items) == 2
    assert {item["message"] for item in items} == {"startup", "low supply"}


def test_reload_respects_maxlen_and_continues_id_sequence(path: Path) -> None:
    first = EventLog(maxlen=3, persist_path=path)
    for i in range(5):
        first.emit("info", "system", f"event-{i}")

    second = EventLog(maxlen=3, persist_path=path)
    items = second.list(limit=10)
    assert len(items) == 3
    assert items[0]["message"] == "event-4"  # newest first

    new_event = second.emit("info", "system", "event-5")
    assert new_event["id"] > 5  # sequence continues, no id collision


def test_load_ignores_corrupt_lines(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '{"id": 1, "ts": 1, "level": "info", "category": "system", '
        '"message": "ok", "detail": null, "context": {}}\n'
        "not-json-at-all\n"
        "\n",
        encoding="utf-8",
    )

    log = EventLog(maxlen=5, persist_path=path)
    items = log.list(limit=10)

    assert len(items) == 1
    assert items[0]["message"] == "ok"


def test_compaction_keeps_file_bounded(path: Path) -> None:
    log = EventLog(maxlen=5, persist_path=path)
    for i in range(120):  # well past the compaction threshold
        log.emit("info", "system", f"event-{i}")

    lines = path.read_text(encoding="utf-8").splitlines()
    # Bounded well below "one line per emit" (120) — compaction ran.
    assert len(lines) < 120

    # And the log itself is still correct after all that churn.
    items = log.list(limit=10)
    assert items[0]["message"] == "event-119"

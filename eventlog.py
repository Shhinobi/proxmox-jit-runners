"""In-process structured event log for the orchestrator dashboard."""

from __future__ import annotations

import json
import threading
import time
import traceback
from collections import deque
from pathlib import Path
from typing import Any

# Compact the on-disk mirror back down to maxlen every N appends, so it
# doesn't grow unbounded between restarts while still staying append-only
# (cheap) most of the time.
_COMPACT_EVERY = 50


class EventLog:
    """Thread-safe ring buffer of structured operational events.

    Optionally mirrors each event to a JSONL file so the operator-facing
    "what just happened" narrative survives a process restart — the deque
    alone is wiped on every deploy/crash-restart, while the deliveries/jobs
    tables in SQLite persist but are much terser.
    """

    def __init__(self, maxlen: int = 500, persist_path: str | Path | None = None):
        self._events: deque[dict[str, Any]] = deque(maxlen=max(1, int(maxlen)))
        self._lock = threading.Lock()
        self._seq = 0
        self._persist_path = Path(persist_path) if persist_path else None
        self._appends_since_compact = 0
        if self._persist_path is not None:
            self._load_persisted()

    def _load_persisted(self) -> None:
        path = self._persist_path
        assert path is not None
        if not path.is_file():
            return
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return
        maxlen = self._events.maxlen or 500
        for line in lines[-maxlen:]:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            self._events.append(event)
            self._seq = max(self._seq, int(event.get("id") or 0))

    def _persist_locked(self, event: dict[str, Any]) -> None:
        """Append (and periodically compact) the on-disk mirror.

        Caller must already hold self._lock. Best-effort: disk trouble here
        must never break the in-memory log itself.
        """
        path = self._persist_path
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, separators=(",", ":")) + "\n")
            self._appends_since_compact += 1
            if self._appends_since_compact >= _COMPACT_EVERY:
                self._compact_locked()
        except OSError:
            pass

    def _compact_locked(self) -> None:
        path = self._persist_path
        if path is None:
            return
        try:
            lines = "\n".join(json.dumps(e, separators=(",", ":")) for e in self._events)
            path.write_text(lines + ("\n" if lines else ""), encoding="utf-8")
            self._appends_since_compact = 0
        except OSError:
            pass

    def emit(
        self,
        level: str,
        category: str,
        message: str,
        *,
        detail: str | None = None,
        **context: Any,
    ) -> dict[str, Any]:
        level = str(level or "info").lower()
        if level not in {"debug", "info", "warn", "error"}:
            level = "info"
        clean_context = {
            key: value
            for key, value in context.items()
            if value is not None and value != ""
        }
        with self._lock:
            self._seq += 1
            event = {
                "id": self._seq,
                "ts": int(time.time()),
                "level": level,
                "category": str(category or "system"),
                "message": str(message),
                "detail": str(detail) if detail else None,
                "context": clean_context,
            }
            self._events.append(event)
            self._persist_locked(event)
            return event

    def exception(
        self,
        category: str,
        message: str,
        exc: BaseException,
        **context: Any,
    ) -> dict[str, Any]:
        detail = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )[-4000:]
        return self.emit(
            "error",
            category,
            message,
            detail=detail or str(exc),
            error=str(exc)[:500],
            **context,
        )

    def list(
        self,
        *,
        limit: int = 100,
        level: str | None = None,
        category: str | None = None,
        min_id: int | None = None,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._lock:
            items = list(self._events)
        if min_id is not None:
            items = [item for item in items if int(item["id"]) > int(min_id)]
        if level:
            wanted = str(level).lower()
            if wanted == "warn":
                items = [item for item in items if item["level"] in {"warn", "error"}]
            elif wanted == "error":
                items = [item for item in items if item["level"] == "error"]
            else:
                items = [item for item in items if item["level"] == wanted]
        if category:
            wanted_cat = str(category).lower()
            items = [
                item for item in items if str(item.get("category", "")).lower() == wanted_cat
            ]
        # newest first
        items.reverse()
        return items[:limit]

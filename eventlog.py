"""In-process structured event log for the orchestrator dashboard."""

from __future__ import annotations

import threading
import time
import traceback
from collections import deque
from typing import Any


class EventLog:
    """Thread-safe ring buffer of structured operational events."""

    def __init__(self, maxlen: int = 500):
        self._events: deque[dict[str, Any]] = deque(maxlen=max(1, int(maxlen)))
        self._lock = threading.Lock()
        self._seq = 0

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

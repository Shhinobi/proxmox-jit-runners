"""Injectable time source.

Backoff windows, retry deadlines, and the reconcile pass budget are all
time-driven, and testing them against the real clock means either sleeping in
tests or reaching into the database to rewrite timestamps (which is what
`tests/test_service.py` does today with raw `UPDATE ... SET updated_at=0` SQL).

`Clock` is a thin seam over the three time primitives the orchestrator uses, so
tests can substitute `tests.fakes.FakeClock` and advance time explicitly.

Deliberately NOT used by `Database`: SQLite timestamps are written with
`int(time.time())` in ~20 places and several existing tests manipulate those
rows directly. Threading a clock through the data layer is a separate, larger
change; keeping it out here means this seam can land without touching them.
"""

from __future__ import annotations

import asyncio
import time


class Clock:
    """The real clock. Stateless, so a shared instance is fine."""

    def now(self) -> int:
        """Wall-clock epoch seconds, matching how the database stores times."""
        return int(time.time())

    def monotonic(self) -> float:
        """Monotonic seconds, for measuring durations and deadlines.

        Never use `now()` for a duration: a clock step (NTP, a suspended host)
        would make a pass look instantaneous or hours long.
        """
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


SYSTEM_CLOCK = Clock()

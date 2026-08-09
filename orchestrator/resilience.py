"""Circuit breaker for outbound dependencies.

Motivation, concretely: during the thin-pool exhaustion incident every reconcile pass
walked ~26 instances, and each one paid a full GitHub retry ladder against a
host that was not answering. One pass could not finish inside its own 60s
interval, passes stacked, and the single uvicorn worker — which also serves the
dashboard — was saturated. The containers those passes were supposed to delete
kept their disk the whole time.

A breaker turns "N instances x several seconds of timeout" into "N instances x
zero", which is what lets a pass complete while a dependency is down.

Design notes:

- **One breaker per client, not per installation or repository.** The failure
  domain is the network path to `api.github.com`. N breakers would each need
  their own N consecutive failures before any protection engaged.
- **Only availability failures trip it.** A 401/403/404/422 is a real answer
  from a healthy GitHub; counting those would let one misconfigured repository
  policy open the circuit for the whole fleet.
- **Half-open admits exactly one trial call**, guarded by a lock, so recovery
  does not itself become a thundering herd.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections.abc import Callable

LOGGER = logging.getLogger("runner_orchestrator")


class CircuitState(enum.Enum):
    CLOSED = "closed"
    HALF_OPEN = "half_open"
    OPEN = "open"


class CircuitOpen(RuntimeError):
    """Raised instead of attempting a call while the circuit is open."""

    def __init__(self, name: str, retry_after: float) -> None:
        super().__init__(f"{name} circuit is open; retry in {retry_after:.0f}s")
        self.name = name
        self.retry_after = retry_after


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        open_seconds: float = 30.0,
        max_open_seconds: float = 300.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.open_seconds = open_seconds
        self.max_open_seconds = max_open_seconds
        self._monotonic = monotonic

        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._current_open_window = open_seconds
        self._half_open_lock = asyncio.Lock()
        self._trial_in_flight = False

        self.last_error: str | None = None
        self.last_success_at: float | None = None
        self.transitions = 0

    # --- observation ------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        """Current state, promoting OPEN → HALF_OPEN once the window elapses."""
        if (
            self._state is CircuitState.OPEN
            and self._opened_at is not None
            and self._monotonic() - self._opened_at >= self._current_open_window
        ):
            self._state = CircuitState.HALF_OPEN
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def opened_for(self) -> float | None:
        if self._opened_at is None:
            return None
        return self._monotonic() - self._opened_at

    def retry_after(self) -> float:
        if self._opened_at is None:
            return 0.0
        return max(0.0, self._current_open_window - (self._monotonic() - self._opened_at))

    def snapshot(self) -> dict[str, object]:
        state = self.state
        return {
            "state": state.value,
            "consecutive_failures": self._consecutive_failures,
            "opened_for_seconds": self.opened_for,
            "retry_after_seconds": self.retry_after() if state is not CircuitState.CLOSED else 0.0,
            "last_error": self.last_error,
        }

    # --- gating -----------------------------------------------------------

    def allows(self) -> bool:
        """Whether a call may be attempted right now.

        HALF_OPEN admits a single trial; every other caller is refused until it
        reports back.
        """
        state = self.state
        if state is CircuitState.CLOSED:
            return True
        if state is CircuitState.OPEN:
            return False
        return not self._trial_in_flight

    def before_call(self) -> None:
        """Raise if the call must not be attempted; otherwise mark a trial."""
        state = self.state
        if state is CircuitState.OPEN:
            raise CircuitOpen(self.name, self.retry_after())
        if state is CircuitState.HALF_OPEN:
            if self._trial_in_flight:
                raise CircuitOpen(self.name, self.retry_after())
            self._trial_in_flight = True

    # --- reporting --------------------------------------------------------

    def record_success(self) -> None:
        self._trial_in_flight = False
        self.last_success_at = self._monotonic()
        self._consecutive_failures = 0
        self.last_error = None
        if self._state is not CircuitState.CLOSED:
            LOGGER.info("%s circuit closed after a successful call", self.name)
            self.transitions += 1
        self._state = CircuitState.CLOSED
        self._opened_at = None
        self._current_open_window = self.open_seconds

    def record_failure(self, error: str) -> None:
        self._trial_in_flight = False
        self.last_error = error
        self._consecutive_failures += 1

        if self._state is CircuitState.HALF_OPEN:
            # The trial failed: back off harder rather than re-probing at the
            # same cadence against something still broken.
            self._current_open_window = min(
                self.max_open_seconds, max(self.open_seconds, self._current_open_window * 2)
            )
            self._open(error)
            return

        if self._consecutive_failures >= self.failure_threshold:
            self._open(error)

    def _open(self, error: str) -> None:
        if self._state is not CircuitState.OPEN:
            LOGGER.warning(
                "%s circuit opened after %d consecutive failures (window %.0fs): %s",
                self.name,
                self._consecutive_failures,
                self._current_open_window,
                error,
            )
            self.transitions += 1
        self._state = CircuitState.OPEN
        self._opened_at = self._monotonic()

    def note_probe_success(self) -> None:
        """An out-of-band probe reached the dependency.

        Shortens the open window so recovery is noticed within a probe interval
        instead of up to `max_open_seconds`. Deliberately does NOT force the
        circuit closed: the probe and the real calls may not share a code path,
        and only a real call proves the dependency usable.
        """
        if self._state is CircuitState.OPEN and self._opened_at is not None:
            self._current_open_window = min(self._current_open_window, self.open_seconds)
            self._opened_at = min(self._opened_at, self._monotonic() - self._current_open_window)

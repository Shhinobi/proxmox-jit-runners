from __future__ import annotations

import pytest

from orchestrator.resilience import CircuitBreaker, CircuitOpen, CircuitState


class FakeMonotonic:
    """Explicit time, so window arithmetic is asserted rather than slept through."""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _breaker(**kwargs: object) -> tuple[CircuitBreaker, FakeMonotonic]:
    clock = FakeMonotonic()
    breaker = CircuitBreaker("test", monotonic=clock, **kwargs)  # type: ignore[arg-type]
    return breaker, clock


def test_starts_closed_and_allows_calls() -> None:
    breaker, _ = _breaker()
    assert breaker.state is CircuitState.CLOSED
    assert breaker.allows()
    breaker.before_call()  # must not raise


def test_opens_after_consecutive_failures() -> None:
    breaker, _ = _breaker(failure_threshold=3)
    for _ in range(2):
        breaker.record_failure("transport")
    assert breaker.state is CircuitState.CLOSED, "below threshold must stay closed"

    breaker.record_failure("transport")
    assert breaker.state is CircuitState.OPEN
    assert not breaker.allows()


def test_success_resets_the_failure_run() -> None:
    """Consecutive means consecutive: an intervening success clears the count."""
    breaker, _ = _breaker(failure_threshold=3)
    breaker.record_failure("transport")
    breaker.record_failure("transport")
    breaker.record_success()
    breaker.record_failure("transport")

    assert breaker.state is CircuitState.CLOSED
    assert breaker.consecutive_failures == 1


def test_open_circuit_refuses_without_attempting() -> None:
    """This is the whole point: N instances x timeout becomes N x zero."""
    breaker, _ = _breaker(failure_threshold=1)
    breaker.record_failure("transport")

    with pytest.raises(CircuitOpen):
        breaker.before_call()


def test_half_opens_after_the_cooldown_then_closes_on_success() -> None:
    breaker, clock = _breaker(failure_threshold=1, open_seconds=30.0)
    breaker.record_failure("transport")

    clock.advance(29.0)
    assert breaker.state is CircuitState.OPEN

    clock.advance(2.0)
    assert breaker.state is CircuitState.HALF_OPEN
    breaker.before_call()
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


def test_half_open_admits_exactly_one_trial() -> None:
    """Recovery must not itself become a thundering herd."""
    breaker, clock = _breaker(failure_threshold=1, open_seconds=10.0)
    breaker.record_failure("transport")
    clock.advance(11.0)

    breaker.before_call()  # the one trial
    with pytest.raises(CircuitOpen):
        breaker.before_call()


def test_failed_trial_reopens_with_a_wider_window() -> None:
    """Back off harder rather than re-probing something still broken."""
    breaker, clock = _breaker(failure_threshold=1, open_seconds=10.0, max_open_seconds=100.0)
    breaker.record_failure("transport")

    clock.advance(11.0)
    breaker.before_call()
    breaker.record_failure("still broken")

    clock.advance(11.0)
    assert breaker.state is CircuitState.OPEN, "the window must have widened past 10s"
    clock.advance(10.0)
    assert breaker.state is CircuitState.HALF_OPEN


def test_open_window_is_capped() -> None:
    breaker, clock = _breaker(failure_threshold=1, open_seconds=10.0, max_open_seconds=40.0)
    breaker.record_failure("transport")
    for _ in range(6):
        clock.advance(1000.0)
        breaker.before_call()
        breaker.record_failure("still broken")

    clock.advance(41.0)
    assert breaker.state is CircuitState.HALF_OPEN


def test_probe_success_shortens_the_window_without_forcing_closed() -> None:
    """Only a real call proves the dependency usable; a probe only hurries us."""
    breaker, clock = _breaker(failure_threshold=1, open_seconds=30.0, max_open_seconds=300.0)
    breaker.record_failure("transport")
    clock.advance(31.0)
    breaker.before_call()
    breaker.record_failure("still broken")  # window now 60s

    clock.advance(5.0)
    breaker.note_probe_success()

    assert breaker.state is CircuitState.HALF_OPEN
    assert breaker.state is not CircuitState.CLOSED


def test_snapshot_reports_state_for_the_dashboard() -> None:
    breaker, clock = _breaker(failure_threshold=1, open_seconds=30.0)
    assert breaker.snapshot()["state"] == "closed"

    breaker.record_failure("boom")
    snapshot = breaker.snapshot()
    assert snapshot["state"] == "open"
    assert snapshot["consecutive_failures"] == 1
    assert snapshot["last_error"] == "boom"
    assert 0 < float(snapshot["retry_after_seconds"]) <= 30.0

    clock.advance(31.0)
    breaker.before_call()
    breaker.record_success()
    assert breaker.snapshot()["state"] == "closed"
    assert breaker.snapshot()["last_error"] is None

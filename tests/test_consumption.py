"""Tests for the consumption arithmetic.

No database, no fixtures, no container. Every case here is a few numbers in and one number
out, which is what the extraction from models.py was for: these are the failure modes that
actually matter, and none of them needs PostgreSQL to provoke.
"""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from accounting_service.consumption import (
    ConsumptionWindow,
    RateSample,
    estimate_consumption,
)

HOUR = timedelta(hours=1)
START = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
END = START + HOUR


def window(start: datetime = START, end: datetime = END) -> ConsumptionWindow:
    return ConsumptionWindow(start=start, end=end)


def samples(*pairs: tuple[datetime, float]) -> list[RateSample]:
    return [RateSample(at=at, rate=rate) for at, rate in pairs]


def test_no_samples_is_unknown_rather_than_zero() -> None:
    assert estimate_consumption([], window()) is None


def test_a_single_sample_is_unknown() -> None:
    """One observation means the resource existed for zero measured time."""
    assert estimate_consumption(samples((START, 8.0)), window()) is None


def test_constant_rate_across_the_whole_window() -> None:
    """8 units per second held for an hour is 8 * 3600."""
    result = estimate_consumption(samples((START - HOUR, 8.0), (END + HOUR, 8.0)), window())

    assert result == pytest.approx(8.0 * 3600)


def test_rate_ramping_linearly_is_averaged() -> None:
    """A rate rising 0 to 10 over the window averages 5, so 5 * 3600."""
    result = estimate_consumption(samples((START, 0.0), (END, 10.0)), window())

    assert result == pytest.approx(5.0 * 3600)


def test_resource_appearing_mid_window_is_not_charged_before_it_existed() -> None:
    """First sample halfway through, so only the second half accrues."""
    midpoint = START + timedelta(minutes=30)
    result = estimate_consumption(samples((midpoint, 4.0), (END + HOUR, 4.0)), window())

    assert result == pytest.approx(4.0 * 1800)


def test_resource_disappearing_mid_window_stops_accruing() -> None:
    """Sampling stops halfway, so the resource is assumed gone from that point."""
    midpoint = START + timedelta(minutes=30)
    result = estimate_consumption(samples((START - HOUR, 4.0), (midpoint, 4.0)), window())

    assert result == pytest.approx(4.0 * 1800)


def test_rate_of_zero_is_a_measurement_of_nothing() -> None:
    """Distinct from None: samples exist and they say nothing was consumed."""
    result = estimate_consumption(samples((START - HOUR, 0.0), (END + HOUR, 0.0)), window())

    assert result == 0.0


def test_step_change_between_samples_is_interpolated_not_stepped() -> None:
    """The model is a straight line between samples, so a step reads as a ramp.

    Worth pinning down because it is a modelling choice rather than an accident. A resource
    that doubles instantly is billed as though it grew smoothly since the previous sample.
    """
    result = estimate_consumption(samples((START, 2.0), (END, 4.0)), window())

    assert result == pytest.approx(3.0 * 3600)


def test_multi_day_window_counts_every_day() -> None:
    """A window longer than a day used to lose its whole-day part.

    The previous implementation used timedelta.seconds, which is the sub-day remainder, so a
    three-day window was billed as whatever was left over after the days were discarded. The
    one-hour windows the ingester uses never showed it. Task 6's day, week and month cycles
    would have.
    """
    start = datetime(2025, 3, 1, tzinfo=UTC)
    end = start + timedelta(days=3)
    result = estimate_consumption(
        samples((start - HOUR, 1.0), (end + HOUR, 1.0)),
        window(start, end),
    )

    assert result == pytest.approx(3 * 24 * 3600)


def test_samples_at_the_same_instant_do_not_divide_by_zero() -> None:
    """Two collectors reporting the same timestamp should not crash the estimate."""
    result = estimate_consumption(samples((START, 5.0), (START, 7.0), (END + HOUR, 5.0)), window())

    assert result is not None


def test_naive_timestamps_are_taken_as_utc() -> None:
    """A naive value is labelled, never shifted by the local offset."""
    sample = RateSample(at=datetime(2025, 1, 1, 12, 0), rate=1.0)

    assert sample.at == START


def test_naive_window_bounds_are_taken_as_utc() -> None:
    assert ConsumptionWindow(start=datetime(2025, 1, 1, 12, 0), end=datetime(2025, 1, 1, 13, 0)).start == START


def test_a_reversed_window_is_rejected() -> None:
    """Nothing checked this before, and timedelta.seconds turned it into a huge positive."""
    with pytest.raises(ValidationError, match="is after end"):
        ConsumptionWindow(start=END, end=START)


def test_a_zero_length_window_consumes_nothing() -> None:
    result = estimate_consumption(samples((START - HOUR, 8.0), (START + HOUR, 8.0)), window(START, START))

    assert result == pytest.approx(0.0)


def test_samples_are_immutable() -> None:
    """Frozen so a caller cannot adjust the evidence after it has been read."""
    sample = RateSample(at=START, rate=1.0)

    with pytest.raises(ValidationError):
        sample.rate = 2.0

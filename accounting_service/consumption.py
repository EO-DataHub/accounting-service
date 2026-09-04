"""Estimating resource consumption from rate samples.

This is the arithmetic behind storage-style billing, separated from the database. A collector
reports the rate at which a resource is being consumed at a point in time - 8GB of storage
held is "8 GB-seconds per second" - and consumption over a period is the integral of that
rate. Samples are sparse, so the rate between them is interpolated linearly.

Nothing here touches a session. The queries stay on
`BillableResourceConsumptionRateSample`, which reads the samples and hands them over as
values. That is what lets the interesting part be tested without a database: the failure
modes worth worrying about are arithmetic, not SQL.
"""

import itertools
from collections.abc import Sequence
from datetime import datetime
from typing import NamedTuple, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from accounting_service.timestamps import as_utc


class RateSample(BaseModel):
    """One observation of the rate at which a resource is being consumed.

    `rate` is in the billing item's units divided by seconds. For storage measured in
    GB-seconds, a rate of 8 means 8GB is held, and 8 GB-seconds accrue every second.
    """

    model_config = ConfigDict(frozen=True)

    at: datetime
    rate: float

    @field_validator("at")
    @classmethod
    def _must_be_utc(cls, value: datetime) -> datetime:
        return as_utc(value)


class ConsumptionWindow(BaseModel):
    """The period consumption is being estimated over."""

    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime

    @field_validator("start", "end")
    @classmethod
    def _must_be_utc(cls, value: datetime) -> datetime:
        return as_utc(value)

    @model_validator(mode="after")
    def _start_before_end(self) -> Self:
        # Nothing checked this before. A reversed window would produce a negative duration,
        # and the old code's use of timedelta.seconds turned that into a very large positive
        # one rather than a negative or an error.
        if self.start > self.end:
            raise ValueError(f"window start {self.start.isoformat()} is after end {self.end.isoformat()}")

        return self

    @property
    def duration_seconds(self) -> float:
        return (self.end - self.start).total_seconds()

    def offset_of(self, at: datetime) -> float:
        """Seconds from the start of the window to `at`. Negative if `at` precedes it."""
        return (at - self.start).total_seconds()


class _RatePoint(NamedTuple):
    """A rate at an offset in seconds from the start of the window."""

    offset: float
    rate: float


def _interpolate(at: datetime, before: RateSample, after: RateSample) -> float:
    """The rate at `at`, on the straight line between two samples that bracket it."""
    span = (after.at - before.at).total_seconds()

    if span == 0:
        # Two samples at the same instant. Rare, but a division by zero rather than a
        # judgement call, so take the earlier rate and move on.
        return before.rate

    proportion = (at - before.at).total_seconds() / span

    return before.rate + proportion * (after.rate - before.rate)


def estimate_consumption(samples: Sequence[RateSample], window: ConsumptionWindow) -> float | None:
    """Estimate total consumption over `window`, or None if there is no evidence of any.

    `samples` must be ordered by time and should extend at least one sample either side of
    the window, which is what `find_data_for_interval` selects. Where they do not, the
    resource is assumed not to have existed: consumption is taken as zero before the first
    sample and after the last.

    Returning None rather than 0.0 distinguishes "no samples, so nothing is known" from
    "samples exist and they say nothing was consumed". Callers charge nothing either way, but
    only the second is a measurement.

    One sample is treated the same as none. Under the assumption above, a resource seen
    exactly once existed for zero time.
    """
    if len(samples) <= 1:
        return None

    first, last = samples[0], samples[-1]

    if first.at > window.start:
        # The resource had not appeared when the window opened, so it accrues nothing until
        # the first sample and then steps up to the rate observed there.
        opening = _RatePoint(offset=window.offset_of(first.at), rate=0.0)
    else:
        opening = _RatePoint(offset=0.0, rate=_interpolate(window.start, first, samples[1]))

    if last.at < window.end:
        # Sampling stopped inside the window, so the resource is assumed destroyed at the
        # last sample rather than surviving unmeasured to the end of it.
        closing = _RatePoint(offset=window.offset_of(last.at), rate=0.0)
    else:
        closing = _RatePoint(
            offset=window.duration_seconds,
            rate=_interpolate(window.end, samples[-2], last),
        )

    within = [
        _RatePoint(offset=window.offset_of(sample.at), rate=sample.rate)
        for sample in samples
        if window.start < sample.at <= window.end
    ]

    # A trapezoidal integration over the rate curve: each pair of adjacent points contributes
    # its mean rate multiplied by the time between them. Adjacent points sharing an offset
    # contribute nothing, which is how the step at the first and last sample is expressed.
    points = [opening, *within, closing]

    return sum(
        (later.offset - earlier.offset) * (earlier.rate + later.rate) / 2.0
        for earlier, later in itertools.pairwise(points)
    )

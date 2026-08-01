from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


class MutableClock:
    """A controllable UTC clock so lease-expiry paths are testable."""

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


@pytest.fixture
def clock_factory():
    return MutableClock

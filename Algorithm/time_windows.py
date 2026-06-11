"""Shared time-window helpers for matching and persistence.

Rider windows are anchored to the rider's flight date. If a rider's
``latest_time`` is earlier than ``earliest_time``, the window crosses midnight
and the end belongs to the following calendar day.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Sequence, Tuple

from rider_data import RiderLite


def rider_interval(rider: RiderLite) -> Tuple[datetime, datetime]:
    """Return a rider's normalized start/end datetimes."""
    start = datetime.fromisoformat(f"{rider.date}T{rider.earliest_time}")
    end = datetime.fromisoformat(f"{rider.date}T{rider.latest_time}")
    if end < start:
        end += timedelta(days=1)
    return start, end


def common_window(riders: Sequence[RiderLite]) -> Tuple[datetime, datetime]:
    """Return the shared group window as normalized datetimes."""
    if not riders:
        raise ValueError("Cannot compute a common time window without riders.")

    starts, ends = [], []
    for rider in riders:
        start, end = rider_interval(rider)
        starts.append(start)
        ends.append(end)

    return max(starts), min(ends)


def common_window_or_none(
    riders: Sequence[RiderLite],
    *,
    allow_touching: bool = True,
) -> Optional[Tuple[datetime, datetime]]:
    """Return the shared group window, or ``None`` when no overlap exists."""
    if not riders:
        return None

    latest_start, earliest_end = common_window(riders)
    if allow_touching:
        if latest_start > earliest_end:
            return None
    elif latest_start >= earliest_end:
        return None
    return latest_start, earliest_end


def time_string(value: datetime) -> str:
    """Format a normalized datetime for time-only CSV/DB fields."""
    return value.time().replace(microsecond=0).isoformat()

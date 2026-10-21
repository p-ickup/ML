"""Pure output-shaping helpers, ported from ML/Algorithm/commit_payload.py.

The DB-commit/RPC parts of the original module (``build_matching_commit_payload``,
``commit_matching_run``, retry/validation machinery) are intentionally not
ported — this service has no transactional store to write to. Only the
pure functions needed to describe a match to an API caller are kept.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence, Tuple

from . import config
from .rider import RiderLite
from .rule_matching import Match
from .time_windows import common_window, time_string


def determine_uber_type(group_size: int, bag_units: int) -> Optional[str]:
    """
    Determine uber_type based on group size and bag units.

    Rules:
    - 2-3 riders: 0-4 bags = X, 5-10 bags = XL, 11-12 bags = XXL
    - 4 riders: 0-3 bags = X, 4-7 bags = XL, 8-10 bags = XXL, 11+ = None
    - 5 riders: 0-5 bags = XL, 6-8 bags = XXL, 9+ = None
    - 6 riders: 0-3 bags = XL, 4-6 bags = XXL, 7+ = None
    """
    if group_size in (2, 3):
        if bag_units > 12:
            return None
    elif bag_units > 10:
        return None

    if group_size in (2, 3):
        if 0 <= bag_units <= 4:
            return "X"
        if 5 <= bag_units <= 10:
            return "XL"
        if 11 <= bag_units <= 12:
            return "XXL"
        return None

    if group_size == 4:
        if 0 <= bag_units <= 3:
            return "X"
        if 4 <= bag_units <= 7:
            return "XL"
        if 8 <= bag_units <= 10:
            return "XXL"
        return None

    if group_size == 5:
        if 0 <= bag_units <= 5:
            return "XL"
        if 6 <= bag_units <= 8:
            return "XXL"
        return None

    if group_size == 6:
        if 0 <= bag_units <= 3:
            return "XL"
        if 4 <= bag_units <= 6:
            return "XXL"
        return None

    return None


def compute_group_time_window(riders: Sequence[RiderLite]) -> Tuple[str, str]:
    """
    Compute the shared group window as time strings.

    ``earliest_time`` is the latest rider start. ``latest_time`` is the
    earliest rider end. Overnight windows are normalized so persisted group
    windows agree with matching interval behavior.
    """
    if not riders:
        raise ValueError("Cannot compute a group time window without riders.")

    latest_start, earliest_end = common_window(riders)
    return time_string(latest_start), time_string(earliest_end)


def match_datetime_from_earliest(m: Match) -> Tuple[str, str]:
    """Ride date/time for the API response."""
    if m.suggested_time_iso:
        dt = datetime.fromisoformat(m.suggested_time_iso)
        return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()

    dt, _ = common_window(m.riders)
    return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()


def bag_units(riders: Sequence[RiderLite]) -> int:
    num_large_bags = sum(int(r.bags_no_large or 0) for r in riders)
    num_normal_bags = sum(int(r.bags_no or 0) for r in riders)
    units = (num_large_bags * config.LARGE_BAG_MULTIPLIER) + num_normal_bags
    if config.PERSONAL_CONSTRAINT:
        units += sum(int(getattr(r, "bag_no_personal", 0) or 0) for r in riders)
    return units

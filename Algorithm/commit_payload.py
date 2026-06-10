"""Build and commit the atomic production matching payload.

This module keeps production persistence concerns out of ``main.py``.  The
matcher builds a complete commit payload first, validates the operational
invariants that can be checked client-side, and then sends the payload to one
Postgres RPC. The RPC is the transaction boundary.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import config
from rider_data import RiderLite
from ruleMatching import Match
from vouchers import is_ride_date_covered


class MatchingCommitError(ValueError):
    """Raised when a matching commit payload violates required invariants."""


class MatchingCommitRetryExhausted(RuntimeError):
    """Raised when transient commit failures continue after all retry attempts."""


_TRANSIENT_ERROR_MARKERS = (
    "timeout",
    "timed out",
    "temporarily unavailable",
    "connection reset",
    "connection aborted",
    "connection refused",
    "network",
    "service unavailable",
    "bad gateway",
    "gateway timeout",
    "too many requests",
)

_TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def bags_total(r: RiderLite) -> int:
    return int(r.bags_no or 0) + int(r.bags_no_large or 0)


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
        raise MatchingCommitError("Cannot compute a group time window without riders.")

    starts, ends = [], []
    for r in riders:
        s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
        e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
        if e < s:
            e += timedelta(days=1)
        starts.append(s)
        ends.append(e)

    latest_start = max(starts)
    earliest_end = min(ends)
    return (
        latest_start.time().replace(microsecond=0).isoformat(),
        earliest_end.time().replace(microsecond=0).isoformat(),
    )


def match_datetime_from_earliest(m: Match) -> Tuple[str, str]:
    """Official ride date/time for persistence."""
    if m.suggested_time_iso:
        dt = datetime.fromisoformat(m.suggested_time_iso)
        return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()

    starts = []
    for r in m.riders:
        s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
        starts.append(s)
    dt = max(starts)
    return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()


def _bag_units(riders: Sequence[RiderLite]) -> int:
    num_large_bags = sum(int(r.bags_no_large or 0) for r in riders)
    num_normal_bags = sum(int(r.bags_no or 0) for r in riders)
    bag_units = (num_large_bags * config.LARGE_BAG_MULTIPLIER) + num_normal_bags
    if config.PERSONAL_CONSTRAINT:
        bag_units += sum(int(getattr(r, "bag_no_personal", 0) or 0) for r in riders)
    return bag_units


def cleanup_flight_ids(connect_matches: Sequence[Match]) -> List[int]:
    """Return the flight ids whose existing ride memberships should be replaced."""
    return sorted(
        {
            int(r.flight_id)
            for m in connect_matches
            for r in m.riders
            if r.flight_id is not None
        }
    )


def build_matching_commit_payload(
    *,
    run_id: str,
    matches: Sequence[Match],
    all_riders: Sequence[RiderLite],
    connect_for_cleanup: Sequence[Match],
) -> Dict[str, Any]:
    considered_ids = {int(r.flight_id) for r in all_riders}
    matched_ids = {int(r.flight_id) for m in matches for r in m.riders}
    unmatched_ids = sorted(considered_ids - matched_ids)

    groups: List[Dict[str, Any]] = []
    for m in matches:
        ride_date, match_time = match_datetime_from_earliest(m)
        group_earliest_time, group_latest_time = compute_group_time_window(m.riders)
        ride_type = getattr(m, "ride_type", None)
        group_size = len(m.riders)
        uber_type = ride_type or determine_uber_type(group_size, _bag_units(m.riders))

        ride_date_for_cover = datetime.fromisoformat(m.suggested_time_iso).date()
        to_airport = m.riders[0].to_airport
        write_subsidized = bool(
            getattr(m, "group_subsidy", False)
            and is_ride_date_covered(ride_date_for_cover, to_airport)
        )
        airport = (m.riders[0].airport or "").upper()

        members = []
        for r in m.riders:
            members.append(
                {
                    "user_id": r.user_id,
                    "flight_id": int(r.flight_id),
                    "date": ride_date,
                    "time": match_time,
                    "earliest_time": group_earliest_time,
                    "latest_time": group_latest_time,
                    "source": "ml",
                    "is_verified": False,
                    "is_subsidized": write_subsidized,
                    "uber_type": uber_type,
                }
            )

        groups.append(
            {
                "ride_date": ride_date,
                "ride_type": ride_type,
                "airport": airport,
                "to_airport": to_airport,
                "is_subsidized": write_subsidized,
                "members": members,
            }
        )

    payload = {
        "run_id": run_id,
        "groups": groups,
        "matched_flight_ids": sorted(matched_ids),
        "unmatched_flight_ids": unmatched_ids,
        "connect_cleanup_flight_ids": cleanup_flight_ids(connect_for_cleanup),
    }
    validate_matching_commit_payload(payload)
    return payload


def validate_matching_commit_payload(payload: Dict[str, Any]) -> None:
    """Validate invariants before sending the payload to the transactional RPC."""
    run_id = payload.get("run_id")
    if not run_id:
        raise MatchingCommitError("Matching commit payload requires a run_id.")

    groups = payload.get("groups")
    if not isinstance(groups, list):
        raise MatchingCommitError("Matching commit payload requires a groups list.")

    seen_matched: set[int] = set()
    for group in groups:
        members = group.get("members") if isinstance(group, dict) else None
        if not members:
            raise MatchingCommitError("Matching commit payload contains an empty group.")
        if not group.get("ride_date"):
            raise MatchingCommitError("Matching commit group is missing ride_date.")
        if group.get("airport") in (None, ""):
            raise MatchingCommitError("Matching commit group is missing airport.")
        if "to_airport" not in group:
            raise MatchingCommitError("Matching commit group is missing to_airport.")
        if "is_subsidized" not in group:
            raise MatchingCommitError("Matching commit group is missing is_subsidized.")

        for member in members:
            flight_id = member.get("flight_id")
            if flight_id is None:
                raise MatchingCommitError("Matching commit member is missing flight_id.")
            flight_id = int(flight_id)
            if flight_id in seen_matched:
                raise MatchingCommitError(f"Flight {flight_id} appears in multiple match groups.")
            seen_matched.add(flight_id)

            for field in ("user_id", "date", "time", "earliest_time", "latest_time", "source"):
                if member.get(field) in (None, ""):
                    raise MatchingCommitError(f"Flight {flight_id} is missing required field {field}.")

    unmatched_ids = {int(fid) for fid in payload.get("unmatched_flight_ids", [])}
    overlap = seen_matched & unmatched_ids
    if overlap:
        raise MatchingCommitError(
            f"Flights cannot be both matched and unmatched in the same commit: {sorted(overlap)}"
        )


def is_transient_commit_error(exc: Exception) -> bool:
    """Return True for failures that are reasonable to retry with the same run_id."""
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    try:
        if status is not None and int(status) in _TRANSIENT_STATUS_CODES:
            return True
    except (TypeError, ValueError):
        pass

    code = getattr(exc, "code", None)
    try:
        if code is not None and int(code) in _TRANSIENT_STATUS_CODES:
            return True
    except (TypeError, ValueError):
        pass

    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_ERROR_MARKERS)


def commit_matching_run(
    sb: Any,
    *,
    run_id: str,
    payload: Dict[str, Any],
    max_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> Dict[str, Any]:
    """Call the single transactional production commit RPC."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")

    validate_matching_commit_payload(payload)

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = sb.rpc(
                "commit_matching_run",
                {
                    "p_run_id": run_id,
                    "p_payload": payload,
                },
            ).execute()
            return resp.data or {}
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts or not is_transient_commit_error(exc):
                raise
            time.sleep(retry_delay_seconds * (2 ** (attempt - 1)))

    raise MatchingCommitRetryExhausted(
        f"commit_matching_run failed after {max_attempts} attempts"
    ) from last_exc

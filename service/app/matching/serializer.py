"""Turn a MatchResult into plain dicts for the API response.

Ported from the CSV row-building logic in ML/Algorithm/main.py
(``_write_matches_csv`` / ``_write_unmatched_with_reasons``), minus the
CSV file writing — this just returns dicts.
"""

from __future__ import annotations

from typing import Any, Dict, List

from . import config
from .payload import compute_group_time_window, determine_uber_type, match_datetime_from_earliest
from .pipeline import MatchResult
from .rule_matching import Match


def _considered_bags(m: Match) -> int:
    num_large_bags = sum(int(r.bags_no_large or 0) for r in m.riders)
    num_normal_bags = sum(int(r.bags_no or 0) for r in m.riders)
    considered = (num_large_bags * config.LARGE_BAG_MULTIPLIER) + num_normal_bags
    if config.PERSONAL_CONSTRAINT:
        considered += sum(int(getattr(r, "bag_no_personal", 0) or 0) for r in m.riders)
    return considered


def _bags_total(m: Match) -> int:
    return sum(
        int(r.bags_no or 0) + int(r.bags_no_large or 0) + int(getattr(r, "bag_no_personal", 0) or 0)
        for r in m.riders
    )


def serialize_match(m: Match, ride_id: int) -> Dict[str, Any]:
    match_date, _ = match_datetime_from_earliest(m)
    earliest_time, latest_time = compute_group_time_window(m.riders)

    group_size = len(m.riders)
    considered_bags = _considered_bags(m)
    uber_type = getattr(m, "ride_type", None) or determine_uber_type(group_size, considered_bags)

    return {
        "ride_id": ride_id,
        "bucket_key": m.bucket_key or "",
        "date": match_date,
        "num_riders": group_size,
        "suggested_time": m.suggested_time_iso or "",
        "earliest_time": earliest_time,
        "latest_time": latest_time,
        "considered_bags": considered_bags,
        "bags_total": _bags_total(m),
        "riders": [
            {"user_id": r.user_id, "flight_id": r.flight_id, "name": r.name}
            for r in m.riders
        ],
        "subsidized": bool(getattr(m, "group_subsidy", False)),
        "ride_type": getattr(m, "ride_type", None),
        "uber_type": uber_type,
    }


def serialize_unmatched(rider, diagnostics: Dict[int, dict]) -> Dict[str, Any]:
    info = diagnostics.get(rider.flight_id, {})
    return {
        "user_id": rider.user_id,
        "flight_id": rider.flight_id,
        "name": rider.name,
        "airport": rider.airport,
        "to_airport": rider.to_airport,
        "date": rider.date,
        "earliest_time": rider.earliest_time,
        "latest_time": rider.latest_time,
        "bucket_key": info.get("bucket_key", ""),
        "reason": info.get("reason", "unknown"),
        "details": info.get("details", {}),
    }


def serialize_result(result: MatchResult) -> Dict[str, Any]:
    matches_sorted = sorted(
        result.matches,
        key=lambda m: (
            (match_datetime_from_earliest(m)[0]) or "",
            m.suggested_time_iso or "",
            m.bucket_key or "",
        ),
    )
    matches_out: List[Dict[str, Any]] = [
        serialize_match(m, ride_id) for ride_id, m in enumerate(matches_sorted, start=1)
    ]

    unmatched_sorted = sorted(
        result.unmatched,
        key=lambda r: (r.date or "", r.earliest_time or "", r.latest_time or "", r.airport or ""),
    )
    unmatched_out = [serialize_unmatched(r, result.diagnostics) for r in unmatched_sorted]

    return {
        "status": "success",
        "summary": {
            "buckets": result.bucket_count,
            "groups": len(matches_out),
            "matched_riders": sum(m["num_riders"] for m in matches_out),
            "unmatched_riders": len(unmatched_out),
        },
        "matches": matches_out,
        "unmatched": unmatched_out,
    }

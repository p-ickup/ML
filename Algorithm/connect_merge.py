"""
Connect merge: combine existing DB matched rides, new run matches, and unmatched riders
into Connect shuttles per config (CONNECT_ARRIVAL / CONNECT_DEPARTURE / CONNECT_SIZE*).

Uses connect_policy.py for airports, directions, and tier sizes.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import connect_policy as cp
from rider_data import RiderLite, normalize_airport, normalize_matching_status
from ruleMatching import Match, _group_to_match, _interval
from supabase import Client
from time_windows import common_window_or_none


def _as_date_string(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value or "")


def _as_time_string(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M:%S")
    text = str(value).strip()
    if "T" in text:
        text = text.split("T")[-1]
    if "." in text:
        text = text.split(".")[0]
    if len(text) == 5:
        return f"{text}:00"
    return text


def _common_window_riders(riders: Sequence[RiderLite]) -> Optional[Tuple[Any, Any]]:
    return common_window_or_none(riders, allow_touching=True)


def _fetch_rides_in_range(sb: Client, start_date: str, end_date: str) -> Dict[int, Dict[str, Any]]:
    resp = (
        sb.table("Rides")
        .select("*")
        .gte("ride_date", start_date)
        .lte("ride_date", end_date)
        .execute()
    )
    return {int(row["ride_id"]): row for row in (resp.data or [])}


def _fetch_matches_for_rides(sb: Client, ride_ids: Sequence[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(0, len(ride_ids), 100):
        chunk = list(ride_ids[i : i + 100])
        if not chunk:
            continue
        resp = sb.table("Matches").select("*").in_("ride_id", chunk).execute()
        out.extend(resp.data or [])
    return out


def _fetch_flights(sb: Client, flight_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    clean = sorted({int(fid) for fid in flight_ids})
    for i in range(0, len(clean), 100):
        chunk = clean[i : i + 100]
        if not chunk:
            continue
        resp = (
            sb.table("Flights")
            .select(
                "flight_id,user_id,flight_no,airline_iata,airport,to_airport,date,"
                "earliest_time,latest_time,matching_status,bag_no,bag_no_large,bag_no_personal,terminal"
            )
            .in_("flight_id", chunk)
            .execute()
        )
        for row in resp.data or []:
            out[int(row["flight_id"])] = row
    return out


def _fetch_users(sb: Client, user_ids: Sequence[str]) -> Dict[str, Dict[str, str]]:
    clean = sorted({uid for uid in user_ids if uid})
    out: Dict[str, Dict[str, str]] = {}
    for i in range(0, len(clean), 100):
        chunk = clean[i : i + 100]
        if not chunk:
            continue
        resp = sb.table("Users").select("user_id,school,firstname,lastname").in_("user_id", chunk).execute()
        for row in resp.data or []:
            name = f"{row.get('firstname') or ''} {row.get('lastname') or ''}".strip()
            out[str(row["user_id"])] = {
                "school": row.get("school") or "",
                "name": name or None,
            }
    return out


def _flight_to_rider_lite(flight: Dict[str, Any], user_info: Dict[str, Dict[str, str]]) -> Optional[RiderLite]:
    uid = flight.get("user_id")
    if not uid:
        return None
    info = user_info.get(str(uid), {})
    school = (info.get("school") or "").strip()
    if school.upper() != "POMONA":
        return None
    rider = RiderLite(
        user_id=str(uid),
        flight_id=int(flight["flight_id"]),
        flight_no=(int(flight["flight_no"]) if flight.get("flight_no") is not None else None),
        airline_iata=flight.get("airline_iata"),
        earliest_time=_as_time_string(flight.get("earliest_time")),
        latest_time=_as_time_string(flight.get("latest_time")),
        airport=normalize_airport(flight.get("airport")),
        to_airport=bool(flight.get("to_airport")),
        date=_as_date_string(flight.get("date")),
        terminal=flight.get("terminal"),
        matching_status=normalize_matching_status(flight.get("matching_status")),
        school=school,
        name=info.get("name"),
        bags_no=(int(flight["bag_no"]) if flight.get("bag_no") is not None else None),
        bags_no_large=(int(flight["bag_no_large"]) if flight.get("bag_no_large") is not None else None),
        bag_no_personal=(int(flight["bag_no_personal"]) if flight.get("bag_no_personal") is not None else None),
    )
    if not cp.rider_in_connect_scope(rider):
        return None
    return rider


def fetch_existing_connect_riders(
    sb: Client,
    start_date: str,
    end_date: str,
    exclude_flight_ids: Set[int],
) -> Tuple[List[RiderLite], Dict[int, int]]:
    """Pomona riders on non-Connect existing rides in scope (LAX/ONT per config)."""
    rides = _fetch_rides_in_range(sb, start_date, end_date)
    if not rides:
        return [], {}

    matches = _fetch_matches_for_rides(sb, list(rides.keys()))
    flight_ids = [int(m["flight_id"]) for m in matches if m.get("flight_id") is not None]
    flights_by_id = _fetch_flights(sb, flight_ids)
    user_ids = [f.get("user_id") for f in flights_by_id.values() if f.get("user_id")]
    user_info = _fetch_users(sb, user_ids)

    by_ride: Dict[int, List[RiderLite]] = {}
    for m in matches:
        ride_id = m.get("ride_id")
        fid = m.get("flight_id")
        if ride_id is None or fid is None:
            continue
        ride_id = int(ride_id)
        fid = int(fid)
        if fid in exclude_flight_ids:
            continue
        ride = rides.get(ride_id)
        if not ride:
            continue
        if str(ride.get("ride_type") or "").strip().upper() == "CONNECT":
            continue
        flight = flights_by_id.get(fid)
        if not flight:
            continue
        rider = _flight_to_rider_lite(flight, user_info)
        if not rider:
            continue
        by_ride.setdefault(ride_id, []).append(rider)

    riders_out: List[RiderLite] = []
    old_ride_by_flight: Dict[int, int] = {}
    for ride_id, group in by_ride.items():
        if _common_window_riders(group) is None:
            continue
        for r in group:
            riders_out.append(r)
            old_ride_by_flight[r.flight_id] = ride_id
    return riders_out, old_ride_by_flight


def _generate_connect_candidates(riders: List[RiderLite]) -> List[List[RiderLite]]:
    min_size, max_size = cp.connect_search_bounds()
    if min_size is None or max_size is None:
        return []

    ordered = sorted(riders, key=lambda r: (_interval(r)[0], _interval(r)[1], r.flight_id))
    candidates_by_key: Dict[Tuple[int, ...], List[RiderLite]] = {}

    def backtrack(start_idx: int, selected: List[RiderLite]) -> None:
        count = len(selected)
        if cp.fits_connect_size(count) and _common_window_riders(selected) is not None:
            key = tuple(sorted(r.flight_id for r in selected))
            candidates_by_key[key] = list(selected)
        if count >= max_size:
            return

        current_window = _common_window_riders(selected) if selected else None
        current_end = current_window[1] if current_window else None

        for idx in range(start_idx, len(ordered)):
            rider = ordered[idx]
            if current_end is not None and _interval(rider)[0] > current_end:
                break
            trial = selected + [rider]
            if _common_window_riders(trial) is None:
                continue
            backtrack(idx + 1, trial)

    backtrack(0, [])
    return sorted(
        candidates_by_key.values(),
        key=lambda g: (-len(g), _interval(g[0])[0]),
    )


def _filter_maximal_groups(groups: List[List[RiderLite]]) -> List[List[RiderLite]]:
    keyed = [(g, frozenset(r.flight_id for r in g)) for g in groups]
    out: List[List[RiderLite]] = []
    for idx, (group, fids) in enumerate(keyed):
        if any(i != idx and fids < other for i, (_, other) in enumerate(keyed)):
            continue
        out.append(group)
    return out


def _select_non_overlapping(groups: List[List[RiderLite]]) -> List[List[RiderLite]]:
    selected: List[List[RiderLite]] = []
    used: Set[int] = set()
    for group in groups:
        ids = {r.flight_id for r in group}
        if ids & used:
            continue
        selected.append(group)
        used.update(ids)
    return selected


def _singletons_if_merge(
    group: List[RiderLite],
    matched_by_ride: Dict[int, List[RiderLite]],
) -> List[RiderLite]:
    selected = {r.flight_id for r in group}
    touched = {
        getattr(r, "source_ride_id", None)
        for r in group
        if getattr(r, "source_ride_id", None) is not None
    }
    singletons: List[RiderLite] = []
    for ride_id in touched:
        remaining = [
            r for r in matched_by_ride.get(int(ride_id), [])
            if r.flight_id not in selected
        ]
        if len(remaining) == 1:
            singletons.extend(remaining)
    return singletons


def _tag_source_ride_ids(
    riders: List[RiderLite],
    old_ride_by_flight: Dict[int, int],
    new_match_ride_by_flight: Dict[int, int],
) -> None:
    for r in riders:
        if r.flight_id in new_match_ride_by_flight:
            r.source_ride_id = new_match_ride_by_flight[r.flight_id]  # type: ignore[attr-defined]
        elif r.flight_id in old_ride_by_flight:
            r.source_ride_id = old_ride_by_flight[r.flight_id]  # type: ignore[attr-defined]


def _merge_pool(
    pool: List[RiderLite],
    old_ride_by_flight: Dict[int, int],
    new_match_ride_by_flight: Dict[int, int],
) -> Tuple[List[Match], List[RiderLite]]:
    min_size, _ = cp.connect_search_bounds()
    if min_size is None or len(pool) < min_size:
        return [], list(pool)

    _tag_source_ride_ids(pool, old_ride_by_flight, new_match_ride_by_flight)
    matched_by_ride: Dict[int, List[RiderLite]] = {}
    for r in pool:
        sid = getattr(r, "source_ride_id", None)
        if sid is not None and int(sid) > 0:
            matched_by_ride.setdefault(int(sid), []).append(r)

    connect_matches: List[Match] = []
    candidates = _filter_maximal_groups(_generate_connect_candidates(pool))
    selected = _select_non_overlapping(candidates)
    used_ids: Set[int] = set()

    for group in selected:
        if len(_singletons_if_merge(group, matched_by_ride)) > 2:
            continue
        m = _group_to_match(group, bucket_key=cp.bucket_key_for_rider(group[0]))
        m.ride_type = "Connect"
        connect_matches.append(m)
        used_ids.update(r.flight_id for r in group)

    leftover = [r for r in pool if r.flight_id not in used_ids]
    return connect_matches, leftover


def merge_connect_with_existing(
    sb: Optional[Client],
    all_matches: List[Match],
    all_unmatched: List[RiderLite],
    run_flight_ids: Set[int],
    start_date: str,
    end_date: str,
) -> Tuple[List[Match], List[RiderLite], List[Match]]:
    if not cp.connect_enabled():
        return all_matches, all_unmatched, []

    min_size, _ = cp.connect_search_bounds()
    if min_size is None:
        return all_matches, all_unmatched, []

    new_match_ride_by_flight: Dict[int, int] = {}
    run_riders: List[RiderLite] = []
    other_matches: List[Match] = []

    for m in all_matches:
        if getattr(m, "ride_type", None) == "Connect":
            other_matches.append(m)
            continue
        if not m.riders or not cp.rider_in_connect_scope(m.riders[0]):
            other_matches.append(m)
            continue
        for r in m.riders:
            run_riders.append(r)
            new_match_ride_by_flight[r.flight_id] = -1

    run_riders.extend(r for r in all_unmatched if cp.rider_in_connect_scope(r))

    old_ride_by_flight: Dict[int, int] = {}
    existing: List[RiderLite] = []
    if sb is not None:
        existing, old_ride_by_flight = fetch_existing_connect_riders(
            sb, start_date, end_date, exclude_flight_ids=run_flight_ids
        )

    # Pool key: (date, airport, to_airport)
    pools: Dict[Tuple[str, str, bool], List[RiderLite]] = {}
    seen: Set[int] = set()
    for r in run_riders + existing:
        if not (start_date <= r.date <= end_date):
            continue
        if r.flight_id in seen:
            continue
        seen.add(r.flight_id)
        key = (r.date, r.airport, r.to_airport)
        pools.setdefault(key, []).append(r)

    connect_matches: List[Match] = []
    leftover_riders: List[RiderLite] = []
    for key in sorted(pools.keys()):
        cm, lo = _merge_pool(pools[key], old_ride_by_flight, new_match_ride_by_flight)
        connect_matches.extend(cm)
        leftover_riders.extend(lo)

    leftover_matches: List[Match] = []
    by_source: Dict[Optional[int], List[RiderLite]] = {}
    for r in leftover_riders:
        sid = getattr(r, "source_ride_id", None)
        src_key = int(sid) if sid is not None and int(sid) > 0 else None
        by_source.setdefault(src_key, []).append(r)

    still_unmatched: List[RiderLite] = []
    for _, group in by_source.items():
        if len(group) >= 2 and _common_window_riders(group):
            leftover_matches.append(_group_to_match(group, bucket_key=cp.bucket_key_for_rider(group[0])))
        else:
            still_unmatched.extend(group)

    scopes = ", ".join(
        f"{'TO' if t else 'FROM'} {a}" for a, t in cp.enabled_connect_scopes()
    )
    print(
        f"Connect merge ({scopes or 'disabled'}): {len(connect_matches)} Connect group(s), "
        f"{len(leftover_matches)} small group(s), {len(still_unmatched)} unmatched in scope"
    )
    return other_matches + leftover_matches + connect_matches, still_unmatched, connect_matches

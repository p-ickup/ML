"""
Plan LAX Connect shuttle candidates from existing matches plus unmatched flights.

This script is intentionally review-first:

1. Fetch existing future Matches/Rides.
2. Fetch future unmatched LAX outbound Flights.
3. Build possible Connect shuttle groups by date and rider time windows.
4. Write a CSV proposal for human verification.
5. Optionally apply explicitly selected proposal IDs after review.

Run from Algorithm/:

    python3 temp_merge_existing_to_connect.py
    python3 temp_merge_existing_to_connect.py --days-ahead 20
    python3 temp_merge_existing_to_connect.py --csv ../matches/connect_shuttle_candidates.csv
    python3 temp_merge_existing_to_connect.py --apply-proposal-ids 2,4,7
    python3 temp_merge_existing_to_connect.py --apply-proposal-ids 2,4,7 --confirm-apply

The normal Connect tier is 7-25 riders by default, matching config.py.
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv

load_dotenv()

import config
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Set SUPABASE_URL and SUPABASE_SECRET_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

CONNECT_MIN = getattr(config, "LAX_CONNECT_SHUTTLE_MIN", 7)
CONNECT_MAX = getattr(config, "LAX_CONNECT_SHUTTLE_TIER1_MAX", 25)

REPORT_FIELDS = [
    "stranded_singletons_count",
    "proposal_id",
    "date",
    "num_riders",
    "suggested_time",
    "common_window_start",
    "common_window_end",
    "existing_ride_ids",
    "unmatched_flight_ids",
    "all_flight_ids",
    "source_summary",
    "source_detail",
    "leftover_singleton_flight_ids",
    "leftover_repair_groups",
    "per_rider_windows",
    "status",
]


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


def _parse_dt(date_str: str, time_str: str) -> datetime:
    t = _as_time_string(time_str)
    if not t:
        raise ValueError("missing time")
    return datetime.fromisoformat(f"{date_str}T{t}")


def _airport_is_lax(value: Any) -> bool:
    return str(value or "").strip().upper() == "LAX"


def _window_present(row: Dict[str, Any]) -> bool:
    return bool(_as_date_string(row.get("date")) and _as_time_string(row.get("earliest_time")) and _as_time_string(row.get("latest_time")))


def _rider_window(row: Dict[str, Any]) -> Tuple[datetime, datetime]:
    start = _parse_dt(_as_date_string(row["date"]), row["earliest_time"])
    end = _parse_dt(_as_date_string(row["date"]), row["latest_time"])
    if end < start:
        end += timedelta(days=1)
    return start, end


def _common_window(riders: Sequence[Dict[str, Any]]) -> Optional[Tuple[datetime, datetime]]:
    if not riders:
        return None
    starts, ends = [], []
    for rider in riders:
        start, end = _rider_window(rider)
        starts.append(start)
        ends.append(end)
    latest_start = max(starts)
    earliest_end = min(ends)
    if latest_start > earliest_end:
        return None
    return latest_start, earliest_end


def _suggested_time(riders: Sequence[Dict[str, Any]]) -> str:
    window = _common_window(riders)
    if window is None:
        return ""
    start, end = window
    if end <= start:
        chosen = start
    else:
        chosen = end - timedelta(minutes=15)
        if chosen < start:
            chosen = end
    return chosen.time().replace(microsecond=0).isoformat()


def _format_window(row: Dict[str, Any]) -> str:
    start, end = _rider_window(row)
    return f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"


def _fetch_rides(days_ahead: int) -> Dict[int, Dict[str, Any]]:
    today = datetime.today().date()
    end_date = today + timedelta(days=days_ahead)
    resp = (
        sb.table("Rides")
        .select("*")
        .gte("ride_date", today.isoformat())
        .lte("ride_date", end_date.isoformat())
        .execute()
    )
    return {int(row["ride_id"]): row for row in (resp.data or [])}


def _fetch_matches(ride_ids: Sequence[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(0, len(ride_ids), 100):
        chunk = list(ride_ids[i : i + 100])
        if not chunk:
            continue
        resp = sb.table("Matches").select("*").in_("ride_id", chunk).execute()
        out.extend(resp.data or [])
    return out


def _fetch_flights_by_id(flight_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for i in range(0, len(flight_ids), 100):
        chunk = list(flight_ids[i : i + 100])
        if not chunk:
            continue
        resp = (
            sb.table("Flights")
            .select(
                "flight_id,user_id,airport,to_airport,date,earliest_time,latest_time,matched,"
                "bag_no,bag_no_large,bag_no_personal"
            )
            .in_("flight_id", chunk)
            .execute()
        )
        for row in resp.data or []:
            out[int(row["flight_id"])] = row
    return out


def _fetch_future_lax_flights(days_ahead: int) -> List[Dict[str, Any]]:
    today = datetime.today().date()
    end_date = today + timedelta(days=days_ahead)
    resp = (
        sb.table("Flights")
        .select(
            "flight_id,user_id,airport,to_airport,date,earliest_time,latest_time,matched,"
            "bag_no,bag_no_large,bag_no_personal"
        )
        .gte("date", today.isoformat())
        .lte("date", end_date.isoformat())
        .execute()
    )
    rows = []
    for row in resp.data or []:
        if not _airport_is_lax(row.get("airport")):
            continue
        if not bool(row.get("to_airport")):
            continue
        if not _window_present(row):
            continue
        rows.append(row)
    return rows


def _fetch_user_info(user_ids: Sequence[str]) -> Dict[str, Dict[str, str]]:
    clean_ids = sorted({uid for uid in user_ids if uid})
    if not clean_ids:
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for i in range(0, len(clean_ids), 100):
        chunk = clean_ids[i : i + 100]
        resp = sb.table("Users").select("user_id,firstname,lastname,school").in_("user_id", chunk).execute()
        for row in resp.data or []:
            name = f"{row.get('firstname') or ''} {row.get('lastname') or ''}".strip()
            school = row.get("school") or ""
            label = name or row.get("user_id") or ""
            if school:
                label = f"{label} ({school})"
            out[row["user_id"]] = {"label": label, "school": school}
    return out


def _is_pomona_rider(rider: Dict[str, Any], user_info: Dict[str, Dict[str, str]]) -> bool:
    uid = rider.get("user_id") or ""
    school = (user_info.get(uid, {}).get("school") or "").strip().upper()
    return school == "POMONA"


def _filter_units_to_pomona(
    units: Sequence[Dict[str, Any]],
    user_info: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for unit in units:
        riders = []
        for rider in unit["riders"]:
            if not _is_pomona_rider(rider, user_info):
                continue
            uid = rider.get("user_id") or ""
            rider = dict(rider)
            rider["school"] = user_info.get(uid, {}).get("school", "")
            rider["name_label"] = user_info.get(uid, {}).get("label", uid)
            riders.append(rider)
        if not riders:
            continue
        filtered.append({
            **unit,
            "riders": riders,
        })
    return filtered


def _matched_units(rides: Dict[int, Dict[str, Any]], matches: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Set[int]]:
    flight_ids = [int(row["flight_id"]) for row in matches if row.get("flight_id") is not None]
    flights_by_id = _fetch_flights_by_id(flight_ids)
    matched_flight_ids = set(flights_by_id.keys())

    by_ride: Dict[int, List[Dict[str, Any]]] = {}
    for match in matches:
        ride_id = match.get("ride_id")
        flight_id = match.get("flight_id")
        if ride_id is None or flight_id is None:
            continue
        ride = rides.get(int(ride_id), {})
        if str(ride.get("ride_type") or "").strip().upper() == "CONNECT":
            continue
        flight = flights_by_id.get(int(flight_id))
        if not flight:
            continue
        if not _airport_is_lax(flight.get("airport")) or not bool(flight.get("to_airport")):
            continue
        if not _window_present(flight):
            continue

        rider = {
            "source_type": "matched",
            "ride_id": int(ride_id),
            "flight_id": int(flight_id),
            "user_id": flight.get("user_id") or match.get("user_id"),
            "date": _as_date_string(flight.get("date")),
            "earliest_time": _as_time_string(flight.get("earliest_time")),
            "latest_time": _as_time_string(flight.get("latest_time")),
            "bag_no": int(flight.get("bag_no") or 0),
            "bag_no_large": int(flight.get("bag_no_large") or 0),
            "bag_no_personal": int(flight.get("bag_no_personal") or 0),
        }
        by_ride.setdefault(int(ride_id), []).append(rider)

    units: List[Dict[str, Any]] = []
    for ride_id, riders in by_ride.items():
        if _common_window(riders) is None:
            continue
        units.append({
            "unit_type": "existing_ride",
            "unit_id": str(ride_id),
            "date": riders[0]["date"],
            "riders": riders,
        })
    return units, matched_flight_ids


def _unmatched_units(days_ahead: int, matched_flight_ids: Set[int]) -> List[Dict[str, Any]]:
    units: List[Dict[str, Any]] = []
    for flight in _fetch_future_lax_flights(days_ahead):
        flight_id = int(flight["flight_id"])
        if flight_id in matched_flight_ids:
            continue
        if flight.get("matched") is True:
            continue
        rider = {
            "source_type": "unmatched",
            "ride_id": "",
            "flight_id": flight_id,
            "user_id": flight.get("user_id"),
            "date": _as_date_string(flight.get("date")),
            "earliest_time": _as_time_string(flight.get("earliest_time")),
            "latest_time": _as_time_string(flight.get("latest_time")),
            "bag_no": int(flight.get("bag_no") or 0),
            "bag_no_large": int(flight.get("bag_no_large") or 0),
            "bag_no_personal": int(flight.get("bag_no_personal") or 0),
        }
        units.append({
            "unit_type": "unmatched_flight",
            "unit_id": str(flight_id),
            "date": rider["date"],
            "riders": [rider],
        })
    return units


def _unit_start(unit: Dict[str, Any]) -> datetime:
    return min(_rider_window(rider)[0] for rider in unit["riders"])


def _unit_end(unit: Dict[str, Any]) -> datetime:
    return max(_rider_window(rider)[1] for rider in unit["riders"])


def _unit_size(unit: Dict[str, Any]) -> int:
    return len(unit["riders"])


def _all_riders(units: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    riders: List[Dict[str, Any]] = []
    for unit in units:
        riders.extend(unit["riders"])
    return riders


def _rider_key(rider: Dict[str, Any]) -> str:
    return str(rider["flight_id"])


def _generate_candidates_for_date(units: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    riders = sorted(
        _all_riders(units),
        key=lambda rider: (_rider_window(rider)[0], _rider_window(rider)[1], int(rider["flight_id"])),
    )
    candidates_by_key: Dict[Tuple[str, ...], Dict[str, Any]] = {}

    def backtrack(start_idx: int, selected: List[Dict[str, Any]]) -> None:
        count = len(selected)
        if CONNECT_MIN <= count <= CONNECT_MAX:
            window = _common_window(selected)
            if window is not None:
                key = tuple(sorted(_rider_key(r) for r in selected))
                candidates_by_key[key] = {
                    "riders": list(selected),
                    "num_riders": count,
                    "window": window,
                }
        if count >= CONNECT_MAX:
            return

        current_window = _common_window(selected) if selected else None
        current_end = current_window[1] if current_window else None

        for idx in range(start_idx, len(riders)):
            rider = riders[idx]
            if current_end is not None and _rider_window(rider)[0] > current_end:
                break

            next_selected = selected + [rider]
            if _common_window(next_selected) is None:
                continue
            backtrack(idx + 1, next_selected)

    backtrack(0, [])
    return sorted(
        candidates_by_key.values(),
        key=lambda c: (c["num_riders"], c["window"][0], c["window"][1]),
        reverse=True,
    )


def _select_non_overlapping(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    used_flight_ids: Set[str] = set()
    for candidate in candidates:
        keys = {_rider_key(rider) for rider in candidate["riders"]}
        if keys & used_flight_ids:
            continue
        selected.append(candidate)
        used_flight_ids.update(keys)
    return selected


def _proposal_sort_key(candidate: Dict[str, Any]) -> Tuple[str, str, int]:
    riders = candidate.get("riders") or []
    date = riders[0]["date"] if riders else ""
    return (date, _suggested_time(riders), -int(candidate.get("num_riders") or 0))


def _filter_maximal_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keyed = []
    for candidate in candidates:
        flight_ids = frozenset(_rider_key(rider) for rider in candidate["riders"])
        keyed.append((candidate, flight_ids))

    out: List[Dict[str, Any]] = []
    for idx, (candidate, flight_ids) in enumerate(keyed):
        is_strict_subset = False
        for other_idx, (_, other_ids) in enumerate(keyed):
            if idx == other_idx:
                continue
            if flight_ids < other_ids:
                is_strict_subset = True
                break
        if not is_strict_subset:
            out.append(candidate)
    return out


def _pomona_matched_by_ride(units: Sequence[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    by_ride: Dict[int, List[Dict[str, Any]]] = {}
    for unit in units:
        for rider in unit["riders"]:
            if rider.get("source_type") != "matched" or not rider.get("ride_id"):
                continue
            by_ride.setdefault(int(rider["ride_id"]), []).append(rider)
    return by_ride


def _repair_groups_for_singletons(singletons: Sequence[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    remaining = sorted(singletons, key=lambda r: (_rider_window(r)[0], _rider_window(r)[1], int(r["flight_id"])))
    groups: List[List[Dict[str, Any]]] = []
    while remaining:
        seed = remaining[0]
        best: List[Dict[str, Any]] = []
        for other in remaining[1:]:
            pair = [seed, other]
            if _common_window(pair) is not None:
                best = pair
                break
        if not best:
            return []
        group = list(best)
        changed = True
        while changed and len(group) < 7:
            changed = False
            for rider in list(remaining):
                if rider in group:
                    continue
                trial = group + [rider]
                if _common_window(trial) is not None:
                    group = trial
                    changed = True
        groups.append(group)
        used = {_rider_key(r) for r in group}
        remaining = [r for r in remaining if _rider_key(r) not in used]
    return groups


def _replacement_repairs_for_singletons(
    candidate: Dict[str, Any],
    singletons: Sequence[Dict[str, Any]],
) -> List[str]:
    current = list(candidate["riders"])
    repairs: List[str] = []
    used_replacements: Set[str] = set()

    for singleton in singletons:
        fixed = False
        for replacement in list(current):
            if replacement.get("source_type") != "matched":
                continue
            if _rider_key(replacement) in used_replacements:
                continue
            trial = [
                singleton if _rider_key(rider) == _rider_key(replacement) else rider
                for rider in current
            ]
            if _common_window(trial) is None:
                continue
            used_replacements.add(_rider_key(replacement))
            current = trial
            repairs.append(
                f"swap_singleton_flight_{singleton['flight_id']}_into_connect_replacing_flight_{replacement['flight_id']}"
            )
            fixed = True
            break
        if not fixed:
            return []
    return repairs


def _evaluate_candidate(
    candidate: Dict[str, Any],
    matched_by_ride: Dict[int, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    selected_ids = {_rider_key(rider) for rider in candidate["riders"]}
    touched_ride_ids = sorted({
        int(rider["ride_id"])
        for rider in candidate["riders"]
        if rider.get("source_type") == "matched" and rider.get("ride_id")
    })
    singletons: List[Dict[str, Any]] = []
    for ride_id in touched_ride_ids:
        original = matched_by_ride.get(ride_id, [])
        remaining = [r for r in original if _rider_key(r) not in selected_ids]
        if len(remaining) == 1:
            singletons.extend(remaining)

    repair_groups = _repair_groups_for_singletons(singletons) if singletons else []
    replacement_repairs = []
    if singletons and not repair_groups:
        replacement_repairs = _replacement_repairs_for_singletons(candidate, singletons)

    if len(singletons) <= 2:
        status = "VALID_CONNECT_PROPOSAL"
    elif singletons and not repair_groups:
        status = "VALID_WITH_REPLACEMENTS" if replacement_repairs else "INVALID_LEAVES_SINGLETONS"
    else:
        status = "VALID_CONNECT_PROPOSAL"

    repair_bits = []
    for group in repair_groups:
        repair_bits.append("|".join(str(r["flight_id"]) for r in group))
    repair_bits.extend(replacement_repairs)

    return {
        "status": status,
        "leftover_singleton_flight_ids": "|".join(str(r["flight_id"]) for r in singletons),
        "leftover_repair_groups": ";".join(repair_bits),
    }


def _build_report_rows(candidates: List[Dict[str, Any]], matched_by_ride: Dict[int, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, candidate in enumerate(candidates, start=1):
        riders = candidate["riders"]
        evaluation = _evaluate_candidate(candidate, matched_by_ride)
        window_start, window_end = candidate["window"]
        existing_ride_ids = sorted({str(r["ride_id"]) for r in riders if r.get("ride_id")})
        unmatched_flight_ids = sorted(str(r["flight_id"]) for r in riders if r.get("source_type") == "unmatched")
        all_flight_ids = sorted(str(r["flight_id"]) for r in riders)

        matched_count = sum(1 for r in riders if r.get("source_type") == "matched")
        unmatched_count = sum(1 for r in riders if r.get("source_type") == "unmatched")
        source_summary = f"{matched_count} matched riders; {unmatched_count} unmatched riders"

        source_counts: Dict[str, int] = {}
        for rider in riders:
            if rider.get("source_type") == "matched":
                key = f"existing_ride {rider.get('ride_id')}"
            else:
                key = "unmatched_flights"
            source_counts[key] = source_counts.get(key, 0) + 1
        unit_bits = [f"{key} ({count} riders)" for key, count in sorted(source_counts.items())]

        rider_bits = []
        for rider in sorted(riders, key=lambda r: (_rider_window(r)[0], r["flight_id"])):
            name = rider.get("name_label") or rider.get("user_id") or ""
            rider_bits.append(
                f"flight {rider['flight_id']} {name} {rider['source_type']} "
                f"{_format_window(rider)}"
            )

        rows.append({
            "stranded_singletons_count": len(evaluation["leftover_singleton_flight_ids"].split("|")) if evaluation["leftover_singleton_flight_ids"] else 0,
            "proposal_id": idx,
            "date": riders[0]["date"] if riders else "",
            "num_riders": candidate["num_riders"],
            "suggested_time": _suggested_time(riders),
            "common_window_start": window_start.time().replace(microsecond=0).isoformat(),
            "common_window_end": window_end.time().replace(microsecond=0).isoformat(),
            "existing_ride_ids": "|".join(existing_ride_ids),
            "unmatched_flight_ids": "|".join(unmatched_flight_ids),
            "all_flight_ids": "|".join(all_flight_ids),
            "source_summary": source_summary,
            "source_detail": " | ".join(unit_bits),
            "leftover_singleton_flight_ids": evaluation["leftover_singleton_flight_ids"],
            "leftover_repair_groups": evaluation["leftover_repair_groups"],
            "per_rider_windows": " | ".join(rider_bits),
            "status": evaluation["status"],
        })
    return rows


def _parse_int_list(raw: str) -> List[int]:
    out: List[int] = []
    for part in str(raw or "").replace(" ", "").split(","):
        if not part:
            continue
        out.append(int(part))
    return out


def _parse_flight_group(raw: str) -> List[int]:
    return [int(part) for part in raw.split("|") if part.strip().isdigit()]


def _bag_units(riders: Sequence[Dict[str, Any]]) -> int:
    units = sum(
        int(r.get("bag_no_large") or 0) * config.LARGE_BAG_MULTIPLIER
        + int(r.get("bag_no") or 0)
        for r in riders
    )
    if config.PERSONAL_CONSTRAINT:
        units += sum(int(r.get("bag_no_personal") or 0) for r in riders)
    return units


def _determine_uber_type(group_size: int, bag_units: int) -> Optional[str]:
    if group_size in (2, 3):
        if bag_units <= 4:
            return "X"
        if bag_units <= 10:
            return "XL"
        if bag_units <= 12:
            return "XXL"
        return None
    if group_size == 4:
        if bag_units <= 3:
            return "X"
        if bag_units <= 7:
            return "XL"
        if bag_units <= 10:
            return "XXL"
        return None
    if group_size == 5:
        if bag_units <= 5:
            return "XL"
        if bag_units <= 8:
            return "XXL"
        return None
    if group_size == 6:
        if bag_units <= 3:
            return "XL"
        if bag_units <= 6:
            return "XXL"
        return None
    return None


def _fetch_matches_by_flight_ids(flight_ids: Sequence[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    clean_ids = sorted({int(fid) for fid in flight_ids})
    for i in range(0, len(clean_ids), 100):
        chunk = clean_ids[i : i + 100]
        if not chunk:
            continue
        resp = sb.table("Matches").select("*").in_("flight_id", chunk).execute()
        out.extend(resp.data or [])
    return out


def _insert_ride(date: str, time: str, ride_type: Optional[str]) -> int:
    payload: Dict[str, Any] = {"ride_date": date, "ride_time": time}
    if ride_type:
        payload["ride_type"] = ride_type
    resp = sb.table("Rides").insert(payload).execute()
    if not resp.data or "ride_id" not in resp.data[0]:
        raise RuntimeError(f"Failed to create ride for {date} {time}")
    return int(resp.data[0]["ride_id"])


def _insert_group_matches(
    ride_id: int,
    riders: Sequence[Dict[str, Any]],
    ride_type: Optional[str],
    source: str,
) -> None:
    window = _common_window(riders)
    if window is None:
        raise RuntimeError("Cannot insert a group without a common time window")

    date = riders[0]["date"]
    time = _suggested_time(riders)
    earliest_time = window[0].time().replace(microsecond=0).isoformat()
    latest_time = window[1].time().replace(microsecond=0).isoformat()
    if ride_type == "Connect":
        uber_type = "Connect"
    else:
        uber_type = _determine_uber_type(len(riders), _bag_units(riders))
        if uber_type is None:
            raise RuntimeError(
                "Repair group exceeds normal ride bag/group limits: "
                + "|".join(str(r["flight_id"]) for r in riders)
            )

    rows = []
    for rider in riders:
        rows.append({
            "ride_id": ride_id,
            "user_id": rider.get("user_id"),
            "flight_id": int(rider["flight_id"]),
            "date": date,
            "time": time,
            "earliest_time": earliest_time,
            "latest_time": latest_time,
            "source": source,
            "voucher": None,
            "contingency_voucher": None,
            "is_verified": False,
            "is_subsidized": False,
            "uber_type": uber_type,
        })
    sb.table("Matches").insert(rows).execute()


def _repair_groups_for_candidate(
    candidate: Dict[str, Any],
    matched_by_ride: Dict[int, List[Dict[str, Any]]],
    riders_by_flight_id: Dict[int, Dict[str, Any]],
) -> List[List[Dict[str, Any]]]:
    evaluation = _evaluate_candidate(candidate, matched_by_ride)
    groups: List[List[Dict[str, Any]]] = []
    for raw_group in (evaluation.get("leftover_repair_groups") or "").split(";"):
        raw_group = raw_group.strip()
        if not raw_group or raw_group.startswith("swap_"):
            continue
        flight_ids = _parse_flight_group(raw_group)
        if len(flight_ids) < 2:
            continue
        group = [riders_by_flight_id[fid] for fid in flight_ids if fid in riders_by_flight_id]
        if len(group) == len(flight_ids):
            groups.append(group)
    return groups


def _apply_selected_proposals(
    proposals: Sequence[Dict[str, Any]],
    rows: Sequence[Dict[str, Any]],
    selected_ids: Sequence[int],
    matched_by_ride: Dict[int, List[Dict[str, Any]]],
    units: Sequence[Dict[str, Any]],
    confirm: bool,
) -> None:
    rows_by_id = {int(row["proposal_id"]): row for row in rows}
    proposals_by_id = {idx: proposal for idx, proposal in enumerate(proposals, start=1)}
    missing = [pid for pid in selected_ids if pid not in proposals_by_id]
    if missing:
        raise SystemExit(f"Unknown proposal_id(s): {','.join(str(pid) for pid in missing)}")

    riders_by_flight_id = {
        int(rider["flight_id"]): rider
        for unit in units
        for rider in unit["riders"]
    }
    connect_jobs: List[Tuple[int, Dict[str, Any]]] = []
    repair_jobs: List[Tuple[int, List[Dict[str, Any]]]] = []
    all_new_group_flight_ids: Set[int] = set()

    for proposal_id in selected_ids:
        row = rows_by_id[proposal_id]
        if row["status"] not in ("VALID_CONNECT_PROPOSAL", "VALID_WITH_REPLACEMENTS"):
            raise SystemExit(f"Proposal {proposal_id} is not valid: {row['status']}")
        candidate = proposals_by_id[proposal_id]
        candidate_ids = {int(r["flight_id"]) for r in candidate["riders"]}
        overlap = all_new_group_flight_ids & candidate_ids
        if overlap:
            raise SystemExit(f"Selected proposals overlap on flight_id(s): {sorted(overlap)}")
        all_new_group_flight_ids.update(candidate_ids)
        connect_jobs.append((proposal_id, candidate))

        for repair_group in _repair_groups_for_candidate(candidate, matched_by_ride, riders_by_flight_id):
            repair_ids = {int(r["flight_id"]) for r in repair_group}
            overlap = all_new_group_flight_ids & repair_ids
            if overlap:
                raise SystemExit(f"Repair group overlaps selected Connect riders: {sorted(overlap)}")
            all_new_group_flight_ids.update(repair_ids)
            repair_jobs.append((proposal_id, repair_group))

    old_matches = _fetch_matches_by_flight_ids(sorted(all_new_group_flight_ids))
    old_ride_ids = sorted({
        int(row["ride_id"])
        for row in old_matches
        if row.get("ride_id") is not None
    })

    print("Apply plan:")
    for proposal_id, candidate in connect_jobs:
        ids = "|".join(str(r["flight_id"]) for r in candidate["riders"])
        print(f"  Connect proposal {proposal_id}: {len(candidate['riders'])} riders -> flights {ids}")
    for proposal_id, repair_group in repair_jobs:
        ids = "|".join(str(r["flight_id"]) for r in repair_group)
        print(f"  Repair group from proposal {proposal_id}: {len(repair_group)} riders -> flights {ids}")
    print(f"  Existing ride IDs that may be split/cleaned up: {'|'.join(str(rid) for rid in old_ride_ids) or '(none)'}")

    if not confirm:
        print("No Supabase changes were made. Re-run with --confirm-apply to write this plan.")
        return

    created_ride_ids: List[int] = []
    for proposal_id, candidate in connect_jobs:
        riders = candidate["riders"]
        ride_id = _insert_ride(riders[0]["date"], _suggested_time(riders), "Connect")
        _insert_group_matches(ride_id, riders, "Connect", "ml_connect_merge")
        created_ride_ids.append(ride_id)
        print(f"Created Connect ride {ride_id} from proposal {proposal_id}.")

    for proposal_id, repair_group in repair_jobs:
        ride_id = _insert_ride(repair_group[0]["date"], _suggested_time(repair_group), None)
        _insert_group_matches(ride_id, repair_group, None, "ml_connect_repair")
        created_ride_ids.append(ride_id)
        print(f"Created repair ride {ride_id} from proposal {proposal_id}.")

    for old_match in old_matches:
        sb.table("Matches").delete().eq("ride_id", old_match["ride_id"]).eq("flight_id", old_match["flight_id"]).execute()

    if all_new_group_flight_ids:
        sb.table("Flights").update({"matched": True, "original_unmatched": False}).in_(
            "flight_id", sorted(all_new_group_flight_ids)
        ).execute()

    for old_ride_id in old_ride_ids:
        remaining = sb.table("Matches").select("flight_id").eq("ride_id", old_ride_id).execute()
        if not remaining.data:
            sb.table("Rides").delete().eq("ride_id", old_ride_id).execute()

    print(
        f"Applied {len(connect_jobs)} Connect proposal(s), "
        f"{len(repair_jobs)} repair group(s), created ride IDs: {'|'.join(str(rid) for rid in created_ride_ids)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan LAX Connect shuttle candidates for review.")
    parser.add_argument("--days-ahead", type=int, default=20, help="Look from today through N days ahead.")
    parser.add_argument(
        "--csv",
        default="../matches/connect_shuttle_candidates.csv",
        help="CSV output path for proposal review.",
    )
    parser.add_argument(
        "--selected-only",
        action="store_true",
        help="Write only a greedy non-overlapping shortlist. Default writes all candidate proposals.",
    )
    parser.add_argument(
        "--include-subsets",
        action="store_true",
        help="Include candidate groups that are strict subsets of larger candidates. Default writes maximal candidates only.",
    )
    parser.add_argument(
        "--apply-proposal-ids",
        default="",
        help="Comma-separated proposal_id values to apply after review, for example 2,4,7.",
    )
    parser.add_argument(
        "--confirm-apply",
        action="store_true",
        help="Actually write selected proposal IDs to Supabase. Without this flag, apply mode prints a plan only.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    rides = _fetch_rides(args.days_ahead)
    matches = _fetch_matches(list(rides.keys()))
    matched_units, matched_flight_ids = _matched_units(rides, matches)
    unmatched_units = _unmatched_units(args.days_ahead, matched_flight_ids)

    user_ids = [
        rider.get("user_id")
        for unit in matched_units + unmatched_units
        for rider in unit["riders"]
        if rider.get("user_id")
    ]
    user_info = _fetch_user_info(user_ids)
    matched_units = _filter_units_to_pomona(matched_units, user_info)
    unmatched_units = _filter_units_to_pomona(unmatched_units, user_info)
    matched_by_ride = _pomona_matched_by_ride(matched_units)
    units = matched_units + unmatched_units

    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for unit in units:
        by_date.setdefault(unit["date"], []).append(unit)

    proposals: List[Dict[str, Any]] = []
    for date, date_units in sorted(by_date.items()):
        candidates = _generate_candidates_for_date(date_units)
        if not args.include_subsets:
            candidates = _filter_maximal_candidates(candidates)
        if args.selected_only:
            valid_candidates = [
                candidate
                for candidate in candidates
                if _evaluate_candidate(candidate, matched_by_ride)["status"] in ("VALID_CONNECT_PROPOSAL", "VALID_WITH_REPLACEMENTS")
            ]
            proposals.extend(_select_non_overlapping(valid_candidates))
        else:
            proposals.extend(candidates)

    proposals.sort(key=_proposal_sort_key)
    rows = _build_report_rows(proposals, matched_by_ride)

    os.makedirs(os.path.dirname(args.csv), exist_ok=True)
    with open(args.csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    selected_ids = _parse_int_list(args.apply_proposal_ids)
    if selected_ids:
        _apply_selected_proposals(
            proposals=proposals,
            rows=rows,
            selected_ids=selected_ids,
            matched_by_ride=matched_by_ride,
            units=units,
            confirm=args.confirm_apply,
        )

    matched_riders = sum(len(unit["riders"]) for unit in matched_units)
    unmatched_riders = sum(len(unit["riders"]) for unit in unmatched_units)
    valid_count = sum(1 for row in rows if row["status"] in ("VALID_CONNECT_PROPOSAL", "VALID_WITH_REPLACEMENTS"))
    replacement_valid_count = sum(1 for row in rows if row["status"] == "VALID_WITH_REPLACEMENTS")
    invalid_singleton_count = sum(1 for row in rows if row["status"] == "INVALID_LEAVES_SINGLETONS")
    proposed_riders = sum(
        int(row["num_riders"])
        for row in rows
        if row["status"] in ("VALID_CONNECT_PROPOSAL", "VALID_WITH_REPLACEMENTS")
    )
    print(f"Existing matched Pomona LAX outbound riders considered: {matched_riders}")
    print(f"Unmatched Pomona LAX outbound riders considered: {unmatched_riders}")
    mode = "selected non-overlapping proposals" if args.selected_only else "all candidate proposals"
    print(f"Valid Connect proposals ({mode}): {valid_count} | Valid proposal rider-count sum: {proposed_riders}")
    print(f"Valid only with replacement suggestions: {replacement_valid_count}")
    print(f"Invalid proposals leaving matched singletons: {invalid_singleton_count}")
    print(f"Wrote review CSV: {args.csv}")
    if not selected_ids or not args.confirm_apply:
        print("No Supabase changes were made.")


if __name__ == "__main__":
    main()

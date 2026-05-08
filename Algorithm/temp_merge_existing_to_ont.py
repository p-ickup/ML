"""
Plan ONT combined-shuttle candidates from existing matches plus unmatched flights.

Review-first flow:
1) Gather ONT outbound riders from existing matched rides and unmatched flights.
2) Keep Pomona riders only, and only dates in MM-DD range 05-12..05-20.
3) Exhaustively build merge candidates with overlapping windows and total size 6..25.
4) Write a review CSV.
5) Optionally apply selected proposal IDs.

Run from Algorithm/:
    python3 temp_merge_existing_to_ont.py
    python3 temp_merge_existing_to_ont.py --csv ../matches/ont_merge_candidates.csv
    python3 temp_merge_existing_to_ont.py --include-subsets
    python3 temp_merge_existing_to_ont.py --apply-proposal-ids 1,3
    python3 temp_merge_existing_to_ont.py --apply-proposal-ids 1,3 --confirm-apply
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

from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Set SUPABASE_URL and SUPABASE_SECRET_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

ONT_MIN = 6
ONT_MAX = 25
DEFAULT_START_MMDD = "05-12"
DEFAULT_END_MMDD = "05-20"

REPORT_FIELDS = [
    "proposal_id",
    "date",
    "num_riders",
    "num_units",
    "num_unmatched_added",
    "suggested_time",
    "common_window_start",
    "common_window_end",
    "existing_ride_ids",
    "unmatched_flight_ids",
    "all_flight_ids",
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


def _window_present(row: Dict[str, Any]) -> bool:
    return bool(
        _as_date_string(row.get("date"))
        and _as_time_string(row.get("earliest_time"))
        and _as_time_string(row.get("latest_time"))
    )


def _airport_is_ont(value: Any) -> bool:
    return str(value or "").strip().upper() == "ONT"


def _parse_mmdd(mmdd: str, year: int) -> datetime.date:
    return datetime.strptime(f"{year}-{mmdd}", "%Y-%m-%d").date()


def _fetch_rides_by_date(start_date: str, end_date: str) -> Dict[int, Dict[str, Any]]:
    resp = (
        sb.table("Rides")
        .select("*")
        .gte("ride_date", start_date)
        .lte("ride_date", end_date)
        .execute()
    )
    return {int(row["ride_id"]): row for row in (resp.data or [])}


def _fetch_matches_by_ride_ids(ride_ids: Sequence[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(0, len(ride_ids), 100):
        chunk = list(ride_ids[i : i + 100])
        if not chunk:
            continue
        resp = sb.table("Matches").select("*").in_("ride_id", chunk).execute()
        out.extend(resp.data or [])
    return out


def _fetch_flights_by_ids(flight_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    clean = sorted({int(fid) for fid in flight_ids})
    for i in range(0, len(clean), 100):
        chunk = clean[i : i + 100]
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


def _fetch_unmatched_ont_flights(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    resp = (
        sb.table("Flights")
        .select(
            "flight_id,user_id,airport,to_airport,date,earliest_time,latest_time,matched,"
            "bag_no,bag_no_large,bag_no_personal"
        )
        .gte("date", start_date)
        .lte("date", end_date)
        .execute()
    )
    rows: List[Dict[str, Any]] = []
    for row in resp.data or []:
        # Treat both NULL and False as unmatched; True is matched.
        if row.get("matched") is True:
            continue
        if not _airport_is_ont(row.get("airport")):
            continue
        if not bool(row.get("to_airport")):
            continue
        if not _window_present(row):
            continue
        rows.append(row)
    return rows


def _fetch_user_schools(user_ids: Sequence[str]) -> Dict[str, str]:
    clean = sorted({uid for uid in user_ids if uid})
    out: Dict[str, str] = {}
    for i in range(0, len(clean), 100):
        chunk = clean[i : i + 100]
        if not chunk:
            continue
        resp = sb.table("Users").select("user_id,school").in_("user_id", chunk).execute()
        for row in resp.data or []:
            out[str(row["user_id"])] = str(row.get("school") or "")
    return out


def _is_pomona(rider: Dict[str, Any], schools: Dict[str, str]) -> bool:
    uid = str(rider.get("user_id") or "")
    return (schools.get(uid) or "").strip().upper() == "POMONA"


def _window_for_riders(riders: Sequence[Dict[str, Any]]) -> Optional[Tuple[datetime, datetime]]:
    if not riders:
        return None
    starts: List[datetime] = []
    ends: List[datetime] = []
    for r in riders:
        start = _parse_dt(_as_date_string(r["date"]), r["earliest_time"])
        end = _parse_dt(_as_date_string(r["date"]), r["latest_time"])
        if end < start:
            end += timedelta(days=1)
        starts.append(start)
        ends.append(end)
    s = max(starts)
    e = min(ends)
    if s > e:
        return None
    return s, e


def _suggested_time(window: Tuple[datetime, datetime]) -> str:
    start, end = window
    if end <= start:
        chosen = start
    else:
        chosen = end - timedelta(minutes=15)
        if chosen < start:
            chosen = end
    return chosen.time().replace(microsecond=0).isoformat()


def _build_units(
    rides: Dict[int, Dict[str, Any]],
    matches: List[Dict[str, Any]],
    flights_by_id: Dict[int, Dict[str, Any]],
    unmatched_flights: Sequence[Dict[str, Any]],
    schools: Dict[str, str],
) -> List[Dict[str, Any]]:
    units: List[Dict[str, Any]] = []

    by_ride: Dict[int, List[Dict[str, Any]]] = {}
    for m in matches:
        rid = m.get("ride_id")
        if rid is None:
            continue
        by_ride.setdefault(int(rid), []).append(m)

    # Existing matched rides: only all-Pomona ONT outbound rides.
    for ride_id, rows in by_ride.items():
        riders: List[Dict[str, Any]] = []
        all_ont_to = True
        all_pomona = True
        for row in rows:
            fid = row.get("flight_id")
            if fid is None:
                all_ont_to = False
                break
            f = flights_by_id.get(int(fid))
            if not f or not _airport_is_ont(f.get("airport")) or not bool(f.get("to_airport")):
                all_ont_to = False
                break
            if not _window_present(f):
                all_ont_to = False
                break
            if not _is_pomona(f, schools):
                all_pomona = False
                break
            riders.append(f)
        if not all_ont_to or not all_pomona or len(riders) < 1:
            continue
        w = _window_for_riders(riders)
        if not w:
            continue
        units.append(
            {
                "type": "matched_ride",
                "ride_id": ride_id,
                "date": riders[0]["date"],
                "riders": riders,
                "start": w[0],
                "end": w[1],
            }
        )

    # Unmatched ONT outbound flights as singleton units.
    for f in unmatched_flights:
        if not _is_pomona(f, schools):
            continue
        riders = [f]
        w = _window_for_riders(riders)
        if not w:
            continue
        units.append(
            {
                "type": "unmatched_singleton",
                "ride_id": None,
                "date": f["date"],
                "riders": riders,
                "start": w[0],
                "end": w[1],
            }
        )

    return units


def _candidate_from_units(
    group_units: Sequence[Dict[str, Any]],
    window: Tuple[datetime, datetime],
) -> Dict[str, Any]:
    riders: List[Dict[str, Any]] = []
    for u in group_units:
        riders.extend(u["riders"])
    flight_ids = sorted({int(r["flight_id"]) for r in riders})
    return {
        "date": group_units[0]["date"],
        "units": list(group_units),
        "riders": riders,
        "flight_ids": flight_ids,
        "window": window,
    }


def _build_candidates_for_date(units: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not units:
        return []
    ordered = sorted(units, key=lambda u: (u["start"], u["end"]))
    raw: List[Dict[str, Any]] = []
    suffix_rider_counts = [0] * (len(ordered) + 1)

    for idx in range(len(ordered) - 1, -1, -1):
        suffix_rider_counts[idx] = suffix_rider_counts[idx + 1] + len(
            ordered[idx]["riders"]
        )

    def backtrack(
        start_index: int,
        group_units: List[Dict[str, Any]],
        common_start: Optional[datetime],
        common_end: Optional[datetime],
        rider_count: int,
    ) -> None:
        if (
            rider_count >= ONT_MIN
            and common_start is not None
            and common_end is not None
        ):
            raw.append(_candidate_from_units(group_units, (common_start, common_end)))

        if start_index >= len(ordered):
            return

        if rider_count + suffix_rider_counts[start_index] < ONT_MIN:
            return

        for idx in range(start_index, len(ordered)):
            u = ordered[idx]
            next_count = rider_count + len(u["riders"])
            if next_count > ONT_MAX:
                continue
            next_start = (
                u["start"] if common_start is None else max(common_start, u["start"])
            )
            next_end = u["end"] if common_end is None else min(common_end, u["end"])
            if next_start > next_end:
                continue
            group_units.append(u)
            backtrack(idx + 1, group_units, next_start, next_end, next_count)
            group_units.pop()

    backtrack(0, [], None, None, 0)

    # Deduplicate by exact flight_id set.
    dedup: Dict[Tuple[int, ...], Dict[str, Any]] = {}
    for c in sorted(
        raw,
        key=lambda x: (
            -len(x["flight_ids"]),
            -sum(1 for u in x["units"] if u["type"] == "unmatched_singleton"),
            -(x["window"][1] - x["window"][0]).total_seconds(),
            len(x["units"]),
        ),
    ):
        key = tuple(c["flight_ids"])
        if key not in dedup:
            dedup[key] = c
    return list(dedup.values())


def _filter_maximal(candidates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    sets = [set(c["flight_ids"]) for c in candidates]
    for i, c in enumerate(candidates):
        s = sets[i]
        if any(i != j and s < sets[j] for j in range(len(candidates))):
            continue
        out.append(c)
    return out


def _select_non_overlapping(candidates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(
        candidates,
        key=lambda c: (-len(c["riders"]), len(c["flight_ids"]), c["date"]),
    )
    selected: List[Dict[str, Any]] = []
    used: Set[int] = set()
    for c in ordered:
        ids = set(c["flight_ids"])
        if ids & used:
            continue
        selected.append(c)
        used.update(ids)
    return selected


def _pick_best_candidate(candidates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Pick one best candidate:
    1) most unmatched riders added
    2) most riders total
    3) widest common window
    4) fewer source units (prefer cleaner merges)
    5) deterministic by flight_ids
    """
    if not candidates:
        return []
    best = min(
        candidates,
        key=lambda c: (
            -sum(1 for u in c["units"] if u["type"] == "unmatched_singleton"),
            -len(c["riders"]),
            -(c["window"][1] - c["window"][0]).total_seconds(),
            len(c["units"]),
            c["flight_ids"],
        ),
    )
    return [best]


def _build_report_rows(candidates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, c in enumerate(candidates, start=1):
        existing_ride_ids = sorted(
            {
                int(u["ride_id"])
                for u in c["units"]
                if u.get("ride_id") is not None and u["type"] == "matched_ride"
            }
        )
        unmatched_ids = sorted(
            {
                int(r["flight_id"])
                for u in c["units"]
                if u["type"] == "unmatched_singleton"
                for r in u["riders"]
            }
        )
        win = c["window"]
        rows.append(
            {
                "proposal_id": idx,
                "date": c["date"],
                "num_riders": len(c["riders"]),
                "num_units": len(c["units"]),
                "num_unmatched_added": sum(
                    1 for u in c["units"] if u["type"] == "unmatched_singleton"
                ),
                "suggested_time": _suggested_time(win),
                "common_window_start": win[0].time().replace(microsecond=0).isoformat(),
                "common_window_end": win[1].time().replace(microsecond=0).isoformat(),
                "existing_ride_ids": "|".join(str(x) for x in existing_ride_ids),
                "unmatched_flight_ids": "|".join(str(x) for x in unmatched_ids),
                "all_flight_ids": "|".join(str(x) for x in c["flight_ids"]),
                "status": "VALID_ONT_COMBINE",
            }
        )
    return rows


def _parse_int_list(raw: str) -> List[int]:
    out: List[int] = []
    for part in str(raw or "").replace(" ", "").split(","):
        if not part:
            continue
        out.append(int(part))
    return out


def _fetch_matches_by_flight_ids(flight_ids: Sequence[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    clean = sorted({int(fid) for fid in flight_ids})
    for i in range(0, len(clean), 100):
        chunk = clean[i : i + 100]
        if not chunk:
            continue
        resp = sb.table("Matches").select("*").in_("flight_id", chunk).execute()
        out.extend(resp.data or [])
    return out


def _insert_ride(date: str, time: str) -> int:
    payload: Dict[str, Any] = {"ride_date": date, "ride_time": time, "ride_type": "Connect"}
    resp = sb.table("Rides").insert(payload).execute()
    if not resp.data or "ride_id" not in resp.data[0]:
        raise RuntimeError(f"Failed to create ride for {date} {time}")
    return int(resp.data[0]["ride_id"])


def _insert_group_matches(ride_id: int, riders: Sequence[Dict[str, Any]], source: str) -> None:
    w = _window_for_riders(riders)
    if not w:
        raise RuntimeError("Cannot insert group without a common window")
    date = _as_date_string(riders[0]["date"])
    time = _suggested_time(w)
    earliest_time = w[0].time().replace(microsecond=0).isoformat()
    latest_time = w[1].time().replace(microsecond=0).isoformat()

    rows = []
    for rider in riders:
        rows.append(
            {
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
                "uber_type": "Connect",
            }
        )
    sb.table("Matches").insert(rows).execute()


def _apply_selected_proposals(
    proposals: Sequence[Dict[str, Any]],
    selected_ids: Sequence[int],
    confirm: bool,
) -> None:
    by_id = {idx: p for idx, p in enumerate(proposals, start=1)}
    missing = [pid for pid in selected_ids if pid not in by_id]
    if missing:
        raise SystemExit(f"Unknown proposal_id(s): {','.join(str(x) for x in missing)}")

    picked: List[Tuple[int, Dict[str, Any]]] = []
    used_flight_ids: Set[int] = set()
    for pid in selected_ids:
        p = by_id[pid]
        overlap = used_flight_ids & set(p["flight_ids"])
        if overlap:
            raise SystemExit(f"Selected proposals overlap on flight_id(s): {sorted(overlap)}")
        used_flight_ids.update(p["flight_ids"])
        picked.append((pid, p))

    old_matches = _fetch_matches_by_flight_ids(sorted(used_flight_ids))
    old_ride_ids = sorted({int(m["ride_id"]) for m in old_matches if m.get("ride_id") is not None})

    print("Apply plan:")
    for pid, p in picked:
        print(
            f"  Proposal {pid}: {len(p['riders'])} riders -> flights "
            f"{'|'.join(str(fid) for fid in p['flight_ids'])}"
        )
    print(f"  Existing ride IDs affected: {'|'.join(str(r) for r in old_ride_ids) or '(none)'}")
    if not confirm:
        print("No Supabase changes were made. Re-run with --confirm-apply to write this plan.")
        return

    created: List[int] = []
    for pid, p in picked:
        ride_id = _insert_ride(p["date"], _suggested_time(p["window"]))
        _insert_group_matches(ride_id, p["riders"], source="ml_ont_connect_merge")
        created.append(ride_id)
        print(f"Created ONT combined ride {ride_id} from proposal {pid}.")

    for m in old_matches:
        (
            sb.table("Matches")
            .delete()
            .eq("ride_id", m["ride_id"])
            .eq("flight_id", m["flight_id"])
            .execute()
        )

    if used_flight_ids:
        sb.table("Flights").update({"matched": True, "original_unmatched": False}).in_(
            "flight_id",
            sorted(used_flight_ids),
        ).execute()

    for old_ride_id in old_ride_ids:
        remaining = sb.table("Matches").select("flight_id").eq("ride_id", old_ride_id).execute()
        if not remaining.data:
            sb.table("Rides").delete().eq("ride_id", old_ride_id).execute()

    print(
        f"Applied {len(picked)} proposal(s), created rides: {'|'.join(str(r) for r in created)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan ONT combine candidates for review.")
    parser.add_argument(
        "--csv",
        default="../matches/ont_merge_candidates.csv",
        help="CSV output path.",
    )
    parser.add_argument(
        "--start-mmdd",
        default=DEFAULT_START_MMDD,
        help=f"Start MM-DD inclusive (default: {DEFAULT_START_MMDD}).",
    )
    parser.add_argument(
        "--end-mmdd",
        default=DEFAULT_END_MMDD,
        help=f"End MM-DD inclusive (default: {DEFAULT_END_MMDD}).",
    )
    parser.add_argument(
        "--selected-only",
        action="store_true",
        help="Write only non-overlapping proposals selected greedily.",
    )
    parser.add_argument(
        "--include-subsets",
        action="store_true",
        help="Write every valid exhaustive subset instead of only maximal proposals.",
    )
    parser.add_argument(
        "--best-only",
        action="store_true",
        help="Write only the single best proposal across all dates.",
    )
    parser.add_argument(
        "--apply-proposal-ids",
        default="",
        help="Comma-separated proposal_id values to apply.",
    )
    parser.add_argument(
        "--confirm-apply",
        action="store_true",
        help="Actually write selected proposals to Supabase.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    year = datetime.today().year
    start_date = _parse_mmdd(args.start_mmdd, year)
    end_date = _parse_mmdd(args.end_mmdd, year)
    if start_date > end_date:
        raise SystemExit("start-mmdd must be <= end-mmdd")

    rides = _fetch_rides_by_date(start_date.isoformat(), end_date.isoformat())
    matches = _fetch_matches_by_ride_ids(list(rides.keys())) if rides else []
    matched_flight_ids = [int(m["flight_id"]) for m in matches if m.get("flight_id") is not None]
    flights_by_id = _fetch_flights_by_ids(matched_flight_ids) if matched_flight_ids else {}

    unmatched = _fetch_unmatched_ont_flights(start_date.isoformat(), end_date.isoformat())

    user_ids: List[str] = []
    for f in flights_by_id.values():
        uid = f.get("user_id")
        if uid:
            user_ids.append(str(uid))
    for f in unmatched:
        uid = f.get("user_id")
        if uid:
            user_ids.append(str(uid))
    schools = _fetch_user_schools(user_ids)

    units = _build_units(rides, matches, flights_by_id, unmatched, schools)
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for u in units:
        by_date.setdefault(_as_date_string(u["date"]), []).append(u)

    proposals: List[Dict[str, Any]] = []
    for _, date_units in sorted(by_date.items()):
        cands = _build_candidates_for_date(date_units)
        if not args.include_subsets:
            cands = _filter_maximal(cands)
        if args.selected_only:
            cands = _select_non_overlapping(cands)
        proposals.extend(cands)

    if args.best_only:
        proposals = _pick_best_candidate(proposals)

    proposals.sort(key=lambda c: (c["date"], -len(c["riders"]), c["flight_ids"]))
    rows = _build_report_rows(proposals)

    out_dir = os.path.dirname(args.csv)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    selected_ids = _parse_int_list(args.apply_proposal_ids)
    if selected_ids:
        _apply_selected_proposals(proposals, selected_ids, args.confirm_apply)

    considered_matched = sum(
        len(u["riders"]) for u in units if u["type"] == "matched_ride"
    )
    considered_unmatched = sum(
        len(u["riders"]) for u in units if u["type"] == "unmatched_singleton"
    )
    print(
        f"Considered Pomona ONT outbound riders in range {args.start_mmdd}..{args.end_mmdd}: "
        f"matched={considered_matched}, unmatched={considered_unmatched}"
    )
    print(f"Valid ONT combine proposals (size {ONT_MIN}-{ONT_MAX}): {len(rows)}")
    print(f"Wrote review CSV: {args.csv}")
    if not selected_ids or not args.confirm_apply:
        print("No Supabase changes were made.")


if __name__ == "__main__":
    main()

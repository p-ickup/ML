"""
Temporary script: look at existing matches in the next 20 days, find groups that can be
combined into a Connect shuttle.
- Only flights TO LAX (outbound: to_airport True, airport LAX). Not FROM LAX / not ONT.
- Combined group size must be 8-25 people or 30-55 people (Connect shuttle tiers).
- All groups in a merge must have overlapping times (including 0-min touch, e.g. 7pm-7pm).
- Writes a CSV with merge report (original uber types, ride_ids combined).
- For each merge found: update DB to use one ride_id, set Connect, clear voucher, set suggested time.
Run from Algorithm/ with: python temp_merge_existing_to_connect.py
"""
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Add parent so we can import config and use supabase
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

import config
from supabase import create_client

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Set SUPABASE_URL and SUPABASE_SECRET_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# Connect shuttle size bounds (same as config)
MIN_S = getattr(config, "LAX_CONNECT_SHUTTLE_MIN", 8)
T1_MAX = getattr(config, "LAX_CONNECT_SHUTTLE_TIER1_MAX", 25)
T2_MIN = getattr(config, "LAX_CONNECT_SHUTTLE_TIER2_MIN", 30)
T2_MAX = getattr(config, "LAX_CONNECT_SHUTTLE_TIER2_MAX", 55)
OVERLAP_GRACE_MIN = getattr(config, "OVERLAP_GRACE_MIN", 10)
ALLOW_TOUCHING = getattr(config, "ALLOW_TOUCHING", True)


def _parse_dt(date_str: str, time_str: str) -> datetime:
    """Parse date and time strings (time may be HH:MM:SS or full ISO)."""
    t = str(time_str) if time_str else "00:00:00"
    if "T" in t:
        t = t.split("T")[-1][:12]
    if len(t) < 8:
        t = t + "00" * (8 - len(t))
    return datetime.fromisoformat(f"{date_str}T{t}")


def _rider_window(r: Dict[str, Any]) -> Tuple[datetime, datetime]:
    """(start, end) for a rider row. Prefer flight window (earliest_time_from_flight/latest_time_from_flight) when present."""
    d = r.get("date")
    if d is None:
        d = ""
    if hasattr(d, "strftime"):
        d = d.strftime("%Y-%m-%d")
    d = str(d)
    earliest = r.get("earliest_time_from_flight") or r.get("earliest_time")
    latest = r.get("latest_time_from_flight") or r.get("latest_time")
    s = _parse_dt(d, earliest)
    e = _parse_dt(d, latest)
    if e < s:
        e += timedelta(days=1)
    return s, e


def have_overlap(riders: List[Dict[str, Any]]) -> bool:
    """True if all riders have a common time overlap (including 0-min touch with grace)."""
    if not riders:
        return False
    starts = []
    ends = []
    for r in riders:
        s, e = _rider_window(r)
        starts.append(s)
        ends.append(e)
    latest_start = max(starts)
    earliest_end = min(ends)
    overlap_min = (earliest_end - latest_start).total_seconds() / 60.0
    if overlap_min >= 0:
        return True
    if ALLOW_TOUCHING and (-overlap_min) <= OVERLAP_GRACE_MIN:
        return True
    return False


def merged_suggested_time(riders: List[Dict[str, Any]]) -> str:
    """
    Suggested time: same rule as matching.
    TO airport: 15 min before latest time of overlap (if possible), else latest time.
    FROM airport: 15 min after earliest time of overlap.
    """
    if not riders:
        return "00:00:00"
    starts = []
    ends = []
    for r in riders:
        s, e = _rider_window(r)
        starts.append(s)
        ends.append(e)
    latest_start = max(starts)
    earliest_end = min(ends)
    to_airport = bool(riders[0].get("to_airport"))
    if earliest_end <= latest_start:
        chosen = latest_start
    else:
        if to_airport:
            chosen = earliest_end - timedelta(minutes=15)
            if chosen < latest_start:
                chosen = earliest_end
        else:
            chosen = latest_start + timedelta(minutes=15)
            if chosen > earliest_end:
                chosen = earliest_end
    return chosen.time().replace(microsecond=0).isoformat()


def main():
    days_ahead = 20
    today = datetime.today().date()
    end_date = (today + timedelta(days=days_ahead)).isoformat()

    # 1) Fetch Rides in date range
    ride_resp = (
        sb.table("Rides")
        .select("*")
        .gte("ride_date", today.isoformat())
        .lte("ride_date", end_date)
        .execute()
    )
    rides = {r["ride_id"]: r for r in (ride_resp.data or [])}
    ride_ids = list(rides.keys())
    if not ride_ids:
        print("No rides in the next 20 days.")
        return

    # 2) Fetch Matches for those ride_ids
    all_matches: List[Dict] = []
    for i in range(0, len(ride_ids), 100):
        chunk = ride_ids[i : i + 100]
        mat_resp = sb.table("Matches").select("*").in_("ride_id", chunk).execute()
        all_matches.extend(mat_resp.data or [])

    if not all_matches:
        print("No matches for those rides.")
        return

    # 3) Fetch Flights for flight_ids to get airport, to_airport, date, earliest_time, latest_time
    flight_ids = list({m["flight_id"] for m in all_matches})
    flights_by_id: Dict[int, Dict] = {}
    for j in range(0, len(flight_ids), 100):
        fc = flight_ids[j : j + 100]
        fl_resp = sb.table("Flights").select("flight_id,airport,to_airport,date,earliest_time,latest_time").in_("flight_id", fc).execute()
        for f in fl_resp.data or []:
            flights_by_id[int(f["flight_id"])] = f

    # Enrich each match row with flight info
    for m in all_matches:
        fid = m.get("flight_id")
        if fid is not None:
            fl = flights_by_id.get(int(fid))
            if fl:
                m["airport"] = (fl.get("airport") or "").strip().upper()
                m["to_airport"] = bool(fl.get("to_airport"))
                m["date"] = fl.get("date")
                m["earliest_time_from_flight"] = fl.get("earliest_time")
                m["latest_time_from_flight"] = fl.get("latest_time")
                if isinstance(m["date"], datetime):
                    m["date"] = m["date"].strftime("%Y-%m-%d")

    # 4) Build groups by ride_id (only flights TO LAX — outbound; not FROM LAX, not ONT)
    by_ride: Dict[int, List[Dict]] = {}
    for m in all_matches:
        rid = m["ride_id"]
        if m.get("airport") != "LAX" or not m.get("to_airport"):
            continue
        by_ride.setdefault(rid, []).append(m)

    # 5) Group by date for merge logic
    by_date: Dict[str, List[Tuple[int, List[Dict]]]] = {}
    for ride_id, rows in by_ride.items():
        if not rows:
            continue
        date_val = rows[0].get("date") or rides.get(ride_id, {}).get("ride_date")
        if date_val and hasattr(date_val, "strftime"):
            date_val = date_val.strftime("%Y-%m-%d")
        by_date.setdefault(str(date_val), []).append((ride_id, rows))

    # 6) Find merge candidates: clusters of ride_ids with common overlap and size in [8,25] or [30,55]
    merge_candidates: List[Dict] = []
    for date_key, ride_groups in by_date.items():
        # ride_groups = [(ride_id, [match rows]), ...]
        remaining = list(ride_groups)
        while remaining:
            ride_id0, rows0 = remaining[0]
            cluster_rides = [(ride_id0, rows0)]
            all_riders = list(rows0)
            all_ride_ids = {ride_id0}
            remaining = remaining[1:]
            flight_ids_seen = {r["flight_id"] for r in rows0}
            while True:
                added = False
                for i, (rid, rows) in enumerate(remaining):
                    if any(r["flight_id"] in flight_ids_seen for r in rows):
                        continue
                    combined = all_riders + rows
                    if not have_overlap(combined):
                        continue
                    cluster_rides.append((rid, rows))
                    all_riders = combined
                    all_ride_ids.add(rid)
                    flight_ids_seen |= {r["flight_id"] for r in rows}
                    remaining.pop(i)
                    added = True
                    break
                if not added:
                    break
            n = len(all_riders)
            # Only merge if size is 8-25 or 30-55 (Connect shuttle tiers)
            if (T2_MIN <= n <= T2_MAX) or (MIN_S <= n <= T1_MAX):
                # Record merge candidate
                keep_ride_id = min(all_ride_ids)
                suggested_time = merged_suggested_time(all_riders)
                original_ubers = {rid: (rides.get(rid) or {}).get("ride_type") or "unknown" for rid in all_ride_ids}
                merge_candidates.append({
                    "date": date_key,
                    "keep_ride_id": keep_ride_id,
                    "ride_ids_combined": sorted(all_ride_ids),
                    "num_riders": n,
                    "suggested_time": suggested_time,
                    "original_uber_by_ride_id": original_ubers,
                    "all_riders": all_riders,
                })

    # 7) Write CSV report
    report_path = os.path.join(os.path.dirname(__file__), "..", "matches", "merge_to_connect_report.csv")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    rows_csv = []
    for c in merge_candidates:
        ride_ids_str = "|".join(str(x) for x in c["ride_ids_combined"])
        ubers_str = "|".join(f"{rid}:{c['original_uber_by_ride_id'].get(rid,'')}" for rid in c["ride_ids_combined"])
        rows_csv.append({
            "date": c["date"],
            "keep_ride_id": c["keep_ride_id"],
            "ride_ids_combined": ride_ids_str,
            "num_riders": c["num_riders"],
            "suggested_time": c["suggested_time"],
            "original_uber_by_ride_id": ubers_str,
        })
    if rows_csv:
        with open(report_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["date", "keep_ride_id", "ride_ids_combined", "num_riders", "suggested_time", "original_uber_by_ride_id"])
            w.writeheader()
            w.writerows(rows_csv)
        print(f"Wrote merge report to {report_path} ({len(rows_csv)} merge(s)).")
    else:
        print("No merge candidates found. No CSV written.")
        return

    # 8) Apply DB updates for each merge
    for c in merge_candidates:
        keep = c["keep_ride_id"]
        others = [x for x in c["ride_ids_combined"] if x != keep]
        all_rid = c["ride_ids_combined"]
        suggested_time = c["suggested_time"]

        # Update Rides: keep_ride_id gets Connect, voucher null, ride_time = suggested_time
        sb.table("Rides").update({
            "ride_type": "Connect",
            "voucher": None,
            "ride_time": suggested_time,
        }).eq("ride_id", keep).execute()

        # Matches: all rows with ride_id in all_rid -> set ride_id = keep, time = suggested_time, uber_type = Connect, voucher = null
        sb.table("Matches").update({
            "ride_id": keep,
            "time": suggested_time,
            "uber_type": "Connect",
            "voucher": None,
            "contingency_voucher": None,
        }).in_("ride_id", all_rid).execute()

        # Delete other Rides rows (so we don't have orphan rides)
        for rid in others:
            sb.table("Rides").delete().eq("ride_id", rid).execute()

        print(f"Merged ride_ids {all_rid} -> kept {keep}, set Connect, voucher cleared, time={suggested_time}.")

    print("Done.")


if __name__ == "__main__":
    main()

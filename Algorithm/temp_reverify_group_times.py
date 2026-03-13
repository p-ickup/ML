"""
Temporary script: reverify group suggested times using the correct rules.
- TO airport: 15 min before latest time of overlap (if possible), else latest time.
- FROM airport: 15 min after earliest time of overlap.
- Connect-type rides are left unchanged.
- Writes a CSV of what was changed (ride_id, old_time, new_time, ...) and updates the DB.
Run from Algorithm/ with: python3 temp_reverify_group_times.py
"""
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Set SUPABASE_URL and SUPABASE_SECRET_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def _parse_dt(date_str: str, time_str: str) -> datetime:
    t = str(time_str) if time_str else "00:00:00"
    if "T" in t:
        t = t.split("T")[-1][:12]
    if len(t) < 8:
        t = t + "00" * (8 - len(t))
    return datetime.fromisoformat(f"{date_str}T{t}")


def _rider_window(r: Dict[str, Any]) -> Tuple[datetime, datetime]:
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


def _time_to_str(t: Any) -> str:
    """Normalize time to HH:MM:SS for comparison."""
    if t is None:
        return "00:00:00"
    s = str(t).strip()
    if "T" in s:
        s = s.split("T")[-1][:12]
    try:
        parts = s.replace("-", ":").split(":")
        if len(parts) >= 3:
            h, m, sec = int(parts[0]) % 24, int(parts[1]) % 60, int(parts[2].split(".")[0]) % 60
            return f"{h:02d}:{m:02d}:{sec:02d}"
        if len(parts) == 2:
            h, m = int(parts[0]) % 24, int(parts[1]) % 60
            return f"{h:02d}:{m:02d}:00"
    except (ValueError, IndexError):
        pass
    return "00:00:00"


def compute_suggested_time(riders: List[Dict[str, Any]]) -> str:
    """
    TO airport: 15 min before latest time of overlap (if possible), else latest time.
    FROM airport: 15 min after earliest time of overlap.
    """
    if not riders:
        return "00:00:00"
    starts, ends = [], []
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
    t = chosen.time().replace(microsecond=0)
    return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}"


def main():
    days_ahead = 60  # look at next 60 days of rides
    today = datetime.today().date()
    end_date = today + timedelta(days=days_ahead)

    ride_resp = (
        sb.table("Rides")
        .select("*")
        .gte("ride_date", today.isoformat())
        .lte("ride_date", end_date.isoformat())
        .execute()
    )
    rides = list(ride_resp.data or [])
    if not rides:
        print("No rides in date range.")
        return

    ride_ids = [r["ride_id"] for r in rides]
    all_matches: List[Dict] = []
    for i in range(0, len(ride_ids), 100):
        chunk = ride_ids[i : i + 100]
        mat_resp = sb.table("Matches").select("*").in_("ride_id", chunk).execute()
        all_matches.extend(mat_resp.data or [])

    if not all_matches:
        print("No matches for those rides.")
        return

    flight_ids = list({m["flight_id"] for m in all_matches})
    flights_by_id: Dict[int, Dict] = {}
    for j in range(0, len(flight_ids), 100):
        fc = flight_ids[j : j + 100]
        fl_resp = (
            sb.table("Flights")
            .select("flight_id,airport,to_airport,date,earliest_time,latest_time")
            .in_("flight_id", fc)
            .execute()
        )
        for f in fl_resp.data or []:
            flights_by_id[int(f["flight_id"])] = f

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
                if hasattr(m.get("date"), "strftime"):
                    m["date"] = m["date"].strftime("%Y-%m-%d")

    by_ride: Dict[int, List[Dict]] = {}
    for m in all_matches:
        by_ride.setdefault(m["ride_id"], []).append(m)

    rides_by_id = {r["ride_id"]: r for r in rides}
    changes: List[Dict[str, Any]] = []

    for ride_id, match_rows in by_ride.items():
        ride = rides_by_id.get(ride_id)
        if not ride:
            continue
        ride_type = (ride.get("ride_type") or "").strip()
        if ride_type.upper() == "CONNECT":
            continue
        current_time = _time_to_str(ride.get("ride_time"))
        computed = compute_suggested_time(match_rows)
        if computed != current_time:
            ride_date = ride.get("ride_date")
            if hasattr(ride_date, "strftime"):
                ride_date = ride_date.strftime("%Y-%m-%d")
            changes.append({
                "ride_id": ride_id,
                "ride_date": ride_date,
                "ride_type": ride_type or "(blank)",
                "num_riders": len(match_rows),
                "old_time": current_time,
                "new_time": computed,
            })
            sb.table("Rides").update({"ride_time": computed}).eq("ride_id", ride_id).execute()
            sb.table("Matches").update({"time": computed}).eq("ride_id", ride_id).execute()

    report_path = os.path.join(os.path.dirname(__file__), "..", "matches", "reverify_times_changes.csv")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    if changes:
        with open(report_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ride_id", "ride_date", "ride_type", "num_riders", "old_time", "new_time"])
            w.writeheader()
            w.writerows(changes)
        print(f"Updated {len(changes)} ride(s). Wrote {report_path}")
        for c in changes:
            print(f"  ride_id={c['ride_id']} {c['old_time']} -> {c['new_time']}")
    else:
        print("No times needed changing. No CSV written.")
    print("Done.")


if __name__ == "__main__":
    main()

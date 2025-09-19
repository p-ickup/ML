"""
Main entry point for pickup system v1 MVP
Streamlined version that directly handles ride creation and matching
"""

import argparse
import csv
import os
from datetime import datetime, timedelta
from typing import List, Tuple

from buckets import bucket_names, make_buckets
from dotenv import load_dotenv
from rider_data import RiderData, RiderLite
from ruleMatching import Match, match_bucket
from supabase import Client, create_client

load_dotenv() 

# Load environment variables for Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
print(SUPABASE_URL)
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client and location cache
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# sum of bags for a rider (None -> 0)
def _bags_total(r: RiderLite) -> int:
    return int(r.bags_no or 0) + int(r.bags_no_large or 0)


# compute earliest intersection start for a group (date_str, time_str)
def _match_datetime_from_earliest(m: Match) -> Tuple[str, str]:
    starts = []
    ends = []
    for r in m.riders:
        s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
        e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
        starts.append(s)
        ends.append(e)
    start = max(starts)          # earliest feasible start (begin of overlap)
    end = min(ends)
    # guard: if no real overlap, fallback to suggested midpoint already computed
    if start >= end:
        dt = datetime.fromisoformat(m.suggested_time_iso)
    else:
        dt = start
    return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()


# split ISO to (date, time) if needed elsewhere
def _split_iso(dt_iso: str) -> Tuple[str, str]:
    dt = datetime.fromisoformat(dt_iso)
    return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()


# write matches to csv (one row per rider, grouped by a simulated ride_id; uses earliest-overlap date/time)
def _write_matches_csv(matches: List[Match], csv_path: str) -> None:
    rows = []
    for idx, m in enumerate(matches, start=1):
        sim_ride_id = idx  # simulated for readability; DB ride_id will be different in write mode
        match_date, match_time = _match_datetime_from_earliest(m)
        for r in m.riders:
            rows.append({
                "ride_id_simulated": sim_ride_id,
                "bucket_key": m.bucket_key or "",
                "date": match_date,                 # earliest overlap date
                "time": match_time,                 # earliest overlap time
                "suggested_time_iso": m.suggested_time_iso,
                "terminal": m.terminal or "",
                "user_id": r.user_id,
                "flight_id": r.flight_id,
                "flight_no": r.flight_no if r.flight_no is not None else "",
                "airport": r.airport,
                "to_airport": r.to_airport,
                "bags_total": _bags_total(r),
                "bags_no": r.bags_no or 0,
                "bags_no_large": r.bags_no_large or 0,
                "voucher_given": False,             # default for CSV parity
            })
    if not rows:
        print("No matches to write.")
        return
    fieldnames = list(rows[0].keys())
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote dry-run CSV with {len(matches)} groups → {csv_path}")


# persist matches to Supabase (create one Rides row per group; insert Matches rows with SAME ride_id;
# set Flights.matched = true; include earliest-overlap date/time and voucher_given=false)
def _write_matches_db(sb: Client, matches: List[Match]) -> None:
    if not matches:
        print("No matches to write.")
        return

    for m in matches:
        # choose earliest-overlap as the official match datetime for both Rides.ride_date and Matches.(date,time)
        ride_date, match_time = _match_datetime_from_earliest(m)

        # 1) create the ride row and get the unique ride_id (shared by the whole group)
        ride_resp = sb.table("Rides").insert({"ride_date": ride_date}).execute()
        if not ride_resp.data or "ride_id" not in ride_resp.data[0]:
            print("Failed to create ride row; skipping one group.")
            continue
        ride_id = ride_resp.data[0]["ride_id"]

        # 2) insert each member with SAME ride_id into Matches (include date/time and voucher_given)
        rows = []
        flight_ids = []
        for r in m.riders:
            rows.append({
                "ride_id": ride_id,                # shared group id
                "user_id": r.user_id,
                "flight_id": r.flight_id,
                "date": ride_date,                 # earliest overlap date
                "time": match_time,                # earliest overlap time (time w/o tz)
                "status": "suggested",            
                "source": "ml",                  
                "voucher_given": False,            # default
            })
            flight_ids.append(r.flight_id)

        if rows:
            sb.table("Matches").insert(rows).execute()

        # 3) mark all flights in the group as matched
        if flight_ids:
            sb.table("Flights").update({"matched": True}).in_("flight_id", flight_ids).execute()

    print(f"Wrote {len(matches)} groups to Supabase and updated Flights.matched.")


# run full pipeline (fetch → bucket → match → write or dry-run)
def run(dry_run: bool = False, csv_path: str = "matches_dryrun.csv") -> None:
    rd = RiderData(supabase)
    riders = rd.fetch_riders()
    if not riders:
        print("No candidate riders found.")
        return

    buckets = make_buckets(riders)

    all_matches: List[Match] = []
    all_unmatched: List[RiderLite] = []

    # iterate buckets and match
    for name, riders_in_bucket in buckets.items():
        matches, leftovers = match_bucket(riders_in_bucket, bucket_key=name)
        all_matches.extend(matches)
        all_unmatched.extend(leftovers)

    # report summary
    print(f"Buckets: {len(buckets)} | Groups: {len(all_matches)} | Unmatched: {len(all_unmatched)}")

    # write output
    if dry_run:
        _write_matches_csv(all_matches, csv_path)
    else:
        _write_matches_db(supabase, all_matches)


# cli entry
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pickup matcher")
    parser.add_argument("--dry-run", action="store_true", help="do not write to DB; export CSV instead")
    parser.add_argument("--csv", type=str, default="matches_dryrun.csv", help="CSV path for --dry-run output")
    args = parser.parse_args()

    run(dry_run=args.dry_run, csv_path=args.csv)

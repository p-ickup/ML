"""
Main entry point for pickup system v1 MVP
Streamlined version that directly handles ride creation and matching
"""

import argparse
import csv
import os
from datetime import datetime, timedelta
from typing import List

from buckets2 import bucket_names, make_buckets
from rider_data import RiderData, RiderLite
from ruleMatching import Match, match_bucket
from supabase import Client, create_client

# Load environment variables for Supabase and Google Maps API keys
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client and location cache
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# sum of bags for a rider (None -> 0)
def _bags_total(r: RiderLite) -> int:
    return int(r.bags_no or 0) + int(r.bags_no_large or 0)


# write matches to csv (one row per rider, grouped by ride_no)
def _write_matches_csv(matches: List[Match], csv_path: str) -> None:
    # assign a local ride_no for dry-run output
    rows = []
    for idx, m in enumerate(matches, start=1):
        for r in m.riders:
            rows.append({
                "ride_no": idx,
                "bucket_key": m.bucket_key or "",
                "suggested_time_iso": m.suggested_time_iso,
                "terminal": m.terminal or "",
                "user_id": r.user_id,
                "flight_id": r.flight_id,
                "flight_no": r.flight_no if r.flight_no is not None else "",
                "airport": r.airport,
                "to_airport": r.to_airport,
                "date": r.date,
                "bags_total": _bags_total(r),
                "bags_no": r.bags_no or 0,
                "bags_no_large": r.bags_no_large or 0,
            })
    if not rows:
        print("No matches to write.")
        return
    fieldnames = list(rows[0].keys())
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote dry-run CSV with {len(matches)} matches → {csv_path}")


# persist matches to Supabase (create Rides, then Matches rows, and set Flights.matched = true)
def _write_matches_db(sb: Client, matches: List[Match]) -> None:
    if not matches:
        print("No matches to write.")
        return

    for m in matches:
        ride_date = m.riders[0].date
        ride_resp = sb.table("Rides").insert({"ride_date": ride_date}).execute()
        if not ride_resp.data:
            print("Failed to create ride row; skipping match.")
            continue
        ride_id = ride_resp.data[0]["ride_id"]

        rows = []
        flight_ids = []
        for r in m.riders:
            rows.append({
                "ride_id": ride_id,
                "user_id": r.user_id,
                "flight_id": r.flight_id
            })
            flight_ids.append(r.flight_id)

        if rows:
            sb.table("Matches").insert(rows).execute()

        # mark flights as matched
        if flight_ids:
            sb.table("Flights").update({"matched": True}).in_("flight_id", flight_ids).execute()

    print(f"Wrote {len(matches)} matches to Supabase and updated Flights.matched.")


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
    print(f"Buckets: {len(buckets)} | Matches: {len(all_matches)} | Unmatched: {len(all_unmatched)}")

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

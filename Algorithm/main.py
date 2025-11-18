"""
Main entry point for pickup system v1 MVP
Streamlined version that directly handles ride creation and matching
"""

import argparse
import csv
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from buckets import bucket_names, make_buckets
from dotenv import load_dotenv
from rider_data import RiderData, RiderLite
from ruleMatching import Match, match_bucket
from supabase import Client, create_client

from vouchers import assign_vouchers

load_dotenv() 

# Load environment variables for Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
print(SUPABASE_URL)
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")

# Initialize Supabase client and location cache
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# sum of bags for a rider (None -> 0)
def _bags_total(r: RiderLite) -> int:
    return int(r.bags_no or 0) + int(r.bags_no_large or 0)


# compute earliest intersection start for a group (date_str, time_str)
def _match_datetime_from_earliest(m: Match) -> Tuple[str, str]:
    starts, ends = [], []
    for r in m.riders:
        s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
        e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
        starts.append(s); ends.append(e)
    latest_start = max(starts)
    earliest_end = min(ends)
    if earliest_end <= latest_start:
        dt = latest_start  # boundary/touching case
    else:
        dt = latest_start
    return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()

# split ISO to (date, time) if needed elsewhere
def _split_iso(dt_iso: str) -> Tuple[str, str]:
    dt = datetime.fromisoformat(dt_iso)
    return dt.date().isoformat(), dt.time().replace(microsecond=0).isoformat()

# write matches to csv (one row per rider, grouped by a simulated ride_id; uses earliest-overlap date/time)
def _write_matches_csv(matches: List[Match], csv_path: str) -> None:
    """
    Write matches to a CSV (one row per matched ride group).
    Includes earliest_time, latest_time, and suggested_time for easy inspection.
    """
    if not matches:
        print("No matches to write.")
        return

    rows = []
    for idx, m in enumerate(matches, start=1):
        sim_ride_id = idx

        # compute window across all riders
        starts, ends = [], []
        for r in m.riders:
            s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
            e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
            starts.append(s)
            ends.append(e)

        latest_start = max(starts)
        earliest_end = min(ends)

        match_date = latest_start.date().isoformat()
        earliest_time = latest_start.time().replace(microsecond=0).isoformat()
        latest_time = earliest_end.time().replace(microsecond=0).isoformat()

        suggested_time_iso = m.suggested_time_iso or ""
        try:
            suggested_time = (
                datetime.fromisoformat(suggested_time_iso).time().replace(microsecond=0).isoformat()
                if suggested_time_iso else ""
            )
        except Exception:
            suggested_time = ""

        # rider details
        rider_user_ids = [r.user_id for r in m.riders]
        total_bags = sum(
            int(r.bags_no or 0) +
            int(r.bags_no_large or 0) +
            int(getattr(r, "bag_no_personal", 0) or 0)
            for r in m.riders
        )

        # per-rider time ranges for quick verification
        # sorted by earliest_time to read top-to-bottom
        riders_sorted = sorted(m.riders, key=lambda rr: rr.earliest_time)
        match_times = "[" + ", ".join(
            f"{r.earliest_time}-{r.latest_time}" for r in riders_sorted
        ) + "]"
        # default vouchers
        voucher = ""
        contingency_list: List[str] = [""] * len(m.riders)

        rows.append({
            "ride_id_simulated": sim_ride_id,
            "bucket_key": m.bucket_key or "",
            "date": match_date,
            "earliest_time": earliest_time,
            "latest_time": latest_time,
            "suggested_time": suggested_time,
            "suggested_time_iso": suggested_time_iso,
            "terminal": m.terminal or "",
            "match_times": match_times,                # <— NEW COLUMN
            "bags_total": total_bags,
            "num_riders": len(m.riders),
            "riders": json.dumps(rider_user_ids),
            "voucher": m.group_voucher,
            "contingency_vouchers": json.dumps([getattr(r, "contingency_voucher", "") for r in m.riders]),
            "subsidized": any(r.subsidized for r in m.riders)
        })

    fieldnames = list(rows[0].keys())
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote dry-run CSV with {len(rows)} group rows → {csv_path}")

def _write_unmatched_with_reasons(unmatched: List[RiderLite], reasons: dict, csv_path: str) -> None:
    rows = []
    for r in unmatched:
        info = reasons.get(r.flight_id, {})
        rows.append({
            "user_id": r.user_id,
            "flight_id": r.flight_id,
            "airport": r.airport,
            "to_airport": r.to_airport,
            "date": r.date,
            "earliest_time": r.earliest_time,
            "latest_time": r.latest_time,
            "bags_no": r.bags_no or 0,
            "bags_no_large": r.bags_no_large or 0,
            "terminal": r.terminal or "",
            "bucket_key": info.get("bucket_key", ""),
            "reason": info.get("reason", "unknown"),
            "details": info.get("details", {}),
        })
    if not rows:
        print("No unmatched riders to write.")
        return
    with open(csv_path, "w", newline="") as f:
        import csv
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} unmatched riders with reasons → {csv_path}")

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
                "source": "ml",                  
                "voucher": getattr(r, "group_voucher", ""),
                "contingency_voucher": getattr(r, "contingency_voucher", None),
                "is_verified": False,
                "subsidized": r.subsidized,        # ← NEW LINE
            })
            flight_ids.append(r.flight_id)

        if rows:
            sb.table("Matches").insert(rows).execute()

        # 3) mark all flights in the group as matched
        if flight_ids:
            sb.table("Flights").update({"matched": True}).in_("flight_id", flight_ids).execute()

    print(f"Wrote {len(matches)} groups to Supabase and updated Flights.matched.")


def apply_group_subsidy(matches: list[Match], thresholds: Dict[str, int]) -> None:
    """
    A group is subsidized only if:
      • the group's size >= airport threshold (e.g., LAX:3, ONT:2)
      • AND all riders are Pomona students.
    """
    for m in matches:
        if not m.riders:
            continue

        # Check airport majority (should already be consistent)
        counts: Dict[str, int] = {}
        for r in m.riders:
            a = (r.airport or "").upper()
            counts[a] = counts.get(a, 0) + 1

        group_airport = max(counts.items(), key=lambda kv: kv[1])[0]

        # Get required group size threshold for this airport
        need = thresholds.get(group_airport)
        if need is None:
            continue

        # Check size requirement
        if len(m.riders) < need:
            continue

        # NEW RULE: all riders must be Pomona students
        all_pomona = all(
            (r.school or "").strip().upper() == "POMONA"
            for r in m.riders
        )

        if not all_pomona:
            continue

        # If both conditions satisfied → subsidize entire group
        for r in m.riders:
            r.subsidized = True


# run full pipeline (fetch → bucket → match → write or dry-run)
def run(
    dry_run: bool = False,
    csv_path: str = "../matches/matches_dryrun.csv",
    unmatched_csv_path: str = "../matches/unmatched_reasons_dryrun.csv",
    days_ahead: int = 10,
    vouchers_csv_path: str = "../vouchers/Thanksgiving.csv"
) -> None:
    rd = RiderData(supabase)
    riders = rd.fetch_riders(max_days_ahead=days_ahead)

    # Print how many rider forms were loaded + the date range
    if riders:
        dates = sorted({r.date for r in riders})
        print(
            f"Fetched {len(riders)} rider forms "
            f"(dates: {dates[0]} → {dates[-1]})"
        )
    else:
        print("Fetched 0 rider forms.")
        print("No candidate riders found.")
        return

    buckets = make_buckets(riders)

    all_matches: List[Match] = []
    all_unmatched: List[RiderLite] = []
    all_diag: dict[int, dict] = {}

    # iterate buckets and match
    for name, riders_in_bucket in buckets.items():
        matches, leftovers, diag = match_bucket(riders_in_bucket, bucket_key=name)  # 3 values
        all_matches.extend(matches)
        all_unmatched.extend(leftovers)
        all_diag.update(diag)
        
    # group subsidiy
    apply_group_subsidy(
        all_matches,
        thresholds={"LAX": 3, "ONT": 2}
    )

    assign_vouchers(all_matches, voucher_csv_path=vouchers_csv_path, dry_run=dry_run)

    # report summary
    print(f"Buckets: {len(buckets)} | Groups: {len(all_matches)} | Unmatched: {len(all_unmatched)}")

    _write_matches_csv(all_matches, csv_path)
    _write_unmatched_with_reasons(all_unmatched, all_diag, unmatched_csv_path)

    # DB writes only if not dry-run
    if not dry_run:
        ride_ids = _write_matches_db(supabase, all_matches)
    else:
        ride_ids = []


# cli entry
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pickup matcher")
    parser.add_argument("--dry-run", action="store_true", help="do not write to DB; export CSV instead")
    parser.add_argument("--csv", type=str, default="../matches/matches_dryrun.csv", help="CSV path for --dry-run output")
    parser.add_argument("--days-ahead", type=int, default=10, help="only consider flights within N days ahead")
    parser.add_argument("--vouchers", type=str, default="../vouchers/Thanksgiving.csv",help="Path to vouchers CSV")
    args = parser.parse_args()

    run(
        dry_run=args.dry_run,
        csv_path=args.csv,
        days_ahead=args.days_ahead,
        vouchers_csv_path=args.vouchers,
    )




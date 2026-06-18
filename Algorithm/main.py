"""
Main entry point for the Pickup matching pipeline.

Dry-run writes review CSVs. Production commits rides, matches, flight updates,
voucher consumption, and Connect cleanup through the transactional Supabase RPC.
"""

import argparse
import csv
import json
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import algorithmStatus
import config
import connect_policy as cp
from buckets import make_buckets
from commit_payload import (
    build_matching_commit_payload,
    commit_matching_run,
    compute_group_time_window,
    determine_uber_type,
    match_datetime_from_earliest,
)
from dotenv import load_dotenv
from rider_data import RiderData, RiderLite
from connect_merge import merge_connect_with_existing
from ruleMatching import Match, _ont_post_process_unmatched, match_bucket, refresh_match_suggested_times
from supabase import create_client

from vouchers import assign_vouchers, is_ride_date_covered

load_dotenv() 

# Load environment variables for Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")

# Initialize Supabase client and location cache
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# write matches to csv (one row per rider, grouped by a simulated ride_id; uses earliest-overlap date/time)
def _write_matches_csv(
    matches: List[Match], 
    all_riders: List[RiderLite], 
    csv_path: str,
) -> None:
    """
    Write matches to a CSV (one row per matched ride group).
    Also prints how many flights were matched vs unmatched.
    """

    if not matches:
        print("No matches to write.")
        return

    rows = []
    for idx, m in enumerate(matches, start=1):
        sim_ride_id = idx

        match_date, _ = match_datetime_from_earliest(m)
        earliest_time, latest_time = compute_group_time_window(m.riders)

        suggested_time_iso = m.suggested_time_iso or ""
        try:
            suggested_time = (
                datetime.fromisoformat(suggested_time_iso).time().replace(microsecond=0).isoformat()
                if suggested_time_iso else ""
            )
        except Exception:
            suggested_time = ""

        # rider details
        rider_names = [r.name or r.user_id for r in m.riders]
        # Calculate number of large bags, normal bags, and personal bags (counts, not units)
        num_large_bags = sum(int(r.bags_no_large or 0) for r in m.riders)
        num_normal_bags = sum(int(r.bags_no or 0) for r in m.riders)
        num_personal_bags = sum(int(getattr(r, "bag_no_personal", 0) or 0) for r in m.riders)
        # Considered bags in units: large bags count as LARGE_BAG_MULTIPLIER, normal as 1, personal as 0 (if PERSONAL_CONSTRAINT is False)
        considered_bags = (num_large_bags * config.LARGE_BAG_MULTIPLIER) + num_normal_bags
        if config.PERSONAL_CONSTRAINT:
            considered_bags += num_personal_bags
        # Total bags (all types: normal + large + personal)
        bags_total = sum(
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
        # Determine uber_type: use ride_type (e.g. Connect) if set, else from group size and bags
        group_size = len(m.riders)
        uber_type = getattr(m, "ride_type", None) or determine_uber_type(group_size, considered_bags)

        rows.append({
            "ride_id_simulated": sim_ride_id,
            "bucket_key": m.bucket_key or "",
            "date": match_date,
            "num_riders": group_size,
            "suggested_time": suggested_time,
            "earliest_time": earliest_time,
            "latest_time": latest_time,
            "match_times": match_times,
            "considered_bags": considered_bags,
            
            "bags_total": bags_total,
            "num_large_bags": num_large_bags,
            "num_normal_bags": num_normal_bags,
            "num_personal_bags": num_personal_bags,
            "riders": json.dumps(rider_names),
            "voucher": m.group_voucher,
            "contingency_vouchers": json.dumps([getattr(r, "contingency_voucher", "") for r in m.riders]),
            "subsidized": getattr(m, "group_subsidy", False),
            "uber_type": uber_type
        })

    rows.sort(key=lambda row: (
        row.get("date") or "",
        row.get("suggested_time") or row.get("earliest_time") or "",
        row.get("bucket_key") or "",
    ))
    for idx, row in enumerate(rows, start=1):
        row["ride_id_simulated"] = idx

    fieldnames = list(rows[0].keys())
    
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Matches CSV saved to: {csv_path}")

    considered_ids = {r.flight_id for r in all_riders}

    matched_ids = {
        r.flight_id
        for m in matches
        for r in m.riders
    }

    unmatched_ids = list(considered_ids - matched_ids)

    print(
        f"Wrote dry-run CSV with {len(rows)} matched groups → {csv_path}\n"
        f"Matched flights: {len(matched_ids)} | Unmatched flights: {len(unmatched_ids)}"
    )

def _write_unmatched_with_reasons(
    unmatched: List[RiderLite], 
    reasons: dict, 
    csv_path: str,
) -> None:
    rows = []
    for r in unmatched:
        info = reasons.get(r.flight_id, {})
        rows.append({
            "user_id": r.user_id,
            "name": getattr(r, "name", None) or "",
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

    rows.sort(key=lambda row: (
        row.get("date") or "",
        row.get("earliest_time") or "",
        row.get("latest_time") or "",
        row.get("airport") or "",
        row.get("name") or "",
    ))
    
    with open(csv_path, "w", newline="") as f:
        import csv
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"Unmatched reasons CSV saved to: {csv_path}")
    
    print(f"Wrote {len(rows)} unmatched riders with reasons → {csv_path}")


def _final_lax_unmatched_retry(
    all_matches: List[Match],
    all_unmatched: List[RiderLite],
) -> Tuple[List[Match], List[RiderLite]]:
    """
    When Connect Shuttle option is on: one more pass over LAX unmatched riders.
    Try to form NEW groups only from LAX unmatched (re-bucket them and run
    match_bucket on each bucket with 2+ riders). We do NOT add these riders
    into existing groups — only form new groups among themselves (e.g. 3 riders
    with exact overlap can become one new group).
    """
    connect_unmatched = [r for r in all_unmatched if cp.rider_in_connect_scope(r)]
    if not connect_unmatched:
        return all_matches, all_unmatched

    lax_buckets = make_buckets(connect_unmatched)
    new_matches: List[Match] = []
    lax_leftovers: List[RiderLite] = []
    for name, riders_in_bucket in lax_buckets.items():
        if len(riders_in_bucket) >= 2:
            # Form new groups only from this bucket; allow groups of 3 with max 12 bags (Uber XXL)
            matches, leftovers, _ = match_bucket(riders_in_bucket, bucket_key=name, final_pass=True)
            new_matches.extend(matches)
            lax_leftovers.extend(leftovers)
        else:
            lax_leftovers.extend(riders_in_bucket)

    if not new_matches:
        return all_matches, all_unmatched

    matched_flight_ids = {r.flight_id for m in new_matches for r in m.riders}
    non_connect_unmatched = [r for r in all_unmatched if not cp.rider_in_connect_scope(r)]
    all_unmatched_new = non_connect_unmatched + lax_leftovers
    print(f"  Connect-scope final retry (new groups only): {len(new_matches)} new group(s), {len(matched_flight_ids)} riders matched")
    return all_matches + new_matches, all_unmatched_new


def apply_group_subsidy(
    matches: list[Match],
    thresholds: Optional[Dict[str, int]] = None,
) -> None:
    if thresholds is None:
        thresholds = config.SUBSIDY_MIN_GROUP_SIZE

    # First, reset ALL riders to False to ensure clean state
    # Also track which riders appear in which matches
    rider_to_matches: Dict[int, List[int]] = {}  # rider id -> list of match indices
    all_riders_set: set = set()  # Track all unique rider objects
    
    for i, m in enumerate(matches, start=1):
        for r in m.riders:
            r.subsidized = False
            rider_id = id(r)
            all_riders_set.add(rider_id)
            if rider_id not in rider_to_matches:
                rider_to_matches[rider_id] = []
            rider_to_matches[rider_id].append(i)
    
    # Warn if any rider appears in multiple matches
    for rider_id, match_indices in rider_to_matches.items():
        if len(match_indices) > 1:
            print(f"WARNING: Rider object {rider_id} appears in multiple matches: {match_indices}")

    # PASS 1: Determine which matches qualify for subsidy
    match_qualifies: Dict[int, bool] = {}  # match index -> qualifies
    for i, m in enumerate(matches, start=1):
        if not m.riders:
            match_qualifies[i] = False
            continue

        group_airport = (m.riders[0].airport or "").strip().upper()
        need = thresholds.get(group_airport)

        group_size = len(m.riders)
        
        all_pomona = all((r.school or "").strip().upper() == "POMONA"
                         for r in m.riders)

        # Ride date/direction must be covered (when COVERED_DATES_EXPLICIT) for subsidy
        ride_date = datetime.fromisoformat(m.suggested_time_iso).date()
        to_airport = m.riders[0].to_airport
        covered = is_ride_date_covered(ride_date, to_airport)

        qualifies = (
            need is not None and
            group_size >= need and
            all_pomona and
            covered
        )
        
        
        # Store group-level flag on Match
        m.group_subsidy = qualifies
        match_qualifies[i] = qualifies

    # PASS 2: Set subsidized for each rider within each match
    # Within a match, ALL riders must have the same subsidized value (the match's qualification)
    # This ensures consistency even if a rider appears in multiple matches
    for i, m in enumerate(matches, start=1):
        qualifies = match_qualifies.get(i, False)
        # All riders in this match get the same subsidized value
        for r in m.riders:
            r.subsidized = qualifies
        
        # Verify all riders in this match have the same value (force consistency)
        subsidized_values = {r.subsidized for r in m.riders}
        if len(subsidized_values) > 1:
            print(f"ERROR: Match {i} has conflicting subsidized values after setting: {subsidized_values}")
            print(f"  Forcing all riders to match qualification: {qualifies}")
            # Force all to match the match's qualification
            for r in m.riders:
                r.subsidized = qualifies



# run full pipeline (fetch → bucket → match → write or dry-run)
def run(
    dry_run: bool = False,
    csv_path: str = "../matches/matches_dryrun.csv",
    unmatched_csv_path: str = "../matches/unmatched_reasons_dryrun.csv",
    days_ahead: int = 10,
    days_ahead_start: Optional[int] = None,
    vouchers_csv_path: str = "../vouchers/Thanksgiving.csv"
) -> None:
    # AlgorithmStatus tracking (only for non-dry-run executions)
    status_id: Optional[str] = None
    run_id: Optional[str] = None
    algorithm_name = "pickup_matching"
    
    try:
        rd = RiderData(supabase)
        riders = rd.fetch_riders(
            max_days_ahead=days_ahead, min_days_ahead=days_ahead_start
        )
        
        # Set up AlgorithmStatus tracking (only for non-dry-run executions)
        if not dry_run:
            target_scope = algorithmStatus.determine_target_scope(riders)
            
            # Get or create algorithm status record
            status_id = algorithmStatus.get_or_create_algorithm_status(supabase, algorithm_name, target_scope)
            
            # Get run_id from the status record if it exists
            if status_id:
                status_resp = (
                    supabase.table("AlgorithmStatus")
                    .select("run_id")
                    .eq("id", status_id)
                    .execute()
                )
                if status_resp.data and len(status_resp.data) > 0:
                    run_id = status_resp.data[0].get("run_id")
            
            # Generate run_id if not already set
            if not run_id:
                run_id = str(uuid.uuid4())
                if status_id:
                    supabase.table("AlgorithmStatus").update({"run_id": run_id}).eq("id", status_id).execute()

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
            if not dry_run:
                algorithmStatus.update_algorithm_status(supabase, status_id, "success", run_id=run_id)
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
            
        # ONT post-processing: Try to match unmatched individuals with groups of 4 on same day
        all_matches, all_unmatched = _ont_post_process_unmatched(all_matches, all_unmatched)

        # Final LAX retry when Connect Shuttle is on: re-run matching on LAX unmatched (e.g. 3 with exact overlap)
        if cp.connect_enabled():
            all_matches, all_unmatched = _final_lax_unmatched_retry(all_matches, all_unmatched)

        # Connect merge: existing DB groups + this run + unmatched (LAX/ONT per config)
        connect_for_cleanup: List[Match] = []
        if cp.connect_enabled():
            run_flight_ids = {r.flight_id for r in riders}
            start_date = min(r.date for r in riders)
            end_date = max(r.date for r in riders)
            all_matches, all_unmatched, connect_for_cleanup = merge_connect_with_existing(
                supabase,
                all_matches,
                all_unmatched,
                run_flight_ids=run_flight_ids,
                start_date=start_date,
                end_date=end_date,
            )

        # Final pickup times (TO: 15 min before overlap end; FROM: 15 min after overlap start)
        time_updates = refresh_match_suggested_times(all_matches, skip_connect=True)
        if time_updates:
            print(f"Refreshed suggested pickup time on {time_updates} group(s).")

        # Subsidy once the final group list is known (ONT splits, LAX retry, Connect merge).
        # Production voucher assignment is committed transactionally by the DB RPC.
        apply_group_subsidy(all_matches)
        if dry_run:
            assign_vouchers(
                all_matches,
                voucher_csv_path=vouchers_csv_path,
                dry_run=True,
            )
        
        # report summary
        print(f"Buckets: {len(buckets)} | Groups: {len(all_matches)} | Unmatched: {len(all_unmatched)}")

        if dry_run:
            _write_matches_csv(all_matches, riders, csv_path)
            _write_unmatched_with_reasons(all_unmatched, all_diag, unmatched_csv_path)
        else:
            commit_payload = build_matching_commit_payload(
                run_id=run_id,
                matches=all_matches,
                all_riders=riders,
                connect_for_cleanup=connect_for_cleanup,
            )
            commit_result = commit_matching_run(
                supabase,
                run_id=run_id,
                payload=commit_payload,
            )
            print(f"Committed matching run atomically: {commit_result}")
            # Update status to success
            algorithmStatus.update_algorithm_status(supabase, status_id, "success", run_id=run_id)
            
    except Exception as e:
        # Update status to failed with error message
        error_msg = str(e)
        print(f"Algorithm execution failed: {error_msg}")
        if not dry_run and status_id:
            algorithmStatus.update_algorithm_status(supabase, status_id, "failed", error_message=error_msg, run_id=run_id)
        raise  # Re-raise the exception after logging


# cli entry
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pickup matcher")
    parser.add_argument("--dry-run", action="store_true", help="do not write to DB; export review CSVs instead")
    parser.add_argument("--csv", type=str, default="../matches/matches_dryrun.csv", help="CSV path for --dry-run output")
    parser.add_argument(
        "--days-ahead", type=int, default=10, help="inclusive end: only flights on or before (today + N days)"
    )
    parser.add_argument(
        "--days-ahead-start",
        type=int,
        default=None,
        help="inclusive start: only flights on or after (today + N days). Omit to keep the legacy lower bound (flight date strictly after today).",
    )
    parser.add_argument("--vouchers", type=str, default="../vouchers/Thanksgiving.csv", help="Path to dry-run vouchers CSV")
    args = parser.parse_args()

    run(
        dry_run=args.dry_run,
        csv_path=args.csv,
        days_ahead=args.days_ahead,
        days_ahead_start=args.days_ahead_start,
        vouchers_csv_path=args.vouchers,
    )

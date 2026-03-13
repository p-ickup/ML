"""
Main entry point for pickup system v1 MVP
Streamlined version that directly handles ride creation and matching
"""

import argparse
import csv
import json
import os
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import algorithmStatus
import config
import force_match as nepo
import storage
from buckets import bucket_names, make_buckets
from dotenv import load_dotenv
from rider_data import RiderData, RiderLite
from ruleMatching import Match, _ont_post_process_unmatched, match_bucket, merge_groups_into_connect_shuttles
from supabase import Client, create_client

from vouchers import assign_vouchers, is_ride_date_covered

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


def _determine_uber_type(group_size: int, bag_units: int) -> Optional[str]:
    """
    Determine uber_type based on group size and bag units.
    
    Rules:
    - 2-3 riders: 0-4 bags = X, 5-10 bags = XL, 11-12 bags = XXL
    - 4 riders: 0-3 bags = X, 4-7 bags = XL, 8-10 bags = XXL, 11+ = None (not allowed)
    - 5 riders: 0-5 bags = XL, 6-8 bags = XXL, 9+ = None (not allowed)
    - 6 riders: 0-3 bags = XL, 4-6 bags = XXL, 7+ = None (not allowed)
    
    Hard limit: 12 bag units for 2-3 riders, 10 bag units for 4+ riders
    """
    # Group-size-aware hard limit check
    if group_size == 2 or group_size == 3:
        if bag_units > 12:
            return None
    else:
        if bag_units > 10:
            return None
    
    if group_size == 2 or group_size == 3:
        if 0 <= bag_units <= 4:
            return "X"
        elif 5 <= bag_units <= 10:
            return "XL"
        elif 11 <= bag_units <= 12:
            return "XXL"
        else:
            return None  # Not allowed
    
    elif group_size == 4:
        if 0 <= bag_units <= 3:
            return "X"
        elif 4 <= bag_units <= 7:
            return "XL"
        elif 8 <= bag_units <= 10:
            return "XXL"
        else:
            return None  # 11+ not allowed
    
    elif group_size == 5:
        if 0 <= bag_units <= 5:
            return "XL"
        elif 6 <= bag_units <= 8:
            return "XXL"
        else:
            return None  # 9+ not allowed
    
    elif group_size == 6:
        if 0 <= bag_units <= 3:
            return "XL"
        elif 4 <= bag_units <= 6:
            return "XXL"
        else:
            return None  # 7+ not allowed
    
    else:
        # Group size 1 or > 6 not supported
        return None


# compute group time window (earliest_time and latest_time across all riders)
def _compute_group_time_window(riders: List[RiderLite]) -> Tuple[str, str]:
    """
    Compute the time window for a group of riders.
    Returns (earliest_time, latest_time) as ISO time strings.
    earliest_time = max of all riders' earliest_time (latest start)
    latest_time = min of all riders' latest_time (earliest end)
    """
    starts, ends = [], []
    for r in riders:
        s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
        e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
        starts.append(s)
        ends.append(e)
    latest_start = max(starts)
    earliest_end = min(ends)
    earliest_time = latest_start.time().replace(microsecond=0).isoformat()
    latest_time = earliest_end.time().replace(microsecond=0).isoformat()
    return earliest_time, latest_time

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
def _write_matches_csv(
    matches: List[Match], 
    all_riders: List[RiderLite], 
    csv_path: str,
    sb: Optional[Client] = None,
    dry_run: bool = False
) -> None:
    """
    Write matches to a CSV (one row per matched ride group).
    Also prints how many flights were matched vs unmatched.
    """
    
    # print("\n================= DEBUG: GROUPS AT CSV WRITE =================")
    # for idx, m in enumerate(matches, start=1):
    #     print(f"[CSV {idx}] bucket={m.bucket_key}, size={len(m.riders)}")
    #     for r in m.riders:
    #         print(
    #             f"   flight_id={r.flight_id}, user_id={r.user_id}, "
    #             f"school={r.school}, subsidized={r.subsidized}, id={id(r)}"
    #         )
    # print("===============================================================\n")
    
    
    if not matches:
        print("No matches to write.")
        return

    rows = []
    for idx, m in enumerate(matches, start=1):
        sim_ride_id = idx
        
        # # DEBUG BLOCK
        # print(f"\n[CSV WRITE — GROUP {sim_ride_id}] "
        #     f"bucket={m.bucket_key}, size={len(m.riders)}, "
        #     f"airport={m.riders[0].airport if m.riders else None}")
        
        # compute window across all riders
        starts, ends = [], []
        for r in m.riders:
            # print(f"   Rider flight_id={r.flight_id}, user_id={r.user_id}, "
            #   f"school={r.school}, subsidized={r.subsidized}")
            s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
            e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
            starts.append(s)
            ends.append(e)

        latest_start = max(starts)
        earliest_end = min(ends)

        match_date = latest_start.date().isoformat()
        # Use shared helper function to compute group time window
        earliest_time, latest_time = _compute_group_time_window(m.riders)

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
        # default vouchers
        voucher = ""
        contingency_list: List[str] = [""] * len(m.riders)

        # Determine uber_type: use ride_type (e.g. Connect) if set, else from group size and bags
        group_size = len(m.riders)
        uber_type = getattr(m, "ride_type", None) or _determine_uber_type(group_size, considered_bags)

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

    fieldnames = list(rows[0].keys())
    
    # Write to local file first (for both dry-run and real runs)
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    # If dry-run and using Supabase Storage, upload to Supabase Storage with timestamp
    if dry_run and config.USE_SUPABASE_STORAGE and sb is not None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        storage_filename = f"matches_dryrun_{timestamp}.csv"
        print(f"Uploading dry-run matches CSV to Supabase Storage: {storage_filename}")
        storage.upload_file(sb, config.STORAGE_DRYRUNS_BUCKET, storage_filename, csv_path)
        print(f"Successfully uploaded to {config.STORAGE_DRYRUNS_BUCKET}/{storage_filename}")
    else:
        print(f"Matches CSV saved to local file: {csv_path}")

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
    sb: Optional[Client] = None,
    dry_run: bool = False
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
    
    # Write to local file first
    with open(csv_path, "w", newline="") as f:
        import csv
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    
    # If dry-run and using Supabase Storage, upload to Supabase Storage with timestamp
    if dry_run and config.USE_SUPABASE_STORAGE and sb is not None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        storage_filename = f"unmatched_reasons_dryrun_{timestamp}.csv"
        print(f"Uploading dry-run unmatched reasons CSV to Supabase Storage: {storage_filename}")
        storage.upload_file(sb, config.STORAGE_DRYRUNS_BUCKET, storage_filename, csv_path)
        print(f"Successfully uploaded to {config.STORAGE_DRYRUNS_BUCKET}/{storage_filename}")
    else:
        print(f"Unmatched reasons CSV saved to local file: {csv_path}")
    
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
    lax_unmatched = [r for r in all_unmatched if (r.airport or "").strip().upper() == "LAX"]
    if not lax_unmatched:
        return all_matches, all_unmatched

    lax_buckets = make_buckets(lax_unmatched)
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
    non_lax_unmatched = [r for r in all_unmatched if (r.airport or "").strip().upper() != "LAX"]
    all_unmatched_new = non_lax_unmatched + lax_leftovers
    print(f"  LAX final retry (new groups only): {len(new_matches)} new group(s), {len(matched_flight_ids)} riders matched")
    return all_matches + new_matches, all_unmatched_new


# persist matches to Supabase (create one Rides row per group; insert Matches rows with SAME ride_id;
# set Flights.matched = true; include earliest-overlap date/time and voucher_given=false)
def _write_matches_db(sb: Client, matches: List[Match], all_riders: List[RiderLite]) -> None:
    if not matches:
        print("No matches to write.")
        return

    for m in matches:
        # choose earliest-overlap as the official match datetime for both Rides.ride_date and Matches.(date,time)
        ride_date, match_time = _match_datetime_from_earliest(m)

        # Calculate bag units for uber_type determination
        num_large_bags = sum(int(r.bags_no_large or 0) for r in m.riders)
        num_normal_bags = sum(int(r.bags_no or 0) for r in m.riders)
        bag_units = (num_large_bags * config.LARGE_BAG_MULTIPLIER) + num_normal_bags
        if config.PERSONAL_CONSTRAINT:
            num_personal_bags = sum(int(getattr(r, "bag_no_personal", 0) or 0) for r in m.riders)
            bag_units += num_personal_bags
        
        # Determine uber_type: use ride_type (e.g. Connect) if set, else from group size and bags
        group_size = len(m.riders)
        uber_type = getattr(m, "ride_type", None) or _determine_uber_type(group_size, bag_units)

        # Rides with uncovered dates must not be written as subsidized (enforce at persist time)
        ride_date_for_cover = datetime.fromisoformat(m.suggested_time_iso).date()
        to_airport = m.riders[0].to_airport
        covered = is_ride_date_covered(ride_date_for_cover, to_airport)
        write_subsidized = bool(getattr(m, "group_subsidy", False) and covered)

        # Calculate group time window (earliest_time and latest_time across all riders)
        # Use shared helper function to ensure consistency with CSV output
        group_earliest_time, group_latest_time = _compute_group_time_window(m.riders)

        # 1) create the ride row and get the unique ride_id (shared by the whole group)
        ride_resp = sb.table("Rides").insert({"ride_date": ride_date}).execute()
        if not ride_resp.data or "ride_id" not in ride_resp.data[0]:
            print("Failed to create ride row; skipping one group.")
            continue
        ride_id = ride_resp.data[0]["ride_id"]

        # 2) insert each member with SAME ride_id into Matches (is_subsidized only if covered)
        rows = []
        flight_ids = []
        for r in m.riders:
            rows.append({
                "ride_id": ride_id,                # shared group id
                "user_id": r.user_id,
                "flight_id": r.flight_id,
                "date": ride_date,                 # earliest overlap date
                "time": match_time,                # earliest overlap time (time w/o tz)
                "earliest_time": group_earliest_time,  # group's earliest time window
                "latest_time": group_latest_time,      # group's latest time window
                "source": "ml",                  
                "voucher": getattr(r, "group_voucher", ""),
                "contingency_voucher": getattr(r, "contingency_voucher", None),
                "is_verified": False,
                "is_subsidized": write_subsidized,   # false if ride date not covered
                "uber_type": uber_type,           # same for all riders in the group
            })
            flight_ids.append(r.flight_id)

        if rows:
            sb.table("Matches").insert(rows).execute()

        # 3) mark all flights in the group as matched; original_unmatched = False (was matched this run)
        if flight_ids:
            sb.table("Flights").update({"matched": True, "original_unmatched": False}).in_("flight_id", flight_ids).execute()

    considered_ids = {r.flight_id for r in all_riders}

    matched_ids = {
        r.flight_id
        for m in matches
        for r in m.riders
    }

    unmatched_ids = list(considered_ids - matched_ids)

    if unmatched_ids:
        sb.table("Flights").update({"matched": False, "original_unmatched": True}).in_("flight_id", unmatched_ids).execute()

    print(
        f"Wrote {len(matches)} groups to Supabase and updated Flights.matched / original_unmatched. "
        f"Marked {len(matched_ids)} matched=True (original_unmatched=False) and {len(unmatched_ids)} matched=False (original_unmatched=True)."
    )


def apply_group_subsidy(matches: list[Match], thresholds: Dict[str, int]) -> None:

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
    vouchers_csv_path: str = "../vouchers/Thanksgiving.csv"
) -> None:
    # AlgorithmStatus tracking (only for non-dry-run executions)
    status_id: Optional[str] = None
    run_id: Optional[str] = None
    algorithm_name = "pickup_matching"
    
    try:
        rd = RiderData(supabase)
        riders = rd.fetch_riders(max_days_ahead=days_ahead)
        
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
            # Try nepo matching first (if enabled)
            nepo_match, remaining_riders = nepo.force_nepo_match(riders_in_bucket, bucket_key=name)
            if nepo_match:
                all_matches.append(nepo_match)
                riders_in_bucket = remaining_riders  # Continue with remaining riders
            
            # Regular matching on remaining riders
            matches, leftovers, diag = match_bucket(riders_in_bucket, bucket_key=name)  # 3 values
            all_matches.extend(matches)
            all_unmatched.extend(leftovers)
            all_diag.update(diag)
            
        # group subsidiy
        apply_group_subsidy(
            all_matches,
            thresholds={"LAX": 3, "ONT": 2}
        )

        # Assign vouchers (uses Supabase Storage if enabled, otherwise local files)
        assign_vouchers(
            all_matches, 
            voucher_csv_path=vouchers_csv_path, 
            dry_run=dry_run,
            sb=supabase if (not dry_run and config.USE_SUPABASE_STORAGE) else None
        )

        # ONT post-processing: Try to match unmatched individuals with groups of 4 on same day
        all_matches, all_unmatched = _ont_post_process_unmatched(
            all_matches, 
            all_unmatched,
            voucher_csv_path=vouchers_csv_path,
            dry_run=dry_run,
            sb=supabase if (not dry_run and config.USE_SUPABASE_STORAGE) else None
        )

        # Final LAX retry when Connect Shuttle is on: re-run matching on LAX unmatched (e.g. 3 with exact overlap)
        if getattr(config, "LAX_CONNECT_SHUTTLE_MIN", None) is not None:
            all_matches, all_unmatched = _final_lax_unmatched_retry(all_matches, all_unmatched)

        # After all matches: try to merge LAX departure groups with overlapping times into Connect shuttles
        if getattr(config, "LAX_CONNECT_SHUTTLE_MIN", None) is not None:
            all_matches = merge_groups_into_connect_shuttles(all_matches)
        
        # report summary
        print(f"Buckets: {len(buckets)} | Groups: {len(all_matches)} | Unmatched: {len(all_unmatched)}")

        _write_matches_csv(
            all_matches, 
            riders, 
            csv_path, 
            sb=supabase if (dry_run and config.USE_SUPABASE_STORAGE) else None, 
            dry_run=dry_run
        )
        _write_unmatched_with_reasons(
            all_unmatched, 
            all_diag, 
            unmatched_csv_path, 
            sb=supabase if (dry_run and config.USE_SUPABASE_STORAGE) else None, 
            dry_run=dry_run
        )

        # DB writes only if not dry-run
        if not dry_run:
            ride_ids = _write_matches_db(supabase, all_matches, riders)
            # Update status to success
            algorithmStatus.update_algorithm_status(supabase, status_id, "success", run_id=run_id)
        else:
            ride_ids = []
            
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




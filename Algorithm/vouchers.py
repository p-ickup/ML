# vouchers.py
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import config
import pandas as pd
import storage
from rider_data import RiderLite
from ruleMatching import Match
from supabase import Client


def is_ride_date_covered(ride_date: date, to_airport: bool) -> bool:
    """
    True if the ride is in a "covered" date for voucher purposes.
    When COVERED_DATES_EXPLICIT is False, all dates are covered.
    When True, only dates in COVERED_DATES_OUTBOUND (to_airport True) or
    COVERED_DATES_INBOUND (to_airport False) are covered.
    """
    if not getattr(config, "COVERED_DATES_EXPLICIT", False):
        return True
    mm_dd = ride_date.strftime("%m-%d")
    if to_airport:
        allowed = getattr(config, "COVERED_DATES_OUTBOUND", [])
    else:
        allowed = getattr(config, "COVERED_DATES_INBOUND", [])
    return mm_dd in allowed


def _parse_month_day(s: str, year: int):
    # convert "November 21" into a date object for the given year
    return datetime.strptime(f"{s} {year}", "%B %d %Y").date()


def _parse_csv_bool(val) -> bool:
    """
    Safe CSV boolean parse. pd.Series.astype(bool) maps string 'False' to True
    (non-empty strings are truthy), which breaks the USED column.
    """
    if val is True or val is False:
        return val
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    s = str(val).strip().lower()
    if s in ("true", "1", "yes", "t"):
        return True
    if s in ("false", "0", "no", "f", ""):
        return False
    return False


def load_voucher_pool(csv_path: str) -> pd.DataFrame:
    # load voucher CSV and normalize columns
    df = pd.read_csv(csv_path)

    df["Contingency"] = df["Contingency"].map(_parse_csv_bool)
    df["TO_AIRPORT"] = df["TO_AIRPORT"].map(_parse_csv_bool)
    df["USED"] = df["USED"].map(_parse_csv_bool)
    df["AIRPORT"] = df["AIRPORT"].str.upper().str.strip()

    # parse date ranges into date objects
    year = datetime.now().year
    df["start_date"] = df["Date (start)"].apply(lambda x: _parse_month_day(x, year))
    df["end_date"]   = df["Date (end)"].apply(lambda x: _parse_month_day(x, year))

    return df


def _find_group_voucher(df, airport, ride_date):
    candidates = df[
        (~df["USED"]) &
        (df["AIRPORT"] == airport) &
        (~df["Contingency"]) &
        (df["start_date"] <= ride_date) &
        (df["end_date"] >= ride_date)
    ]
    if candidates.empty:
        return None
    return candidates.index[0]



def _find_contingency_voucher(df, airport, ride_date: date):
    """
    Unused contingency voucher for airport whose date range contains ride_date.
    ride_date must be the group's scheduled ride date (from match.suggested_time_iso),
    not individual form windows — same semantics as group vouchers.
    """
    candidates = df[
        (~df["USED"]) &
        (df["AIRPORT"] == airport) &
        (df["Contingency"]) &
        (df["start_date"] <= ride_date) &
        (df["end_date"] >= ride_date)
    ]
    if candidates.empty:
        return None
    return candidates.index[0]


def assign_vouchers(
    matches: List[Match], 
    voucher_csv_path: str, 
    dry_run: bool = False,
    sb: Optional[Client] = None
):
    """
    Assign vouchers to matches.
    
    For dry-run: Uses local file (creates temp copy, doesn't modify original).
    For real run: Uses Supabase Storage with atomic update pattern:
    1. Download active.csv from Supabase Storage
    2. Work on temp file
    3. Archive old active.csv
    4. Upload updated active.csv
    5. Only mark success after all steps complete
    
    Args:
        matches: List of matches to assign vouchers to
        voucher_csv_path: Local file path (for dry-run) or ignored (for real run)
        dry_run: If True, don't update vouchers in storage
        sb: Supabase client (required for non-dry-run)
    """
    temp_file_path: Optional[str] = None
    
    try:
        if dry_run:
            # Dry run: use local file, create temp copy so original never changes
            temp_path = voucher_csv_path + ".dryrun.csv"
            shutil.copyfile(voucher_csv_path, temp_path)
            working_path = temp_path
        else:
            # Real run: check if using Supabase Storage or local files
            if config.USE_SUPABASE_STORAGE:
                # Use Supabase Storage with atomic update pattern
                if sb is None:
                    raise ValueError("Supabase client required when USE_SUPABASE_STORAGE is True")
                
                # Step 1: Download active.csv from Supabase Storage
                print("Downloading active vouchers from Supabase Storage...")
                temp_file_path = storage.download_file(
                    sb, 
                    config.STORAGE_VOUCHERS_BUCKET, 
                    config.VOUCHERS_ACTIVE_FILE
                )
                working_path = temp_file_path
                
                # Verify file was downloaded
                if not Path(working_path).exists():
                    raise FileNotFoundError(f"Failed to download {config.VOUCHERS_ACTIVE_FILE} from Supabase Storage")
            else:
                # Use local file (backward compatibility)
                working_path = voucher_csv_path

        # Load voucher pool
        df = load_voucher_pool(working_path)

        # Clear all voucher fields first so final state matches current groups only
        # (e.g. after Connect merge or any in-memory reuse).
        for match in matches:
            match.group_voucher = None
            for r in match.riders or []:
                r.group_voucher = None
                r.contingency_voucher = None

        # Assign vouchers to matches
        for match in matches:
            riders = match.riders
            if not riders:
                continue
            # Connect Shuttles do not get vouchers
            if getattr(match, "ride_type", None) == "Connect":
                match.group_voucher = None
                for r in riders:
                    r.group_voucher = None
                    r.contingency_voucher = None
                continue
            airport = (riders[0].airport or "").upper()
            ride_date = datetime.fromisoformat(match.suggested_time_iso).date()
            to_airport = riders[0].to_airport
            covered = is_ride_date_covered(ride_date, to_airport)

            # --- GROUP VOUCHER ASSIGNMENT (only when covered if COVERED_DATES_EXPLICIT) ---
            if getattr(match, "group_subsidy", False) and covered:
                idx = _find_group_voucher(df, airport, ride_date)
                if idx is not None:
                    voucher = df.loc[idx, "voucher_link"]
                    match.group_voucher = voucher
                    for r in riders:
                        r.group_voucher = voucher
                    df.at[idx, "USED"] = True
                else:
                    match.group_voucher = None
                    for r in riders:
                        r.group_voucher = None
            elif getattr(match, "group_subsidy", False) and not covered:
                match.group_voucher = None
                for r in riders:
                    r.group_voucher = None

            # --- CONTINGENCY VOUCHERS (inbound only, subsidized groups only, and only when covered) ---
            is_subsidized = getattr(match, "group_subsidy", False)
            if is_subsidized and covered:
                for r in riders:
                    if not r.to_airport:  # inbound
                        idx = _find_contingency_voucher(df, airport, ride_date)
                        if idx is not None:
                            voucher = df.loc[idx, "voucher_link"]
                            r.contingency_voucher = voucher
                            df.at[idx, "USED"] = True
                        else:
                            r.contingency_voucher = None
                    else:
                        r.contingency_voucher = None
            else:
                for r in riders:
                    r.contingency_voucher = None

        # Save updated vouchers
        df.to_csv(working_path, index=False)
        
        # For real run: atomic update pattern (only if using Supabase Storage)
        if not dry_run and config.USE_SUPABASE_STORAGE and sb is not None:
            # Step 2: Archive old active.csv before updating
            print("Archiving previous active vouchers...")
            storage.archive_file(
                sb,
                config.STORAGE_VOUCHERS_BUCKET,
                config.VOUCHERS_ACTIVE_FILE,
                config.VOUCHERS_ARCHIVE_FOLDER
            )
            
            # Step 3: Upload updated active.csv
            print("Uploading updated active vouchers to Supabase Storage...")
            storage.upload_file(
                sb,
                config.STORAGE_VOUCHERS_BUCKET,
                config.VOUCHERS_ACTIVE_FILE,
                working_path
            )
            print("Successfully updated active vouchers in Supabase Storage")
        elif not dry_run and not config.USE_SUPABASE_STORAGE:
            # Local file mode: file is already saved to working_path (which is voucher_csv_path)
            print(f"Updated vouchers saved to local file: {working_path}")
        
        return working_path
        
    finally:
        # Clean up temp file if we created one for Supabase Storage
        if temp_file_path and Path(temp_file_path).exists():
            Path(temp_file_path).unlink(missing_ok=True)

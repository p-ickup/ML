# vouchers.py
import shutil
from datetime import date, datetime
from typing import List

import config
import pandas as pd
from ruleMatching import Match


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
    cleaned = " ".join(str(s).strip().split())
    return datetime.strptime(f"{cleaned} {year}", "%B %d %Y").date()


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
):
    """
    Assign vouchers to matches from a local CSV pool.

    The production pipeline no longer uses this function to consume vouchers;
    production voucher consumption happens in the transactional DB commit RPC.
    Dry-run: copies to ``<path>.dryrun.csv`` and updates the copy only.
    Legacy/non-pipeline use with dry_run=False updates ``voucher_csv_path``.
    """
    if dry_run:
        working_path = voucher_csv_path + ".dryrun.csv"
        shutil.copyfile(voucher_csv_path, working_path)
    else:
        working_path = voucher_csv_path

    df = load_voucher_pool(working_path)

    for match in matches:
        match.group_voucher = None
        for r in match.riders or []:
            r.group_voucher = None
            r.contingency_voucher = None

    for match in matches:
        riders = match.riders
        if not riders:
            continue
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

        is_subsidized = getattr(match, "group_subsidy", False)
        if is_subsidized and covered:
            for r in riders:
                if not r.to_airport:
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

    df.to_csv(working_path, index=False)
    if dry_run:
        print(f"Voucher dry-run copy updated: {working_path}")
    else:
        print(f"Updated vouchers saved to: {working_path}")

    return working_path

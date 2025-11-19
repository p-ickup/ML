# vouchers.py
import shutil
from datetime import datetime
from typing import List

import pandas as pd
from rider_data import RiderLite
from ruleMatching import Match


def _parse_month_day(s: str, year: int):
    # convert "November 21" into a date object for the given year
    return datetime.strptime(f"{s} {year}", "%B %d %Y").date()


def load_voucher_pool(csv_path: str) -> pd.DataFrame:
    # load voucher CSV and normalize columns
    df = pd.read_csv(csv_path)

    df["Contingency"] = df["Contingency"].astype(bool)
    df["TO_AIRPORT"] = df["TO_AIRPORT"].astype(bool)
    df["USED"] = df["USED"].astype(bool)
    df["AIRPORT"] = df["AIRPORT"].str.upper().str.strip()

    # parse date ranges into date objects
    year = datetime.now().year
    df["start_date"] = df["Date (start)"].apply(lambda x: _parse_month_day(x, year))
    df["end_date"]   = df["Date (end)"].apply(lambda x: _parse_month_day(x, year))

    return df


def _find_group_voucher(df, airport, ride_date):
    candidates = df[
        (df["USED"] == False) &
        (df["AIRPORT"] == airport) &
        (df["Contingency"] == False) &
        (df["start_date"] <= ride_date) &
        (df["end_date"] >= ride_date)
    ]
    if candidates.empty:
        return None
    return candidates.index[0]



def _find_contingency_voucher(df, airport):
    # find an unused contingency voucher for the airport
    candidates = df[
        (df["USED"] == False) &
        (df["AIRPORT"] == airport) &
        (df["Contingency"] == True)
    ]
    if candidates.empty:
        return None
    return candidates.index[0]


def assign_vouchers(matches: List[Match], voucher_csv_path: str, dry_run: bool = False):
    # if dry run, copy CSV so original never changes
    if dry_run:
        temp_path = voucher_csv_path + ".dryrun.csv"
        shutil.copyfile(voucher_csv_path, temp_path)
        working_path = temp_path
    else:
        working_path = voucher_csv_path

    df = load_voucher_pool(working_path)

    for match in matches:
        riders = match.riders
        airport = (riders[0].airport or "").upper()
        ride_date = datetime.fromisoformat(match.suggested_time_iso).date()

        # --- GROUP VOUCHER ASSIGNMENT (regardless of direction) ---
        if getattr(match, "group_subsidy", False):
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

        # --- CONTINGENCY VOUCHERS (inbound only) ---
        for r in riders:
            if not r.to_airport:  # inbound
                idx = _find_contingency_voucher(df, airport)
                if idx is not None:
                    voucher = df.loc[idx, "voucher_link"]
                    r.contingency_voucher = voucher
                    df.at[idx, "USED"] = True
                else:
                    r.contingency_voucher = None

    df.to_csv(working_path, index=False)
    return working_path

# This file will be in charge of checking if matches from our matches that were generated from running our algorithm are subsidized 
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import rider_data
from supabase import Client

#policy configuration
ASPC_DATES: List[str] = ["2025-10-11", "2025-10-13", "2025-10-14"]
OUTBOUND_START = 6
OUTBOUND_END   = 18
INBOUND_START  = 10
INBOUND_END    = 22
ELIGIBLE_SCHOOL = "Pomona"


#result container for UI/diagnostics
@dataclass
class ASPCResult:
    eligible_window: bool
    exact_operational_date: bool
    guaranteed: bool
    message: str


#'HH:MM' → float hour (e.g., '06:30' → 6.5)
def _parse_hours_hhmm(time_str: str) -> Optional[float]:
    try:
        hh, mm = time_str.split(":")
        return int(hh) + int(mm) / 60.0
    except Exception:
        return None


#check if flight_date within ±days of any target date
def _within_plusminus_days(flight_date_str: str, target_dates: List[str], days: int = 2) -> bool:
    try:
        f = date.fromisoformat(flight_date_str)
    except Exception:
        return False
    for d in target_dates:
        try:
            t = date.fromisoformat(d)
        except Exception:
            continue
        if abs((f - t).days) <= days:
            return True
    return False


#check if flight_date exactly matches a target date
def _is_exact_date(flight_date_str: str, target_dates: List[str]) -> bool:
    return flight_date_str in target_dates


#determine ASPC subsidy eligibility
def check_aspc_subsidy_eligibility(
    *,
    date_of_flight: Optional[str],
    earliest_arrival: Optional[str],
    latest_arrival: Optional[str],
    user_school: Optional[str],
    to_airport: bool
) -> ASPCResult:
    if not date_of_flight or not earliest_arrival or not latest_arrival or not user_school:
        return ASPCResult(False, False, False, "Missing required data.")

    if user_school != ELIGIBLE_SCHOOL:
        return ASPCResult(False, False, False, "Not eligible: non-Pomona student.")

    within_window = _within_plusminus_days(date_of_flight, ASPC_DATES, days=2)
    exact_date = _is_exact_date(date_of_flight, ASPC_DATES)

    if not within_window:
        return ASPCResult(
            eligible_window=False,
            exact_operational_date=False,
            guaranteed=False,
            message=(
                "You are not eligible for ASPC subsidy because your flight date is more than ±2 days "
                "from the guaranteed dates (October 11, 13, and 14). You can still use P-ICKUP to "
                "coordinate non-subsidized rides."
            ),
        )

    earliest = _parse_hours_hhmm(earliest_arrival)
    latest   = _parse_hours_hhmm(latest_arrival)
    if earliest is None or latest is None:
        return ASPCResult(within_window, exact_date, False, "Invalid time format; expected 'HH:MM'.")

    is_guaranteed = False
    warning_message = ""

    if to_airport:
        if earliest >= OUTBOUND_START and latest <= OUTBOUND_END:
            if exact_date:
                is_guaranteed = True
                warning_message = (
                    "✅ You are guaranteed a subsidized ride! Your time range falls within ASPC guaranteed "
                    "hours (6:00 AM – 6:00 PM) on an operational date."
                )
            else:
                warning_message = (
                    "You are not guaranteed a subsidized ride because your flight is within ±2 days of operational dates, "
                    "but it may still be possible if 2+ riders are grouped for ONT. Check out the policy page for more details."
                )
        else:
            if earliest < OUTBOUND_START:
                reason = "your time range is before 6:00 AM"
            elif latest > OUTBOUND_END:
                reason = "your time range is after 6:00 PM"
            else:
                reason = "your time range is outside the guaranteed window (6:00 AM – 6:00 PM)"
            if exact_date:
                warning_message = (
                    f"You are not guaranteed a subsidized ride because {reason}, but it may still be possible if 2+ riders "
                    "are grouped for ONT. Check out the policy page for more details."
                )
            else:
                warning_message = (
                    f"You are not guaranteed a subsidized ride because {reason} and your flight is within ±2 days of operational dates, "
                    "but it may still be possible if 2+ riders are grouped for ONT. Check out the policy page for more details."
                )
    else:
        if earliest >= INBOUND_START and latest <= INBOUND_END:
            if exact_date:
                is_guaranteed = True
                warning_message = (
                    "✅ You are guaranteed a subsidized ride! Your arrival time falls within ASPC guaranteed "
                    "hours (10:00 AM – 10:00 PM) on an operational date."
                )
            else:
                warning_message = (
                    "You are not guaranteed a subsidized ride because your flight is within ±2 days of operational dates, "
                    "but it may still be possible if 2+ riders are grouped for ONT. Check out the policy page for more details."
                )
        else:
            if earliest < INBOUND_START:
                reason = "your arrival time is before 10:00 AM"
            elif latest > INBOUND_END:
                reason = "your arrival time is after 10:00 PM"
            else:
                reason = "your arrival time is outside the guaranteed window (10:00 AM – 10:00 PM)"
            if exact_date:
                warning_message = (
                    f"You are not guaranteed a subsidized ride because {reason}, but it may still be possible if 2+ riders "
                    "are grouped for ONT. Check out the policy page for more details."
                )
            else:
                warning_message = (
                    f"You are not guaranteed a subsidized ride because {reason} and your flight is within ±2 days of operational dates, "
                    "but it may still be possible if 2+ riders are grouped for ONT. Check out the policy page for more details."
                )

    return ASPCResult(within_window, exact_date, is_guaranteed, warning_message)


#update Matches.is_subsidized for a flight
def update_matches_subsidy_for_flight(sb: Client, flight_id: int, is_subsidized: bool) -> None:
    sb.table("Matches").update({"is_subsidized": bool(is_subsidized)}).eq("flight_id", flight_id).execute()


#batch runner that computes and writes subsidy for many riders
def run_aspc_subsidy(sb: Client, riders: List["rider_data.RiderLite"]) -> None:
    updated = skipped = guaranteed = 0
    for r in riders:
        if not r.date or not r.earliest_time or not r.latest_time or not r.school:
            skipped += 1
            continue
        res = check_aspc_subsidy_eligibility(
            date_of_flight=r.date,
            earliest_arrival=r.earliest_time,
            latest_arrival=r.latest_time,
            user_school=r.school,
            to_airport=r.to_airport,
        )
        update_matches_subsidy_for_flight(sb, r.flight_id, res.guaranteed)
        guaranteed += int(res.guaranteed)
        updated += 1
    print(f"Updated {updated} flights; guaranteed True: {guaranteed}; skipped {skipped}.")


#if run directly: execute against all future unmatched riders
if __name__ == "__main__":
    import os

    from dotenv import load_dotenv
    from supabase import create_client

    #lazy import to avoid hard dependency when used as a library
    try:
        from rider_data import \
            RiderData  # expects your existing RiderData/RiderLite
    except Exception as e:
        raise SystemExit(f"subsidization.py needs rider_data.RiderData when run as a script: {e}")

    #env + client
    load_dotenv()
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

    #fetch and run
    rd = RiderData(sb)
    riders = rd.fetch_riders()
    run_aspc_subsidy(sb, riders)

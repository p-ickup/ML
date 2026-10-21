"""Read-only rider fetch from Supabase, ported from
ML/Algorithm/rider_data.py's RiderData class.

Deliberately read-only: this mirrors ``fetch_flights``/``fetch_users``/
``fetch_riders`` only. The write-side of that pipeline (matching_status
updates, Connect-group merge, voucher consumption, the production commit
RPC) is not ported — this service reports matches, it doesn't persist them.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from supabase import Client

from .matching.rider import RiderLite, normalize_airport, normalize_terminal


def _normalize_matching_status(raw: Optional[str]) -> str:
    status = str(raw or "").strip().lower()
    if status in {"submitted", "matched", "unmatched"}:
        return status
    return "submitted"


def fetch_flights(
    sb: Client,
    max_days_ahead: Optional[int] = 10,
    min_days_ahead: Optional[int] = None,
) -> List[dict]:
    """Future flights within an optional [min, max] day offset from today, excluding already-matched ones."""
    today = datetime.today().date()
    if min_days_ahead is not None and max_days_ahead is not None and min_days_ahead > max_days_ahead:
        raise ValueError(
            f"min_days_ahead ({min_days_ahead}) cannot exceed max_days_ahead ({max_days_ahead})"
        )

    q = (
        sb.table("Flights")
        .select(
            "flight_id,user_id,flight_no,airline_iata,earliest_time,latest_time,"
            "airport,date,to_airport,terminal,matching_status,bag_no,bag_no_large,bag_no_personal"
        )
        .order("date", desc=False)
        .order("earliest_time", desc=False)
    )
    if min_days_ahead is None:
        q = q.gt("date", today.isoformat())
    else:
        start_date = (today + timedelta(days=min_days_ahead)).isoformat()
        q = q.gte("date", start_date)

    if max_days_ahead is not None:
        end_date = (today + timedelta(days=max_days_ahead)).isoformat()
        q = q.lte("date", end_date)

    resp = q.execute()
    rows = []
    for row in resp.data or []:
        # Only unmatched flights are candidates for a new matching run.
        if _normalize_matching_status(row.get("matching_status")) == "matched":
            continue
        rows.append(row)
    return rows


def fetch_users(sb: Client, user_ids: List[str]) -> Dict[str, Dict[str, str]]:
    if not user_ids:
        return {}
    resp = (
        sb.table("Users")
        .select("user_id,school,firstname,lastname")
        .in_("user_id", list(set(uid for uid in user_ids if uid)))
        .execute()
    )
    result: Dict[str, Dict[str, str]] = {}
    for row in resp.data or []:
        if not row.get("school"):
            continue
        firstname = row.get("firstname") or ""
        lastname = row.get("lastname") or ""
        full_name = f"{firstname} {lastname}".strip() if (firstname or lastname) else None
        result[row["user_id"]] = {"school": row.get("school"), "name": full_name}
    return result


def fetch_unmatched_riders(
    sb: Client,
    max_days_ahead: Optional[int] = 10,
    min_days_ahead: Optional[int] = None,
) -> List[RiderLite]:
    """Build RiderLite objects for unmatched flights, joined with each rider's school/name."""
    flights = fetch_flights(sb, max_days_ahead=max_days_ahead, min_days_ahead=min_days_ahead)
    uid_list = [f["user_id"] for f in flights if f.get("user_id")]
    user_info_by_uid = fetch_users(sb, uid_list)

    riders: List[RiderLite] = []
    for f in flights:
        user_info = user_info_by_uid.get(f["user_id"])
        if not user_info or not user_info.get("school"):
            continue

        riders.append(
            RiderLite(
                user_id=f["user_id"],
                flight_id=int(f["flight_id"]),
                flight_no=(int(f["flight_no"]) if f.get("flight_no") is not None else None),
                airline_iata=f.get("airline_iata"),
                earliest_time=str(f.get("earliest_time")),
                latest_time=str(f.get("latest_time")),
                airport=normalize_airport(f.get("airport")),
                to_airport=bool(f.get("to_airport")),
                date=str(f.get("date")),
                terminal=normalize_terminal(f.get("terminal")),
                school=user_info.get("school"),
                name=user_info.get("name"),
                bags_no=(int(f["bag_no"]) if f.get("bag_no") is not None else 0),
                bags_no_large=(int(f["bag_no_large"]) if f.get("bag_no_large") is not None else 0),
                bag_no_personal=(int(f["bag_no_personal"]) if f.get("bag_no_personal") is not None else 0),
            )
        )

    return riders

# this class is in charge of getting the neccessary data from our supabase database
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from supabase import Client


# normalize terminal strings (strip, uppercase, map common patterns)
def normalize_terminal(raw: Optional[str]) -> str:
    if not raw:
        return "UNKNOWN"

    term = str(raw).strip().upper()

    # numeric terminal (e.g., "1", "Terminal 2")
    m = re.search(r"\b\d+\b", term)
    if m:
        return m.group(0)

    # single letter terminal (A, B, C…)
    if re.fullmatch(r"[A-Z]", term):
        return term

    # international-style terminals
    if "INTL" in term or "INTERNATIONAL" in term or "TBIT" in term:
        return "INTL"

    return term


# normalize airport names to IATA codes when possible
def normalize_airport(raw: Optional[str]) -> str:
    if not raw:
        return "UNKNOWN"

    airport = str(raw).strip().upper()
    return airport


# row shape used by buckets/matching
@dataclass
class RiderLite:
    user_id: str
    flight_id: int
    flight_no: Optional[int]
    earliest_time: str
    latest_time: str
    airport: str
    to_airport: bool
    date: str
    terminal: Optional[str]
    matched: bool
    school: str
    bags_no: Optional[int]
    bags_no_large: Optional[int]
    bag_no_personal: Optional[int]
    subsidized: bool = False

class RiderData:
    # minimal fetch layer for flights + users → RiderLite objects

    def __init__(self, sb: Client):
        self.sb = sb
        self.riders: List[RiderLite] = []

    #fetch future flights within a max horizon (default 10 days)
    def fetch_flights(self, max_days_ahead: Optional[int] = 10) -> List[dict]:
        today = datetime.today().date()
        q = (
            self.sb.table("Flights")
            .select(
                "flight_id,user_id,flight_no,earliest_time,latest_time,"
                "airport,date,to_airport,terminal,matched,bag_no,bag_no_large,bag_no_personal"
            )
            .gt("date", today.isoformat())
            .is_("matched", None)
            .order("date", desc=False)
            .order("earliest_time", desc=False)
        )

        if max_days_ahead is not None:
            end_date = (today + timedelta(days=max_days_ahead)).isoformat()
            q = q.lte("date", end_date)

        resp = q.execute()
        
        print("\n=== DEBUG: RAW FLIGHT ROWS ===")
        for row in resp.data or []:
            print(row)

        print("=== END DEBUG ===\n")

        return resp.data or []


    # fetch school info for given user_ids
    def fetch_users(self, user_ids: List[str]) -> Dict[str, str]:
        if not user_ids:
            return {}
        resp = (
            self.sb.table("Users")
            .select("user_id,school")
            .in_("user_id", list(set(uid for uid in user_ids if uid)))
            .execute()
        )
        return {row["user_id"]: row["school"] for row in (resp.data or []) if row.get("school")}

    # build RiderLite objects from flights + schools (normalized airport + terminal)
    def fetch_riders(self, max_days_ahead: Optional[int] = 10) -> List[RiderLite]:
        flights = self.fetch_flights(max_days_ahead=max_days_ahead)
        uid_list = [f["user_id"] for f in flights if f.get("user_id")]
        school_by_uid = self.fetch_users(uid_list)
        riders: List[RiderLite] = []

        print("\n=== DEBUG: CONSTRUCTED FLIGHT -> RIDERLITE MAPPING ===")
        for f in flights:
            print("FLIGHT:", f)
            print("=== END DEBUG ===\n")
            school = school_by_uid.get(f["user_id"])
            if not school:
                continue
            
            riders.append(
                RiderLite(
                    user_id=f["user_id"],
                    flight_id=int(f["flight_id"]),
                    flight_no=(int(f["flight_no"]) if f.get("flight_no") is not None else None),
                    earliest_time=str(f.get("earliest_time")),
                    latest_time=str(f.get("latest_time")),
                    airport=normalize_airport(f.get("airport")),
                    to_airport=bool(f.get("to_airport")),
                    date=str(f.get("date")),
                    terminal=normalize_terminal(f.get("terminal")),
                    matched=bool(f.get("matched", False)),
                    school=school,
                    bags_no=(int(f["bag_no"]) if f.get("bag_no") is not None else None),
                    bags_no_large=(int(f["bag_no_large"]) if f.get("bag_no_large") is not None else None),
                    bag_no_personal=(int(f["bag_no_personal"]) if f.get("bag_no_personal") is not None else None),
                    subsidized=False, 
                )
            )

        self.riders = riders
        return self.riders

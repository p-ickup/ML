# rider.py — ported from ML/Algorithm/rider_data.py, minus the Supabase fetch layer.
import re
from dataclasses import dataclass
from typing import Optional


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

    return str(raw).strip().upper()


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
    school: str
    bags_no: Optional[int]
    bags_no_large: Optional[int]
    bag_no_personal: Optional[int]
    name: Optional[str] = None
    airline_iata: Optional[str] = None
    subsidized: bool = False
    # populated later by the pipeline (not supplied by the caller)
    group_voucher: Optional[str] = None
    contingency_voucher: Optional[str] = None


def rider_lite_from_input(data: dict) -> RiderLite:
    """Build a RiderLite from a request payload dict (JSON-decoded rider)."""
    if not data.get("school"):
        raise ValueError(f"Rider {data.get('flight_id')} is missing required field 'school'.")

    return RiderLite(
        user_id=str(data["user_id"]),
        flight_id=int(data["flight_id"]),
        flight_no=(int(data["flight_no"]) if data.get("flight_no") is not None else None),
        airline_iata=data.get("airline_iata"),
        earliest_time=str(data["earliest_time"]),
        latest_time=str(data["latest_time"]),
        airport=normalize_airport(data.get("airport")),
        to_airport=bool(data.get("to_airport")),
        date=str(data["date"]),
        terminal=normalize_terminal(data.get("terminal")),
        school=data["school"],
        name=data.get("name"),
        bags_no=(int(data["bags_no"]) if data.get("bags_no") is not None else 0),
        bags_no_large=(int(data["bags_no_large"]) if data.get("bags_no_large") is not None else 0),
        bag_no_personal=(int(data["bag_no_personal"]) if data.get("bag_no_personal") is not None else 0),
    )

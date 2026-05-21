"""
Connect shuttle policy helpers (read from config.py).
"""

from __future__ import annotations

from typing import List, Optional, Tuple

import config


def connect_enabled() -> bool:
    arrival = getattr(config, "CONNECT_ARRIVAL", None) or []
    departure = getattr(config, "CONNECT_DEPARTURE", None) or []
    return bool(arrival or departure)


def connect_tiers() -> List[Tuple[int, int]]:
    """Return (min, max) tiers; larger max tier first for group formation."""
    tiers: List[Tuple[int, int]] = []
    for name in ("CONNECT_SIZE2", "CONNECT_SIZE1"):
        raw = getattr(config, name, None)
        if raw and len(raw) >= 2:
            tiers.append((int(raw[0]), int(raw[1])))
    return sorted(tiers, key=lambda t: (-t[1], -t[0]))


def fits_connect_size(count: int) -> bool:
    return any(lo <= count <= hi for lo, hi in connect_tiers())


def connect_search_bounds() -> Tuple[Optional[int], Optional[int]]:
    tiers = connect_tiers()
    if not tiers:
        return None, None
    return min(lo for lo, _ in tiers), max(hi for _, hi in tiers)


def allowed_airports(to_airport: bool) -> List[str]:
    if to_airport:
        raw = getattr(config, "CONNECT_DEPARTURE", None) or []
    else:
        raw = getattr(config, "CONNECT_ARRIVAL", None) or []
    return [str(a).strip().upper() for a in raw if str(a).strip()]


def enabled_connect_scopes() -> List[Tuple[str, bool]]:
    """(airport, to_airport) pairs enabled by config."""
    scopes: List[Tuple[str, bool]] = []
    for airport in allowed_airports(True):
        scopes.append((airport, True))
    for airport in allowed_airports(False):
        scopes.append((airport, False))
    return scopes


def rider_in_connect_scope(rider) -> bool:
    airport = (getattr(rider, "airport", None) or "").strip().upper()
    to_airport = bool(getattr(rider, "to_airport", False))
    allowed = allowed_airports(to_airport)
    if not allowed:
        return False
    return airport in allowed


def bucket_key_for_rider(rider) -> str:
    direction = "TO" if rider.to_airport else "FROM"
    school = (getattr(rider, "school", None) or "POMONA").strip().upper()
    return f"{direction} {rider.airport} | {school}"


def bucket_key_in_connect_scope(bucket_key: Optional[str]) -> bool:
    if not bucket_key or not connect_enabled():
        return False
    head = bucket_key.split("|")[0].strip().upper()
    if head.startswith("TO "):
        airport = head[3:].strip()
        return airport in allowed_airports(True)
    if head.startswith("FROM "):
        airport = head[5:].strip()
        return airport in allowed_airports(False)
    return False

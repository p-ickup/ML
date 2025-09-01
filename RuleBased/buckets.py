# this file is meant to go through our whole database and bucket the users into their respective buckets for our hard constraints
# this means going through all flights and seperating matching forms into buckets based on if they are going to the airport or from the airport and the range of times they are available to be picked up


from __future__ import annotations

from collections import defaultdict
from datetime import date, time, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import h3
import location_cache  # only needed if include_coords=True

# Types
RiderLite = Dict[str, Any]
BucketKey = Tuple[bool, str]  # (to_airport, AIRPORT)

# TYPES:
RiderLite = Dict[str, Any]
# TO_AIPORT, AIRPORT
BucketKey = Tuple[bool, str]

# H3 Functions
def to_grid_id(lat: float, lon: float, resolution: int) -> str:
    """
    Convert lat/lon coordinates to H3 grid ID at specified resolution.
    
    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees  
        resolution: H3 resolution (0-15, where 9 = ~174m hexagons)
        
    Returns:
        H3 hexagon identifier string
    """
    if not (-90 <= lat <= 90):
        raise ValueError(f"Invalid latitude: {lat}. Must be between -90 and 90")
    if not (-180 <= lon <= 180):
        raise ValueError(f"Invalid longitude: {lon}. Must be between -180 and 180")
    
    if not (0 <= resolution <= 15):
        raise ValueError(f"Invalid H3 resolution: {resolution}. Must be between 0 and 15")
    
    return h3.latlng_to_cell(lat, lon, resolution)

def haversine_m(a_latlon: Tuple[float, float], b_latlon: Tuple[float, float]) -> float:
    """
    Calculate great-circle distance between two lat/lon points in meters.
    
    Args:
        a_latlon: Tuple of (lat, lon) for first point
        b_latlon: Tuple of (lat, lon) for second point
        
    Returns:
        Distance in meters
    """
    lat1, lon1 = a_latlon
    lat2, lon2 = b_latlon
    
    return h3.point_dist((lat1, lon1), (lat2, lon2), unit='m')

# functions for normalizing airport
def _normalize_airport(airport: str) -> str:
    """
    Normalize airport code to uppercase and strip whitespace
    """
    return airport.upper().strip()

def _normalize_terminal(terminal: Optional[str]) -> Optional[str]:
    """
    Normalize terminal code to uppercase and strip whitespace.
    Account for when terminal is null, when "Terminal 4" or "10" is passed, and when "T4" is passed
    """
    if terminal is None:
        return None
    
    terminal = terminal.upper().strip()
    
    # Handle "Terminal X" format
    if terminal.startswith("TERMINAL "):
        number = terminal[9:]  # Remove "TERMINAL " prefix
        if number.isdigit():
            return f"T{number}"
    
    # Handle plain numbers
    if terminal.isdigit():
        return f"T{terminal}"
    
    # Handle "TX" format (already normalized)
    if terminal.startswith("T") and terminal[1:].isdigit():
        return terminal
    
    # Return as-is for other formats
    return terminal

def _attach_local_tz(d: date, t: time, tzinfo) -> datetime:
    """
    Combine DB's local date+time into a timezone-aware *local* datetime.
    We do not convert to UTC; matching will compare in this same tz.
    """
    # Build a naive local datetime then attach tzinfo=America/Los_Angeles
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, t.microsecond, tzinfo=tzinfo)


# Rows -> RiderLite
def prepare_riders(rows: Iterable[Mapping[str, Any]], *, tzinfo, include_coords: bool = False, 
                   loc: Optional["location_cache.LocationCache"] = None, 
                   h3_resolution: int = 9) -> List[RiderLite]:
    """
    Convert DB rows into RiderLite objects for bucketing.
    
    Args:
        rows: Database rows with flight/user data
        tzinfo: Timezone info for local time conversion
        include_coords: Whether to include lat/lon coordinates
        loc: LocationCache instance for coordinate lookup
        h3_resolution: H3 resolution for grid ID generation (default: 9 = ~174m hexagons)
    """
    riders: List[RiderLite] = []
    
    for row in rows:
        # Normalize airport and terminal
        airport = _normalize_airport(row["airport"])
        to_airport = bool(row["to_airport"])
        terminal = _normalize_terminal(row["terminal"])
        
        # Attach local timezone
        earliest_time = _attach_local_tz(row["date"], row["earliest_time"], tzinfo)
        latest_time = _attach_local_tz(row["date"], row["latest_time"], tzinfo)
        
        # Get school, user_id, and flight_id
        school = row["school"]
        user_id = row["user_id"]
        flight_id = row["flight_id"]
        
        # Build RiderLite
        rider: RiderLite = {
            "flight_id": row["flight_id"],
            "user_id": row["user_id"],
            "airport": airport,
            "to_airport": to_airport,
            "terminal": terminal,
            "school": school,
            "earliest_ts": earliest_time,
            "latest_ts": latest_time,
        }  
        
        # add coords if needed
        if include_coords:
            if loc is None:
                raise ValueError("include_coords=True but no LocationCache instance provided")
            if to_airport:
                lat, lon = loc.get_coordinates(school, "SCHOOL")
            else:
                lat, lon = loc.get_coordinates(airport, "AIRPORT")
            if lat is not None and lon is not None:
                rider["lat"] = float(lat)
                rider["lon"] = float(lon)
                
                # Add H3 grid ID if coordinates are available
                try:
                    grid_id = to_grid_id(lat, lon, h3_resolution)
                    rider["grid_id"] = grid_id
                except (ValueError, Exception) as e:
                    print(f"Warning: Could not generate H3 grid ID for {school if to_airport else airport}: {e}")
                    rider["grid_id"] = None
                
        riders.append(rider)
    return riders


# Bucketing
def partition_riders(riders: Sequence[RiderLite]) -> Dict[BucketKey, Dict[str, Any]]:
    """
    Partition riders into buckets based on their airport and whether they are going to the airport or from the airport.
    """
    # defaultdict is a dictionary that will return an empty list if the key is not found
    buckets: Dict[BucketKey, Dict[str, Any]] = defaultdict(lambda: {"all": []})
    
    for rider in riders:
        key: BucketKey = (rider["to_airport"], rider["airport"])
        buckets[key]["all"].append(rider)
        
    # return a copy of the buckets
    return {k: {"all": v["all"][:]} for k, v in buckets.items()}






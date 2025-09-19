# feature_engineering.py
from datetime import datetime
import numpy as np
import pandas as pd


def build_raw_features(loc_cache, flight_row, earliest_time):
    """
    Given one "flight" record (plus user info) from Supabase,
    produce the raw (unnormalized) numeric features in a dict.
    """
    
    # Get school name from user row and airport name from flight row
    school_name = flight_row.get("school")
    airport_name = flight_row.get("airport")
    
    # Fetch coordinates using the LocationCache object
    # You may need to add suffixes like " College" or " Airport"
    home_lat, home_lng = loc_cache.get_coordinates(school_name + " College")
    air_lat, air_lng = loc_cache.get_coordinates(airport_name + " Airport")
    
    # Parse the flight's datetime from ISO format (e.g., "2025-01-01T13:00:00")
    flight_datetime = datetime.fromisoformat(flight_row["date"])
    
    # Compute time in minutes between the flight and the earliest time in the batch
    elapsed_minutes = (flight_datetime - earliest_time).total_seconds() / 60.0
    
    # Ensure no negative values if the flight is in the past
    if elapsed_minutes < 0:
        elapsed_minutes = 0
        
    # Perform cyclical encoding to capture time-of-year periodicity
    minutes_in_year = 365 * 24 * 60
    datetime_sin = np.sin(2 * np.pi * elapsed_minutes / minutes_in_year)
    datetime_cos = np.cos(2 * np.pi * elapsed_minutes / minutes_in_year)
    
    # Extract additional numerical features from the flight row
    # These include user-defined constraints and preferences
    min_wait = flight_row["earliest_time"]
    max_wait = flight_row["latest_time"]
    bag_no = flight_row["bag_no"]
    max_price = flight_row["max_price"]
    dropoff_range = flight_row["max_dropoff"]
    
    # Return a dictionary of all the unnormalized numeric features
    return {
        "home_lat": home_lat,
        "home_lng": home_lng,
        "airport_lat": air_lat,
        "airport_lng": air_lng,
        "ElapsedTime": elapsed_minutes,
        "datetime_sin": datetime_sin,
        "datetime_cos": datetime_cos,
        "MinWaitTime": min_wait,
        "MaxWaitTime": max_wait,
        "BagNumber": bag_no,
        "MaxSpendingRange": max_price,
        "DropOffRange": dropoff_range,
    }

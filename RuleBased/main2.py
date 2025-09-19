#!/usr/bin/env python3
"""
Main entry point for pickup system v0 MVP
Streamlined version that directly handles ride creation and matching
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List

from config import GRID_RES
# Local imports
from location_cache import LocationCache
# Third-party imports
from supabase import Client, create_client

from RuleBased.buckets import partition_riders, prepare_riders

# Load environment variables for Supabase and Google Maps API keys
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Initialize Supabase client and location cache
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
loc_cache = LocationCache(GOOGLE_API_KEY) if GOOGLE_API_KEY else None


def fetch_flights():
    """
    Retrieve flights scheduled between 2 to 4 days from now that are not yet matched.
    Only keep those with valid date and earliest_time.
    """
    now = datetime.now().replace(hour=0, minute=0, second=0)
    lower_bound = (now + timedelta(days=2)).replace(hour=0, minute=0, second=0)
    upper_bound = (now + timedelta(days=4)).replace(hour=0, minute=0, second=0)

    response = supabase.table("Flights").select("*").eq("matched", False).execute()
    flights = response.data

    filtered = []
    for f in flights:
        try:
            flight_datetime = datetime.fromisoformat(f["date"] + "T" + f["earliest_time"])
            if lower_bound <= flight_datetime <= upper_bound:
                filtered.append(f)
        except Exception as e:
            print(f"Skipping flight {f['flight_id']}: {e}")
    return filtered, now


def fetch_users(user_ids):
    """
    Retrieve school info for a list of user IDs.
    """
    return supabase.table("Users").select("user_id, school").in_("user_id", user_ids).execute().data


def generate_matches():
    """
    Main logic to fetch flights, run matching engine, and save matched rides to Supabase.
    """
    flights, now = fetch_flights()
    if not flights:
        print("No flights in timeframe.")
        return

    # Get unique users and map them to their school
    user_ids = list({f["user_id"] for f in flights})
    users = fetch_users(user_ids)
    user_map = {u["user_id"]: u["school"] for u in users}

    # Add school info to flights
    for flight in flights:
        uid = flight["user_id"]
        school = user_map.get(uid)
        if not school:
            print(f"No school info for user {uid}, skipping.")
            continue
        flight["school"] = school

    print(f"Processing {len(flights)} eligible flights")

    # Use buckets.py for matching (similar to previous main.py approach)
    try:
        # Prepare riders using buckets.py
        riders = prepare_riders(
            flights,
            tzinfo=datetime.now().astimezone().tzinfo,  # Use local timezone
            include_coords=loc_cache is not None,
            loc=loc_cache,
            h3_resolution=GRID_RES,
        )
        
        # Partition riders into buckets
        buckets = partition_riders(riders)
        
        if not buckets:
            print("No riders to match.")
            return
            
        print(f"Processing {len(riders)} riders in {len(buckets)} buckets")
        
        # Simple matching: take first 2-4 riders from each bucket
        matched = []
        ride_id_counter = 1
        
        # Get existing max ride_id
        response = supabase.table("Matches").select("ride_id").order("ride_id", desc=True).limit(1).execute()
        if response.data:
            ride_id_counter = response.data[0]["ride_id"] + 1
        
        for bucket_key, bucket_data in buckets.items():
            bucket_riders = bucket_data["all"]  # buckets.py returns {"all": [riders]}
            if len(bucket_riders) < 2:
                continue
                
            # Take up to 4 riders from this bucket
            group_size = min(len(bucket_riders), 4)
            group_riders = bucket_riders[:group_size]
            
            print(f"[Bucket {bucket_key}] Size: {group_size} | Airport: {group_riders[0].get('airport', 'Unknown')}")
            
            # Create ride group
            ride_id = ride_id_counter
            ride_id_counter += 1
            
            for rider in group_riders:
                matched.append({
                    "ride_id": ride_id,
                    "user_id": rider["user_id"],
                    "flight_id": rider["flight_id"],
                    "created_at": datetime.now().isoformat()
                })
                # Mark flight as matched
                supabase.table("Flights").update({"matched": True}).eq("flight_id", rider["flight_id"]).execute()
        
        # Save all matched users to the Matches table
        if matched:
            supabase.table("Matches").insert(matched).execute()
            print(f"✅ Inserted {len(matched)} match entries.")

    except Exception as e:
        print(f"Error in matching: {e}")
        import traceback
        traceback.print_exc()


# Script entry point
if __name__ == "__main__":
    generate_matches()




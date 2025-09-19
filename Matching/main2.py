# main.py
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from feature_engineer2 import build_raw_features
from matching2 import Matcher
from scipy.spatial.distance import pdist, squareform
from supabase import create_client

from location_cache import LocationCache

# Load environment variables for Supabase and Google Maps API keys
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Initialize Supabase client, location cache, and KMeans matcher
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
loc_cache = LocationCache(GOOGLE_API_KEY)
matcher = Matcher()

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
    Main logic to fetch flights, compute features, cluster passengers, 
    apply constraints, and save matched rides to Supabase.
    """
    flights, now = fetch_flights()
    if not flights:
        print("No flights in timeframe.")
        return

    # Get unique users and map them to their school
    user_ids = list({f["user_id"] for f in flights})
    users = fetch_users(user_ids)
    user_map = {u["user_id"]: u["school"] for u in users}

    raw_rows = []
    metadata = []

    # Build raw features for each flight
    for flight in flights:
        uid = flight["user_id"]
        school = user_map.get(uid)
        if not school:
            print(f"No school info for user {uid}, skipping.")
            continue
        flight["school"] = school

        try:
            raw = build_raw_features(loc_cache, flight, earliest_time=now)
            raw["user_id"] = uid
            raw["flight_id"] = flight["flight_id"]
            metadata.append({
                "user_id": uid,
                "flight_id": flight["flight_id"],
                "airport": flight["airport"],
                "date": flight["date"]
            })
            raw_rows.append(raw)
        except Exception as e:
            print(f"Feature extraction failed for flight {flight['flight_id']}: {e}")

    # Normalize and cluster raw features
    df = pd.DataFrame(raw_rows)
    df_weighted = matcher.normalize_and_weight(df)
    df["cluster"] = matcher.predict_clusters(df_weighted)
    df_clustered = matcher.apply_bag_constraint(df)

    # Assign ride IDs based on clusters
    # Fetch the maximum ride_id currently in the Matches table
    response = supabase.table("Matches").select("ride_id").order("ride_id", desc=True).limit(1).execute()
    existing_rides = response.data

    # Start from max(existing ride_id) + 1 or 1 if there are no existing rides
    ride_id_counter = (existing_rides[0]["ride_id"] + 1) if existing_rides else 1
    grouped = df_clustered.groupby("cluster")
    matched = []

    for cluster_id, group in grouped:
        # Skip clusters with fewer than 3 people
        if not (2 <= len(group) <= 4):
            print(f"Cluster {cluster_id} has only {len(group)} users — skipping (leaving unmatched).")
            continue

        # Compute cluster quality
        # Get cluster centroid from trained KMeans model
        centroid = matcher.model.cluster_centers_[cluster_id]

        # Compute distance from each row in the cluster to the centroid
        group["distance_to_centroid"] = group[matcher.features].apply(
            lambda row: np.linalg.norm(row.values - centroid), axis=1
        )

        # Compute average distance and convert to a quality score
        avg_distance = group["distance_to_centroid"].mean()
        match_quality = 1 / (1 + avg_distance)

        print(f"[Cluster {cluster_id}] Size: {len(group)} | Avg distance: {avg_distance:.4f} | Quality: {match_quality:.4f}")

        # Insert match group into Matches table
        ride_id = ride_id_counter
        ride_id_counter += 1

        for _, row in group.iterrows():
            f = next(m for m in metadata if m["flight_id"] == row["flight_id"])
            matched.append({
                "ride_id": ride_id,
                "user_id": f["user_id"],
                "flight_id": f["flight_id"],
                "created_at": datetime.now().isoformat()
            })
            supabase.table("Flights").update({"matched": True}).eq("flight_id", f["flight_id"]).execute()

    # Save all matched users to the Matches table
    if matched:
        supabase.table("Matches").insert(matched).execute()
        print(f"✅ Inserted {len(matched)} match entries.")

# Script entry point
if __name__ == "__main__":
    generate_matches()

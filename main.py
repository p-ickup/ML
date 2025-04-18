# main.py
import os
from datetime import datetime

import pandas as pd
from supabase import Client, create_client

from feature_engineer import build_raw_features
from location_cache import LocationCache
from matching import KMeansMatcher

# ENV CONFIG
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

MODEL_PATH = "models/kmeans_model.pkl"  # your saved KMeans model

# Initialize
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
loc_cache = LocationCache(GOOGLE_API_KEY)
matcher = KMeansMatcher(model_path=MODEL_PATH)

# Columns to scale (the same ones from your training code)
FEATURE_COLS = [
    "BagNumber",
    "MaxSpendingRange",
    "DropOffRange",
    "home_lat",
    "home_lng",
    "airport_lat",
    "airport_lng",
    "MinWaitTime",
    "MaxWaitTime",
    "ElapsedTime",
    "datetime_sin",
    "datetime_cos"
]

def fetch_unmatched_flights():
    # Example: select flights that are not yet in Matches, or have no ride_id
    # This depends on your schema. If you have a "to_airport" column, you can filter on that too.
    resp = supabase.table("Flights").select("*").execute()
    if resp.get("error"):
        raise RuntimeError(resp["error"]["message"])
    return resp["data"]

def fetch_users(user_ids):
    # Fetch user info in bulk
    resp = supabase.table("Users").select("user_id, school").in_("user_id", user_ids).execute()
    if resp.get("error"):
        raise RuntimeError(resp["error"]["message"])
    return resp["data"]

def main():
    # 1) Decide your "earliest_time" baseline
    #    In your code snippet, you mentioned "always use our current date/time as base"
    earliest_time = datetime.now()

    # 2) Fetch flights
    flights = fetch_unmatched_flights()
    if not flights:
        print("No flights found.")
        return

    # 3) Fetch user details from Users
    user_ids = list({f["user_id"] for f in flights if "user_id" in f})
    users = fetch_users(user_ids)

    # Make a dictionary of user_id -> user_data
    user_map = {u["user_id"]: u for u in users}

    # 4) Build raw features for each flight
    rows_for_df = []
    for flight in flights:
        user_id = flight["user_id"]
        # Merge user info (like "school") into flight
        user_school = user_map.get(user_id, {}).get("school", None)

        # We'll store them in flight dict so the build_raw_features function can see them
        flight["school"] = user_school  # flight has "airport" & "date" columns already

        try:
            raw_feats = build_raw_features(loc_cache, flight, earliest_time)

            # Add identifying columns you might need later
            raw_feats["flight_id"] = flight["flight_id"]
            raw_feats["user_id"] = flight["user_id"]
            raw_feats["airport"] = flight["airport"]
            raw_feats["date"] = flight["date"]  # keep the original so we can group
            raw_feats["to_airport"] = flight.get("to_airport", None)

            rows_for_df.append(raw_feats)
        except Exception as e:
            print(f"Error building features for flight_id {flight['flight_id']}: {e}")

    if not rows_for_df:
        print("No valid flights to process.")
        return

    flights_df = pd.DataFrame(rows_for_df)

    # 5) Predict clusters
    # We'll do the scaling (MinMax + Standard) inside the matcher, so just pass flights_df:
    clusters = matcher.predict_clusters(flights_df[FEATURE_COLS], FEATURE_COLS)
    flights_df["cluster_id"] = clusters

    # (Optional) Write cluster_id back to Supabase "Flights"
    for idx, row in flights_df.iterrows():
        f_id = row["flight_id"]
        c_id = row["cluster_id"]
        supabase.table("Flights").update({"cluster_id": c_id}).eq("flight_id", f_id).execute()

    # 6) Form matches (optional example logic)
    # if you want to group flights by cluster, airport, date, etc.
    matched_pairs = matcher.form_matches(flights_df)

    # 7) Insert matches into "Matches" table (or however you store them)
    for pair in matched_pairs:
        data = {
            "user1_id": pair["user1"],
            "user2_id": pair["user2"],
            "airport": pair["airport"],
            "date": pair["date"],
            "cluster_id": pair["cluster_id"],
            # your schema might have a "ride_id" or so
        }
        resp = supabase.table("Matches").insert(data).execute()
        if resp.get("error"):
            print(f"Error creating match: {resp['error']['message']}")
        else:
            print(f"Match created: {data}")

if __name__ == "__main__":
    main()

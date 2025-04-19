# location_cache.py
import csv
import os

import googlemaps

# Define the name of the local cache CSV file
CACHE_FILE = "location_cache.csv"

class LocationCache:
    def __init__(self, google_api_key):
        # Initialize with a Google Maps API key and set up the client
        self.google_api_key = google_api_key
        self.gmaps = googlemaps.Client(key=google_api_key)
        
        # Initialize an empty dictionary to serve as the in-memory cache
        self.cache = {}
        
        # Load any existing cached data from the CSV file
        self._load_cache()

    def _load_cache(self):
        """
        Load location coordinates from the cache CSV file into the in-memory dictionary.
        This prevents redundant API calls for previously searched locations.
        """
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) == 3:
                        loc_name, lat, lng = row
                        self.cache[loc_name.lower()] = (float(lat), float(lng))

    def _save_cache(self):
        """
        Save the current in-memory cache dictionary to the CSV file.
        This persists the coordinates across sessions.
        """
        with open(CACHE_FILE, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for loc_name, coords in self.cache.items():
                lat, lng = coords
                writer.writerow([loc_name, lat, lng])

    def get_coordinates(self, location_name: str):
        """
        Retrieve coordinates (latitude, longitude) for a given location name.
        First checks the cache; if not found, fetches from the Google Maps API.
        """
        if not location_name:
            return None, None

        key = location_name.lower()
        
        # Check if the coordinates are already in the cache
        if key in self.cache:
            return self.cache[key]

        # If not cached, call the API to get coordinates
        lat, lng = self._fetch_from_google(location_name)
        
        # If valid coordinates are returned, update the cache and save to file
        if lat and lng:
            self.cache[key] = (lat, lng)
            self._save_cache()
        
        return lat, lng

    def _fetch_from_google(self, query):
        """
        Use the Google Geocoding API to fetch coordinates for a given location string.
        Returns a tuple (latitude, longitude) or (None, None) on failure.
        """
        try:
            result = self.gmaps.geocode(query)
            if result:
                location = result[0]["geometry"]["location"]
                return location["lat"], location["lng"]
        except Exception as e:
            print(f"Error fetching coordinates for {query}: {e}")
        return None, None

# location_cache.py
import os
import csv
import googlemaps

CACHE_FILE = "location_cache.csv"

class LocationCache:
    def __init__(self, google_api_key):
        self.google_api_key = google_api_key
        self.gmaps = googlemaps.Client(key=google_api_key)
        self.cache = {}
        self._load_cache()

    def _load_cache(self):
        """Load location cache from a local CSV file into self.cache dict."""
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) == 3:
                        loc_name, lat, lng = row
                        self.cache[loc_name.lower()] = (float(lat), float(lng))

    def _save_cache(self):
        """Save current self.cache dictionary to a CSV file."""
        with open(CACHE_FILE, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for loc_name, coords in self.cache.items():
                lat, lng = coords
                writer.writerow([loc_name, lat, lng])

    def get_coordinates(self, location_name: str):
        """Return lat/lng from cache or fetch from Google if not present."""
        if not location_name:
            return None, None

        key = location_name.lower()
        if key in self.cache:
            return self.cache[key]

        # if it's not in the cache call Google Maps Geocoding
        lat, lng = self._fetch_from_google(location_name)
        if lat and lng:
            self.cache[key] = (lat, lng)
            self._save_cache()
        return lat, lng

    def _fetch_from_google(self, query):
        """Call Google Geocoding API."""
        try:
            result = self.gmaps.geocode(query)
            if result:
                location = result[0]["geometry"]["location"]
                return location["lat"], location["lng"]
        except Exception as e:
            print(f"Error fetching coordinates for {query}: {e}")
        return None, None

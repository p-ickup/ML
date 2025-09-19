# This file is meant to be used when users input locations in order to prevent redundant API calls since there are a limited number of locations we can consider
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
        """Load location cache from a local CSV file into our self.cache dict
        """
        # if path exists try to read cache_file
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, mode='r', newline='', encoding='utf-8') as location_file:
                reader = csv.reader(location_file)
                for row in reader:
                    # TYPE, NAME, lat, lng
                    if len(row) == 4: 
                        location_type, loc_name, lat, lng = row
                        # Store as (type, lat, lng) tuple
                        self.cache[loc_name.lower()] = (location_type, float(lat), float(lng))
                            
    def _save_cache(self):
        """Save current self.cache dictionary to a CSV file to act as our cache.
        """
        with open(CACHE_FILE, mode='w', newline='', encoding='utf-8') as location_file:
            writer = csv.writer(location_file)
            for loc_name, cache_data in self.cache.items():
                location_type, lat, lng = cache_data
                writer.writerow([location_type, loc_name, lat, lng])
                
    def get_coordinates(self, location_name: str, location_type: str = None):
        """
        Retrieve coordinates (latitude, longitude) for a given location name.
        First checks the cache; if not found, fetches from the Google Maps API.
        
        Args:
            location_name: Name of the location (school, airport, etc.)
            location_type: Type of location (SCHOOL, AIRPORT, etc.) - optional
        """
        # prevent null values
        if not location_name:
            return None, None

        key = location_name.lower()
        
        # check if the coordinates are already in the cache
        if key in self.cache:
            # get just the lat, lng from the cache data (type, lat, lng)
            _, lat, lng = self.cache[key]
            return lat, lng

        # If not in cache, fetch from Google Maps API
        lat, lng = self._fetch_from_google(location_name)
        if lat and lng:
            # store with location type (use provided type or default to "LOCATION")
            cache_type = location_type.upper() if location_type else "LOCATION"
            self.cache[key] = (cache_type, lat, lng)
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
                
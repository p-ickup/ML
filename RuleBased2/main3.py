"""
Main entry point for pickup system v1 MVP
Streamlined version that directly handles ride creation and matching
"""

import os
from datetime import datetime, timedelta

from rider_data import RiderData
from supabase import Client, create_client

# Load environment variables for Supabase and Google Maps API keys
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client and location cache
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

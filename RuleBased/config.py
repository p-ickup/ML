# Configuration file for pickup system v0 MVP
# Single source of truth for knobs/policies

# Time & Spatial Bucket Sizes
TIME_SLOT_MIN = 15  # discretizes pickup/dropoff times into manageable buckets
GRID_RES = 9        # H3 resolution for spatial bucketing - creates ~174m hexagons
GRID_KRING = 1      # expands pickup search area by 1 hexagon ring around origin

# Spatial Constraints
MAX_PICKUP_SPREAD_M = 1200  # maximum pairwise distance among members in meters

# Weights for Matching Algorithm (v0 - simplified)
W_TIME = 1.0        # weight for time-based matching - prioritizes time compatibility
W_SPATIAL = 0.8     # weight for spatial proximity - prioritizes nearby pickups

# Policy Toggles
TERMINAL_MODE = "strict"  # terminal matching policy - strict enforces exact terminal matches only

# Read Horizon
READ_HORIZON_MIN = 90  # how far ahead to look for eligible flights in minutes

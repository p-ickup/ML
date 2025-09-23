# Configuration file for pickup system v1 MVP
# Single source of truth for knobs/policies

# Group Matching Constraints
NUM_BAGS = 10

# Policy Toggles
TERMINAL_MODE = "slack"  # terminal matching policy - strict enforces exact terminal matches only

# Read Horizon
READ_HORIZON_MIN = 90  # how far ahead to look for eligible flights in minutes

# Treat small gaps at window edges as acceptable overlap
OVERLAP_GRACE_MIN = 5   # adjust to taste (e.g., 3 or 5)
ALLOW_TOUCHING = True   # if True, 0-minute/touching windows are allowed (via grace)

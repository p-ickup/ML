# Configuration file for pickup system v1 MVP
# Single source of truth for knobs/policies

# Group Matching Constraints
MAX_TOTAL_BAGS = 8
MAX_LARGE_BAGS = 4

# Policy Toggles
TERMINAL_MODE = "slack"  # terminal matching policy - strict enforces exact terminal matches only

# Read Horizon
READ_HORIZON_MIN = 90  # how far ahead to look for eligible flights in minutes

# Treat small gaps at window edges as acceptable overlap
OVERLAP_GRACE_MIN = 10  # adjust to taste (e.g., 3 or 5)
ALLOW_TOUCHING = True   # if True, 0-minute/touching windows are allowed (via grace)

# Treat touching windows as at least this many minutes for scoring
TOUCH_FLOOR_MIN = 3       # e.g., give 3 "virtual" minutes for score if overlap is 0

# Local improvement pass (absorb leftovers into groups if it helps)
LOCAL_IMPROVE_PASSES = 1  # number of sweeps over leftovers
LOCAL_IMPROVE_TOL = 0.0   # require >= this improvement in group score to add a rider

# Enable optional split of a 4-person group into (3,2) if it rescues a leftover
SPLIT_4_TO_3_2 = True  # set False to disable

ABSORB_LEFTOVERS = True

# Prefer subsidized groups when making matching decisions
PREFER_SUBSIDIZED = False  # set True to prioritize subsidized groups

# Offset minutes applied when picking suggested match time
TO_AIRPORT_OFFSET_MIN = -10       # leave 10 min earlier (move backward)
FROM_AIRPORT_OFFSET_MIN = 10      # leave 10 min later (move forward)

COMPATIBLE_SCHOOLS = {
    "POMONA": ["POMONA"],   # Pomona matches only with Pomona (exclusive)
}

"""
Matching Strategies:
1. leftover_match — form new groups from leftovers (pairs → expand to 4).
2. absorb — insert leftovers into existing groups (fill 3→4, then 2→3).
3. split — split 4-person groups into (3 + 2) to rescue a leftover.

Default: MATCHING_STRATEGIES = ["leftover_match", "split", "absorb"]
"""

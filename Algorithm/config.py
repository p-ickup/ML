# Configuration file for pickup system v1 MVP
# Single source of truth for knobs/policies

# Group Matching Constraints
NUM_BAGS = 10

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

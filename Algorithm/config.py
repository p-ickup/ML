# Pickup matching — settings for python3 main.py
# Change values here; the matcher, Connect merge, subsidies, and vouchers all read this file.

# GROUP LIMITS
# Max bags and riders allowed in one matched Uber group (before Connect merge).
MAX_TOTAL_BAGS = 10          # max bag units per group (large bags count as LARGE_BAG_MULTIPLIER units)
MAX_LARGE_BAGS = 5             # max number of large bags per group
MAX_GROUP_SIZE = 5             # max riders per normal matched group (2–5 typical)
LARGE_BAG_MULTIPLIER = 2       # each large bag counts as this many units toward MAX_TOTAL_BAGS
PERSONAL_CONSTRAINT = False    # True = personal bags also count toward MAX_TOTAL_BAGS

# MATCHING BEHAVIOR
# How riders are paired into groups inside each bucket (same airport, direction, school).
TERMINAL_MODE = "slack"        # "slack" = different terminals OK | "strict" = same terminal only

OVERLAP_GRACE_MIN = 10         # minutes: small gap at window edge still counts as overlap
ALLOW_TOUCHING = True          # True = zero-minute overlap allowed (uses grace above)
TOUCH_FLOOR_MIN = 3            # minutes: score for groups that only touch (no real overlap)

SPLIT_4_TO_3_2 = True          # True = try splitting a 4-person group into 3+2 to rescue a leftover
ABSORB_LEFTOVERS = True         # True = try placing leftovers into existing groups when it helps

SAME_FLIGHT_PRIORITY = True    # True = prefer same flight (airline + number + date) even with weak overlap


# CONNECT SHUTTLES
# Large shared rides merged after normal matching. Empty list = Connect off that direction.
# CONNECT_SIZE = [min riders, max riders]; tier 2 is tried before tier 1 when forming groups.
CONNECT_DEPARTURE = ["LAX", "ONT"]   # school → airport (to_airport=True); e.g. ["LAX"] or []
CONNECT_ARRIVAL = ["LAX", "ONT"]     # airport → school (to_airport=False); e.g. [] to disable inbound

CONNECT_SIZE1 = [6, 12]              # smaller tier: groups of 6–12 riders
CONNECT_SIZE2 = [12, 24]             # larger tier: groups of 12–24 riders (preferred when possible)

# SCHOOLS
# Who can be matched together. Each school maps to schools it may ride with.
COMPATIBLE_SCHOOLS = {
    "POMONA": ["POMONA"],   # Pomona riders only match other Pomona riders
}

# SUBSIDY & VOUCHERS
# Minimum matched group size required for subsidy eligibility by airport.
SUBSIDY_MIN_GROUP_SIZE = {
    "LAX": 3,
    "ONT": 2,
}

# Which ride dates get Pomona subsidy and voucher assignment (MM-DD, any year).
COVERED_DATES_EXPLICIT = True   # True = only dates in the lists below are subsidized/voucher-eligible
COVERED_DATES_OUTBOUND = [       # school → airport (to_airport=True)
    "05-12", "05-13", "05-14", "05-15", "05-16", "05-17", "05-18", "05-19","10-09", "10-10", "10-11"
]
COVERED_DATES_INBOUND = [        # airport → school (to_airport=False)
    "03-20", "03-21", "03-22","06-29", "06-30", "07-01"
]

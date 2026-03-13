# buckets.py
from collections import defaultdict
from typing import Dict, List

import config
from rider_data import RiderLite


# build a readable bucket label from direction + airport
def _bucket_label(r: RiderLite, allowed_group: str) -> str:
    direction = "TO" if r.to_airport else "FROM"
    return f"{direction} {r.airport} | {allowed_group}"

def _school_group_for(r: RiderLite) -> str:
    """
    - If the rider's school has explicit restrictions in COMPATIBLE_SCHOOLS,
      use that compatibility list as the bucket group.
    - Otherwise, unrestricted schools are grouped into the universal "ALL" group.
    """

    school = (r.school or "").upper().strip()
    restricted_list = config.COMPATIBLE_SCHOOLS.get(school)

    if restricted_list is not None:
        # Restricted group → form a stable key like "POMONA" or "SCRIPPS-CMC-HMC"
        return "-".join(sorted(s.upper() for s in restricted_list))

    # Unrestricted → ALL schools match each other
    return "ALL"

# group riders by airport + direction
def make_buckets(riders: List[RiderLite]) -> Dict[str, List[RiderLite]]:
    buckets: Dict[str, List[RiderLite]] = defaultdict(list)
    for r in riders:
        group_key = _school_group_for(r)
        bucket_key = _bucket_label(r, group_key)
        buckets[bucket_key].append(r)

    return dict(buckets)

# optional: get a stable list of bucket names (sorted)
def bucket_names(buckets: Dict[str, List[RiderLite]]) -> List[str]:
    return sorted(buckets.keys())

# buckets.py
from collections import defaultdict
from typing import Dict, List

from rider_data import RiderLite


# build a readable bucket label from direction + airport
def _bucket_label(r: RiderLite) -> str:
    return f"{'TO' if r.to_airport else 'FROM'} {r.airport}"

# group riders by airport + direction
def make_buckets(riders: List[RiderLite]) -> Dict[str, List[RiderLite]]:
    buckets: Dict[str, List[RiderLite]] = defaultdict(list)
    for r in riders:
        key = _bucket_label(r)
        buckets[key].append(r)
    return dict(buckets)

# optional: get a stable list of bucket names (sorted)
def bucket_names(buckets: Dict[str, List[RiderLite]]) -> List[str]:
    return sorted(buckets.keys())

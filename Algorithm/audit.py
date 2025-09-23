# audit.py
# helpers to explain why riders didn't match

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import config as config
from rider_data import RiderLite


# parse a rider window
def _interval(r: RiderLite) -> Tuple[datetime, datetime]:
    s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
    e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
    return s, e

# bags for rider
def _bags_for(r: RiderLite) -> int:
    return int(r.bags_no or 0) + int(r.bags_no_large or 0)

# reason for blocking a PAIR (None => feasible)
def pair_block_reason(a: RiderLite, b: RiderLite) -> Optional[str]:
    a0, a1 = _interval(a); b0, b1 = _interval(b)
    if a1 <= b0 or b1 <= a0:
        return "no_time_overlap"
    if (_bags_for(a) + _bags_for(b)) > config.NUM_BAGS:
        return "bag_capacity"
    if config.TERMINAL_MODE == "strict":
        if (a.terminal or "") != (b.terminal or ""):
            return "terminal_mismatch"
    return None

# top reason from counters
def _top_reason(counts: Dict[str, int]) -> str:
    if not counts:
        return "unknown"
    priority = ["no_time_overlap", "terminal_mismatch", "bag_capacity", "dominated_by_better_pair", "singleton_bucket"]
    # pick max count; on tie, earlier in priority wins
    best_key = max(counts, key=lambda k: (counts[k], -(priority.index(k) if k in priority else 999)))
    return best_key

# build scored pairs + per-rider counters using provided score_group()
def build_scored_pairs_with_diag(
    riders: List[RiderLite],
    score_group,  # callable: (members: List[RiderLite]) -> float (float('-inf') if invalid)
) -> Tuple[List[Tuple[int,int,float]], Dict[int, Dict[str, int]]]:
    pairs: List[Tuple[int,int,float]] = []
    diag: Dict[int, Dict[str, int]] = {i: {"no_time_overlap": 0, "terminal_mismatch": 0, "bag_capacity": 0} for i in range(len(riders))}
    n = len(riders)
    for i in range(n):
        for j in range(i + 1, n):
            reason = pair_block_reason(riders[i], riders[j])
            if reason is None:
                score = score_group([riders[i], riders[j]])
                if score != float("-inf"):
                    pairs.append((i, j, score))
                else:
                    # conservative default
                    diag[i]["no_time_overlap"] += 1
                    diag[j]["no_time_overlap"] += 1
            else:
                diag[i][reason] += 1
                diag[j][reason] += 1
    pairs.sort(key=lambda t: t[2], reverse=True)
    return pairs, diag

# finalize diagnostics for leftover riders
def finalize_unmatched_diag(
    riders: List[RiderLite],
    bucket_key: Optional[str],
    pairs: List[Tuple[int,int,float]],
    pair_diag: Dict[int, Dict[str,int]],
    used_indices: set,
) -> Dict[int, Dict]:
    out: Dict[int, Dict] = {}
    # if nobody matched in this bucket and len==1, mark singleton
    if len(riders) == 1 and not used_indices:
        r = riders[0]
        out[r.flight_id] = {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}}
        return out

    # leftovers: dominated or intrinsic constraints
    for idx, r in enumerate(riders):
        if idx in used_indices:
            continue
        counts = dict(pair_diag.get(idx, {}))
        dominated = False
        for (i, j, _s) in pairs:
            if idx in (i, j):
                partner = j if idx == i else i
                if partner in used_indices:
                    dominated = True
                    break
        if dominated:
            counts["dominated_by_better_pair"] = counts.get("dominated_by_better_pair", 0) + 1
        reason = _top_reason(counts)
        out[r.flight_id] = {"bucket_key": bucket_key or "", "reason": reason, "details": counts}
    return out

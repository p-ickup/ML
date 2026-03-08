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
    return int(r.bags_no or 0) + int(r.bags_no_large or 0) + int(r.bag_no_personal or 0)

# constrained bags for rider (excludes personal if PERSONAL_CONSTRAINT is False)
# Large bags count as LARGE_BAG_MULTIPLIER in the total (but still count as 1 for MAX_LARGE_BAGS constraint)
def _bags_for_constrained(r: RiderLite) -> int:
    large_bags = int(r.bags_no_large or 0)
    normal_bags = int(r.bags_no or 0)
    total = (large_bags * config.LARGE_BAG_MULTIPLIER) + normal_bags
    if config.PERSONAL_CONSTRAINT:
        total += int(r.bag_no_personal or 0)
    return total

# reason for blocking a PAIR (None => feasible)
def pair_block_reason(a: RiderLite, b: RiderLite) -> Optional[str]:
    a0, a1 = _interval(a); b0, b1 = _interval(b)

    # overlap with grace
    latest_start = max(a0, b0)
    earliest_end = min(a1, b1)
    overlap_min = (earliest_end - latest_start).total_seconds() / 60.0

    # If overlap is negative but within grace, permit as touching
    if overlap_min < 0:
        if config.ALLOW_TOUCHING and (-overlap_min) <= config.OVERLAP_GRACE_MIN:
            pass  # allow as feasible
        else:
            return "no_time_overlap"

    # bag capacity for a pair (respects PERSONAL_CONSTRAINT setting)
    # Final pass can set FINAL_PASS_PAIR_BAG_LIMIT so pairs that form group-of-3 with 12 bags are allowed
    pair_limit = getattr(config, "FINAL_PASS_PAIR_BAG_LIMIT", None)
    if pair_limit is None:
        pair_limit = config.MAX_TOTAL_BAGS
    if (_bags_for_constrained(a) + _bags_for_constrained(b)) > pair_limit:
        return "bag_capacity"

    # terminal (strict)
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
    score_group,  # callable: (members: List[RiderLite]) -> float
) -> Tuple[List[Tuple[int,int,float]], Dict[int, Dict[str, int]], Dict[int, int]]:
    pairs: List[Tuple[int,int,float]] = []
    diag: Dict[int, Dict[str, int]] = {
        i: {"no_time_overlap": 0, "terminal_mismatch": 0, "bag_capacity": 0}
        for i in range(len(riders))
    }
    feasible_count: Dict[int, int] = {i: 0 for i in range(len(riders))}

    n = len(riders)
    for i in range(n):
        for j in range(i + 1, n):
            reason = pair_block_reason(riders[i], riders[j])
            if reason is None:
                score = score_group([riders[i], riders[j]])
                if score != float("-inf"):
                    pairs.append((i, j, score))
                    feasible_count[i] += 1
                    feasible_count[j] += 1
                else:
                    # conservative default when group-level validation rejects
                    diag[i]["no_time_overlap"] += 1
                    diag[j]["no_time_overlap"] += 1
            else:
                diag[i][reason] += 1
                diag[j][reason] += 1

    pairs.sort(key=lambda t: t[2], reverse=True)
    return pairs, diag, feasible_count

# finalize diagnostics for leftover riders
def finalize_unmatched_diag(
    riders: List[RiderLite],
    bucket_key: Optional[str],
    pairs: List[Tuple[int,int,float]],
    pair_diag: Dict[int, Dict[str,int]],
    feasible_count: Dict[int, int],
    used_indices: set,
) -> Dict[int, Dict]:
    out: Dict[int, Dict] = {}

    # singleton
    if len(riders) == 1 and not used_indices:
        r = riders[0]
        out[r.flight_id] = {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}}
        return out

    for idx, r in enumerate(riders):
        if idx in used_indices:
            continue

        if feasible_count.get(idx, 0) > 0:
            # had ≥1 feasible partner but lost them to a better group
            details = dict(pair_diag.get(idx, {}))
            details["dominated_by_better_pair"] = 1
            out[r.flight_id] = {
                "bucket_key": bucket_key or "",
                "reason": "dominated_by_better_pair",
                "details": details,
            }
        else:
            # zero feasible partners → choose the blocking reason that dominated
            counts = pair_diag.get(idx, {})
            reason = _top_reason(counts)
            out[r.flight_id] = {
                "bucket_key": bucket_key or "",
                "reason": reason,
                "details": counts,
            }

    return out

# matching.py
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import config3 as config
from rider_data import RiderLite


# shape returned to main for persistence / writing
@dataclass
class Match:
    riders: List[RiderLite]
    suggested_time_iso: str
    terminal: Optional[str]
    bucket_key: Optional[str] = None


# parse a rider's window into datetimes
def _interval(r: RiderLite) -> Tuple[datetime, datetime]:
    start = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
    end = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
    return start, end


# compute intersection [start,end) and its minutes for any group
def _intersection(members: List[RiderLite]) -> Tuple[datetime, datetime, int]:
    starts = []
    ends = []
    for m in members:
        s, e = _interval(m)
        starts.append(s)
        ends.append(e)
    start = max(starts)
    end = min(ends)
    minutes = int((end - start).total_seconds() // 60)
    return start, end, minutes


# count bags for a rider (None → 0)
def _bags_for(r: RiderLite) -> int:
    return int(r.bags_no or 0) + int(r.bags_no_large or 0)


# total bags in a group
def _bags_total(members: List[RiderLite]) -> int:
    return sum(_bags_for(m) for m in members)


# quick validity check (size, time overlap, bag capacity, terminal strict)
def _is_valid_group(members: List[RiderLite]) -> bool:
    if not (2 <= len(members) <= 4):
        return False
    _, _, minutes = _intersection(members)
    if minutes <= 0:
        return False
    if _bags_total(members) > config.NUM_BAGS:
        return False
    if config.TERMINAL_MODE == "strict":
        terms = [m.terminal or "" for m in members]
        if len(set(terms)) > 1:
            return False
    return True


# terminal mismatch penalty (used only in relaxed mode)
def _terminal_penalty(members: List[RiderLite]) -> float:
    terms = [m.terminal or "" for m in members]
    same = len(set(terms)) <= 1
    if config.TERMINAL_MODE == "strict":
        return 0.0
    return 0.0 if same else 0.5


# score any group: higher is better
def _score_group(members: List[RiderLite], validate: bool = False) -> float:
    if validate and not _is_valid_group(members):
        return float("-inf")

    # time_fit: prefer larger shared window (minutes)
    _, _, time_fit_min = _intersection(members)
    if time_fit_min <= 0:
        return float("-inf")

    # terminal penalty (relaxed mode only)
    tpen = _terminal_penalty(members)

    # optional: slight bonus for better bag utilization without exceeding capacity
    # fill in [0,1]; tiny weight to not dominate time overlap
    fill = min(_bags_total(members) / max(1, config.NUM_BAGS), 1.0)
    bag_bonus = 0.5 * fill

    # simple scalar score: time fit dominates, terminal mismatch penalized
    score = time_fit_min - (tpen * 1000.0) + bag_bonus
    return score


# build and score all feasible pairs within a bucket
def _build_scored_pairs(riders: List[RiderLite]) -> List[Tuple[int, int, float]]:
    pairs: List[Tuple[int, int, float]] = []
    n = len(riders)
    for i in range(n):
        for j in range(i + 1, n):
            score = _score_group([riders[i], riders[j]], validate=True)
            if score != float("-inf"):
                pairs.append((i, j, score))
    pairs.sort(key=lambda t: t[2], reverse=True)
    return pairs


# choose a maximal set of non-overlapping pairs by descending score
def _select_pairs(pairs: List[Tuple[int, int, float]]) -> List[Tuple[int, int]]:
    used = set()
    selected: List[Tuple[int, int]] = []
    for i, j, _ in pairs:
        if i in used or j in used:
            continue
        selected.append((i, j))
        used.add(i)
        used.add(j)
    return selected


# greedily expand a pair to up to 4 members (respecting validity and score)
def _expand_group(seed: List[RiderLite], candidates: List[RiderLite]) -> List[RiderLite]:
    group = list(seed)
    while len(group) < 4:
        best = None
        best_score = float("-inf")
        for c in candidates:
            if c in group:
                continue
            trial = group + [c]
            score = _score_group(trial, validate=True)
            if score > best_score:
                best_score = score
                best = c
        if best is None:
            break
        group.append(best)
    return group


# convert a group to a Match with suggested time at overlap midpoint
def _group_to_match(group: List[RiderLite], bucket_key: Optional[str] = None) -> Match:
    start, end, _ = _intersection(group)
    mid = start + (end - start) / 2
    terms = [g.terminal or "" for g in group]
    terminal = terms[0] if len(set(terms)) == 1 else None
    return Match(
        riders=group,
        suggested_time_iso=mid.replace(second=0, microsecond=0).isoformat(),
        terminal=terminal,
        bucket_key=bucket_key,
    )


# perform matching inside a single bucket
def match_bucket(riders: List[RiderLite], bucket_key: Optional[str] = None) -> Tuple[List[Match], List[RiderLite]]:
    if len(riders) < 2:
        return [], list(riders)

    pairs = _build_scored_pairs(riders)
    if not pairs:
        return [], list(riders)

    idx_pairs = _select_pairs(pairs)

    used = set()
    matches: List[Match] = []

    for i, j in idx_pairs:
        if i in used or j in used:
            continue
        base = [riders[i], riders[j]]
        group = _expand_group(base, riders)
        for g in group:
            used.add(riders.index(g))
        matches.append(_group_to_match(group, bucket_key=bucket_key))

    leftovers = [r for k, r in enumerate(riders) if k not in used]
    return matches, leftovers

# matching.py
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import audit
import config as config
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


def _effective_overlap_minutes(members: List[RiderLite]) -> int:
    # compute overlap across all members with grace
    starts, ends = [], []
    for m in members:
        s, e = _interval(m)
        starts.append(s); ends.append(e)
    latest_start = max(starts)
    earliest_end = min(ends)
    overlap_min = (earliest_end - latest_start).total_seconds() / 60.0
    if overlap_min >= 0:
        return int(overlap_min)  # actual overlap
    # negative overlap: allow within grace as zero-length effective overlap
    if config.ALLOW_TOUCHING and (-overlap_min) <= config.OVERLAP_GRACE_MIN:
        return 0
    return -1  # signals no feasible overlap even with grace


# count bags for a rider (None → 0)
def _bags_for(r: RiderLite) -> int:
    return int(r.bags_no or 0) + int(r.bags_no_large or 0) + int(r.bag_no_personal or 0)


# total bags in a group
def _bags_total(members: List[RiderLite]) -> int:
    return sum(_bags_for(m) for m in members)


# quick validity check (size, time overlap, bag capacity, terminal strict)
def _is_valid_group(members: List[RiderLite]) -> bool:
    if not (2 <= len(members) <= 4):
        return False

    eff_min = _effective_overlap_minutes(members)
    if eff_min < 0:  # truly no overlap even with grace
        return False

    if _bags_total(members) > config.NUM_BAGS:
        return False

    if config.TERMINAL_MODE == "strict":
        terms = [m.terminal or "" for m in members]
        if len(set(terms)) > 1:
            return False

    return True


def _best_absorb_slot(matches: List[Match], rider: RiderLite) -> Optional[Tuple[int, List[RiderLite], float]]:
    """
    For a single leftover rider, find the best existing group to absorb into.
    Returns (match_index, new_group_members, score_delta) or None if no valid insertion.
    """
    best: Optional[Tuple[int, List[RiderLite], float]] = None

    # Prefer filling 3->4 first, then 2->3, then 1->2 (rare), tie-break by best score gain
    size_order = [3, 2, 1]
    for target_size in size_order:
        for mi, M in enumerate(matches):
            group = M.riders
            if len(group) != target_size:
                continue

            trial = group + [rider]
            if not _is_valid_group(trial):
                continue

            # score delta = score(trial) - score(group)
            old_score = _score_group(group, validate=True)
            new_score = _score_group(trial, validate=True)
            delta = new_score - old_score

            if best is None or delta > best[2]:
                best = (mi, trial, delta)

        if best is not None:
            # we found a slot with the preferred size; no need to check smaller sizes
            break

    return best


def _third_pass_absorb_leftovers(
    matches: List[Match],
    leftovers: List[RiderLite],
    bucket_key: Optional[str] = None
) -> Tuple[List[Match], List[RiderLite]]:
    """
    Greedy absorption pass:
    - For each leftover L, try to insert into an existing group with len<4.
    - Prefer 3->4, then 2->3, picking the insertion with the highest score gain.
    - Recompute Match (suggested_time_iso/terminal) when a group changes.
    """
    if not leftovers or not matches:
        return matches, leftovers

    ms = list(matches)
    lo = list(leftovers)

    changed = True
    while changed:
        changed = False
        i = 0
        while i < len(lo):
            L = lo[i]
            best = _best_absorb_slot(ms, L)
            if best is None:
                i += 1
                continue

            mi, new_members, _ = best
            # rewrite the match with updated riders
            ms[mi] = _group_to_match(new_members, bucket_key=bucket_key)
            # remove L from leftovers
            lo.pop(i)
            changed = True
            # do NOT increment i, since we just removed current index
        # loop ends when no leftover can be absorbed in this sweep

    return ms, lo


def _split_full_group_for_leftovers(
    matches: List[Match],
    leftovers: List[RiderLite],
    bucket_key: Optional[str] = None
) -> Tuple[List[Match], List[RiderLite]]:
    """
    Try to split any 4-person group into a 3-person group + a 2-person group by pairing
    one member of the 4 with a leftover. We keep doing this greedily until no change.
    """
    if not leftovers:
        return matches, leftovers

    changed = True
    # Work on copies we can mutate
    ms = list(matches)
    lo = list(leftovers)

    while changed:
        changed = False

        # find first leftover that we can rescue
        li = None
        for idx_l, L in enumerate(lo):
            rescued = False

            # check every existing 4-person group
            for mi, M in enumerate(ms):
                if len(M.riders) != 4:
                    continue

                group4 = M.riders

                # try removing exactly one member X
                for x_idx in range(4):
                    X = group4[x_idx]
                    group3 = group4[:x_idx] + group4[x_idx+1:]

                    # both the remaining 3 and the pair (X, L) must be valid
                    if not _is_valid_group(group3):
                        continue
                    if not _is_valid_group([X, L]):
                        continue

                    # success: build new Match objects
                    match3 = _group_to_match(group3, bucket_key=bucket_key)
                    match2 = _group_to_match([X, L], bucket_key=bucket_key)

                    # replace the old 4-group with the 3-group and append the 2-group
                    ms[mi] = match3
                    ms.append(match2)

                    # remove the rescued leftover from leftovers
                    lo.pop(idx_l)

                    changed = True
                    rescued = True
                    break  # stop trying other X in this group

                if rescued:
                    break  # stop scanning more groups for this leftover

            if rescued:
                # we changed ms/lo; restart outer search
                break
        # loop continues if changed == True

    return ms, lo


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

    eff_min = _effective_overlap_minutes(members)
    if eff_min < 0:
        return float("-inf")

    # touching → give a tiny floor so they’re not always siphoned away
    time_fit_min = eff_min if eff_min > 0 else config.TOUCH_FLOOR_MIN

    tpen = _terminal_penalty(members)
    fill = min(_bags_total(members) / max(1, config.NUM_BAGS), 1.0)
    bag_bonus = 0.5 * fill

    return time_fit_min - (tpen * 1000.0) + bag_bonus



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


def _second_pass_leftovers(leftovers: List[RiderLite], bucket_key: Optional[str] = None) -> Tuple[List[Match], List[RiderLite]]:
    # try to form new groups from leftovers only
    if len(leftovers) < 2:
        return [], leftovers

    pairs = _build_scored_pairs(leftovers)
    if not pairs:
        return [], leftovers

    idx_pairs = _select_pairs(pairs)

    # map objects to indices once (robust + O(1))
    idx_map = {id(r): i for i, r in enumerate(leftovers)}

    used: set[int] = set()
    new_matches: List[Match] = []

    for i, j in idx_pairs:
        if i in used or j in used:
            continue
        base = [leftovers[i], leftovers[j]]
        group = _expand_group(base, leftovers)
        for g in group:
            used.add(idx_map[id(g)])
        new_matches.append(_group_to_match(group, bucket_key=bucket_key))

    remaining = [r for k, r in enumerate(leftovers) if k not in used]
    return new_matches, remaining


# convert a group to a Match with suggested time at overlap midpoint
def _group_to_match(group: List[RiderLite], bucket_key: Optional[str] = None) -> Match:
    starts, ends = [], []
    for g in group:
        s, e = _interval(g)
        starts.append(s); ends.append(e)
    latest_start = max(starts)
    earliest_end = min(ends)

    if earliest_end <= latest_start:
        # touching or no real overlap; place at the boundary start
        mid = latest_start
    else:
        mid = latest_start + (earliest_end - latest_start) / 2

    terms = [g.terminal or "" for g in group]
    terminal = terms[0] if len(set(terms)) == 1 else None
    return Match(
        riders=group,
        suggested_time_iso=mid.replace(second=0, microsecond=0).isoformat(),
        terminal=terminal,
        bucket_key=bucket_key,
    )


# perform matching inside a single bucket
def match_bucket(riders: List[RiderLite], bucket_key: Optional[str] = None) -> Tuple[List[Match], List[RiderLite], dict]:
    if len(riders) < 2:
        # singleton buckets
        diag = {r.flight_id: {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}} for r in riders}
        return [], list(riders), diag

    # NEW: stable identity → index map for this bucket
    idx_map = {id(r): i for i, r in enumerate(riders)}

    # build pairs + per-rider counters using audit
    pairs, pair_diag, feasible_count = audit.build_scored_pairs_with_diag(
        riders,
        score_group=lambda members: _score_group(members, validate=True),
    )
    if not pairs:
        diag = {r.flight_id: {"bucket_key": bucket_key or "", "reason": audit._top_reason(pair_diag.get(i, {})), "details": pair_diag.get(i, {})}
                for i, r in enumerate(riders)}
        return [], list(riders), diag

    idx_pairs = _select_pairs(pairs)
    used = set()
    matches: List[Match] = []
    for i, j in idx_pairs:
        if i in used or j in used:
            continue
        base = [riders[i], riders[j]]
        group = _expand_group(base, riders)
        for g in group:
            used.add(idx_map[id(g)])
        matches.append(_group_to_match(group, bucket_key=bucket_key))

    leftovers = [r for k, r in enumerate(riders) if k not in used]

    # SECOND PASS: try to form groups among leftovers only
    more_matches, leftovers = _second_pass_leftovers(leftovers, bucket_key=bucket_key)
    matches.extend(more_matches)
    
    # OPTIONAL THIRD STEP: split a 4 into (3,2) if it rescues a leftover
    if getattr(config, "SPLIT_4_TO_3_2", False):
        matches, leftovers = _split_full_group_for_leftovers(matches, leftovers, bucket_key=bucket_key)
        
    # OPTIONAL THIRD STEP B: absorb leftovers into existing groups (prefer 3->4, then 2->3)
    if getattr(config, "ABSORB_LEFTOVERS", True):  # default on
        matches, leftovers = _third_pass_absorb_leftovers(matches, leftovers, bucket_key=bucket_key)
        
    # compose diagnostics for leftovers
    diag = audit.finalize_unmatched_diag(riders, bucket_key, pairs, pair_diag, feasible_count, used)
    return matches, leftovers, diag
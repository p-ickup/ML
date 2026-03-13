# matching.py
import heapq
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
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
    group_voucher: Optional[str] = None   # ← NEW FIELD
    ride_type: Optional[str] = None       # e.g. "Connect" for LAX large groups


# parse a rider's window into datetimes
def _interval(r: RiderLite) -> Tuple[datetime, datetime]:
    start = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
    end   = datetime.fromisoformat(f"{r.date}T{r.latest_time}")

    # Handle overnight (cross-midnight) windows
    if end < start:
        end += timedelta(days=1)

    return start, end


# calculate time window duration in minutes for a rider
def _time_window_duration(r: RiderLite) -> int:
    """
    Calculate the duration of a rider's time window in minutes.
    Shorter duration = more constrained = should be matched first.
    """
    start, end = _interval(r)
    duration = (end - start).total_seconds() / 60.0
    return int(duration)



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


def _are_same_flight(members: List[RiderLite]) -> bool:
    """
    Check if all members are on the same flight (same airline_iata + flight_no + date).
    """
    if not members:
        return False
    
    # Get flight identifiers for all members
    flight_keys = []
    for m in members:
        if m.airline_iata and m.flight_no is not None:
            flight_keys.append((m.airline_iata.upper(), m.flight_no, m.date))
        elif m.flight_no is not None:
            # Fallback to just flight_no if airline_iata is missing
            flight_keys.append((None, m.flight_no, m.date))
        else:
            return False  # Can't determine if same flight without flight info
    
    # Check if all are the same
    return len(set(flight_keys)) == 1


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
    
    # If SAME_FLIGHT_PRIORITY is enabled and all members are on the same flight,
    # allow larger negative overlaps (up to a generous grace period)
    if config.SAME_FLIGHT_PRIORITY and _are_same_flight(members):
        # Allow up to 60 minutes of negative overlap for same-flight matches
        if (-overlap_min) <= 60:
            return 0  # Treat as zero overlap (touching)
    
    return -1  # signals no feasible overlap even with grace


# count bags for a rider (None → 0)
def _bags_for(r: RiderLite) -> Tuple[int, int, int]:
    large = int(r.bags_no_large or 0)
    normal = int(r.bags_no or 0)
    personal = int(r.bag_no_personal or 0)
    return large, normal, personal


def _bags_totals(members: List[RiderLite]) -> Tuple[int, int, int, int]:
    total_large = 0
    total_normal = 0
    total_personal = 0
    for m in members:
        L, N, P = _bags_for(m)
        total_large += L
        total_normal += N
        total_personal += P
    # Large bags count as LARGE_BAG_MULTIPLIER in total_all (but still count as 1 for MAX_LARGE_BAGS constraint)
    # If PERSONAL_CONSTRAINT is False, exclude personal bags from total_all
    if config.PERSONAL_CONSTRAINT:
        total_all = (total_large * config.LARGE_BAG_MULTIPLIER) + total_normal + total_personal
    else:
        total_all = (total_large * config.LARGE_BAG_MULTIPLIER) + total_normal
    return total_large, total_normal, total_personal, total_all


# When True, final-pass rules allow groups of 3 with max 12 bags (Uber XXL) and relaxed large-bag limit
_final_pass_group3_rules = False


# quick validity check (size, time overlap, bag capacity, terminal strict)
def _is_valid_group(members: List[RiderLite]) -> bool:
    if not (2 <= len(members) <= config.MAX_GROUP_SIZE):
        return False

    eff_min = _effective_overlap_minutes(members)
    if eff_min < 0:  # truly no overlap even with grace
        # Exception: if SAME_FLIGHT_PRIORITY is enabled and all members are on same flight,
        # allow the match even with poor overlap (effective_overlap_minutes will return 0 for same-flight)
        if config.SAME_FLIGHT_PRIORITY and _are_same_flight(members):
            pass  # Allow same-flight matches even with poor overlap
        else:
            return False

    # BAG CAPACITY RULES (configurable)
    total_large, total_normal, total_personal, total_all = _bags_totals(members)
    group_size = len(members)

    # Final pass: allow groups of 3 with max 12 bag units and up to 12 large bags (Uber XXL)
    if _final_pass_group3_rules and group_size == 3:
        if total_all > 12 or total_large > 12:
            return False
    else:
        # Check large bag limit
        if total_large > config.MAX_LARGE_BAGS:
            return False
        # Check total bag limit (groups of 5 must have < 8 bags, groups of 3 can have <= 12, others <= 10)
        if group_size == 5:
            if total_all >= 8:  # Groups of 5 must have <= 8 bags
                return False
        elif group_size == 3:
            if total_all > 12:  # Groups of 3 can have <= 12 bags
                return False
        else:
            if total_all > config.MAX_TOTAL_BAGS:  # Groups of 2 or 4 can have <= 10
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
    Checks that rider's flight_id is not already in any match.
    """
    # Build set of all flight_ids already matched
    matched_flight_ids = {r.flight_id for m in matches for r in m.riders}
    
    # If this rider's flight_id is already matched, cannot absorb
    if rider.flight_id in matched_flight_ids:
        return None
    
    best: Optional[Tuple[int, List[RiderLite], float]] = None

    # Prefer filling larger groups first (e.g., 4->5, then 3->4, then 2->3, then 1->2), tie-break by best score gain
    # Build size order dynamically based on MAX_GROUP_SIZE (largest first, then smaller)
    max_size = config.MAX_GROUP_SIZE
    size_order = list(range(max_size - 1, 0, -1))  # e.g., if MAX_GROUP_SIZE=5: [4, 3, 2, 1]
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
    - For each leftover L, try to insert into an existing group with len < MAX_GROUP_SIZE.
    - Prefer larger groups first (e.g., 4->5, then 3->4, then 2->3), picking the insertion with the highest score gain.
    - Recompute Match (suggested_time_iso/terminal) when a group changes.
    """
    if not leftovers or not matches:
        return matches, leftovers

    # Sort leftovers by time constraint (most constrained first)
    # This ensures we prioritize matching the hardest-to-match riders first
    leftovers = sorted(leftovers, key=_time_window_duration)

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
    Ensures no flight_id appears in multiple matches.
    """
    if not leftovers:
        return matches, leftovers

    # Sort leftovers by time constraint (most constrained first)
    # This ensures we prioritize matching the hardest-to-match riders first
    leftovers = sorted(leftovers, key=_time_window_duration)

    # Build set of all flight_ids already matched
    matched_flight_ids = {r.flight_id for m in matches for r in m.riders}

    changed = True
    # Work on copies we can mutate
    ms = list(matches)
    lo = list(leftovers)

    while changed:
        changed = False

        # find first leftover that we can rescue
        li = None
        for idx_l, L in enumerate(lo):
            # Skip if this leftover's flight_id is already matched
            if L.flight_id in matched_flight_ids:
                continue
                
            rescued = False

            # check every existing MAX_GROUP_SIZE-person group
            for mi, M in enumerate(ms):
                if len(M.riders) != config.MAX_GROUP_SIZE:
                    continue

                group4 = M.riders

                # try removing exactly one member X
                for x_idx in range(config.MAX_GROUP_SIZE):
                    X = group4[x_idx]
                    group3 = group4[:x_idx] + group4[x_idx+1:]

                    # both the remaining 3 and the pair (X, L) must be valid
                    if not _is_valid_group(group3):
                        continue
                    if not _is_valid_group([X, L]):
                        continue
                    
                    # Safety check: ensure L's flight_id isn't already matched
                    if L.flight_id in matched_flight_ids:
                        continue

                    # success: build new Match objects
                    match3 = _group_to_match(group3, bucket_key=bucket_key)
                    match2 = _group_to_match([X, L], bucket_key=bucket_key)

                    # replace the old 4-group with the 3-group and append the 2-group
                    ms[mi] = match3
                    ms.append(match2)
                    
                    # Update matched_flight_ids: L is now matched
                    matched_flight_ids.add(L.flight_id)

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
    
    # If SAME_FLIGHT_PRIORITY is enabled and all members are on same flight,
    # allow scoring even with negative overlap (it will be treated as 0)
    is_same_flight = config.SAME_FLIGHT_PRIORITY and _are_same_flight(members)
    
    if eff_min < 0 and not is_same_flight:
        return float("-inf")

    # touching → give a tiny floor so they're not always siphoned away
    # For same-flight matches with poor overlap, use floor value
    if is_same_flight and eff_min < 0:
        time_fit_min = config.TOUCH_FLOOR_MIN
    else:
        time_fit_min = eff_min if eff_min > 0 else config.TOUCH_FLOOR_MIN

    tpen = _terminal_penalty(members)
    _, _, _, total_all = _bags_totals(members)

    # reward groups that use bag capacity well (scaled to MAX_TOTAL_BAGS)
    fill = min(total_all / config.MAX_TOTAL_BAGS, 1.0)
    bag_bonus = 0.5 * fill
    
    # Bonus for groups with riders on the same flight (airline_iata + flight_no + date)
    # Create flight identifiers by combining airline_iata and flight_no
    flight_ids = []
    for m in members:
        if m.airline_iata and m.flight_no is not None:
            flight_ids.append(f"{m.airline_iata}{m.flight_no}")
        elif m.flight_no is not None:
            # Fallback to just flight_no if airline_iata is missing
            flight_ids.append(str(m.flight_no))
    
    if flight_ids:
        flight_counts = Counter(flight_ids)
        max_same_flight = max(flight_counts.values())
        
        # Check if all members are on the same flight (same airline_iata + flight_no + date)
        is_same_flight = _are_same_flight(members)
        
        if config.SAME_FLIGHT_PRIORITY and is_same_flight:
            # Massive bonus for same-flight matches when priority mode is enabled
            # This ensures same-flight matches are prioritized even with poor time overlap
            # Bonus scales with group size: 2 riders = 1000, 3 riders = 2000, 4 riders = 3000, etc.
            same_flight_bonus = (max_same_flight - 1) * 1000.0 if max_same_flight > 1 else 0.0
        else:
            # Normal bonus: 2 points per rider on the same flight (e.g., 2 riders = 2 points, 3 riders = 4 points, 4 riders = 6 points)
            # This encourages matching people on the same flight
            same_flight_bonus = (max_same_flight - 1) * 2.0 if max_same_flight > 1 else 0.0
    else:
        same_flight_bonus = 0.0

    return time_fit_min - (tpen * 1000.0) + bag_bonus + same_flight_bonus



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


# greedily expand a pair to up to MAX_GROUP_SIZE members (respecting validity and score)
def _expand_group(seed: List[RiderLite], candidates: List[RiderLite], matched_flight_ids: Optional[set] = None) -> List[RiderLite]:
    if matched_flight_ids is None:
        matched_flight_ids = set()
    # Track flight_ids already in the seed group
    seed_flight_ids = {r.flight_id for r in seed}
    group = list(seed)
    while len(group) < config.MAX_GROUP_SIZE:
        best = None
        best_score = float("-inf")
        for c in candidates:
            # Skip if already in group (by object identity)
            if c in group:
                continue
            # Skip if flight_id already matched elsewhere
            if c.flight_id in matched_flight_ids:
                continue
            # Skip if flight_id already in this group
            if c.flight_id in seed_flight_ids:
                continue
            trial = group + [c]
            score = _score_group(trial, validate=True)
            if score > best_score:
                best_score = score
                best = c
        if best is None:
            break
        group.append(best)
        seed_flight_ids.add(best.flight_id)
    return group


def _second_pass_leftovers(leftovers: List[RiderLite], bucket_key: Optional[str] = None, matched_flight_ids: Optional[set] = None) -> Tuple[List[Match], List[RiderLite]]:
    # try to form new groups from leftovers only
    if len(leftovers) < 2:
        return [], leftovers
    
    if matched_flight_ids is None:
        matched_flight_ids = set()
    
    # Sort leftovers by time constraint (most constrained first)
    leftovers = sorted(leftovers, key=_time_window_duration)

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
        # Check if either rider's flight_id is already matched
        if leftovers[i].flight_id in matched_flight_ids or leftovers[j].flight_id in matched_flight_ids:
            continue
        base = [leftovers[i], leftovers[j]]
        group = _expand_group(base, leftovers, matched_flight_ids=matched_flight_ids)
        for g in group:
            used.add(idx_map[id(g)])
            matched_flight_ids.add(g.flight_id)
        new_matches.append(_group_to_match(group, bucket_key=bucket_key))

    remaining = [r for k, r in enumerate(leftovers) if k not in used]
    return new_matches, remaining

def _promote_lax_twos(matches: List[Match], bucket_key: Optional[str]) -> List[Match]:
    """
    LAX rule:
    For any 2-person group in a LAX bucket:
      - If there are other matches in the SAME bucket/date,
      - Try to upgrade the 2-group to a 3-group by stealing exactly one rider
      from another matched group (donor group),
      - Only perform swaps where BOTH resulting groups remain valid.
      - Ensures no flight_id appears in multiple matches.

    This runs AFTER all normal matching passes, without changing any core logic.
    """
    if not bucket_key or "LAX" not in bucket_key:
        return matches  # only modify LAX buckets

    # Group matches by date so promotions only occur within same date
    matches_by_date: Dict[str, List[Match]] = {}
    for m in matches:
        d = m.riders[0].date
        matches_by_date.setdefault(d, []).append(m)

    # Work on the existing objects (Match objects are mutable)
    for date, ms in matches_by_date.items():
        # all 2-person matches
        twos = [m for m in ms if len(m.riders) == 2]

        # need at least 2 matches total to perform donor stealing
        if len(ms) <= 1:
            continue

        for m2 in twos:
            A, B = m2.riders  # the two riders to promote
            # Build set of flight_ids already in other matches (not m2 or donor)
            other_flight_ids = {r.flight_id for m in ms if m is not m2 for r in m.riders}

            # search donor groups
            for donor in ms:
                if donor is m2:
                    continue  # cannot steal from itself

                # try each rider in donor group
                for X in list(donor.riders):
                    # Check: X's flight_id must not be in A or B (shouldn't happen, but safety check)
                    if X.flight_id == A.flight_id or X.flight_id == B.flight_id:
                        continue
                    
                    new_big = [A, B, X]
                    new_donor = [r for r in donor.riders if r is not X]

                    # enforce group sizes 2–MAX_GROUP_SIZE
                    if not (2 <= len(new_donor) <= config.MAX_GROUP_SIZE):
                        continue

                    # both groups must remain valid
                    if not _is_valid_group(new_big):
                        continue
                    if not _is_valid_group(new_donor):
                        continue
                    
                    # Check: after swap, no flight_id should appear in multiple matches
                    # X moves from donor to m2, so we need to check if X.flight_id conflicts
                    # Since X is moving from donor to m2, and we're removing X from donor,
                    # the only potential conflict is if X.flight_id is already in another match
                    # But we're only moving within the same date's matches, so this should be safe
                    # However, let's be explicit: X should not be in any other match
                    x_in_other = any(X.flight_id in {r.flight_id for r in m.riders} 
                                    for m in ms if m is not m2 and m is not donor)
                    if x_in_other:
                        continue  # X is already in another match, skip

                    # --- APPLY THE SWAP ---

                    # rewrite the upgraded group
                    rebuilt_big = _group_to_match(new_big, bucket_key)
                    m2.riders = rebuilt_big.riders
                    m2.suggested_time_iso = rebuilt_big.suggested_time_iso
                    m2.terminal = rebuilt_big.terminal

                    # rewrite donor group
                    rebuilt_donor = _group_to_match(new_donor, bucket_key)
                    donor.riders = rebuilt_donor.riders
                    donor.suggested_time_iso = rebuilt_donor.suggested_time_iso
                    donor.terminal = rebuilt_donor.terminal

                    # only perform ONE successful upgrade per 2-group
                    break

                else:
                    # donor group had no valid donor X → try next donor group
                    continue

                # break after successful swap
                break

    return matches


def _combine_pairs_into_fours(matches: List[Match], bucket_key: Optional[str] = None) -> List[Match]:
    """
    Try to combine pairs of 2-person matches into 4-person matches.
    Returns updated matches list with combined pairs where possible.
    """
    if not matches:
        return matches
    
    # Find all 2-person matches
    twos = [m for m in matches if len(m.riders) == 2]
    if len(twos) < 2:
        return matches  # Need at least 2 pairs to combine
    
    # Build set of all flight_ids already in non-2-person matches
    other_flight_ids = {r.flight_id for m in matches if len(m.riders) != 2 for r in m.riders}
    
    # Try to combine pairs
    combined_indices = set()
    new_matches = []
    combined_pairs = []
    
    for i, m1 in enumerate(twos):
        if i in combined_indices:
            continue
        
        A, B = m1.riders
        # Check if already in another match
        if A.flight_id in other_flight_ids or B.flight_id in other_flight_ids:
            continue
        
        best_match = None
        best_score = float("-inf")
        best_idx = None
        
        for j, m2 in enumerate(twos):
            if j <= i or j in combined_indices:
                continue
            
            C, D = m2.riders
            # Check if already in another match
            if C.flight_id in other_flight_ids or D.flight_id in other_flight_ids:
                continue
            
            # Try combining the two pairs
            combined_group = [A, B, C, D]
            if not _is_valid_group(combined_group):
                continue
            
            # Score the combined group
            combined_score = _score_group(combined_group, validate=True)
            if combined_score > best_score:
                best_score = combined_score
                best_match = m2
                best_idx = j
        
        if best_match is not None:
            # Combine the two pairs
            combined_group = [A, B] + best_match.riders
            combined_match = _group_to_match(combined_group, bucket_key=bucket_key)
            combined_pairs.append(combined_match)
            combined_indices.add(i)
            combined_indices.add(best_idx)
    
    # Build result: keep non-2-person matches, keep uncombined 2-person matches, add combined pairs
    result = [m for m in matches if len(m.riders) != 2]  # Keep non-2-person matches
    result.extend([twos[i] for i in range(len(twos)) if i not in combined_indices])  # Keep uncombined pairs
    result.extend(combined_pairs)  # Add combined pairs
    
    if combined_pairs:
        print(f"  Combined {len(combined_pairs)} pairs of 2-person matches into {len(combined_pairs)} 4-person matches")
    
    return result


def _lax_optimize_4_and_2(matches: List[Match], bucket_key: Optional[str] = None) -> List[Match]:
    """
    For LAX/ONT buckets: Group by date, then if there's a group of 4 and a group of 2 with overlapping times,
    try removing one person from the group of 4 and adding them to the group of 2.
    This creates: 4-person + 2-person → 3-person + 3-person
    
    Returns updated matches list.
    """
    if not bucket_key or ("LAX" not in bucket_key and "ONT" not in bucket_key):
        return matches
    
    if not matches:
        return matches
    
    # Group matches by date for efficiency
    matches_by_date: Dict[str, List[Match]] = {}
    for m in matches:
        if not m.riders:
            continue
        d = m.riders[0].date
        matches_by_date.setdefault(d, []).append(m)
    
    optimized_matches = []
    total_swaps = 0
    
    for date, ms in matches_by_date.items():
        # Find 2-person and MAX_GROUP_SIZE-person groups for this date
        twos = [m for m in ms if len(m.riders) == 2]
        fours = [m for m in ms if len(m.riders) == config.MAX_GROUP_SIZE]
        others = [m for m in ms if len(m.riders) not in [2, config.MAX_GROUP_SIZE]]  # Keep other groups as-is
        
        if not twos or not fours:
            # No optimization possible for this date, keep all matches as-is
            optimized_matches.extend(ms)
            continue
        
        modified_twos = set()
        modified_fours = set()
        new_groups = []
        
        for two_idx, m2 in enumerate(twos):
            if two_idx in modified_twos:
                continue
            
            A, B = m2.riders
            
            # Compute overlap window for the 2-person group
            m2_start_a, m2_end_a = _interval(A)
            m2_start_b, m2_end_b = _interval(B)
            m2_start = max(m2_start_a, m2_start_b)  # Latest start (overlap start)
            m2_end = min(m2_end_a, m2_end_b)  # Earliest end (overlap end)
            
            # If no overlap in the 2-person group itself, skip
            if m2_end < m2_start:
                continue
            
            best_swap = None
            best_four_idx = None
            best_score_improvement = float("-inf")
            
            for four_idx, m4 in enumerate(fours):
                if four_idx in modified_fours:
                    continue
                
                # Compute overlap window for the 4-person group
                m4_starts = []
                m4_ends = []
                for r in m4.riders:
                    s, e = _interval(r)
                    m4_starts.append(s)
                    m4_ends.append(e)
                m4_start = max(m4_starts)  # Latest start (overlap start)
                m4_end = min(m4_ends)  # Earliest end (overlap end)
                
                # If no overlap in the 4-person group itself, skip
                if m4_end < m4_start:
                    continue
                
                # Check if the two groups' overlap windows intersect
                if m2_end < m4_start or m4_end < m2_start:
                    continue  # No time overlap between groups
                
                # Try each rider in the 4-person group
                for X in m4.riders:
                    # Create new groups: 2-person + X = 3-person, 4-person - X = 3-person
                    new_group_of_3_from_2 = [A, B, X]
                    remaining_from_4 = [r for r in m4.riders if r is not X]
                    
                    # Both must be valid 3-person groups
                    if not _is_valid_group(new_group_of_3_from_2):
                        continue
                    if not _is_valid_group(remaining_from_4):
                        continue
                    
                    # Calculate score improvement
                    old_score_2 = _score_group(m2.riders, validate=True)
                    old_score_4 = _score_group(m4.riders, validate=True)
                    new_score_3a = _score_group(new_group_of_3_from_2, validate=True)
                    new_score_3b = _score_group(remaining_from_4, validate=True)
                    
                    total_old_score = old_score_2 + old_score_4
                    total_new_score = new_score_3a + new_score_3b
                    score_improvement = total_new_score - total_old_score
                    
                    # Only consider if it improves the score
                    if score_improvement > best_score_improvement:
                        best_score_improvement = score_improvement
                        best_swap = (new_group_of_3_from_2, remaining_from_4)
                        best_four_idx = four_idx
            
            if best_swap is not None and best_score_improvement > 0:
                # Apply the swap
                new_group_3a, new_group_3b = best_swap
                
                # Create new match objects
                match_3a = _group_to_match(new_group_3a, bucket_key=bucket_key)
                match_3b = _group_to_match(new_group_3b, bucket_key=bucket_key)
                
                new_groups.append(match_3a)
                new_groups.append(match_3b)
                
                modified_twos.add(two_idx)
                modified_fours.add(best_four_idx)
                total_swaps += 1
        
        # Add unmodified 2-person and 4-person groups
        for i, m2 in enumerate(twos):
            if i not in modified_twos:
                optimized_matches.append(m2)
        
        for i, m4 in enumerate(fours):
            if i not in modified_fours:
                optimized_matches.append(m4)
        
        # Add the new 3-person groups
        optimized_matches.extend(new_groups)
        
        # Add other groups (3-person groups that weren't modified)
        optimized_matches.extend(others)
    
    if total_swaps > 0:
        airport = "LAX/ONT" if "LAX" in bucket_key or "ONT" in bucket_key else ""
        print(f"  {airport} optimization: converted {total_swaps} pairs of (4+2) groups into {total_swaps * 2} 3-person groups")
    
    return optimized_matches


# convert a group to a Match with suggested time at overlap midpoint
def _group_to_match(group: List[RiderLite], bucket_key: Optional[str] = None) -> Match:
    # Compute overlap window
    starts, ends = [], []
    for g in group:
        s, e = _interval(g)
        starts.append(s)
        ends.append(e)

    latest_start = max(starts)
    earliest_end = min(ends)

    # All riders in a group ALWAYS share same direction (bucket guarantee)
    to_airport = group[0].to_airport

    # Time assignment based on direction
    # TO airport: 15 minutes before the latest time of the overlap (if possible), otherwise the latest time
    # FROM airport: 15 minutes after the earliest time of the overlap
    if earliest_end <= latest_start:
        # touching window fallback
        chosen = latest_start
    else:
        if to_airport:
            # going TO airport → 15 min before latest time (earliest_end); if that's before overlap start, use latest time
            chosen = earliest_end - timedelta(minutes=15)
            if chosen < latest_start:
                chosen = earliest_end
        else:
            # from airport → 15 min after earliest overlap (latest_start)
            chosen = latest_start + timedelta(minutes=15)
            if chosen > earliest_end:
                chosen = earliest_end

    # Normalize seconds
    chosen = chosen.replace(second=0, microsecond=0)

    # Terminal handling (strict mode)
    terms = [g.terminal or "" for g in group]
    terminal = terms[0] if len(set(terms)) == 1 else None

    return Match(
        riders=group,
        suggested_time_iso=chosen.isoformat(),
        terminal=terminal,
        bucket_key=bucket_key,
    )


def _ont_post_process_unmatched(
    matches: List[Match],
    unmatched: List[RiderLite],
    voucher_csv_path: Optional[str] = None,
    dry_run: bool = False,
    sb: Optional[object] = None
) -> Tuple[List[Match], List[RiderLite]]:
    """
    Post-processing for ONT unmatched individuals:
    After all matching is complete, for unmatched individuals on a given day,
    check existing matches (rides) on that same day where time overlap is possible.
    Try to move one person from a group of 4 to be paired with an unmatched individual
    to create a group of 2 (ONT only).
    
    Returns updated matches and remaining unmatched.
    """
    if not unmatched:
        return matches, unmatched
    
    # Filter unmatched to only ONT riders
    ont_unmatched = [u for u in unmatched if (u.airport or "").upper() == "ONT"]
    if not ont_unmatched:
        return matches, unmatched
    
    # Helper function to check if two time windows overlap (quick check)
    def _time_windows_overlap(rider1: RiderLite, rider2: RiderLite) -> bool:
        """Quick check if two riders' time windows overlap."""
        s1 = datetime.fromisoformat(f"{rider1.date}T{rider1.earliest_time}")
        e1 = datetime.fromisoformat(f"{rider1.date}T{rider1.latest_time}")
        if e1 < s1:
            e1 += timedelta(days=1)
        
        s2 = datetime.fromisoformat(f"{rider2.date}T{rider2.earliest_time}")
        e2 = datetime.fromisoformat(f"{rider2.date}T{rider2.latest_time}")
        if e2 < s2:
            e2 += timedelta(days=1)
        
        # Overlap exists if latest_start < earliest_end
        latest_start = max(s1, s2)
        earliest_end = min(e1, e2)
        return latest_start < earliest_end or (config.ALLOW_TOUCHING and latest_start == earliest_end)
    
    # Helper function to compute group time window
    def _group_time_window(riders: List[RiderLite]) -> Tuple[datetime, datetime]:
        """Compute the time window for a group (earliest start, latest end)."""
        starts, ends = [], []
        for r in riders:
            s = datetime.fromisoformat(f"{r.date}T{r.earliest_time}")
            e = datetime.fromisoformat(f"{r.date}T{r.latest_time}")
            if e < s:
                e += timedelta(days=1)
            starts.append(s)
            ends.append(e)
        return max(starts), min(ends)
    
    # Group unmatched by date
    unmatched_by_date: Dict[str, List[RiderLite]] = {}
    for u in ont_unmatched:
        unmatched_by_date.setdefault(u.date, []).append(u)
    
    # Group matches by date and filter for ONT groups of 4
    # Pre-compute time windows for each group of 4 for efficient filtering
    matches_by_date: Dict[str, List[Tuple[Match, Tuple[datetime, datetime]]]] = {}
    for m in matches:
        if not m.riders:
            continue
        # Only process ONT matches
        if (m.riders[0].airport or "").upper() != "ONT":
            continue
        # Only groups of 4
        if len(m.riders) != 4:
            continue
        date = m.riders[0].date
        group_window = _group_time_window(m.riders)
        matches_by_date.setdefault(date, []).append((m, group_window))
    
    # Work on copies
    ms = list(matches)
    remaining_unmatched = list(unmatched)
    matched_flight_ids = {r.flight_id for m in ms for r in m.riders}
    
    # Track newly created matches for voucher assignment
    newly_created_matches: List[Match] = []
    
    changed = True
    while changed:
        changed = False
        
        # Rebuild matches_by_date on each iteration (in case matches changed)
        matches_by_date: Dict[str, List[Tuple[Match, Tuple[datetime, datetime]]]] = {}
        for m in ms:
            if not m.riders:
                continue
            # Only process ONT matches
            if (m.riders[0].airport or "").upper() != "ONT":
                continue
            # Only groups of 4
            if len(m.riders) != 4:
                continue
            date = m.riders[0].date
            group_window = _group_time_window(m.riders)
            matches_by_date.setdefault(date, []).append((m, group_window))
        
        # Rebuild unmatched_by_date (filter to only ONT and not yet matched)
        unmatched_by_date: Dict[str, List[RiderLite]] = {}
        for u in remaining_unmatched:
            if (u.airport or "").upper() != "ONT":
                continue
            if u.flight_id in matched_flight_ids:
                continue
            unmatched_by_date.setdefault(u.date, []).append(u)
        
        # Process each date
        for date, unmatched_riders in unmatched_by_date.items():
            if date not in matches_by_date:
                continue
            
            date_matches_with_windows = matches_by_date[date]
            
            # Try each unmatched rider
            for unmatched_rider in list(unmatched_riders):  # Use list() to avoid modification during iteration
                if unmatched_rider.flight_id in matched_flight_ids:
                    continue
                
                # Pre-compute unmatched rider's time window
                u_start = datetime.fromisoformat(f"{unmatched_rider.date}T{unmatched_rider.earliest_time}")
                u_end = datetime.fromisoformat(f"{unmatched_rider.date}T{unmatched_rider.latest_time}")
                if u_end < u_start:
                    u_end += timedelta(days=1)
                
                # Try each group of 4 on this date
                for match_4, (group_start, group_end) in date_matches_with_windows:
                    if len(match_4.riders) != 4:
                        continue
                    
                    # Quick overlap check: does unmatched rider's window overlap with group's window?
                    latest_start = max(u_start, group_start)
                    earliest_end = min(u_end, group_end)
                    if latest_start >= earliest_end and not (config.ALLOW_TOUCHING and latest_start == earliest_end):
                        continue  # No overlap, skip this group
                    
                    # Try removing each person from the group of 4
                    for x_idx, X in enumerate(match_4.riders):
                        # Quick check: does X's window overlap with unmatched rider's window?
                        if not _time_windows_overlap(X, unmatched_rider):
                            continue  # No overlap, skip this person
                        
                        # Create new groups: 3-person (remaining) and 2-person (X + unmatched)
                        group_3 = [r for i, r in enumerate(match_4.riders) if i != x_idx]
                        group_2 = [X, unmatched_rider]
                        
                        # Check time overlap for the new 2-person group (detailed check)
                        eff_overlap = _effective_overlap_minutes(group_2)
                        if eff_overlap < 0:
                            continue  # No time overlap
                        
                        # Both groups must be valid
                        if not _is_valid_group(group_3):
                            continue
                        if not _is_valid_group(group_2):
                            continue
                        
                        # Safety check: ensure unmatched_rider isn't already matched elsewhere
                        # Note: X.flight_id is already in matched_flight_ids (from match_4), 
                        # but that's fine since we're replacing match_4 with match_3 and adding match_2
                        if unmatched_rider.flight_id in matched_flight_ids:
                            continue
                        
                        # Success: create new matches
                        match_3 = _group_to_match(group_3, bucket_key=match_4.bucket_key)
                        match_2 = _group_to_match(group_2, bucket_key=match_4.bucket_key)
                        
                        # Recalculate group_subsidy for both new matches
                        thresholds = {"LAX": 3, "ONT": 2}
                        for new_match in [match_3, match_2]:
                            if not new_match.riders:
                                new_match.group_subsidy = False
                                continue
                            
                            group_airport = (new_match.riders[0].airport or "").strip().upper()
                            need = thresholds.get(group_airport)
                            group_size = len(new_match.riders)
                            all_pomona = all((r.school or "").strip().upper() == "POMONA"
                                           for r in new_match.riders)
                            
                            qualifies = (
                                need is not None and
                                group_size >= need and
                                all_pomona
                            )
                            new_match.group_subsidy = qualifies
                            
                            # Set subsidized on riders to match the group
                            for r in new_match.riders:
                                r.subsidized = qualifies
                            
                            # Clear old vouchers - will be reassigned below
                            new_match.group_voucher = None
                            for r in new_match.riders:
                                r.group_voucher = None
                                r.contingency_voucher = None
                        
                        # Replace the 4-person group with the 3-person group in ms
                        try:
                            ms_idx = ms.index(match_4)
                            ms[ms_idx] = match_3
                        except ValueError:
                            # Match not found (shouldn't happen, but handle gracefully)
                            continue
                        
                        ms.append(match_2)
                        
                        # Track newly created matches for voucher assignment
                        newly_created_matches.append(match_3)
                        newly_created_matches.append(match_2)
                        
                        # Update matched_flight_ids
                        matched_flight_ids.add(unmatched_rider.flight_id)
                        matched_flight_ids.add(X.flight_id)
                        
                        # Remove from remaining_unmatched
                        if unmatched_rider in remaining_unmatched:
                            remaining_unmatched.remove(unmatched_rider)
                        
                        changed = True
                        break  # Stop trying other X in this group
                    
                    if changed:
                        break  # Stop trying other groups for this unmatched rider
                
                if changed:
                    break  # Restart outer loop
        
        # If no changes, exit loop
        if not changed:
            break
    
    # Assign vouchers to newly created matches
    if newly_created_matches and voucher_csv_path:
        try:
            # Import here to avoid circular imports
            import shutil
            from pathlib import Path

            import vouchers

            # Load voucher pool (same logic as assign_vouchers)
            # IMPORTANT: For dry-run, reuse the same temp file from assign_vouchers to preserve USED flags
            temp_file_path: Optional[str] = None
            try:
                if dry_run:
                    # Dry run: ALWAYS reuse the temp file from assign_vouchers
                    # assign_vouchers creates voucher_csv_path + ".dryrun.csv" with USED flags
                    # We must reuse this same file to preserve those flags
                    temp_path = voucher_csv_path + ".dryrun.csv"
                    if not Path(temp_path).exists():
                        # This shouldn't happen if assign_vouchers ran first, but handle gracefully
                        print(f"Warning: Temp voucher file {temp_path} not found. Creating from original.")
                        shutil.copyfile(voucher_csv_path, temp_path)
                    # Always use the existing temp file (has USED flags from assign_vouchers)
                    working_path = temp_path
                else:
                    # Real run: check if using Supabase Storage or local files
                    if config.USE_SUPABASE_STORAGE:
                        if sb is None:
                            print("Warning: Supabase client not provided, skipping voucher assignment for new matches")
                            return ms, remaining_unmatched
                        
                        # Download active.csv from Supabase Storage
                        import storage
                        temp_file_path = storage.download_file(
                            sb, 
                            config.STORAGE_VOUCHERS_BUCKET, 
                            config.VOUCHERS_ACTIVE_FILE
                        )
                        working_path = temp_file_path
                        
                        if not Path(working_path).exists():
                            print("Warning: Failed to download vouchers, skipping voucher assignment for new matches")
                            return ms, remaining_unmatched
                    else:
                        # Use local file
                        working_path = voucher_csv_path
                
                # Load voucher pool
                df = vouchers.load_voucher_pool(working_path)
                
                # Assign vouchers to newly created matches (respect covered dates when COVERED_DATES_EXPLICIT)
                for match in newly_created_matches:
                    riders = match.riders
                    if not riders:
                        continue
                    # Connect Shuttles do not get vouchers
                    if getattr(match, "ride_type", None) == "Connect":
                        match.group_voucher = None
                        for r in riders:
                            r.group_voucher = None
                            r.contingency_voucher = None
                        continue
                    airport = (riders[0].airport or "").upper()
                    ride_date = datetime.fromisoformat(match.suggested_time_iso).date()
                    to_airport = riders[0].to_airport
                    covered = vouchers.is_ride_date_covered(ride_date, to_airport)
                    
                    # --- GROUP VOUCHER ASSIGNMENT (if subsidized and covered) ---
                    if getattr(match, "group_subsidy", False) and covered:
                        idx = vouchers._find_group_voucher(df, airport, ride_date)
                        if idx is not None:
                            voucher = df.loc[idx, "voucher_link"]
                            match.group_voucher = voucher
                            for r in riders:
                                r.group_voucher = voucher
                            df.at[idx, "USED"] = True
                        else:
                            match.group_voucher = None
                            for r in riders:
                                r.group_voucher = None
                    elif getattr(match, "group_subsidy", False):
                        match.group_voucher = None
                        for r in riders:
                            r.group_voucher = None
                    
                    # --- CONTINGENCY VOUCHERS (inbound only, subsidized and covered) ---
                    is_subsidized = getattr(match, "group_subsidy", False)
                    if is_subsidized and covered:
                        for r in riders:
                            if not r.to_airport:  # inbound
                                idx = vouchers._find_contingency_voucher(df, airport)
                                if idx is not None:
                                    voucher = df.loc[idx, "voucher_link"]
                                    r.contingency_voucher = voucher
                                    df.at[idx, "USED"] = True
                                else:
                                    r.contingency_voucher = None
                            else:
                                r.contingency_voucher = None
                    else:
                        for r in riders:
                            r.contingency_voucher = None
                
                # Save updated vouchers
                df.to_csv(working_path, index=False)
                
                # For real run: upload to Supabase Storage if using it
                if not dry_run and config.USE_SUPABASE_STORAGE and sb is not None:
                    import storage

                    # Archive old active.csv
                    storage.archive_file(
                        sb,
                        config.STORAGE_VOUCHERS_BUCKET,
                        config.VOUCHERS_ACTIVE_FILE,
                        config.VOUCHERS_ARCHIVE_FOLDER
                    )
                    # Upload updated active.csv
                    storage.upload_file(
                        sb,
                        config.STORAGE_VOUCHERS_BUCKET,
                        config.VOUCHERS_ACTIVE_FILE,
                        working_path
                    )
                    print(f"Assigned vouchers to {len(newly_created_matches)} newly created matches")
            finally:
                # Clean up temp file if we created one for Supabase Storage
                if temp_file_path and Path(temp_file_path).exists():
                    Path(temp_file_path).unlink(missing_ok=True)
        except Exception as e:
            print(f"Warning: Failed to assign vouchers to newly created matches: {e}")
            # Continue anyway - matches are still valid
    
    return ms, remaining_unmatched


def _final_leftovers(
    matches: List[Match],
    leftovers: List[RiderLite],
    bucket_key: Optional[str] = None
) -> Tuple[List[Match], List[RiderLite]]:
    """
    Final aggressive pass to place any remaining unmatched riders into existing groups.
    Allows 0-minute overlaps (touching windows, e.g., 9:45 start and 9:45 end).
    Works even if riders were previously dominated or couldn't form pairs.
    Only places riders into groups that are < MAX_GROUP_SIZE.
    """
    if not leftovers or not matches:
        return matches, leftovers
    
    # Build set of all flight_ids already matched
    matched_flight_ids = {r.flight_id for m in matches for r in m.riders}
    
    ms = list(matches)
    lo = list(leftovers)
    remaining_leftovers = []
    
    for leftover in lo:
        # Skip if this rider's flight_id is already matched
        if leftover.flight_id in matched_flight_ids:
            remaining_leftovers.append(leftover)
            continue
        
        best_match_idx = None
        
        # Try to find any existing group < MAX_GROUP_SIZE that can accept this rider
        for mi, M in enumerate(ms):
            group = M.riders
            
            # Skip groups that are already at max size
            if len(group) >= config.MAX_GROUP_SIZE:
                continue
            
            # Check if this rider's flight_id is already in this group
            if leftover.flight_id in {r.flight_id for r in group}:
                continue
            
            # Try adding the leftover to this group
            trial = group + [leftover]
            
            # Check time overlap - allow 0-minute overlaps (touching windows)
            starts = []
            ends = []
            for m in trial:
                s, e = _interval(m)
                starts.append(s)
                ends.append(e)
            latest_start = max(starts)
            earliest_end = min(ends)
            overlap_min = (earliest_end - latest_start).total_seconds() / 60.0
            
            # Allow if overlap >= 0 (including 0-minute touching windows)
            if overlap_min < 0:
                continue
            
            # Check other validity constraints (bags, terminal) but allow 0-minute overlaps
            # Check bag capacity
            total_large, total_normal, total_personal, total_all = _bags_totals(trial)
            if total_large > config.MAX_LARGE_BAGS:
                continue
            # Groups of 5 must have < 8 bags, groups of 3 can have <= 12, others <= 10
            trial_size = len(trial)
            if trial_size == 5:
                if total_all > 8:  # Groups of 5 must have < 8 bags
                    continue
            elif trial_size == 3:
                if total_all > 12:  # Groups of 3 can have <= 12 bags
                    continue
            else:
                if total_all > config.MAX_TOTAL_BAGS:  # Groups of 2 or 4 can have <= 10
                    continue
            
            # Check terminal if in strict mode
            if config.TERMINAL_MODE == "strict":
                terms = [m.terminal or "" for m in trial]
                if len(set(terms)) > 1:
                    continue
            
            # This group can accept the leftover
            best_match_idx = mi
            break  # Take the first valid group we find
        
        if best_match_idx is not None:
            # Add the leftover to the group
            M = ms[best_match_idx]
            new_group = M.riders + [leftover]
            # Recreate the match with the new group
            ms[best_match_idx] = _group_to_match(new_group, bucket_key=bucket_key)
            # Track that this rider is now matched
            matched_flight_ids.add(leftover.flight_id)
        else:
            # Couldn't place this leftover anywhere
            remaining_leftovers.append(leftover)
    
    return ms, remaining_leftovers


def _is_lax_departures_bucket(bucket_key: Optional[str]) -> bool:
    """True if this bucket is LAX departures (TO LAX)."""
    if not bucket_key:
        return False
    return "LAX" in bucket_key and "TO" in bucket_key


def _time_overlap_no_bags(members: List[RiderLite]) -> bool:
    """True if all members have a common time overlap (no bag check). Uses same grace as normal matching."""
    return _effective_overlap_minutes(members) >= 0


def _largest_overlapping_set(riders: List[RiderLite]) -> List[RiderLite]:
    """
    Find a largest set of riders that share a common time overlap (no bags).
    Uses sliding window: sort by start, then find largest [left, right] with min(ends) > start_right.
    """
    if not riders:
        return []
    riders_sorted = sorted(riders, key=lambda r: _interval(r)[0])
    n = len(riders_sorted)
    left = 0
    best_size = 0
    best_range = (0, -1)
    heap: List[Tuple[datetime, int]] = []  # (end, idx)
    for right in range(n):
        s_r, e_r = _interval(riders_sorted[right])
        heapq.heappush(heap, (e_r, right))
        while heap and heap[0][1] < left:
            heapq.heappop(heap)
        while left <= right and heap and s_r >= heap[0][0]:
            left += 1
            while heap and heap[0][1] < left:
                heapq.heappop(heap)
        if left <= right:
            size = right - left + 1
            if size > best_size:
                best_size = size
                best_range = (left, right)
    if best_size == 0:
        return []
    return riders_sorted[best_range[0] : best_range[1] + 1]


def _expand_with_same_flight(core: List[RiderLite], date_riders: List[RiderLite]) -> List[RiderLite]:
    """Expand core by adding all riders on the same date who share a flight with someone in core."""
    core_flight_keys = set()
    for r in core:
        key = (r.airline_iata or "", r.flight_no, r.date)
        core_flight_keys.add(key)
    expanded = list(core)
    added_ids = {id(r) for r in core}
    for r in date_riders:
        if id(r) in added_ids:
            continue
        key = (r.airline_iata or "", r.flight_no, r.date)
        if key in core_flight_keys:
            expanded.append(r)
            added_ids.add(id(r))
    return expanded


def _form_connect_shuttle_groups(
    pool: List[RiderLite],
    bucket_key: Optional[str],
) -> Tuple[List[Match], List[RiderLite]]:
    """
    Partition pool into Connect Shuttle groups (8-25 or 30-55). Prefer 30-55, then 8-25.
    Returns (list of Connect Shuttle matches, remaining riders who didn't fit).
    """
    min_s = getattr(config, "LAX_CONNECT_SHUTTLE_MIN", 8)
    t1_max = getattr(config, "LAX_CONNECT_SHUTTLE_TIER1_MAX", 25)
    t2_min = getattr(config, "LAX_CONNECT_SHUTTLE_TIER2_MIN", 30)
    t2_max = getattr(config, "LAX_CONNECT_SHUTTLE_TIER2_MAX", 55)
    matches: List[Match] = []
    remaining = list(pool)
    # Prefer 30-55 groups first
    while len(remaining) >= t2_min:
        take = min(len(remaining), t2_max)
        group = remaining[:take]
        remaining = remaining[take:]
        m = _group_to_match(group, bucket_key=bucket_key)
        m.ride_type = "Connect"
        matches.append(m)
    # Then 8-25 groups
    while len(remaining) >= min_s:
        take = min(len(remaining), t1_max)
        group = remaining[:take]
        remaining = remaining[take:]
        m = _group_to_match(group, bucket_key=bucket_key)
        m.ride_type = "Connect"
        matches.append(m)
    return matches, remaining


def _try_lax_connect_shuttles(
    riders: List[RiderLite],
    bucket_key: Optional[str],
) -> Tuple[List[Match], List[RiderLite]]:
    """
    For LAX departures only: try to form Connect Shuttle groups (8-25 or 30-55).
    Uses only time overlap (no bags). Expands with same-flight riders even if times don't align.
    If we can form at least one group of min size, return those matches and remaining riders for normal matching.
    If we can't form a group of 8+, return ([], riders) so caller runs normal matching on everyone.
    """
    if not _is_lax_departures_bucket(bucket_key):
        return [], riders
    min_s = getattr(config, "LAX_CONNECT_SHUTTLE_MIN", 8)
    if len(riders) < min_s:
        return [], riders

    riders_by_date: Dict[str, List[RiderLite]] = {}
    for r in riders:
        riders_by_date.setdefault(r.date, []).append(r)

    all_connect_matches: List[Match] = []
    all_remaining: List[RiderLite] = []

    for date, date_riders in riders_by_date.items():
        if len(date_riders) < min_s:
            all_remaining.extend(date_riders)
            continue
        core = _largest_overlapping_set(date_riders)
        if len(core) < min_s:
            all_remaining.extend(date_riders)
            continue
        pool = _expand_with_same_flight(core, date_riders)
        connect_matches, remaining = _form_connect_shuttle_groups(pool, bucket_key)
        if not connect_matches:
            all_remaining.extend(date_riders)
            continue
        all_connect_matches.extend(connect_matches)
        # Anyone in date_riders not in connect_matches and not in remaining goes to remaining
        in_connect = {id(r) for m in connect_matches for r in m.riders}
        in_remaining = {id(r) for r in remaining}
        for r in date_riders:
            if id(r) not in in_connect and id(r) not in in_remaining:
                all_remaining.append(r)
        all_remaining.extend(remaining)

    if not all_connect_matches:
        return [], riders
    return all_connect_matches, all_remaining


def merge_groups_into_connect_shuttles(matches: List[Match]) -> List[Match]:
    """
    After all matching: try to merge LAX departure groups that have overlapping times
    into a single Connect shuttle (8-25 or 30-55). Only considers LAX departures (TO LAX).
    Runs once on the full match list and returns an updated list (some groups replaced by one Connect).
    """
    min_s = getattr(config, "LAX_CONNECT_SHUTTLE_MIN", 8)
    t1_max = getattr(config, "LAX_CONNECT_SHUTTLE_TIER1_MAX", 25)
    t2_min = getattr(config, "LAX_CONNECT_SHUTTLE_TIER2_MIN", 30)
    t2_max = getattr(config, "LAX_CONNECT_SHUTTLE_TIER2_MAX", 55)

    # Split into LAX-departure and other (keep order for others)
    lax_matches: List[Match] = []
    other_matches: List[Match] = []
    for m in matches:
        if _is_lax_departures_bucket(m.bucket_key):
            lax_matches.append(m)
        else:
            other_matches.append(m)

    if not lax_matches:
        return matches

    # Group LAX matches by (bucket_key, date)
    by_bucket_date: Dict[Tuple[str, str], List[Match]] = {}
    for m in lax_matches:
        if not m.riders:
            continue
        key = (m.bucket_key or "", m.riders[0].date)
        by_bucket_date.setdefault(key, []).append(m)

    out_lax: List[Match] = []
    for (bucket_key, date), group_matches in by_bucket_date.items():
        if not bucket_key or not _is_lax_departures_bucket(bucket_key):
            out_lax.extend(group_matches)
            continue
        remaining = list(group_matches)
        # Repeatedly try to merge: find a maximal cluster with common overlap and size in [8,25] or [30,55]
        while remaining:
            cluster: List[Match] = [remaining[0]]
            riders: List[RiderLite] = list(remaining[0].riders)
            remaining = remaining[1:]
            flight_ids = {r.flight_id for r in riders}
            # Greedily add any match that overlaps with current riders and doesn't duplicate flight_id
            while True:
                added = False
                for i, m in enumerate(remaining):
                    if any(r.flight_id in flight_ids for r in m.riders):
                        continue
                    new_riders = riders + m.riders
                    if not _time_overlap_no_bags(new_riders):
                        continue
                    cluster.append(m)
                    riders = new_riders
                    flight_ids |= {r.flight_id for r in m.riders}
                    remaining.pop(i)
                    added = True
                    break
                if not added:
                    break
            n = len(riders)
            if t2_min <= n <= t2_max or min_s <= n <= t1_max:
                merged = _group_to_match(riders, bucket_key=bucket_key)
                merged.ride_type = "Connect"
                out_lax.append(merged)
            else:
                # Cluster doesn't fit Connect size; put back as original matches
                out_lax.extend(cluster)

    # Rebuild full list: other first (same order), then LAX (order by bucket/date)
    result = list(other_matches)
    result.extend(out_lax)
    return result


# perform matching inside a single bucket
def match_bucket(
    riders: List[RiderLite],
    bucket_key: Optional[str] = None,
    final_pass: bool = False,
) -> Tuple[List[Match], List[RiderLite], dict]:
    """
    When final_pass=True (e.g. LAX final retry), allow groups of 3 with max 12 bags (Uber XXL)
    and relaxed large-bag limit so more riders can be matched.
    """
    global _final_pass_group3_rules
    if final_pass:
        _final_pass_group3_rules = True
        setattr(config, "FINAL_PASS_PAIR_BAG_LIMIT", 12)
    try:
        return _match_bucket_impl(riders, bucket_key)
    finally:
        if final_pass:
            _final_pass_group3_rules = False
            if hasattr(config, "FINAL_PASS_PAIR_BAG_LIMIT"):
                delattr(config, "FINAL_PASS_PAIR_BAG_LIMIT")


def _match_bucket_impl(riders: List[RiderLite], bucket_key: Optional[str] = None) -> Tuple[List[Match], List[RiderLite], dict]:
    if len(riders) < 2:
        # singleton buckets
        diag = {r.flight_id: {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}} for r in riders}
        return [], list(riders), diag

    # LAX departures only: try Connect Shuttles (8-25 or 30-55) first; if we form any, run normal matching on the rest
    connect_matches, riders_for_normal = _try_lax_connect_shuttles(riders, bucket_key)
    if connect_matches:
        riders = riders_for_normal
        if len(riders) < 2:
            diag = {r.flight_id: {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}} for r in riders} if riders else {}
            return connect_matches, list(riders), diag
        # Continue with normal matching on remaining riders; we'll merge connect_matches at the end
    else:
        riders = list(riders)

    # Sort riders by time window constraint (most constrained first = shortest windows first)
    # This improves greedy matching by prioritizing hard-to-match riders
    riders = sorted(riders, key=_time_window_duration)

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
        return (connect_matches or []) + [], list(riders), diag

    idx_pairs = _select_pairs(pairs)
    used = set()
    matched_flight_ids: set = set()  # Track flight_ids that are matched
    matches: List[Match] = []
    for i, j in idx_pairs:
        if i in used or j in used:
            continue
        # Check if either rider's flight_id is already matched
        if riders[i].flight_id in matched_flight_ids or riders[j].flight_id in matched_flight_ids:
            continue
        base = [riders[i], riders[j]]
        group = _expand_group(base, riders, matched_flight_ids=matched_flight_ids)
        # Track both object indices and flight_ids
        for g in group:
            used.add(idx_map[id(g)])
            matched_flight_ids.add(g.flight_id)
        matches.append(_group_to_match(group, bucket_key=bucket_key))

    leftovers = [r for k, r in enumerate(riders) if k not in used]

    # SECOND PASS: try to form groups among leftovers only
    more_matches, leftovers = _second_pass_leftovers(leftovers, bucket_key=bucket_key, matched_flight_ids=matched_flight_ids)
    matches.extend(more_matches)
    # Update matched_flight_ids with new matches
    for m in more_matches:
        for r in m.riders:
            matched_flight_ids.add(r.flight_id)
    
    # OPTIONAL THIRD STEP: split a 4 into (3,2) if it rescues a leftover
    if getattr(config, "SPLIT_4_TO_3_2", False):
        matches, leftovers = _split_full_group_for_leftovers(matches, leftovers, bucket_key=bucket_key)
        
    # OPTIONAL THIRD STEP B: absorb leftovers into existing groups (prefer 3->4, then 2->3)
    if getattr(config, "ABSORB_LEFTOVERS", True):  # default on
        matches, leftovers = _third_pass_absorb_leftovers(matches, leftovers, bucket_key=bucket_key)
        
    # Final leftover matching pass
    more_matches, leftovers = _second_pass_leftovers(leftovers, bucket_key=bucket_key, matched_flight_ids=matched_flight_ids)
    matches.extend(more_matches)

    # FINAL PASS: LAX rule → promote 2-person groups to 3-person groups if possible
    matches = _promote_lax_twos(matches, bucket_key)
    
    # OPTIMIZATION PASS: Try to combine pairs of 2-person matches into 4-person matches
    matches = _combine_pairs_into_fours(matches, bucket_key=bucket_key)
    
    # LAX OPTIMIZATION: Try to convert 4-person + 2-person groups into 3-person + 3-person groups
    matches = _lax_optimize_4_and_2(matches, bucket_key=bucket_key)

    # FINAL LEFTOVERS PASS: Try to place any remaining unmatched riders into existing groups
    matches, leftovers = _final_leftovers(matches, leftovers, bucket_key=bucket_key)

    # compose diagnostics for leftovers
    diag = audit.finalize_unmatched_diag(riders, bucket_key, pairs, pair_diag, feasible_count, used)
    # Prepend LAX Connect Shuttle matches if we formed any
    if connect_matches:
        matches = connect_matches + matches
    return matches, leftovers, diag
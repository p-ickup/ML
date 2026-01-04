# matching.py
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


# quick validity check (size, time overlap, bag capacity, terminal strict)
def _is_valid_group(members: List[RiderLite]) -> bool:
    if not (2 <= len(members) <= config.MAX_GROUP_SIZE):
        return False

    eff_min = _effective_overlap_minutes(members)
    if eff_min < 0:  # truly no overlap even with grace
        return False

    # BAG CAPACITY RULES (configurable)
    total_large, total_normal, total_personal, total_all = _bags_totals(members)

    # Check large bag limit
    if total_large > config.MAX_LARGE_BAGS:
        return False

    # Check total bag limit (groups of 5 must have < 8 bags, groups of 3 can have <= 12, others <= 10)
    group_size = len(members)
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
    if eff_min < 0:
        return float("-inf")

    # touching → give a tiny floor so they're not always siphoned away
    time_fit_min = eff_min if eff_min > 0 else config.TOUCH_FLOOR_MIN

    tpen = _terminal_penalty(members)
    _, _, _, total_all = _bags_totals(members)

    # reward groups that use bag capacity well (scaled to MAX_TOTAL_BAGS)
    fill = min(total_all / config.MAX_TOTAL_BAGS, 1.0)
    bag_bonus = 0.5 * fill
    
    # Bonus for groups with riders on the same flight (airline_iata + flight_no)
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
        # Bonus: 2 points per rider on the same flight (e.g., 2 riders = 2 points, 3 riders = 4 points, 4 riders = 6 points)
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
    if earliest_end <= latest_start:
        # touching window fallback
        chosen = latest_start
    else:
        if to_airport:
            # going TO airport → 15 minutes before latest time (earliest_end) if possible
            chosen = earliest_end - timedelta(minutes=15)
            # Clamp to within overlap window
            if chosen < latest_start:
                chosen = latest_start
        else:
            # coming FROM airport → leave earliest: 15 minutes after earliest time overlap (latest_start) if possible
            chosen = latest_start + timedelta(minutes=15)
            # Clamp to within overlap window
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


# perform matching inside a single bucket
def match_bucket(riders: List[RiderLite], bucket_key: Optional[str] = None) -> Tuple[List[Match], List[RiderLite], dict]:
    if len(riders) < 2:
        # singleton buckets
        diag = {r.flight_id: {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}} for r in riders}
        return [], list(riders), diag

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
        return [], list(riders), diag

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
    return matches, leftovers, diag
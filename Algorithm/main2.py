"""
Experimental max-matched matcher.

main2.py keeps the same CLI/output behavior as main.py, but changes the
per-bucket matching strategy:

1. Generate a bounded set of valid candidate groups.
2. Score candidates by total matched riders first.
3. Select a non-overlapping set of groups.
4. Run the same subsidy, voucher, CSV, and Supabase write flow as main.py.
"""

import argparse
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional, Sequence, Set, Tuple

import audit
import config
import force_match as nepo
import main as legacy
import ruleMatching as rm
from buckets import make_buckets
from rider_data import RiderData, RiderLite
from ruleMatching import Match


SUBSIDY_THRESHOLDS = {"LAX": 3, "ONT": 2}

# Runtime controls. Candidate generation is exhaustive within each bucket/date,
# but exact set-packing search can still explode on unusually dense buckets.
LOCAL_SEARCH_PASSES = 2
EXACT_SEARCH_CANDIDATE_LIMIT = 12000
EXACT_SEARCH_NODE_LIMIT = 250000


@dataclass(frozen=True)
class Candidate:
    riders: Tuple[RiderLite, ...]
    match: Match
    flight_ids: frozenset
    score: Tuple[int, int, int, float]


def _same_flight_count(riders: Sequence[RiderLite]) -> int:
    counts: Dict[Tuple[str, Optional[int], str], int] = {}
    for r in riders:
        if r.flight_no is None:
            continue
        key = ((r.airline_iata or "").upper(), r.flight_no, r.date)
        counts[key] = counts.get(key, 0) + 1
    return max(counts.values(), default=0)


def _candidate_score(match: Match) -> Tuple[int, int, int, float]:
    riders = match.riders
    total_riders = len(riders)
    same_flight = _same_flight_count(riders)
    group_count = 1
    quality_score = rm._score_group(riders, validate=True)
    return (total_riders, same_flight, group_count, quality_score)


def _selection_score(candidates: Sequence[Candidate]) -> Tuple[int, int, int, float]:
    riders = sum(c.score[0] for c in candidates)
    same_flight = sum(c.score[1] for c in candidates)
    group_count = sum(c.score[2] for c in candidates)
    quality = sum(c.score[3] for c in candidates)
    return (riders, same_flight, group_count, quality)


def _candidate_sort_key(candidate: Candidate) -> Tuple[int, int, int, float, str]:
    first_date = candidate.match.riders[0].date if candidate.match.riders else ""
    first_time = candidate.match.suggested_time_iso or ""
    return candidate.score + (f"{first_date}|{first_time}",)


def _matches_score(matches: Sequence[Match]) -> Tuple[int, int, int, float]:
    riders_count = 0
    same_flight = 0
    group_count = 0
    quality = 0.0
    for match in matches:
        candidate_score = _candidate_score(match)
        riders_count += candidate_score[0]
        same_flight += candidate_score[1]
        group_count += candidate_score[2]
        quality += candidate_score[3]
    return (riders_count, same_flight, group_count, quality)


def _add_candidate(
    candidates: Dict[Tuple[int, ...], Candidate],
    group: Sequence[RiderLite],
    bucket_key: Optional[str],
) -> None:
    flight_ids = tuple(sorted(r.flight_id for r in group))
    if len(flight_ids) != len(set(flight_ids)):
        return
    if flight_ids in candidates:
        return
    if not rm._is_valid_group(list(group)):
        return

    match = rm._group_to_match(list(group), bucket_key=bucket_key)
    score = _candidate_score(match)
    candidates[flight_ids] = Candidate(
        riders=tuple(group),
        match=match,
        flight_ids=frozenset(flight_ids),
        score=score,
    )


def _group_has_possible_overlap(group: Sequence[RiderLite]) -> bool:
    starts, ends = [], []
    for rider in group:
        start, end = rm._interval(rider)
        starts.append(start)
        ends.append(end)

    latest_start = max(starts)
    earliest_end = min(ends)
    gap_min = (latest_start - earliest_end).total_seconds() / 60.0
    if gap_min <= 0:
        return True
    if config.ALLOW_TOUCHING and gap_min <= config.OVERLAP_GRACE_MIN:
        return True
    return False

def _group_by_date(riders: List[RiderLite]) -> Dict[str, List[RiderLite]]:
    by_date: Dict[str, List[RiderLite]] = {}
    for rider in riders:
        by_date.setdefault(rider.date, []).append(rider)
    for date, date_riders in by_date.items():
        by_date[date] = sorted(date_riders, key=lambda r: (rm._interval(r)[0], rm._interval(r)[1], r.flight_id))
    return by_date


def _generate_candidates(riders: List[RiderLite], bucket_key: Optional[str]) -> List[Candidate]:
    candidates: Dict[Tuple[int, ...], Candidate] = {}

    for date_riders in _group_by_date(riders).values():
        max_size = min(config.MAX_GROUP_SIZE, len(date_riders))
        intervals = [rm._interval(r) for r in date_riders]

        def backtrack(start_idx: int, group_indices: List[int]) -> None:
            group = [date_riders[i] for i in group_indices]
            if len(group) >= 2:
                _add_candidate(candidates, group, bucket_key)
            if len(group_indices) >= max_size:
                return

            current_end = None
            if group_indices:
                current_end = min(intervals[i][1] for i in group_indices)

            for next_idx in range(start_idx, len(date_riders)):
                if current_end is not None:
                    candidate_start = intervals[next_idx][0]
                    if candidate_start > current_end + timedelta(minutes=config.OVERLAP_GRACE_MIN):
                        break

                next_group_indices = group_indices + [next_idx]
                next_group = [date_riders[i] for i in next_group_indices]
                if not _group_has_possible_overlap(next_group):
                    continue

                backtrack(next_idx + 1, next_group_indices)

        backtrack(0, [])

    return sorted(candidates.values(), key=_candidate_sort_key, reverse=True)


def _greedy_select(
    candidates: Sequence[Candidate],
    locked_flight_ids: Optional[Set[int]] = None,
) -> List[Candidate]:
    used: Set[int] = set(locked_flight_ids or set())
    selected: List[Candidate] = []
    for candidate in candidates:
        if candidate.flight_ids & used:
            continue
        selected.append(candidate)
        used.update(candidate.flight_ids)
    return selected


def _improve_selection(candidates: List[Candidate], selected: List[Candidate]) -> List[Candidate]:
    """
    Small local search: remove one chosen group, then rebuild greedily around the
    remaining locked groups. Accept the change only when the total objective
    improves lexicographically.
    """
    current = list(selected)
    current_score = _selection_score(current)

    for _ in range(LOCAL_SEARCH_PASSES):
        improved = False
        for remove_idx in range(len(current)):
            locked = [c for i, c in enumerate(current) if i != remove_idx]
            locked_ids = {fid for c in locked for fid in c.flight_ids}
            rebuilt = locked + _greedy_select(candidates, locked_flight_ids=locked_ids)
            rebuilt_score = _selection_score(rebuilt)
            if rebuilt_score > current_score:
                current = rebuilt
                current_score = rebuilt_score
                improved = True
                break
        if not improved:
            break

    # Normalize order for stable CSV output.
    return sorted(current, key=lambda c: (c.match.riders[0].date, c.match.bucket_key or "", -len(c.riders)))


def _exact_select(candidates: List[Candidate], riders: List[RiderLite]) -> Optional[List[Candidate]]:
    if len(candidates) > EXACT_SEARCH_CANDIDATE_LIMIT:
        return None

    ordered_ids = [r.flight_id for r in sorted(riders, key=lambda r: (r.date, rm._interval(r)[0], r.flight_id))]
    candidates_by_flight: Dict[int, List[Candidate]] = {flight_id: [] for flight_id in ordered_ids}
    for candidate in candidates:
        for flight_id in candidate.flight_ids:
            if flight_id in candidates_by_flight:
                candidates_by_flight[flight_id].append(candidate)

    for flight_id in candidates_by_flight:
        candidates_by_flight[flight_id].sort(key=_candidate_sort_key, reverse=True)

    best: List[Candidate] = []
    best_score = (0, 0, 0, float("-inf"))
    node_count = 0
    exhausted = True

    def search(decided: Set[int], selected: List[Candidate], score: Tuple[int, int, int, float]) -> None:
        nonlocal best, best_score, node_count, exhausted
        node_count += 1
        if node_count > EXACT_SEARCH_NODE_LIMIT:
            exhausted = False
            return

        remaining = len(ordered_ids) - len(decided)
        upper = (score[0] + remaining, score[1] + remaining, 10**9, float("inf"))
        if upper < best_score:
            return

        first_open = None
        for flight_id in ordered_ids:
            if flight_id not in decided:
                first_open = flight_id
                break

        if first_open is None:
            if score > best_score:
                best = list(selected)
                best_score = score
            return

        options = [
            candidate
            for candidate in candidates_by_flight.get(first_open, [])
            if not (candidate.flight_ids & decided)
        ]

        for candidate in options:
            next_score = (
                score[0] + candidate.score[0],
                score[1] + candidate.score[1],
                score[2] + candidate.score[2],
                score[3] + candidate.score[3],
            )
            search(decided | set(candidate.flight_ids), selected + [candidate], next_score)
            if not exhausted:
                return

        search(decided | {first_open}, selected, score)

    search(set(), [], (0, 0, 0, 0.0))
    if not exhausted:
        return None
    return sorted(best, key=lambda c: (c.match.riders[0].date, c.match.bucket_key or "", -len(c.riders)))


def _select_candidates(candidates: List[Candidate], riders: List[RiderLite]) -> List[Candidate]:
    exact = _exact_select(candidates, riders)
    if exact is not None:
        return exact
    selected = _greedy_select(candidates)
    return _improve_selection(candidates, selected)


def match_bucket_max_matched(
    riders: List[RiderLite],
    bucket_key: Optional[str] = None,
) -> Tuple[List[Match], List[RiderLite], dict]:
    if len(riders) < 2:
        diag = {
            r.flight_id: {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}}
            for r in riders
        }
        return [], list(riders), diag

    riders = sorted(riders, key=rm._time_window_duration)

    # Keep the original pipeline order for LAX departures:
    # try Connect shuttles first, then run normal matching on the remainder.
    connect_matches, riders_for_normal = rm._try_lax_connect_shuttles(riders, bucket_key)
    if connect_matches:
        riders = sorted(riders_for_normal, key=rm._time_window_duration)
        if len(riders) < 2:
            diag = {
                r.flight_id: {"bucket_key": bucket_key or "", "reason": "singleton_bucket", "details": {}}
                for r in riders
            }
            return connect_matches, list(riders), diag

    legacy_matches, legacy_leftovers, legacy_diag = rm.match_bucket(riders, bucket_key=bucket_key)
    candidates = _generate_candidates(riders, bucket_key)

    if not candidates:
        return connect_matches + legacy_matches, legacy_leftovers, legacy_diag

    selected = _select_candidates(candidates, riders)
    matches = [candidate.match for candidate in selected]

    matched_ids = {fid for candidate in selected for fid in candidate.flight_ids}
    leftovers = [r for r in riders if r.flight_id not in matched_ids]

    pairs, pair_diag, feasible_count = audit.build_scored_pairs_with_diag(
        riders,
        score_group=lambda members: rm._score_group(members, validate=True),
    )
    used_indices = {i for i, r in enumerate(riders) if r.flight_id in matched_ids}
    diag = audit.finalize_unmatched_diag(riders, bucket_key, pairs, pair_diag, feasible_count, used_indices)

    if _matches_score(legacy_matches) > _matches_score(matches):
        return connect_matches + legacy_matches, legacy_leftovers, legacy_diag

    return connect_matches + matches, leftovers, diag


def _final_lax_unmatched_retry_max_matched(
    all_matches: List[Match],
    all_unmatched: List[RiderLite],
) -> Tuple[List[Match], List[RiderLite]]:
    lax_unmatched = [r for r in all_unmatched if (r.airport or "").strip().upper() == "LAX"]
    if not lax_unmatched:
        return all_matches, all_unmatched

    lax_buckets = make_buckets(lax_unmatched)
    new_matches: List[Match] = []
    lax_leftovers: List[RiderLite] = []
    for name, riders_in_bucket in lax_buckets.items():
        if len(riders_in_bucket) >= 2:
            matches, leftovers, _ = match_bucket_max_matched(riders_in_bucket, bucket_key=name)
            new_matches.extend(matches)
            lax_leftovers.extend(leftovers)
        else:
            lax_leftovers.extend(riders_in_bucket)

    if not new_matches:
        return all_matches, all_unmatched

    matched_flight_ids = {r.flight_id for m in new_matches for r in m.riders}
    non_lax_unmatched = [r for r in all_unmatched if (r.airport or "").strip().upper() != "LAX"]
    print(f"  LAX final retry (max-matched): {len(new_matches)} new group(s), {len(matched_flight_ids)} riders matched")
    return all_matches + new_matches, non_lax_unmatched + lax_leftovers


def run(
    dry_run: bool = False,
    csv_path: str = "../matches/matches_dryrun.csv",
    unmatched_csv_path: str = "../matches/unmatched_reasons_dryrun.csv",
    days_ahead: int = 10,
    days_ahead_start: Optional[int] = None,
    vouchers_csv_path: str = "../vouchers/Thanksgiving.csv",
) -> None:
    status_id: Optional[str] = None
    run_id: Optional[str] = None
    algorithm_name = "pickup_matching_max_matched"

    try:
        rd = RiderData(legacy.supabase)
        riders = rd.fetch_riders(max_days_ahead=days_ahead, min_days_ahead=days_ahead_start)

        if not dry_run:
            target_scope = legacy.algorithmStatus.determine_target_scope(riders)
            status_id = legacy.algorithmStatus.get_or_create_algorithm_status(
                legacy.supabase,
                algorithm_name,
                target_scope,
            )

            if status_id:
                status_resp = (
                    legacy.supabase.table("AlgorithmStatus")
                    .select("run_id")
                    .eq("id", status_id)
                    .execute()
                )
                if status_resp.data and len(status_resp.data) > 0:
                    run_id = status_resp.data[0].get("run_id")

            if not run_id:
                run_id = str(uuid.uuid4())
                if status_id:
                    legacy.supabase.table("AlgorithmStatus").update({"run_id": run_id}).eq("id", status_id).execute()

        if riders:
            dates = sorted({r.date for r in riders})
            print(f"Fetched {len(riders)} rider forms (dates: {dates[0]} -> {dates[-1]})")
        else:
            print("Fetched 0 rider forms.")
            print("No candidate riders found.")
            if not dry_run:
                legacy.algorithmStatus.update_algorithm_status(legacy.supabase, status_id, "success", run_id=run_id)
            return

        buckets = make_buckets(riders)

        all_matches: List[Match] = []
        all_unmatched: List[RiderLite] = []
        all_diag: dict[int, dict] = {}

        for name, riders_in_bucket in buckets.items():
            nepo_match, remaining_riders = nepo.force_nepo_match(riders_in_bucket, bucket_key=name)
            if nepo_match:
                all_matches.append(nepo_match)
                riders_in_bucket = remaining_riders

            matches, leftovers, diag = match_bucket_max_matched(riders_in_bucket, bucket_key=name)
            all_matches.extend(matches)
            all_unmatched.extend(leftovers)
            all_diag.update(diag)

        all_matches, all_unmatched = rm._ont_post_process_unmatched(all_matches, all_unmatched)

        if getattr(config, "LAX_CONNECT_SHUTTLE_MIN", None) is not None:
            all_matches, all_unmatched = _final_lax_unmatched_retry_max_matched(all_matches, all_unmatched)

        legacy.apply_group_subsidy(all_matches, SUBSIDY_THRESHOLDS)
        legacy.assign_vouchers(
            all_matches,
            voucher_csv_path=vouchers_csv_path,
            dry_run=dry_run,
            sb=legacy.supabase if (not dry_run and config.USE_SUPABASE_STORAGE) else None,
        )

        print(f"Buckets: {len(buckets)} | Groups: {len(all_matches)} | Unmatched: {len(all_unmatched)}")

        legacy._write_matches_csv(
            all_matches,
            riders,
            csv_path,
            sb=legacy.supabase if (dry_run and config.USE_SUPABASE_STORAGE) else None,
            dry_run=dry_run,
        )
        legacy._write_unmatched_with_reasons(
            all_unmatched,
            all_diag,
            unmatched_csv_path,
            sb=legacy.supabase if (dry_run and config.USE_SUPABASE_STORAGE) else None,
            dry_run=dry_run,
        )

        if not dry_run:
            legacy._write_matches_db(legacy.supabase, all_matches, riders)
            legacy.algorithmStatus.update_algorithm_status(legacy.supabase, status_id, "success", run_id=run_id)

    except Exception as e:
        error_msg = str(e)
        print(f"Algorithm execution failed: {error_msg}")
        if not dry_run and status_id:
            legacy.algorithmStatus.update_algorithm_status(
                legacy.supabase,
                status_id,
                "failed",
                error_message=error_msg,
                run_id=run_id,
            )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pickup matcher (max-matched candidate search)")
    parser.add_argument("--dry-run", action="store_true", help="do not write to DB; export CSV instead")
    parser.add_argument("--csv", type=str, default="../matches/matches_dryrun.csv", help="CSV path for output")
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=10,
        help="inclusive end: only flights on or before (today + N days)",
    )
    parser.add_argument(
        "--days-ahead-start",
        type=int,
        default=None,
        help="inclusive start: only flights on or after (today + N days). Omit to keep the legacy lower bound.",
    )
    parser.add_argument("--vouchers", type=str, default="../vouchers/Thanksgiving.csv", help="Path to vouchers CSV")
    args = parser.parse_args()

    run(
        dry_run=args.dry_run,
        csv_path=args.csv,
        days_ahead=args.days_ahead,
        days_ahead_start=args.days_ahead_start,
        vouchers_csv_path=args.vouchers,
    )

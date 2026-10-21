"""Orchestration for a single stateless matching run.

Ported from the ``run()`` function in ML/Algorithm/main.py, with the
Supabase fetch, Connect-group DB merge, voucher-pool CSV assignment, and
production commit steps removed — this module only buckets, matches, and
scores subsidy eligibility for the riders it's given.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from . import connect_policy as cp
from .buckets import make_buckets
from .rider import RiderLite
from .rule_matching import Match, _ont_post_process_unmatched, match_bucket, refresh_match_suggested_times
from .subsidy import apply_group_subsidy


@dataclass
class MatchResult:
    matches: List[Match] = field(default_factory=list)
    unmatched: List[RiderLite] = field(default_factory=list)
    diagnostics: Dict[int, dict] = field(default_factory=dict)
    bucket_count: int = 0


def _final_lax_unmatched_retry(
    all_matches: List[Match],
    all_unmatched: List[RiderLite],
) -> Tuple[List[Match], List[RiderLite]]:
    """
    When Connect Shuttle option is on: one more pass over LAX/ONT unmatched riders.
    Try to form NEW groups only from Connect-scope unmatched (re-bucket them and run
    match_bucket on each bucket with 2+ riders). We do NOT add these riders
    into existing groups — only form new groups among themselves.
    """
    connect_unmatched = [r for r in all_unmatched if cp.rider_in_connect_scope(r)]
    if not connect_unmatched:
        return all_matches, all_unmatched

    lax_buckets = make_buckets(connect_unmatched)
    new_matches: List[Match] = []
    lax_leftovers: List[RiderLite] = []
    for name, riders_in_bucket in lax_buckets.items():
        if len(riders_in_bucket) >= 2:
            matches, leftovers, _ = match_bucket(riders_in_bucket, bucket_key=name, final_pass=True)
            new_matches.extend(matches)
            lax_leftovers.extend(leftovers)
        else:
            lax_leftovers.extend(riders_in_bucket)

    if not new_matches:
        return all_matches, all_unmatched

    non_connect_unmatched = [r for r in all_unmatched if not cp.rider_in_connect_scope(r)]
    all_unmatched_new = non_connect_unmatched + lax_leftovers
    return all_matches + new_matches, all_unmatched_new


def run_matching(riders: List[RiderLite]) -> MatchResult:
    """Bucket, match, and score subsidy eligibility for a set of riders."""
    if not riders:
        return MatchResult()

    buckets = make_buckets(riders)

    all_matches: List[Match] = []
    all_unmatched: List[RiderLite] = []
    all_diag: Dict[int, dict] = {}

    for name, riders_in_bucket in buckets.items():
        matches, leftovers, diag = match_bucket(riders_in_bucket, bucket_key=name)
        all_matches.extend(matches)
        all_unmatched.extend(leftovers)
        all_diag.update(diag)

    # ONT post-processing: try to match unmatched individuals with groups of 4 on the same day
    all_matches, all_unmatched = _ont_post_process_unmatched(all_matches, all_unmatched)

    # Final Connect-scope retry: re-run matching on Connect-scope unmatched (e.g. 3 with exact overlap)
    if cp.connect_enabled():
        all_matches, all_unmatched = _final_lax_unmatched_retry(all_matches, all_unmatched)

    # Final pickup times (TO: 15 min before overlap end; FROM: 15 min after overlap start)
    refresh_match_suggested_times(all_matches, skip_connect=True)

    # Subsidy once the final group list is known (ONT splits, Connect retry included)
    apply_group_subsidy(all_matches)

    return MatchResult(
        matches=all_matches,
        unmatched=all_unmatched,
        diagnostics=all_diag,
        bucket_count=len(buckets),
    )

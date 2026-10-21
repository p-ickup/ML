"""Subsidy eligibility, ported from ML/Algorithm/vouchers.py (date coverage check)
and ML/Algorithm/main.py (apply_group_subsidy). Voucher-pool CSV/DB assignment is
intentionally not ported — this service only reports subsidy eligibility, it does
not consume vouchers.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional

from . import config
from .rule_matching import Match


def is_ride_date_covered(ride_date: date, to_airport: bool) -> bool:
    """
    True if the ride is on a "covered" date for subsidy purposes.
    When COVERED_DATES_EXPLICIT is False, all dates are covered.
    When True, only dates in COVERED_DATES_OUTBOUND (to_airport True) or
    COVERED_DATES_INBOUND (to_airport False) are covered.
    """
    if not getattr(config, "COVERED_DATES_EXPLICIT", False):
        return True
    mm_dd = ride_date.strftime("%m-%d")
    if to_airport:
        allowed = getattr(config, "COVERED_DATES_OUTBOUND", [])
    else:
        allowed = getattr(config, "COVERED_DATES_INBOUND", [])
    return mm_dd in allowed


def apply_group_subsidy(
    matches: List[Match],
    thresholds: Optional[Dict[str, int]] = None,
) -> None:
    if thresholds is None:
        thresholds = config.SUBSIDY_MIN_GROUP_SIZE

    for m in matches:
        for r in m.riders:
            r.subsidized = False

    for m in matches:
        if not m.riders:
            m.group_subsidy = False
            continue

        group_airport = (m.riders[0].airport or "").strip().upper()
        need = thresholds.get(group_airport)
        group_size = len(m.riders)

        all_pomona = all((r.school or "").strip().upper() == "POMONA" for r in m.riders)

        ride_date = datetime.fromisoformat(m.suggested_time_iso).date()
        to_airport = m.riders[0].to_airport
        covered = is_ride_date_covered(ride_date, to_airport)

        qualifies = (
            need is not None and
            group_size >= need and
            all_pomona and
            covered
        )

        m.group_subsidy = qualifies
        for r in m.riders:
            r.subsidized = qualifies

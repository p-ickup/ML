"""Shared test helpers (stdlib only — no third-party test deps).

We avoid importing ``main`` anywhere in the suite because it builds a
Supabase client at import time. All tests target the pure-logic modules
(buckets, connect_policy, audit, ruleMatching, vouchers, rider_data
normalizers) which import cleanly without credentials.
"""

import os
import sys
from contextlib import contextmanager
from unittest import mock

# Defensive: ensure Algorithm/ is importable even if this module is loaded
# before the package __init__ (e.g. when running a single file directly).
_ALGORITHM_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Algorithm")
if _ALGORITHM_DIR not in sys.path:
    sys.path.insert(0, _ALGORITHM_DIR)

from rider_data import RiderLite  # noqa: E402  (path set above)


def make_rider(
    flight_id,
    *,
    earliest_time="10:00:00",
    latest_time="12:00:00",
    airport="LAX",
    to_airport=True,
    date="2026-05-12",
    terminal="1",
    school="POMONA",
    bags_no=1,
    bags_no_large=0,
    bag_no_personal=0,
    flight_no=None,
    airline_iata=None,
    name=None,
    matched=False,
) -> RiderLite:
    """Build a RiderLite with sensible defaults; override per test."""
    return RiderLite(
        user_id=f"u{flight_id}",
        flight_id=flight_id,
        flight_no=flight_no,
        earliest_time=earliest_time,
        latest_time=latest_time,
        airport=airport,
        to_airport=to_airport,
        date=date,
        terminal=terminal,
        matched=matched,
        school=school,
        bags_no=bags_no,
        bags_no_large=bags_no_large,
        bag_no_personal=bag_no_personal,
        name=name or f"Rider {flight_id}",
        airline_iata=airline_iata,
    )


@contextmanager
def patch_config(**overrides):
    """Temporarily override attributes on the ``config`` module.

    Usage:
        with patch_config(TERMINAL_MODE="strict", MAX_TOTAL_BAGS=4):
            ...
    Values are restored automatically on exit.
    """
    import config

    with mock.patch.multiple(config, **overrides):
        yield

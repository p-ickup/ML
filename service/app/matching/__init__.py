"""Stateless rider-matching engine, ported from ML/Algorithm.

This package contains the deterministic bucket/matching/subsidy logic from
the pickup-matching pipeline, with the Supabase-backed steps (fetching
riders, Connect-group DB merge, voucher-pool consumption, production commit)
removed. The microservice calls ``run_matching`` with riders supplied in the
request body instead of fetching them from a database.
"""

from .pipeline import MatchResult, run_matching
from .rider import RiderLite, rider_lite_from_input
from .serializer import serialize_result

__all__ = [
    "MatchResult",
    "run_matching",
    "RiderLite",
    "rider_lite_from_input",
    "serialize_result",
]

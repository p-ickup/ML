"""Regression tests for preserving normal groups during Connect evaluation."""

import unittest
from unittest import mock

import connect_merge
from connect_merge import merge_connect_with_existing
from ruleMatching import Match
from tests.helpers import make_rider, patch_config


def _match(*riders):
    return Match(
        riders=list(riders),
        suggested_time_iso="2026-10-10T17:45:00",
        terminal="2",
        bucket_key="TO LAX | POMONA",
    )


class TestConnectMergePreservesNormalMatches(unittest.TestCase):
    def test_valid_pair_survives_when_pool_is_below_connect_minimum(self):
        pair_a = make_rider(2323, date="2026-10-10", earliest_time="16:00:00", latest_time="18:00:00")
        pair_b = make_rider(2324, date="2026-10-10", earliest_time="16:00:00", latest_time="20:00:00")
        unmatched_a = make_rider(2325, date="2026-10-10", earliest_time="22:00:00", latest_time="22:30:00")
        unmatched_b = make_rider(2326, date="2026-10-10", earliest_time="13:30:00", latest_time="14:30:00")

        with patch_config(
            CONNECT_DEPARTURE=["LAX"],
            CONNECT_ARRIVAL=[],
            CONNECT_SIZE1=[6, 12],
            CONNECT_SIZE2=[],
        ):
            matches, unmatched, connect = merge_connect_with_existing(
                None,
                [_match(pair_a, pair_b)],
                [unmatched_a, unmatched_b],
                run_flight_ids={2323, 2324, 2325, 2326},
                start_date="2026-10-10",
                end_date="2026-10-10",
            )

        self.assertEqual([[r.flight_id for r in m.riders] for m in matches], [[2323, 2324]])
        self.assertEqual({r.flight_id for r in unmatched}, {2325, 2326})
        self.assertEqual(connect, [])

    def test_separate_normal_pairs_keep_distinct_source_groups(self):
        riders = [make_rider(i, date="2026-10-10") for i in range(1, 5)]

        with patch_config(
            CONNECT_DEPARTURE=["LAX"],
            CONNECT_ARRIVAL=[],
            CONNECT_SIZE1=[6, 12],
            CONNECT_SIZE2=[],
        ):
            matches, unmatched, connect = merge_connect_with_existing(
                None,
                [_match(riders[0], riders[1]), _match(riders[2], riders[3])],
                [],
                run_flight_ids={1, 2, 3, 4},
                start_date="2026-10-10",
                end_date="2026-10-10",
            )

        self.assertEqual(
            {frozenset(r.flight_id for r in match.riders) for match in matches},
            {frozenset({1, 2}), frozenset({3, 4})},
        )
        self.assertEqual(unmatched, [])
        self.assertEqual(connect, [])

    def test_existing_db_pair_is_not_returned_as_a_new_match(self):
        current_a = make_rider(1, date="2026-10-10")
        current_b = make_rider(2, date="2026-10-10")
        existing_a = make_rider(101, date="2026-10-10")
        existing_b = make_rider(102, date="2026-10-10")

        with patch_config(
            CONNECT_DEPARTURE=["LAX"],
            CONNECT_ARRIVAL=[],
            CONNECT_SIZE1=[6, 12],
            CONNECT_SIZE2=[],
        ), mock.patch.object(
            connect_merge,
            "fetch_existing_connect_riders",
            return_value=([existing_a, existing_b], {101: 9001, 102: 9001}),
        ):
            matches, unmatched, connect = merge_connect_with_existing(
                object(),
                [_match(current_a, current_b)],
                [],
                run_flight_ids={1, 2},
                start_date="2026-10-10",
                end_date="2026-10-10",
            )

        self.assertEqual([[r.flight_id for r in m.riders] for m in matches], [[1, 2]])
        self.assertEqual(unmatched, [])
        self.assertEqual(connect, [])


if __name__ == "__main__":
    unittest.main()

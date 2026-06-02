"""Tests for unmatched-reason diagnostics (audit.py)."""

import unittest

import audit
from tests.helpers import make_rider, patch_config


class TestPairBlockReason(unittest.TestCase):
    def test_overlapping_pair_is_feasible(self):
        a = make_rider(1, earliest_time="10:00:00", latest_time="12:00:00")
        b = make_rider(2, earliest_time="11:00:00", latest_time="13:00:00")
        self.assertIsNone(audit.pair_block_reason(a, b))

    def test_no_time_overlap(self):
        with patch_config(ALLOW_TOUCHING=False):
            a = make_rider(1, earliest_time="08:00:00", latest_time="09:00:00")
            b = make_rider(2, earliest_time="12:00:00", latest_time="13:00:00")
            self.assertEqual(audit.pair_block_reason(a, b), "no_time_overlap")

    def test_bag_capacity(self):
        with patch_config(MAX_TOTAL_BAGS=4, LARGE_BAG_MULTIPLIER=2):
            a = make_rider(1, bags_no=3, bags_no_large=0)
            b = make_rider(2, bags_no=3, bags_no_large=0)
            self.assertEqual(audit.pair_block_reason(a, b), "bag_capacity")

    def test_terminal_mismatch_strict_mode(self):
        with patch_config(TERMINAL_MODE="strict"):
            a = make_rider(1, terminal="1")
            b = make_rider(2, terminal="4")
            self.assertEqual(audit.pair_block_reason(a, b), "terminal_mismatch")

    def test_terminal_ignored_in_slack_mode(self):
        with patch_config(TERMINAL_MODE="slack"):
            a = make_rider(1, terminal="1")
            b = make_rider(2, terminal="4")
            self.assertIsNone(audit.pair_block_reason(a, b))


class TestTopReason(unittest.TestCase):
    def test_empty_is_unknown(self):
        self.assertEqual(audit._top_reason({}), "unknown")

    def test_priority_on_tie(self):
        counts = {"bag_capacity": 2, "no_time_overlap": 2}
        self.assertEqual(audit._top_reason(counts), "no_time_overlap")

    def test_higher_count_wins(self):
        counts = {"bag_capacity": 5, "no_time_overlap": 1}
        self.assertEqual(audit._top_reason(counts), "bag_capacity")


if __name__ == "__main__":
    unittest.main()

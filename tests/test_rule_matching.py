"""Tests for core matching logic (ruleMatching.py)."""

import unittest
from datetime import datetime

import ruleMatching as rm
from tests.helpers import make_rider, patch_config


class TestInterval(unittest.TestCase):
    def test_basic_window(self):
        r = make_rider(1, date="2026-05-12", earliest_time="10:00:00", latest_time="12:00:00")
        start, end = rm._interval(r)
        self.assertEqual(start, datetime(2026, 5, 12, 10, 0))
        self.assertEqual(end, datetime(2026, 5, 12, 12, 0))

    def test_cross_midnight_rolls_end_forward(self):
        r = make_rider(1, date="2026-05-12", earliest_time="23:00:00", latest_time="01:00:00")
        start, end = rm._interval(r)
        self.assertGreater(end, start)
        self.assertEqual(end, datetime(2026, 5, 13, 1, 0))


class TestEffectiveOverlap(unittest.TestCase):
    def test_real_overlap(self):
        a = make_rider(1, earliest_time="10:00:00", latest_time="12:00:00")
        b = make_rider(2, earliest_time="11:00:00", latest_time="13:00:00")
        self.assertEqual(rm._effective_overlap_minutes([a, b]), 60)

    def test_touching_within_grace(self):
        with patch_config(ALLOW_TOUCHING=True, OVERLAP_GRACE_MIN=10):
            a = make_rider(1, earliest_time="10:00:00", latest_time="12:00:00")
            b = make_rider(2, earliest_time="12:05:00", latest_time="13:00:00")
            self.assertEqual(rm._effective_overlap_minutes([a, b]), 0)

    def test_no_overlap_beyond_grace(self):
        with patch_config(ALLOW_TOUCHING=True, OVERLAP_GRACE_MIN=10):
            a = make_rider(1, earliest_time="08:00:00", latest_time="09:00:00")
            b = make_rider(2, earliest_time="12:00:00", latest_time="13:00:00")
            self.assertEqual(rm._effective_overlap_minutes([a, b]), -1)


class TestIsValidGroup(unittest.TestCase):
    def test_valid_pair(self):
        a = make_rider(1, earliest_time="10:00:00", latest_time="12:00:00", bags_no=1)
        b = make_rider(2, earliest_time="11:00:00", latest_time="13:00:00", bags_no=1)
        self.assertTrue(rm._is_valid_group([a, b]))

    def test_too_few_members(self):
        self.assertFalse(rm._is_valid_group([make_rider(1)]))

    def test_too_many_members(self):
        with patch_config(MAX_GROUP_SIZE=5):
            riders = [
                make_rider(i, earliest_time="10:00:00", latest_time="14:00:00", bags_no=0)
                for i in range(6)
            ]
            self.assertFalse(rm._is_valid_group(riders))

    def test_bag_capacity_exceeded(self):
        with patch_config(MAX_TOTAL_BAGS=10, MAX_LARGE_BAGS=5, LARGE_BAG_MULTIPLIER=2):
            a = make_rider(1, earliest_time="10:00:00", latest_time="12:00:00", bags_no=6)
            b = make_rider(2, earliest_time="11:00:00", latest_time="13:00:00", bags_no=6)
            self.assertFalse(rm._is_valid_group([a, b]))


class TestPickupTime(unittest.TestCase):
    def test_to_airport_is_15_before_overlap_end(self):
        # overlap = 11:00 .. 12:00 ; TO airport → 15 min before end = 11:45
        a = make_rider(1, to_airport=True, earliest_time="10:00:00", latest_time="12:00:00")
        b = make_rider(2, to_airport=True, earliest_time="11:00:00", latest_time="13:00:00")
        chosen = rm.pickup_datetime_for_group([a, b])
        self.assertEqual((chosen.hour, chosen.minute), (11, 45))

    def test_from_airport_is_15_after_overlap_start(self):
        # overlap start = 11:00 ; FROM airport → 15 min after start = 11:15
        a = make_rider(1, to_airport=False, earliest_time="10:00:00", latest_time="12:00:00")
        b = make_rider(2, to_airport=False, earliest_time="11:00:00", latest_time="13:00:00")
        chosen = rm.pickup_datetime_for_group([a, b])
        self.assertEqual((chosen.hour, chosen.minute), (11, 15))


class TestRefreshTimes(unittest.TestCase):
    def test_skips_connect_groups(self):
        a = make_rider(1, to_airport=True, earliest_time="10:00:00", latest_time="12:00:00")
        b = make_rider(2, to_airport=True, earliest_time="11:00:00", latest_time="13:00:00")
        m = rm.Match(riders=[a, b], suggested_time_iso="bogus", terminal="1", ride_type="Connect")
        self.assertEqual(rm.refresh_match_suggested_times([m], skip_connect=True), 0)
        self.assertEqual(m.suggested_time_iso, "bogus")

    def test_updates_normal_groups(self):
        a = make_rider(1, to_airport=True, earliest_time="10:00:00", latest_time="12:00:00")
        b = make_rider(2, to_airport=True, earliest_time="11:00:00", latest_time="13:00:00")
        m = rm.Match(riders=[a, b], suggested_time_iso="bogus", terminal="1")
        self.assertEqual(rm.refresh_match_suggested_times([m], skip_connect=True), 1)
        self.assertNotEqual(m.suggested_time_iso, "bogus")


class TestMatchBucket(unittest.TestCase):
    def test_two_overlapping_riders_form_a_group(self):
        with patch_config(TERMINAL_MODE="slack"):
            a = make_rider(1, earliest_time="10:00:00", latest_time="12:00:00", bags_no=1)
            b = make_rider(2, earliest_time="11:00:00", latest_time="13:00:00", bags_no=1)
            matches, unmatched, _diag = rm.match_bucket([a, b], bucket_key="TO LAX | POMONA")
            self.assertEqual(len(matches), 1)
            self.assertEqual(len(matches[0].riders), 2)
            self.assertEqual(unmatched, [])

    def test_non_overlapping_riders_stay_unmatched(self):
        with patch_config(ALLOW_TOUCHING=False, TERMINAL_MODE="slack"):
            a = make_rider(1, earliest_time="08:00:00", latest_time="09:00:00")
            b = make_rider(2, earliest_time="20:00:00", latest_time="21:00:00")
            matches, unmatched, _diag = rm.match_bucket([a, b], bucket_key="TO LAX | POMONA")
            self.assertEqual(matches, [])
            self.assertEqual(len(unmatched), 2)

    def test_single_rider_is_unmatched(self):
        a = make_rider(1)
        matches, unmatched, diag = rm.match_bucket([a], bucket_key="TO LAX | POMONA")
        self.assertEqual(matches, [])
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(diag[a.flight_id]["reason"], "singleton_bucket")


class TestOntPostProcess(unittest.TestCase):
    def test_noop_when_no_unmatched(self):
        matches, unmatched = rm._ont_post_process_unmatched([], [])
        self.assertEqual(matches, [])
        self.assertEqual(unmatched, [])

    def test_noop_when_no_ont_unmatched(self):
        lax_rider = make_rider(1, airport="LAX")
        matches, unmatched = rm._ont_post_process_unmatched([], [lax_rider])
        self.assertEqual(unmatched, [lax_rider])


if __name__ == "__main__":
    unittest.main()

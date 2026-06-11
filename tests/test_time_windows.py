"""Tests for shared time-window normalization."""

import unittest
from datetime import datetime

import time_windows
from tests.helpers import make_rider


class TestTimeWindows(unittest.TestCase):
    def test_rider_interval_keeps_same_day_window(self):
        rider = make_rider(1, date="2026-05-12", earliest_time="10:00:00", latest_time="12:00:00")

        start, end = time_windows.rider_interval(rider)

        self.assertEqual(start, datetime(2026, 5, 12, 10, 0))
        self.assertEqual(end, datetime(2026, 5, 12, 12, 0))

    def test_rider_interval_rolls_overnight_end_to_next_day(self):
        rider = make_rider(1, date="2026-05-12", earliest_time="23:45:00", latest_time="00:30:00")

        start, end = time_windows.rider_interval(rider)

        self.assertEqual(start, datetime(2026, 5, 12, 23, 45))
        self.assertEqual(end, datetime(2026, 5, 13, 0, 30))

    def test_common_window_returns_normalized_group_overlap(self):
        riders = [
            make_rider(1, date="2026-05-12", earliest_time="22:30:00", latest_time="02:30:00"),
            make_rider(2, date="2026-05-12", earliest_time="23:15:00", latest_time="01:45:00"),
            make_rider(3, date="2026-05-12", earliest_time="23:40:00", latest_time="01:10:00"),
        ]

        start, end = time_windows.common_window(riders)

        self.assertEqual(start, datetime(2026, 5, 12, 23, 40))
        self.assertEqual(end, datetime(2026, 5, 13, 1, 10))

    def test_common_window_or_none_rejects_non_overlapping_windows(self):
        riders = [
            make_rider(1, earliest_time="22:00:00", latest_time="23:00:00"),
            make_rider(2, earliest_time="23:30:00", latest_time="00:30:00"),
        ]

        self.assertIsNone(time_windows.common_window_or_none(riders, allow_touching=False))


if __name__ == "__main__":
    unittest.main()

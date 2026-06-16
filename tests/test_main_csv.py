"""Tests for dry-run match CSV output."""

import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ruleMatching import Match
from tests.helpers import make_rider


def _load_main_module():
    if "main" in sys.modules:
        return sys.modules["main"]
    with mock.patch("supabase.create_client", return_value=object()):
        import main

    return main


class TestWriteMatchesCsv(unittest.TestCase):
    def test_cross_midnight_group_window_uses_shared_normalized_overlap(self):
        main = _load_main_module()
        riders = [
            make_rider(1, date="2026-05-12", earliest_time="23:45:00", latest_time="00:30:00"),
            make_rider(2, date="2026-05-12", earliest_time="23:55:00", latest_time="00:20:00"),
        ]
        match = Match(
            riders=riders,
            suggested_time_iso="2026-05-13T00:05:00",
            terminal="1",
            bucket_key="TO LAX | POMONA",
        )

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "matches.csv"

            main._write_matches_csv([match], riders, str(csv_path))

            with csv_path.open() as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2026-05-13")
        self.assertEqual(rows[0]["suggested_time"], "00:05:00")
        self.assertEqual(rows[0]["earliest_time"], "23:55:00")
        self.assertEqual(rows[0]["latest_time"], "00:20:00")


if __name__ == "__main__":
    unittest.main()

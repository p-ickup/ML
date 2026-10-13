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

    def test_creates_missing_output_directory(self):
        # matches/ is gitignored, so a fresh checkout has no output directory yet.
        main = _load_main_module()
        riders = [make_rider(1), make_rider(2)]
        match = Match(riders=riders, suggested_time_iso="2026-05-12T10:00:00", terminal="1")

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "matches" / "matches.csv"

            main._write_matches_csv([match], riders, str(csv_path))

            self.assertTrue(csv_path.exists())


class TestWriteUnmatchedWithReasons(unittest.TestCase):
    def test_creates_missing_output_directory(self):
        main = _load_main_module()

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "matches" / "unmatched.csv"

            main._write_unmatched_with_reasons([make_rider(1)], {}, str(csv_path))

            with csv_path.open() as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reason"], "unknown")


if __name__ == "__main__":
    unittest.main()

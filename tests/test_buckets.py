"""Tests for bucket assignment in buckets.py."""

import unittest

import buckets
from tests.helpers import make_rider


class TestMakeBuckets(unittest.TestCase):
    def test_direction_and_airport_in_key(self):
        to_lax = make_rider(1, to_airport=True, airport="LAX")
        from_ont = make_rider(2, to_airport=False, airport="ONT")

        result = buckets.make_buckets([to_lax, from_ont])

        self.assertIn("TO LAX | POMONA", result)
        self.assertIn("FROM ONT | POMONA", result)

    def test_riders_split_by_bucket(self):
        riders = [
            make_rider(1, to_airport=True, airport="LAX"),
            make_rider(2, to_airport=True, airport="LAX"),
            make_rider(3, to_airport=False, airport="LAX"),
        ]

        result = buckets.make_buckets(riders)

        self.assertEqual(len(result["TO LAX | POMONA"]), 2)
        self.assertEqual(len(result["FROM LAX | POMONA"]), 1)

    def test_unknown_school_groups_into_all(self):
        rider = make_rider(1, school="SOME_UNLISTED_SCHOOL")
        result = buckets.make_buckets([rider])
        self.assertIn("TO LAX | ALL", result)

    def test_bucket_names_sorted(self):
        riders = [
            make_rider(1, to_airport=True, airport="ONT"),
            make_rider(2, to_airport=True, airport="LAX"),
        ]
        names = buckets.bucket_names(buckets.make_buckets(riders))
        self.assertEqual(names, sorted(names))


if __name__ == "__main__":
    unittest.main()

"""Tests for Connect shuttle policy helpers (connect_policy.py)."""

import unittest

import connect_policy as cp
from tests.helpers import make_rider, patch_config


class TestConnectEnabled(unittest.TestCase):
    def test_enabled_when_any_list_nonempty(self):
        with patch_config(CONNECT_DEPARTURE=["LAX"], CONNECT_ARRIVAL=[]):
            self.assertTrue(cp.connect_enabled())

    def test_disabled_when_both_empty(self):
        with patch_config(CONNECT_DEPARTURE=[], CONNECT_ARRIVAL=[]):
            self.assertFalse(cp.connect_enabled())


class TestConnectTiers(unittest.TestCase):
    def test_larger_tier_first(self):
        with patch_config(CONNECT_SIZE1=[6, 12], CONNECT_SIZE2=[12, 24]):
            tiers = cp.connect_tiers()
            self.assertEqual(tiers[0], (12, 24))
            self.assertEqual(tiers[1], (6, 12))

    def test_fits_connect_size(self):
        with patch_config(CONNECT_SIZE1=[6, 12], CONNECT_SIZE2=[12, 24]):
            self.assertTrue(cp.fits_connect_size(6))
            self.assertTrue(cp.fits_connect_size(20))
            self.assertFalse(cp.fits_connect_size(5))
            self.assertFalse(cp.fits_connect_size(25))

    def test_search_bounds(self):
        with patch_config(CONNECT_SIZE1=[6, 12], CONNECT_SIZE2=[12, 24]):
            self.assertEqual(cp.connect_search_bounds(), (6, 24))


class TestScope(unittest.TestCase):
    def test_allowed_airports_directional(self):
        with patch_config(CONNECT_DEPARTURE=["lax", "ONT"], CONNECT_ARRIVAL=["LAX"]):
            self.assertEqual(cp.allowed_airports(to_airport=True), ["LAX", "ONT"])
            self.assertEqual(cp.allowed_airports(to_airport=False), ["LAX"])

    def test_rider_in_connect_scope(self):
        with patch_config(CONNECT_DEPARTURE=["LAX"], CONNECT_ARRIVAL=[]):
            in_scope = make_rider(1, airport="LAX", to_airport=True)
            out_scope = make_rider(2, airport="ONT", to_airport=True)
            inbound = make_rider(3, airport="LAX", to_airport=False)
            self.assertTrue(cp.rider_in_connect_scope(in_scope))
            self.assertFalse(cp.rider_in_connect_scope(out_scope))
            self.assertFalse(cp.rider_in_connect_scope(inbound))

    def test_bucket_key_in_connect_scope(self):
        with patch_config(CONNECT_DEPARTURE=["LAX"], CONNECT_ARRIVAL=[]):
            self.assertTrue(cp.bucket_key_in_connect_scope("TO LAX | POMONA"))
            self.assertFalse(cp.bucket_key_in_connect_scope("FROM LAX | POMONA"))
            self.assertFalse(cp.bucket_key_in_connect_scope("TO ONT | POMONA"))
            self.assertFalse(cp.bucket_key_in_connect_scope(None))


if __name__ == "__main__":
    unittest.main()

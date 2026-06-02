"""Tests for normalization helpers in rider_data.py."""

import unittest

from rider_data import normalize_airport, normalize_terminal


class TestNormalizeTerminal(unittest.TestCase):
    def test_numeric_terminal(self):
        self.assertEqual(normalize_terminal("Terminal 2"), "2")
        self.assertEqual(normalize_terminal("2"), "2")

    def test_single_letter(self):
        self.assertEqual(normalize_terminal("B"), "B")
        self.assertEqual(normalize_terminal(" b "), "B")

    def test_international_variants(self):
        self.assertEqual(normalize_terminal("TBIT"), "INTL")
        self.assertEqual(normalize_terminal("Tom Bradley International"), "INTL")
        self.assertEqual(normalize_terminal("intl"), "INTL")

    def test_missing(self):
        self.assertEqual(normalize_terminal(None), "UNKNOWN")
        self.assertEqual(normalize_terminal(""), "UNKNOWN")


class TestNormalizeAirport(unittest.TestCase):
    def test_uppercases_and_strips(self):
        self.assertEqual(normalize_airport(" lax "), "LAX")
        self.assertEqual(normalize_airport("ont"), "ONT")

    def test_missing(self):
        self.assertEqual(normalize_airport(None), "UNKNOWN")


if __name__ == "__main__":
    unittest.main()

"""Tests for configurable airport subsidy minimums."""

import sys
import unittest
from unittest import mock

from ruleMatching import Match
from tests.helpers import make_rider, patch_config


def _load_main_module():
    if "main" in sys.modules:
        return sys.modules["main"]
    with mock.patch("supabase.create_client", return_value=object()):
        import main

    return main


def _match(airport: str, size: int) -> Match:
    riders = [
        make_rider(
            flight_id,
            airport=airport,
            to_airport=True,
            date="2026-05-12",
            school="POMONA",
        )
        for flight_id in range(1, size + 1)
    ]
    return Match(
        riders=riders,
        suggested_time_iso="2026-05-12T11:45:00",
        terminal="1",
        bucket_key=f"TO {airport} | POMONA",
    )


class TestConfigurableSubsidyMinimums(unittest.TestCase):
    def setUp(self):
        self.main = _load_main_module()

    def test_default_lax_minimum_is_three(self):
        below = _match("LAX", 2)
        qualifying = _match("LAX", 3)

        self.main.apply_group_subsidy([below, qualifying])

        self.assertFalse(below.group_subsidy)
        self.assertTrue(qualifying.group_subsidy)
        self.assertTrue(all(not rider.subsidized for rider in below.riders))
        self.assertTrue(all(rider.subsidized for rider in qualifying.riders))

    def test_default_ont_minimum_is_two(self):
        below = _match("ONT", 1)
        qualifying = _match("ONT", 2)

        self.main.apply_group_subsidy([below, qualifying])

        self.assertFalse(below.group_subsidy)
        self.assertTrue(qualifying.group_subsidy)

    def test_config_override_changes_each_airport_independently(self):
        lax_pair = _match("LAX", 2)
        ont_pair = _match("ONT", 2)

        with patch_config(SUBSIDY_MIN_GROUP_SIZE={"LAX": 2, "ONT": 3}):
            self.main.apply_group_subsidy([lax_pair, ont_pair])

        self.assertTrue(lax_pair.group_subsidy)
        self.assertFalse(ont_pair.group_subsidy)


if __name__ == "__main__":
    unittest.main()

"""Tests for voucher parsing, coverage, and assignment (vouchers.py)."""

import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd
import vouchers
from ruleMatching import Match
from tests.helpers import make_rider, patch_config


VOUCHER_COLUMNS = "Date (start),Date (end),Contingency,voucher_link,TO_AIRPORT,AIRPORT,USED"


def _used(df, row=0):
    return vouchers._parse_csv_bool(df.loc[row, "USED"])


def _subsidized_match(riders, iso, ride_type=None):
    m = Match(riders=riders, suggested_time_iso=iso, terminal="1", ride_type=ride_type)
    m.group_subsidy = True
    return m


class TestParseCsvBool(unittest.TestCase):
    def test_truthy(self):
        for value in ["true", "TRUE", "1", "yes", "t", True]:
            self.assertIs(vouchers._parse_csv_bool(value), True, value)

    def test_falsy(self):
        for value in ["false", "FALSE", "0", "no", "f", "", False, None]:
            self.assertIs(vouchers._parse_csv_bool(value), False, value)

    def test_string_false_is_not_used(self):
        # Regression: pandas astype(bool) treats "False" as True; we must not.
        self.assertIs(vouchers._parse_csv_bool("False"), False)


class TestRideDateCovered(unittest.TestCase):
    def test_all_dates_when_not_explicit(self):
        with patch_config(COVERED_DATES_EXPLICIT=False):
            self.assertTrue(vouchers.is_ride_date_covered(date(2026, 1, 1), to_airport=True))

    def test_outbound_list(self):
        with patch_config(
            COVERED_DATES_EXPLICIT=True,
            COVERED_DATES_OUTBOUND=["05-12"],
            COVERED_DATES_INBOUND=[],
        ):
            self.assertTrue(vouchers.is_ride_date_covered(date(2026, 5, 12), to_airport=True))
            self.assertFalse(vouchers.is_ride_date_covered(date(2026, 5, 13), to_airport=True))

    def test_inbound_list(self):
        with patch_config(
            COVERED_DATES_EXPLICIT=True,
            COVERED_DATES_OUTBOUND=[],
            COVERED_DATES_INBOUND=["03-20"],
        ):
            self.assertTrue(vouchers.is_ride_date_covered(date(2026, 3, 20), to_airport=False))
            self.assertFalse(vouchers.is_ride_date_covered(date(2026, 3, 20), to_airport=True))


class VoucherPoolTestCase(unittest.TestCase):
    """Base class providing a temp voucher CSV per test."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def write_pool(self, rows):
        path = self.tmp_path / "pool.csv"
        path.write_text("\n".join([VOUCHER_COLUMNS] + rows) + "\n")
        return str(path)


class TestLoadVoucherPool(VoucherPoolTestCase):
    def test_normalizes_columns(self):
        path = self.write_pool(["May 10,May 20,False,V1,True,lax,False"])
        df = vouchers.load_voucher_pool(path)
        self.assertEqual(df.loc[0, "AIRPORT"], "LAX")
        self.assertFalse(bool(df.loc[0, "USED"]))
        self.assertFalse(bool(df.loc[0, "Contingency"]))
        self.assertEqual(df.loc[0, "start_date"].month, 5)

    def test_parses_month_day_with_surrounding_whitespace(self):
        path = self.write_pool(["June 29, July 1,False,V1,False,ONT,False"])
        df = vouchers.load_voucher_pool(path)
        self.assertEqual(df.loc[0, "start_date"].month, 6)
        self.assertEqual(df.loc[0, "start_date"].day, 29)
        self.assertEqual(df.loc[0, "end_date"].month, 7)
        self.assertEqual(df.loc[0, "end_date"].day, 1)


class TestAssignVouchers(VoucherPoolTestCase):
    def test_group_voucher_assigned_and_marked_used(self):
        with patch_config(COVERED_DATES_EXPLICIT=False):
            path = self.write_pool(["May 10,May 20,False,GROUP-1,True,LAX,False"])
            a = make_rider(1, airport="LAX", to_airport=True)
            b = make_rider(2, airport="LAX", to_airport=True)
            m = _subsidized_match([a, b], "2026-05-12T11:45:00")

            vouchers.assign_vouchers([m], voucher_csv_path=path, dry_run=False)

            self.assertEqual(m.group_voucher, "GROUP-1")
            self.assertEqual(a.group_voucher, "GROUP-1")
            self.assertTrue(_used(pd.read_csv(path)))

    def test_connect_group_gets_no_voucher(self):
        with patch_config(COVERED_DATES_EXPLICIT=False):
            path = self.write_pool(["May 10,May 20,False,GROUP-1,True,LAX,False"])
            riders = [make_rider(i, airport="LAX", to_airport=True) for i in range(8)]
            m = _subsidized_match(riders, "2026-05-12T11:45:00", ride_type="Connect")

            vouchers.assign_vouchers([m], voucher_csv_path=path, dry_run=False)

            self.assertIsNone(m.group_voucher)
            self.assertFalse(_used(pd.read_csv(path)))

    def test_non_subsidized_group_gets_no_voucher(self):
        with patch_config(COVERED_DATES_EXPLICIT=False):
            path = self.write_pool(["May 10,May 20,False,GROUP-1,True,LAX,False"])
            a = make_rider(1, airport="LAX", to_airport=True)
            b = make_rider(2, airport="LAX", to_airport=True)
            m = Match(riders=[a, b], suggested_time_iso="2026-05-12T11:45:00", terminal="1")

            vouchers.assign_vouchers([m], voucher_csv_path=path, dry_run=False)

            self.assertIsNone(m.group_voucher)
            self.assertFalse(_used(pd.read_csv(path)))

    def test_contingency_voucher_per_inbound_rider(self):
        # Each inbound rider consumes its own contingency voucher, so the pool
        # needs one contingency row per rider.
        with patch_config(COVERED_DATES_EXPLICIT=False):
            path = self.write_pool(
                [
                    "May 10,May 20,False,GROUP-1,False,LAX,False",
                    "May 10,May 20,True,CONT-1,False,LAX,False",
                    "May 10,May 20,True,CONT-2,False,LAX,False",
                ]
            )
            a = make_rider(1, airport="LAX", to_airport=False)
            b = make_rider(2, airport="LAX", to_airport=False)
            m = _subsidized_match([a, b], "2026-05-12T11:15:00")

            vouchers.assign_vouchers([m], voucher_csv_path=path, dry_run=False)

            self.assertEqual(
                {a.contingency_voucher, b.contingency_voucher}, {"CONT-1", "CONT-2"}
            )

    def test_contingency_voucher_runs_out(self):
        # Only one contingency voucher for two inbound riders → second gets none.
        with patch_config(COVERED_DATES_EXPLICIT=False):
            path = self.write_pool(
                [
                    "May 10,May 20,True,CONT-1,False,LAX,False",
                ]
            )
            a = make_rider(1, airport="LAX", to_airport=False)
            b = make_rider(2, airport="LAX", to_airport=False)
            m = _subsidized_match([a, b], "2026-05-12T11:15:00")

            vouchers.assign_vouchers([m], voucher_csv_path=path, dry_run=False)

            assigned = [a.contingency_voucher, b.contingency_voucher]
            self.assertEqual(assigned.count("CONT-1"), 1)
            self.assertEqual(assigned.count(None), 1)

    def test_dry_run_does_not_mutate_source(self):
        with patch_config(COVERED_DATES_EXPLICIT=False):
            path = self.write_pool(["May 10,May 20,False,GROUP-1,True,LAX,False"])
            original = (self.tmp_path / "pool.csv").read_text()
            a = make_rider(1, airport="LAX", to_airport=True)
            b = make_rider(2, airport="LAX", to_airport=True)
            m = _subsidized_match([a, b], "2026-05-12T11:45:00")

            vouchers.assign_vouchers([m], voucher_csv_path=path, dry_run=True)

            # Source untouched; dry-run copy created and updated.
            self.assertEqual((self.tmp_path / "pool.csv").read_text(), original)
            dry_copy = self.tmp_path / "pool.csv.dryrun.csv"
            self.assertTrue(dry_copy.exists())
            self.assertTrue(_used(pd.read_csv(dry_copy)))


if __name__ == "__main__":
    unittest.main()

"""Tests for importing voucher CSVs into public.Vouchers."""

import tempfile
import unittest
from pathlib import Path

import import_vouchers


REPO_ROOT = Path(__file__).resolve().parents[1]
SPRING_BREAK_CSV = REPO_ROOT / "vouchers" / "SpringBreak.csv"


class TestBuildVoucherRows(unittest.TestCase):
    def test_spring_break_csv_maps_to_vouchers_schema(self):
        rows = import_vouchers.build_voucher_rows(
            str(SPRING_BREAK_CSV),
            import_batch_id="11111111-1111-1111-1111-111111111111",
        )

        self.assertEqual(len(rows), 4400)
        first = rows[0]
        self.assertEqual(first["date_start_label"], "March 12")
        self.assertEqual(first["date_end_label"], "March 15")
        self.assertEqual(first["start_date"], "2026-03-12")
        self.assertEqual(first["end_date"], "2026-03-15")
        self.assertEqual(first["airport"], "ONT")
        self.assertTrue(first["to_airport"])
        self.assertFalse(first["contingency"])
        self.assertFalse(first["used"])
        self.assertIsNone(first["used_at"])
        self.assertIsNone(first["used_by_run_id"])
        self.assertIsNone(first["assigned_ride_id"])
        self.assertIsNone(first["assigned_flight_id"])
        self.assertEqual(first["import_batch_id"], "11111111-1111-1111-1111-111111111111")

    def test_rejects_duplicate_voucher_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dupes.csv"
            path.write_text(
                "\n".join(
                    [
                        "Date (start),Date (end),Contingency,voucher_link,TO_AIRPORT,AIRPORT,USED,start_date,end_date",
                        "March 12,March 15,False,https://r.uber.com/ONE,True,ONT,False,2026-03-12,2026-03-15",
                        "March 12,March 15,False,https://r.uber.com/ONE,True,ONT,False,2026-03-12,2026-03-15",
                    ]
                )
                + "\n"
            )

            with self.assertRaises(import_vouchers.VoucherImportError):
                import_vouchers.build_voucher_rows(str(path))

    def test_rejects_preserving_legacy_used_without_run_id(self):
        with self.assertRaises(import_vouchers.VoucherImportError):
            import_vouchers.build_voucher_rows(
                str(SPRING_BREAK_CSV),
                import_as_available=False,
            )


class TestImportVoucherCsv(unittest.TestCase):
    def test_inserts_only_missing_vouchers_in_batches(self):
        class FakeQuery:
            def __init__(self, data=None):
                self.data = data or []

            def in_(self, _column, values):
                self.values = values
                return self

            def execute(self):
                return self

        class FakeTable:
            def __init__(self, parent):
                self.parent = parent

            def select(self, _columns):
                return FakeQuery(
                    [{"voucher_link": link} for link in self.parent.existing_links]
                )

            def insert(self, rows):
                self.parent.inserts.append(rows)
                return FakeQuery()

        class FakeSupabase:
            def __init__(self):
                self.tables = []
                self.inserts = []
                self.existing_links = {
                    "https://r.uber.com/RPGAEMAKVQG",
                    "https://r.uber.com/RBWUMRARYNK",
                }

            def table(self, name):
                self.tables.append(name)
                return FakeTable(self)

        fake = FakeSupabase()
        result = import_vouchers.import_voucher_csv(
            fake,
            str(SPRING_BREAK_CSV),
            import_batch_id="11111111-1111-1111-1111-111111111111",
            batch_size=2000,
        )

        self.assertEqual(result["table"], "Vouchers")
        self.assertEqual(result["rows"], 4400)
        self.assertEqual(result["inserted_rows"], 4398)
        self.assertEqual(result["existing_rows"], 2)
        self.assertEqual([len(batch) for batch in fake.inserts], [2000, 2000, 398])
        inserted_links = {row["voucher_link"] for batch in fake.inserts for row in batch}
        self.assertFalse(fake.existing_links & inserted_links)


if __name__ == "__main__":
    unittest.main()

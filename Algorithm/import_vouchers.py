"""Import voucher CSV files into the production Vouchers table.

The importer only inserts voucher links that are not already in the database.
Existing rows are left untouched so re-importing a CSV cannot reset production
consumption or assignment audit fields.

The legacy CSV ``USED`` column is not used as production consumption state.
Production consumption is recorded transactionally by ``commit_matching_run``
with ``used_at``, ``used_by_run_id``, and assignment fields. Imported vouchers
therefore default to available unless an explicit caller chooses otherwise.
"""

from __future__ import annotations

import argparse
import csv
import os
import uuid
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv
from supabase import create_client
from vouchers import _parse_csv_bool


REQUIRED_COLUMNS = {
    "Date (start)",
    "Date (end)",
    "Contingency",
    "voucher_link",
    "TO_AIRPORT",
    "AIRPORT",
    "USED",
    "start_date",
    "end_date",
}

VOUCHERS_TABLE = "Vouchers"


class VoucherImportError(ValueError):
    """Raised when a voucher CSV cannot be safely imported."""


def _parse_iso_date(value: str, *, row_number: int, column: str) -> date:
    text = (value or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise VoucherImportError(
            f"Row {row_number}: {column} must be an ISO date, got {text!r}."
        ) from exc


def _validate_headers(fieldnames: Optional[Sequence[str]]) -> None:
    present = set(fieldnames or [])
    missing = sorted(REQUIRED_COLUMNS - present)
    if missing:
        raise VoucherImportError(f"Voucher CSV is missing required columns: {missing}")


def build_voucher_rows(
    csv_path: str,
    *,
    import_batch_id: Optional[str] = None,
    import_as_available: bool = True,
) -> List[Dict[str, Any]]:
    """Validate and map a voucher CSV into rows for public.Vouchers."""
    batch_id = import_batch_id or str(uuid.uuid4())
    rows: List[Dict[str, Any]] = []
    seen_links: set[str] = set()

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        _validate_headers(reader.fieldnames)

        for row_number, raw in enumerate(reader, start=2):
            voucher_link = (raw.get("voucher_link") or "").strip()
            if not voucher_link:
                raise VoucherImportError(f"Row {row_number}: voucher_link is required.")
            if voucher_link in seen_links:
                raise VoucherImportError(f"Row {row_number}: duplicate voucher_link {voucher_link!r}.")
            seen_links.add(voucher_link)

            airport = (raw.get("AIRPORT") or "").strip().upper()
            if not airport:
                raise VoucherImportError(f"Row {row_number}: AIRPORT is required.")

            start_date = _parse_iso_date(raw.get("start_date", ""), row_number=row_number, column="start_date")
            end_date = _parse_iso_date(raw.get("end_date", ""), row_number=row_number, column="end_date")
            if end_date < start_date:
                raise VoucherImportError(f"Row {row_number}: end_date cannot be before start_date.")

            legacy_used = _parse_csv_bool(raw.get("USED"))
            if legacy_used and not import_as_available:
                raise VoucherImportError(
                    f"Row {row_number}: preserving USED=true is not supported without a MatchingRuns run_id."
                )

            rows.append(
                {
                    "date_start_label": (raw.get("Date (start)") or "").strip(),
                    "date_end_label": (raw.get("Date (end)") or "").strip(),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "contingency": _parse_csv_bool(raw.get("Contingency")),
                    "voucher_link": voucher_link,
                    "to_airport": _parse_csv_bool(raw.get("TO_AIRPORT")),
                    "airport": airport,
                    "used": False,
                    "used_at": None,
                    "used_by_run_id": None,
                    "assigned_ride_id": None,
                    "assigned_flight_id": None,
                    "import_batch_id": batch_id,
                }
            )

    if not rows:
        raise VoucherImportError("Voucher CSV contains no voucher rows.")
    return rows


def import_voucher_csv(
    sb: Any,
    csv_path: str,
    *,
    import_batch_id: Optional[str] = None,
    batch_size: int = 500,
) -> Dict[str, Any]:
    """Insert voucher CSV rows that are not already in public.Vouchers."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1.")

    rows = build_voucher_rows(csv_path, import_batch_id=import_batch_id)
    existing_links: set[str] = set()
    links = [row["voucher_link"] for row in rows]
    for start in range(0, len(links), batch_size):
        chunk = links[start : start + batch_size]
        existing = (
            sb.table(VOUCHERS_TABLE)
            .select("voucher_link")
            .in_("voucher_link", chunk)
            .execute()
        )
        existing_links.update(row["voucher_link"] for row in (existing.data or []))

    rows_to_insert = [row for row in rows if row["voucher_link"] not in existing_links]
    for start in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[start : start + batch_size]
        if batch:
            sb.table(VOUCHERS_TABLE).insert(batch).execute()

    return {
        "table": VOUCHERS_TABLE,
        "rows": len(rows),
        "inserted_rows": len(rows_to_insert),
        "existing_rows": len(existing_links),
        "import_batch_id": rows[0]["import_batch_id"],
    }


def _client_from_env() -> Any:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_KEY")
    if not url or not key:
        raise VoucherImportError("SUPABASE_URL and SUPABASE_SECRET_KEY are required for --commit.")
    return create_client(url, key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import voucher CSV rows into public.Vouchers.")
    parser.add_argument("csv_path", help="Path to voucher CSV")
    parser.add_argument("--batch-id", default=None, help="Optional UUID to store in import_batch_id")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per Supabase lookup/insert")
    parser.add_argument("--commit", action="store_true", help="Write rows to Supabase. Default is validation only.")
    args = parser.parse_args()

    csv_path = str(Path(args.csv_path))
    rows = build_voucher_rows(csv_path, import_batch_id=args.batch_id)
    print(f"Validated {len(rows)} voucher rows for public.{VOUCHERS_TABLE}.")
    print(f"import_batch_id: {rows[0]['import_batch_id']}")

    if args.commit:
        result = import_voucher_csv(
            _client_from_env(),
            csv_path,
            import_batch_id=rows[0]["import_batch_id"],
            batch_size=args.batch_size,
        )
        print(
            f"Imported {result['inserted_rows']} new rows into public.{result['table']} "
            f"({result['existing_rows']} already existed; {result['rows']} validated)."
        )
    else:
        print("Dry run only. Re-run with --commit to write to Supabase.")


if __name__ == "__main__":
    main()

"""
Temporary maintenance: mark LAX 2-person groups as subsidized and attach unused vouchers.

- Consider rides whose Rides.ride_date is between today and today + N days (default 20).
- Only groups with exactly two Matches rows, all flights at LAX, uber_type != Connect.
- Only when both riders are Pomona students (Users.school == POMONA).
- Respects USED=True (pool row is skipped). Also never assigns the same voucher_link
  to two different rides in one run, even if the CSV has duplicate URLs.
- Contingency: same rules as vouchers.py for inbound legs only.

Run from Algorithm/:

    python3 temp_subsidize_lax_pairs.py --voucher-csv ../vouchers/Summer.csv
    python3 temp_subsidize_lax_pairs.py --voucher-csv ../vouchers/Summer.csv --confirm

Default is dry-run (no DB writes; voucher changes written to *.dryrun.csv only).
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

import config
import pandas as pd
import storage
from supabase import Client, create_client
from vouchers import load_voucher_pool

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if hasattr(value, "date") and not isinstance(value, date):
        try:
            return value.date()
        except Exception:
            pass
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    if "T" in s:
        s = s.split("T")[0]
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        return None


def _fetch_rides(sb: Client, start: date, end: date) -> Dict[int, Dict[str, Any]]:
    resp = (
        sb.table("Rides")
        .select("*")
        .gte("ride_date", start.isoformat())
        .lte("ride_date", end.isoformat())
        .execute()
    )
    return {int(row["ride_id"]): row for row in (resp.data or [])}


def _fetch_matches(sb: Client, ride_ids: Sequence[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(0, len(ride_ids), 100):
        chunk = list(ride_ids[i : i + 100])
        if not chunk:
            continue
        resp = sb.table("Matches").select("*").in_("ride_id", chunk).execute()
        out.extend(resp.data or [])
    return out


def _fetch_user_schools(sb: Client, user_ids: Sequence[str]) -> Dict[str, str]:
    """user_id -> school string."""
    clean = sorted({uid for uid in user_ids if uid})
    out: Dict[str, str] = {}
    for i in range(0, len(clean), 100):
        chunk = clean[i : i + 100]
        if not chunk:
            continue
        resp = sb.table("Users").select("user_id,school").in_("user_id", chunk).execute()
        for row in resp.data or []:
            out[str(row["user_id"])] = str(row.get("school") or "")
    return out


def _all_pomona(rows: List[Dict[str, Any]], user_schools: Dict[str, str]) -> bool:
    for r in rows:
        uid = str(r.get("user_id") or "")
        school = (user_schools.get(uid) or "").strip().upper()
        if school != "POMONA":
            return False
    return True


def _fetch_flights(sb: Client, flight_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for i in range(0, len(flight_ids), 100):
        chunk = list(flight_ids[i : i + 100])
        if not chunk:
            continue
        resp = (
            sb.table("Flights")
            .select("flight_id,airport,to_airport")
            .in_("flight_id", chunk)
            .execute()
        )
        for row in resp.data or []:
            out[int(row["flight_id"])] = row
    return out


def _group_by_ride(matches: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    g: Dict[int, List[Dict[str, Any]]] = {}
    for row in matches:
        rid = row.get("ride_id")
        if rid is None:
            continue
        g.setdefault(int(rid), []).append(row)
    return g


def _airport_lax(flight: Optional[Dict[str, Any]]) -> bool:
    if not flight:
        return False
    return str(flight.get("airport") or "").strip().upper() == "LAX"


def _take_group_voucher_idx(
    df: pd.DataFrame,
    airport: str,
    ride_date: date,
    links_taken_this_run: Set[str],
) -> Optional[Any]:
    """
    First unused pool row: USED=False, valid airport/dates, voucher_link not already
    assigned to another ride in this script run (same URL must not go to two groups).
    """
    mask = (
        (~df["USED"])
        & (df["AIRPORT"] == airport)
        & (~df["Contingency"])
        & (df["start_date"] <= ride_date)
        & (df["end_date"] >= ride_date)
    )
    for idx in df.loc[mask].index:
        link = str(df.loc[idx, "voucher_link"]).strip()
        if not link or link in links_taken_this_run:
            continue
        return idx
    return None


def _take_contingency_voucher_idx(
    df: pd.DataFrame,
    airport: str,
    ride_date: date,
    links_taken_this_run: Set[str],
) -> Optional[Any]:
    mask = (
        (~df["USED"])
        & (df["AIRPORT"] == airport)
        & (df["Contingency"])
        & (df["start_date"] <= ride_date)
        & (df["end_date"] >= ride_date)
    )
    for idx in df.loc[mask].index:
        link = str(df.loc[idx, "voucher_link"]).strip()
        if not link or link in links_taken_this_run:
            continue
        return idx
    return None


def _save_voucher_pool(
    df: pd.DataFrame,
    working_path: str,
    sb: Client,
    dry_run: bool,
) -> None:
    """Write voucher dataframe; mirror vouchers.assign_vouchers storage behavior."""
    df.to_csv(working_path, index=False)
    if dry_run:
        return
    if config.USE_SUPABASE_STORAGE and sb is not None:
        print("Archiving previous active vouchers...")
        storage.archive_file(
            sb,
            config.STORAGE_VOUCHERS_BUCKET,
            config.VOUCHERS_ACTIVE_FILE,
            config.VOUCHERS_ARCHIVE_FOLDER,
        )
        print("Uploading updated active vouchers...")
        storage.upload_file(
            sb,
            config.STORAGE_VOUCHERS_BUCKET,
            config.VOUCHERS_ACTIVE_FILE,
            working_path,
        )
        print("Uploaded active vouchers to Supabase Storage.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Subsidize LAX groups of 2 and assign unused vouchers (date window)."
    )
    parser.add_argument(
        "--voucher-csv",
        default="../vouchers/Summer.csv",
        help="Path to voucher pool CSV (same format as vouchers.py).",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=20,
        help="Include rides with ride_date from today through today+days-ahead (inclusive).",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Apply updates to Matches and voucher file (default: dry-run only).",
    )
    parser.add_argument(
        "--no-skip-if-subsidized",
        action="store_true",
        help="Process even when rows already subsidized with a voucher (consumes new pool rows).",
    )
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Set SUPABASE_URL and SUPABASE_SECRET_KEY")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    today = datetime.today().date()
    end = today + timedelta(days=max(0, args.days_ahead))

    rides = _fetch_rides(sb, today, end)
    if not rides:
        print(f"No rides between {today} and {end}.")
        return

    ride_ids = list(rides.keys())
    all_matches = _fetch_matches(sb, ride_ids)
    if not all_matches:
        print("No matches for rides in range.")
        return

    flight_ids = [int(m["flight_id"]) for m in all_matches if m.get("flight_id") is not None]
    flights = _fetch_flights(sb, flight_ids)
    user_ids_all = [str(m["user_id"]) for m in all_matches if m.get("user_id")]
    user_schools = _fetch_user_schools(sb, user_ids_all)
    by_ride = _group_by_ride(all_matches)

    voucher_path = os.path.abspath(args.voucher_csv)
    if not Path(voucher_path).exists():
        raise SystemExit(f"Voucher file not found: {voucher_path}")

    dry_run = not args.confirm
    temp_storage_path: Optional[str] = None
    try:
        if dry_run:
            working_voucher_path = voucher_path + ".dryrun.csv"
            shutil.copyfile(voucher_path, working_voucher_path)
        elif config.USE_SUPABASE_STORAGE:
            print("Downloading active vouchers from Supabase Storage...")
            temp_storage_path = storage.download_file(
                sb,
                config.STORAGE_VOUCHERS_BUCKET,
                config.VOUCHERS_ACTIVE_FILE,
            )
            working_voucher_path = temp_storage_path
        else:
            working_voucher_path = voucher_path

        df = load_voucher_pool(working_voucher_path)

        processed = 0
        skipped = 0
        skipped_non_pomona = 0
        no_voucher = 0
        # One distinct pool row (and URL) per ride in this run; also honors USED=True on load.
        links_taken_this_run: Set[str] = set()

        for ride_id, rows in sorted(by_ride.items()):
            if len(rows) != 2:
                continue

            ride = rides.get(ride_id)
            if not ride:
                continue

            uber_types = {str(r.get("uber_type") or "") for r in rows}
            if "Connect" in uber_types:
                skipped += 1
                continue

            if not args.no_skip_if_subsidized:
                if all(
                    bool(r.get("is_subsidized"))
                    and str(r.get("voucher") or "").strip()
                    for r in rows
                ):
                    skipped += 1
                    continue

            f0 = flights.get(int(rows[0]["flight_id"]))
            f1 = flights.get(int(rows[1]["flight_id"]))
            if not (_airport_lax(f0) and _airport_lax(f1)):
                continue

            if not _all_pomona(rows, user_schools):
                skipped_non_pomona += 1
                continue

            ride_date = _parse_date(ride.get("ride_date"))
            if ride_date is None:
                ride_date = _parse_date(rows[0].get("date"))
            if ride_date is None:
                skipped += 1
                continue

            if ride_date < today or ride_date > end:
                continue

            gidx = _take_group_voucher_idx(df, "LAX", ride_date, links_taken_this_run)
            if gidx is None:
                print(
                    f"ride_id={ride_id} date={ride_date}: no unused LAX group voucher in range — skip"
                )
                no_voucher += 1
                continue

            group_link = str(df.loc[gidx, "voucher_link"]).strip()
            df.loc[gidx, "USED"] = True
            links_taken_this_run.add(group_link)

            for r in rows:
                fid = int(r["flight_id"])
                fl = flights.get(fid)
                to_airport = bool(fl.get("to_airport")) if fl else True
                cont_link: Optional[str] = None
                if not to_airport:
                    cidx = _take_contingency_voucher_idx(
                        df, "LAX", ride_date, links_taken_this_run
                    )
                    if cidx is not None:
                        cont_link = str(df.loc[cidx, "voucher_link"]).strip()
                        df.loc[cidx, "USED"] = True
                        links_taken_this_run.add(cont_link)

                payload = {
                    "is_subsidized": True,
                    "voucher": group_link,
                    "contingency_voucher": cont_link,
                }

                if dry_run:
                    vu_disp = (
                        group_link[:48] + "..."
                        if len(group_link) > 48
                        else group_link
                    )
                    print(
                        f"[dry-run] ride_id={ride_id} flight_id={fid} "
                        f"subsidized=True voucher={vu_disp}"
                    )
                    if cont_link:
                        print("         contingency_voucher set (inbound)")
                else:
                    sb.table("Matches").update(payload).eq("ride_id", ride_id).eq(
                        "flight_id", fid
                    ).execute()

            processed += 1

        _save_voucher_pool(df, working_voucher_path, sb, dry_run)

        mode = "dry-run" if dry_run else "applied"
        print(
            f"Done ({mode}): processed={processed}, skipped_connect_or_existing={skipped}, "
            f"skipped_non_pomona={skipped_non_pomona}, no_pool_voucher={no_voucher}"
        )
        if dry_run:
            print(f"Voucher pool copy: {working_voucher_path}")
    finally:
        if temp_storage_path and Path(temp_storage_path).exists():
            Path(temp_storage_path).unlink(missing_ok=True)


if __name__ == "__main__":
    main()

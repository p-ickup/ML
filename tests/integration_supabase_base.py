"""Shared helpers for live Supabase integration tests."""

from __future__ import annotations

import importlib
import os
import time
import unittest
from contextlib import ExitStack
from urllib.parse import urlparse
from unittest import mock

import commit_payload
import import_vouchers
from rider_data import RiderLite, normalize_matching_status


class SupabaseIntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.sb = import_vouchers._client_from_env()
        except import_vouchers.VoucherImportError as exc:
            raise AssertionError(
                "Supabase integration tests require SUPABASE_URL and "
                "SUPABASE_SECRET_KEY in .env."
            ) from exc

        cls.host = urlparse(os.getenv("SUPABASE_URL", "")).hostname or "missing SUPABASE_URL host"
        cls.run_suffix = str(int(time.time() * 1000))
        cls.flight_base = int(cls.run_suffix) % 1_000_000_000_000
        cls.auth_user_ids: list[str] = []
        cls.main = importlib.import_module("main")
        cls.main.supabase = cls.sb

    def setUp(self):
        self.run_ids: list[str] = []
        self.flight_ids: list[int] = []
        self.import_batch_ids: list[str] = []
        self.algorithm_status_ids: list[str] = []
        self.algorithm_status_targets: list[str] = []
        self.created_ride_ids: set[int] = set()

    def tearDown(self):
        self._cleanup_test_rows()

    @classmethod
    def tearDownClass(cls):
        for user_id in reversed(cls.auth_user_ids):
            try:
                cls.sb.auth.admin.delete_user(user_id)
            except Exception:
                pass
        for closeable in (getattr(cls.sb, "auth", None), cls.sb):
            close = getattr(closeable, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass

    def _execute(self, query, action):
        try:
            return query.execute()
        except Exception as exc:
            self.fail(
                f"Supabase {action} failed. Host: {self.host}. "
                f"Original error: {type(exc).__name__}: {exc}"
            )

    def _rpc(self, name: str, params: dict, action: str):
        return self._execute(self.sb.rpc(name, params), action)

    def _create_auth_user(self, label: str) -> str:
        email = f"pickup-ml-{self.run_suffix}-{label}@example.com"
        try:
            response = self.sb.auth.admin.create_user(
                {
                    "email": email,
                    "password": f"PickupMLTest-{self.run_suffix}!",
                    "email_confirm": True,
                }
            )
        except Exception as exc:
            self.fail(
                "Supabase auth admin create_user failed. Confirm "
                "SUPABASE_SECRET_KEY is a service-role key. "
                f"Original error: {type(exc).__name__}: {exc}"
            )

        user = getattr(response, "user", None)
        user_id = getattr(user, "id", None)
        if not user_id:
            self.fail("Supabase auth admin create_user did not return a user id.")
        self.auth_user_ids.append(user_id)
        return user_id

    def _insert_user_profile(self, user_id: str, label: str) -> None:
        self._execute(
            self.sb.table("Users").insert(
                {
                    "user_id": user_id,
                    "firstname": f"ML{label}",
                    "lastname": "Integration",
                    "school": "POMONA",
                    "email": f"pickup-ml-{self.run_suffix}-{label}@example.com",
                    "sms_opt_in": False,
                    "role": None,
                    "admin_scope": None,
                }
            ),
            f"insert Users profile {label}",
        )

    def _insert_flight(
        self,
        *,
        flight_id: int,
        user_id: str,
        date: str = "2026-03-22",
        to_airport: bool = False,
        matching_status: str = "submitted",
    ) -> None:
        self.flight_ids.append(flight_id)
        self._execute(
            self.sb.table("Flights").insert(
                {
                    "flight_id": flight_id,
                    "user_id": user_id,
                    "date": date,
                    "earliest_time": "10:00:00",
                    "latest_time": "12:00:00",
                    "airport": "LAX",
                    "to_airport": to_airport,
                    "terminal": "1",
                    "matching_status": normalize_matching_status(matching_status),
                    "original_unmatched": False,
                    "flight_no": 9000 + (flight_id % 1000),
                    "airline_iata": "ZZ",
                    "bag_no": 1,
                    "bag_no_large": 0,
                    "bag_no_personal": 0,
                }
            ),
            f"insert Flights row {flight_id}",
        )

    def _rider(
        self,
        *,
        user_id: str,
        flight_id: int,
        date: str = "2026-03-22",
        earliest_time: str = "10:00:00",
        latest_time: str = "12:00:00",
        airport: str = "LAX",
        to_airport: bool = False,
        terminal: str = "1",
        matching_status: str = "submitted",
        school: str = "POMONA",
        bags_no: int = 1,
        bags_no_large: int = 0,
        bag_no_personal: int = 0,
        label: str = "Pipeline",
    ) -> RiderLite:
        return RiderLite(
            user_id=user_id,
            flight_id=flight_id,
            flight_no=9000 + (flight_id % 1000),
            airline_iata="ZZ",
            earliest_time=earliest_time,
            latest_time=latest_time,
            airport=airport,
            to_airport=to_airport,
            date=date,
            terminal=terminal,
            matching_status=normalize_matching_status(matching_status),
            school=school,
            bags_no=bags_no,
            bags_no_large=bags_no_large,
            bag_no_personal=bag_no_personal,
            name=f"ML {label}",
        )

    def _create_pipeline_rider(
        self,
        *,
        label: str,
        flight_offset: int,
        matching_status: str = "submitted",
        date: str = "2026-03-22",
        earliest_time: str = "10:00:00",
        latest_time: str = "12:00:00",
        airport: str = "LAX",
        to_airport: bool = False,
        terminal: str = "1",
    ) -> RiderLite:
        user_id = self._create_auth_user(label)
        self._insert_user_profile(user_id, label)
        flight_id = self.flight_base + flight_offset
        self._insert_flight(
            flight_id=flight_id,
            user_id=user_id,
            date=date,
            to_airport=to_airport,
            matching_status=matching_status,
        )
        return self._rider(
            user_id=user_id,
            flight_id=flight_id,
            date=date,
            earliest_time=earliest_time,
            latest_time=latest_time,
            airport=airport,
            to_airport=to_airport,
            terminal=terminal,
            matching_status=matching_status,
            label=label,
        )

    def _insert_vouchers(self, batch_id: str, count: int, *, contingency: bool) -> list[str]:
        self.import_batch_ids.append(batch_id)
        rows = []
        links = []
        kind = "CONT" if contingency else "GROUP"
        for idx in range(count):
            link = f"https://r.uber.com/PICKUPML{kind}{self.run_suffix}{batch_id[-2:]}{idx}"
            links.append(link)
            rows.append(
                {
                    "date_start_label": "March 22",
                    "date_end_label": "March 22",
                    "start_date": "2026-03-22",
                    "end_date": "2026-03-22",
                    "contingency": contingency,
                    "voucher_link": link,
                    "to_airport": False,
                    "airport": "LAX",
                    "used": False,
                    "used_at": None,
                    "used_by_run_id": None,
                    "assigned_ride_id": None,
                    "assigned_flight_id": None,
                    "import_batch_id": batch_id,
                }
            )

        self._execute(
            self.sb.table(import_vouchers.VOUCHERS_TABLE).insert(rows),
            f"insert {'contingency' if contingency else 'group'} vouchers",
        )
        return links

    def _payload(
        self,
        *,
        run_id: str,
        flight_ids: list[int],
        user_ids: list[str],
        is_subsidized: bool = True,
        ride_type: str | None = None,
        unmatched_flight_ids: list[int] | None = None,
        connect_cleanup_flight_ids: list[int] | None = None,
    ) -> dict:
        self.run_ids.append(run_id)
        members = []
        for flight_id, user_id in zip(flight_ids, user_ids):
            members.append(
                {
                    "user_id": user_id,
                    "flight_id": flight_id,
                    "date": "2026-03-22",
                    "time": "10:30:00",
                    "earliest_time": "10:00:00",
                    "latest_time": "12:00:00",
                    "source": "ml",
                    "is_verified": False,
                    "is_subsidized": is_subsidized,
                    "uber_type": ride_type or "X",
                }
            )

        return {
            "run_id": run_id,
            "groups": [
                {
                    "ride_date": "2026-03-22",
                    "ride_type": ride_type,
                    "airport": "LAX",
                    "to_airport": False,
                    "is_subsidized": is_subsidized,
                    "members": members,
                }
            ],
            "matched_flight_ids": flight_ids,
            "unmatched_flight_ids": unmatched_flight_ids or [],
            "connect_cleanup_flight_ids": connect_cleanup_flight_ids or [],
        }

    def _commit(self, run_id: str, payload: dict) -> dict:
        result = commit_payload.commit_matching_run(
            self.sb,
            run_id=run_id,
            payload=payload,
            retry_delay_seconds=0,
        )
        self._remember_rides_for_flights(self.flight_ids)
        return result

    def _target(self, label: str) -> str:
        target = f"MLIntegration-{label}-{self.run_suffix}"
        self.algorithm_status_targets.append(target)
        return target

    def _algorithm_status_for_target(self, target: str) -> dict:
        rows = self._execute(
            self.sb.table("AlgorithmStatus")
            .select("id,algorithm_name,target,status,run_id,finished_at,error_message")
            .eq("algorithm_name", "pickup_matching")
            .eq("target", target),
            "select AlgorithmStatus by target",
        ).data or []
        self.assertEqual(len(rows), 1)
        self.algorithm_status_ids.append(rows[0]["id"])
        return rows[0]

    def _matching_run(self, run_id: str) -> dict | None:
        rows = self._execute(
            self.sb.table("MatchingRuns")
            .select("run_id,status,commit_result")
            .eq("run_id", run_id),
            "select MatchingRuns row",
        ).data or []
        if not rows:
            return None
        self.assertEqual(len(rows), 1)
        return rows[0]

    def _run_pipeline_with_riders(self, riders: list[RiderLite], *, target: str, **patches) -> None:
        extra_patchers = patches.pop("patchers", [])
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(self.main.RiderData, "fetch_riders", return_value=riders))
            stack.enter_context(mock.patch.object(self.main.algorithmStatus, "determine_target_scope", return_value=target))
            if patches:
                stack.enter_context(mock.patch.multiple(self.main.config, **patches))
            for extra in extra_patchers:
                stack.enter_context(extra)
            self.main.run(dry_run=False, days_ahead=1, days_ahead_start=0)

    def _remember_rides_for_flights(self, flight_ids: list[int]) -> None:
        if not flight_ids:
            return
        response = self._execute(
            self.sb.table("Matches").select("ride_id").in_("flight_id", flight_ids),
            "select test ride ids",
        )
        for row in response.data or []:
            if row.get("ride_id") is not None:
                self.created_ride_ids.add(int(row["ride_id"]))

    def _cleanup_test_rows(self):
        self._remember_rides_for_flights(self.flight_ids)

        if self.import_batch_ids:
            self._execute(
                self.sb.table(import_vouchers.VOUCHERS_TABLE).delete().in_("import_batch_id", self.import_batch_ids),
                "cleanup Vouchers by import_batch_id",
            )
        if self.run_ids:
            self._execute(
                self.sb.table(import_vouchers.VOUCHERS_TABLE).delete().in_("used_by_run_id", self.run_ids),
                "cleanup Vouchers by used_by_run_id",
            )
        if self.flight_ids:
            self._execute(
                self.sb.table("Matches").delete().in_("flight_id", self.flight_ids),
                "cleanup Matches",
            )
        if self.created_ride_ids:
            self._execute(
                self.sb.table("Rides").delete().in_("ride_id", sorted(self.created_ride_ids)),
                "cleanup Rides",
            )
        if self.run_ids:
            self._execute(
                self.sb.table("MatchingRuns").delete().in_("run_id", self.run_ids),
                "cleanup MatchingRuns",
            )
        if self.algorithm_status_ids:
            self._execute(
                self.sb.table("AlgorithmStatus").delete().in_("id", self.algorithm_status_ids),
                "cleanup AlgorithmStatus",
            )
        if self.algorithm_status_targets:
            self._execute(
                self.sb.table("AlgorithmStatus").delete().in_("target", self.algorithm_status_targets),
                "cleanup AlgorithmStatus by target",
            )
        if self.flight_ids:
            self._execute(
                self.sb.table("Flights").delete().in_("flight_id", self.flight_ids),
                "cleanup Flights",
            )
        if self.auth_user_ids:
            self._execute(
                self.sb.table("Users").delete().in_("user_id", self.auth_user_ids),
                "cleanup Users",
            )

"""Live Supabase tests for voucher import and direct commit RPC behavior."""

from __future__ import annotations

from pathlib import Path

import import_vouchers
from tests.integration_supabase_base import SupabaseIntegrationTestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_VOUCHERS_CSV = REPO_ROOT / "tests" / "fixtures" / "TestVouchers.csv"


class TestVoucherImportTouchesSupabase(SupabaseIntegrationTestCase):
    def test_tracked_voucher_fixture_populates_vouchers_table(self):
        batch_id = f"33333333-3333-3333-3333-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        self.import_batch_ids.append(batch_id)
        expected_rows = import_vouchers.build_voucher_rows(
            str(TEST_VOUCHERS_CSV),
            import_batch_id=batch_id,
        )
        expected_links = [row["voucher_link"] for row in expected_rows]

        result = import_vouchers.import_voucher_csv(
            self.sb,
            str(TEST_VOUCHERS_CSV),
            import_batch_id=batch_id,
            batch_size=2,
        )

        self.assertEqual(result["rows"], 5)
        self.assertEqual(result["inserted_rows"] + result["existing_rows"], 5)
        imported = self._execute(
            self.sb.table(import_vouchers.VOUCHERS_TABLE)
            .select(
                "date_start_label,date_end_label,start_date,end_date,contingency,"
                "voucher_link,to_airport,airport,used,used_at,used_by_run_id,"
                "assigned_ride_id,assigned_flight_id,import_batch_id"
            )
            .in_("voucher_link", expected_links),
            "verify TestVouchers fixture rows",
        )

        rows = imported.data or []
        self.assertEqual(len(rows), 5)
        self.assertEqual({row["voucher_link"] for row in rows}, set(expected_links))
        for row in rows:
            self.assertEqual(row["date_start_label"], "March 22")
            self.assertEqual(row["date_end_label"], "March 22")
            self.assertEqual(row["start_date"], "2026-03-22")
            self.assertEqual(row["end_date"], "2026-03-22")
            self.assertEqual(row["airport"], "LAX")
            self.assertFalse(row["to_airport"])
            self.assertTrue(row["contingency"])
            self.assertFalse(row["used"])
            self.assertIsNone(row["used_at"])
            self.assertIsNone(row["used_by_run_id"])
            self.assertIsNone(row["assigned_ride_id"])
            self.assertIsNone(row["assigned_flight_id"])


class TestCommitMatchingRunTouchesSupabase(SupabaseIntegrationTestCase):
    VOUCHER_SHORTAGE_DATE = "2099-12-30"
    VOUCHER_SHORTAGE_AIRPORT = "TST"

    def test_commit_success_idempotency_and_voucher_consumption(self):
        user_a = self._create_auth_user("success-a")
        user_b = self._create_auth_user("success-b")
        user_unmatched = self._create_auth_user("success-unmatched")
        for label, user_id in [("success-a", user_a), ("success-b", user_b), ("success-unmatched", user_unmatched)]:
            self._insert_user_profile(user_id, label)

        flight_a = self.flight_base + 101
        flight_b = self.flight_base + 102
        flight_unmatched = self.flight_base + 103
        self._insert_flight(flight_id=flight_a, user_id=user_a)
        self._insert_flight(flight_id=flight_b, user_id=user_b)
        self._insert_flight(flight_id=flight_unmatched, user_id=user_unmatched)

        batch_group = f"44444444-4444-4444-4444-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        batch_contingency = f"55555555-5555-5555-5555-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        self._insert_vouchers(batch_group, 1, contingency=False)
        self._insert_vouchers(batch_contingency, 2, contingency=True)

        run_id = f"66666666-6666-6666-6666-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        payload = self._payload(
            run_id=run_id,
            flight_ids=[flight_a, flight_b],
            user_ids=[user_a, user_b],
            unmatched_flight_ids=[flight_unmatched],
        )

        result = self._commit(run_id, payload)

        self.assertFalse(result["idempotent_replay"])
        self.assertEqual(result["groups_inserted"], 1)
        self.assertEqual(result["matches_inserted"], 2)
        self.assertEqual(result["matched_flights_updated"], 2)
        self.assertEqual(result["unmatched_flights_updated"], 1)
        self.assertEqual(result["group_vouchers_used"], 1)
        self.assertEqual(result["contingency_vouchers_used"], 2)

        matches = self._execute(
            self.sb.table("Matches")
            .select("ride_id,flight_id,voucher,contingency_voucher,is_subsidized,source,uber_type")
            .in_("flight_id", [flight_a, flight_b]),
            "verify committed Matches",
        ).data or []
        self.assertEqual(len(matches), 2)
        self.assertEqual({row["flight_id"] for row in matches}, {flight_a, flight_b})
        self.assertTrue(all(row["voucher"] for row in matches))
        self.assertTrue(all(row["contingency_voucher"] for row in matches))
        self.assertTrue(all(row["is_subsidized"] for row in matches))
        self.assertEqual({row["source"] for row in matches}, {"ml"})
        self.assertEqual({row["uber_type"] for row in matches}, {"X"})
        committed_ride_ids = {int(row["ride_id"]) for row in matches}
        self.assertEqual(len(committed_ride_ids), 1)
        rides = self._execute(
            self.sb.table("Rides").select("ride_id,ride_date,ride_type").in_("ride_id", sorted(committed_ride_ids)),
            "verify committed Rides",
        ).data or []
        self.assertEqual(len(rides), 1)
        self.assertEqual(rides[0]["ride_date"], "2026-03-22")
        self.assertIsNone(rides[0]["ride_type"])

        matching_run = self._execute(
            self.sb.table("MatchingRuns").select("run_id,status,commit_result").eq("run_id", run_id),
            "verify MatchingRuns ledger",
        ).data or []
        self.assertEqual(len(matching_run), 1)
        self.assertEqual(matching_run[0]["status"], "committed")
        self.assertEqual(matching_run[0]["commit_result"]["matches_inserted"], 2)

        flights = self._execute(
            self.sb.table("Flights")
            .select("flight_id,matching_status,original_unmatched")
            .in_("flight_id", [flight_a, flight_b, flight_unmatched]),
            "verify committed Flights",
        ).data or []
        by_flight = {row["flight_id"]: row for row in flights}
        self.assertEqual(by_flight[flight_a]["matching_status"], "matched")
        self.assertFalse(by_flight[flight_a]["original_unmatched"])
        self.assertEqual(by_flight[flight_b]["matching_status"], "matched")
        self.assertFalse(by_flight[flight_b]["original_unmatched"])
        self.assertEqual(by_flight[flight_unmatched]["matching_status"], "unmatched")
        self.assertTrue(by_flight[flight_unmatched]["original_unmatched"])

        vouchers = self._execute(
            self.sb.table(import_vouchers.VOUCHERS_TABLE)
            .select("voucher_link,contingency,used,used_at,used_by_run_id,assigned_ride_id,assigned_flight_id")
            .in_("import_batch_id", [batch_group, batch_contingency]),
            "verify consumed Vouchers",
        ).data or []
        self.assertEqual(len(vouchers), 3)
        self.assertTrue(all(row["used"] for row in vouchers))
        self.assertTrue(all(row["used_at"] for row in vouchers))
        self.assertEqual({row["used_by_run_id"] for row in vouchers}, {run_id})
        self.assertTrue(all(row["assigned_ride_id"] for row in vouchers))
        self.assertEqual(
            {row["assigned_flight_id"] for row in vouchers if row["contingency"]},
            {flight_a, flight_b},
        )
        self.assertTrue(all(row["assigned_flight_id"] is None for row in vouchers if not row["contingency"]))

        replay = self._commit(run_id, payload)
        self.assertTrue(replay["idempotent_replay"])

        replay_matches = self._execute(
            self.sb.table("Matches").select("flight_id").in_("flight_id", [flight_a, flight_b]),
            "verify idempotent Matches count",
        ).data or []
        self.assertEqual(len(replay_matches), 2)

    def test_duplicate_flight_failure_rolls_back_ride_match_run_and_voucher_changes(self):
        user = self._create_auth_user("rollback")
        self._insert_user_profile(user, "rollback")
        flight_id = self.flight_base + 201
        self._insert_flight(flight_id=flight_id, user_id=user)

        batch_group = f"77777777-7777-7777-7777-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        self._insert_vouchers(batch_group, 2, contingency=False)

        run_id = f"88888888-8888-8888-8888-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        self.run_ids.append(run_id)
        rides_before = self._execute(
            self.sb.table("Rides").select("ride_id").eq("ride_date", "2026-03-22"),
            "count Rides before rollback test",
        ).data or []
        payload = {
            "run_id": run_id,
            "groups": [
                self._payload(run_id=run_id, flight_ids=[flight_id], user_ids=[user])["groups"][0],
                self._payload(run_id=run_id, flight_ids=[flight_id], user_ids=[user])["groups"][0],
            ],
            "matched_flight_ids": [flight_id],
            "unmatched_flight_ids": [],
            "connect_cleanup_flight_ids": [],
        }

        with self.assertRaises(Exception):
            self._rpc(
                "commit_matching_run",
                {"p_run_id": run_id, "p_payload": payload},
                "commit duplicate-flight rollback payload",
            )

        self.assertEqual(
            self._execute(
                self.sb.table("MatchingRuns").select("run_id").eq("run_id", run_id),
                "verify rollback MatchingRuns",
            ).data or [],
            [],
        )
        rides_after = self._execute(
            self.sb.table("Rides").select("ride_id").eq("ride_date", "2026-03-22"),
            "count Rides after rollback test",
        ).data or []
        self.assertEqual(len(rides_after), len(rides_before))
        self.assertEqual(
            self._execute(
                self.sb.table("Matches").select("flight_id").eq("flight_id", flight_id),
                "verify rollback Matches",
            ).data or [],
            [],
        )
        vouchers = self._execute(
            self.sb.table(import_vouchers.VOUCHERS_TABLE)
            .select("used,used_at,used_by_run_id,assigned_ride_id")
            .eq("import_batch_id", batch_group),
            "verify rollback Vouchers",
        ).data or []
        self.assertEqual(len(vouchers), 2)
        self.assertTrue(all(not row["used"] for row in vouchers))
        self.assertTrue(all(row["used_at"] is None for row in vouchers))
        self.assertTrue(all(row["used_by_run_id"] is None for row in vouchers))
        self.assertTrue(all(row["assigned_ride_id"] is None for row in vouchers))

    def test_missing_group_voucher_rolls_back_entire_commit(self):
        user = self._create_auth_user("missing-group-voucher")
        self._insert_user_profile(user, "missing-group-voucher")
        flight_id = self.flight_base + 251
        self._insert_flight(
            flight_id=flight_id,
            user_id=user,
            date=self.VOUCHER_SHORTAGE_DATE,
        )

        run_id = f"81818181-8181-8181-8181-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        self.run_ids.append(run_id)
        payload = self._payload(
            run_id=run_id,
            flight_ids=[flight_id],
            user_ids=[user],
            is_subsidized=True,
        )
        group = payload["groups"][0]
        group["ride_date"] = self.VOUCHER_SHORTAGE_DATE
        group["airport"] = self.VOUCHER_SHORTAGE_AIRPORT
        group["members"][0]["date"] = self.VOUCHER_SHORTAGE_DATE

        rides_before = self._execute(
            self.sb.table("Rides")
            .select("ride_id")
            .eq("ride_date", self.VOUCHER_SHORTAGE_DATE),
            "count Rides before missing group voucher test",
        ).data or []

        with self.assertRaisesRegex(Exception, "No available group voucher"):
            self._rpc(
                "commit_matching_run",
                {"p_run_id": run_id, "p_payload": payload},
                "commit missing group voucher payload",
            )

        self.assertEqual(
            self._execute(
                self.sb.table("MatchingRuns").select("run_id").eq("run_id", run_id),
                "verify missing group voucher MatchingRuns rollback",
            ).data or [],
            [],
        )
        self.assertEqual(
            self._execute(
                self.sb.table("Matches").select("flight_id").eq("flight_id", flight_id),
                "verify missing group voucher Matches rollback",
            ).data or [],
            [],
        )
        rides_after = self._execute(
            self.sb.table("Rides")
            .select("ride_id")
            .eq("ride_date", self.VOUCHER_SHORTAGE_DATE),
            "count Rides after missing group voucher test",
        ).data or []
        self.assertEqual(len(rides_after), len(rides_before))
        flight = self._execute(
            self.sb.table("Flights")
            .select("matching_status,original_unmatched")
            .eq("flight_id", flight_id),
            "verify missing group voucher Flight rollback",
        ).data[0]
        self.assertEqual(flight["matching_status"], "submitted")
        self.assertFalse(flight["original_unmatched"])

    def test_missing_contingency_voucher_rolls_back_group_voucher_and_commit(self):
        user = self._create_auth_user("missing-contingency-voucher")
        self._insert_user_profile(user, "missing-contingency-voucher")
        flight_id = self.flight_base + 252
        self._insert_flight(
            flight_id=flight_id,
            user_id=user,
            date=self.VOUCHER_SHORTAGE_DATE,
        )

        batch_id = f"82828282-8282-8282-8282-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        self.import_batch_ids.append(batch_id)
        voucher_link = f"https://voucher-test.invalid/GROUP-{self.run_suffix}"
        self._execute(
            self.sb.table(import_vouchers.VOUCHERS_TABLE).insert(
                {
                    "date_start_label": "December 30",
                    "date_end_label": "December 30",
                    "start_date": self.VOUCHER_SHORTAGE_DATE,
                    "end_date": self.VOUCHER_SHORTAGE_DATE,
                    "contingency": False,
                    "voucher_link": voucher_link,
                    "to_airport": False,
                    "airport": self.VOUCHER_SHORTAGE_AIRPORT,
                    "used": False,
                    "used_at": None,
                    "used_by_run_id": None,
                    "assigned_ride_id": None,
                    "assigned_flight_id": None,
                    "import_batch_id": batch_id,
                }
            ),
            "insert group voucher for contingency shortage test",
        )

        run_id = f"83838383-8383-8383-8383-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        self.run_ids.append(run_id)
        payload = self._payload(
            run_id=run_id,
            flight_ids=[flight_id],
            user_ids=[user],
            is_subsidized=True,
        )
        group = payload["groups"][0]
        group["ride_date"] = self.VOUCHER_SHORTAGE_DATE
        group["airport"] = self.VOUCHER_SHORTAGE_AIRPORT
        group["members"][0]["date"] = self.VOUCHER_SHORTAGE_DATE

        rides_before = self._execute(
            self.sb.table("Rides")
            .select("ride_id")
            .eq("ride_date", self.VOUCHER_SHORTAGE_DATE),
            "count Rides before missing contingency voucher test",
        ).data or []

        with self.assertRaisesRegex(Exception, "No available contingency voucher"):
            self._rpc(
                "commit_matching_run",
                {"p_run_id": run_id, "p_payload": payload},
                "commit missing contingency voucher payload",
            )

        self.assertEqual(
            self._execute(
                self.sb.table("MatchingRuns").select("run_id").eq("run_id", run_id),
                "verify missing contingency voucher MatchingRuns rollback",
            ).data or [],
            [],
        )
        self.assertEqual(
            self._execute(
                self.sb.table("Matches").select("flight_id").eq("flight_id", flight_id),
                "verify missing contingency voucher Matches rollback",
            ).data or [],
            [],
        )
        rides_after = self._execute(
            self.sb.table("Rides")
            .select("ride_id")
            .eq("ride_date", self.VOUCHER_SHORTAGE_DATE),
            "count Rides after missing contingency voucher test",
        ).data or []
        self.assertEqual(len(rides_after), len(rides_before))
        voucher = self._execute(
            self.sb.table(import_vouchers.VOUCHERS_TABLE)
            .select("used,used_at,used_by_run_id,assigned_ride_id,assigned_flight_id")
            .eq("voucher_link", voucher_link),
            "verify group voucher rollback after contingency shortage",
        ).data[0]
        self.assertFalse(voucher["used"])
        self.assertIsNone(voucher["used_at"])
        self.assertIsNone(voucher["used_by_run_id"])
        self.assertIsNone(voucher["assigned_ride_id"])
        self.assertIsNone(voucher["assigned_flight_id"])
        flight = self._execute(
            self.sb.table("Flights")
            .select("matching_status,original_unmatched")
            .eq("flight_id", flight_id),
            "verify missing contingency voucher Flight rollback",
        ).data[0]
        self.assertEqual(flight["matching_status"], "submitted")
        self.assertFalse(flight["original_unmatched"])

    def test_connect_cleanup_replaces_old_match_and_removes_orphaned_old_ride(self):
        user = self._create_auth_user("connect")
        self._insert_user_profile(user, "connect")
        flight_id = self.flight_base + 301
        self._insert_flight(flight_id=flight_id, user_id=user, matching_status="matched")

        old_ride = self._execute(
            self.sb.table("Rides").insert({"ride_date": "2026-03-22", "ride_type": None}),
            "insert old Rides row",
        ).data[0]
        old_ride_id = int(old_ride["ride_id"])
        self.created_ride_ids.add(old_ride_id)
        self._execute(
            self.sb.table("Matches").insert(
                {
                    "ride_id": old_ride_id,
                    "user_id": user,
                    "flight_id": flight_id,
                    "date": "2026-03-22",
                    "time": "10:15:00",
                    "earliest_time": "10:00:00",
                    "latest_time": "12:00:00",
                    "source": "ml",
                    "voucher": None,
                    "contingency_voucher": None,
                    "is_verified": False,
                    "is_subsidized": False,
                    "uber_type": "X",
                }
            ),
            "insert old Matches row",
        )

        run_id = f"99999999-9999-9999-9999-{int(self.run_suffix) % 1_000_000_000_000:012d}"
        payload = self._payload(
            run_id=run_id,
            flight_ids=[flight_id],
            user_ids=[user],
            is_subsidized=False,
            ride_type="Connect",
            connect_cleanup_flight_ids=[flight_id],
        )

        result = self._commit(run_id, payload)

        self.assertEqual(result["connect_cleanup_flights"], 1)
        self.assertEqual(result["connect_cleanup_rides_touched"], 1)
        self.assertEqual(
            self._execute(
                self.sb.table("Rides").select("ride_id").eq("ride_id", old_ride_id),
                "verify old ride deleted",
            ).data or [],
            [],
        )

        new_matches = self._execute(
            self.sb.table("Matches").select("ride_id,flight_id").eq("flight_id", flight_id),
            "verify Connect replacement match",
        ).data or []
        self.assertEqual(len(new_matches), 1)
        self.assertNotEqual(int(new_matches[0]["ride_id"]), old_ride_id)
        self.created_ride_ids.add(int(new_matches[0]["ride_id"]))

        new_ride = self._execute(
            self.sb.table("Rides").select("ride_type").eq("ride_id", new_matches[0]["ride_id"]),
            "verify Connect replacement ride",
        ).data[0]
        self.assertEqual(new_ride["ride_type"], "Connect")

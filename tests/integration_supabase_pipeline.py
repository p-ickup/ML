"""Live Supabase tests for AlgorithmStatus and main.run lifecycle behavior."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest import mock

import algorithmStatus
from tests.integration_supabase_base import SupabaseIntegrationTestCase


class TestAlgorithmStatusTouchesSupabase(SupabaseIntegrationTestCase):
    def _status_run_id(self, prefix: str) -> str:
        return f"{prefix}-{int(self.run_suffix) % 1_000_000_000_000:012d}"

    def _select_algorithm_status(self, status_id: str) -> dict:
        rows = self._execute(
            self.sb.table("AlgorithmStatus")
            .select(
                "id,algorithm_name,target,scheduled_for,started_at,finished_at,"
                "status,run_id,error_message,created_at"
            )
            .eq("id", status_id),
            "select AlgorithmStatus row",
        ).data or []
        self.assertEqual(len(rows), 1)
        return rows[0]

    def test_creates_running_status_when_no_scheduled_row_exists(self):
        target = self._target("create")

        status_id = algorithmStatus.get_or_create_algorithm_status(
            self.sb,
            "pickup_matching",
            target,
        )
        self.assertIsNotNone(status_id)
        self.algorithm_status_ids.append(status_id)

        row = self._select_algorithm_status(status_id)
        self.assertEqual(row["algorithm_name"], "pickup_matching")
        self.assertEqual(row["target"], target)
        self.assertEqual(row["status"], "running")
        self.assertIsNotNone(row["scheduled_for"])
        self.assertIsNotNone(row["started_at"])
        self.assertIsNotNone(row["created_at"])
        self.assertIsNotNone(row["run_id"])
        self.assertIsNone(row["finished_at"])
        self.assertIsNone(row["error_message"])

    def test_reuses_due_scheduled_status_and_marks_running(self):
        target = self._target("scheduled")
        scheduled_for = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
        inserted = self._execute(
            self.sb.table("AlgorithmStatus").insert(
                {
                    "algorithm_name": "pickup_matching",
                    "target": target,
                    "scheduled_for": scheduled_for,
                    "status": "scheduled",
                    "started_at": None,
                    "finished_at": None,
                    "run_id": None,
                    "error_message": None,
                }
            ),
            "insert scheduled AlgorithmStatus row",
        ).data[0]
        scheduled_id = inserted["id"]
        self.algorithm_status_ids.append(scheduled_id)

        status_id = algorithmStatus.get_or_create_algorithm_status(
            self.sb,
            "pickup_matching",
            target,
        )
        self.algorithm_status_ids.append(status_id)

        self.assertEqual(status_id, scheduled_id)
        row = self._select_algorithm_status(status_id)
        self.assertEqual(row["status"], "running")
        self.assertIsNotNone(row["started_at"])
        self.assertIsNone(row["finished_at"])
        self.assertIsNone(row["run_id"])
        self.assertIsNone(row["error_message"])

    def test_update_algorithm_status_success_sets_finished_at_and_run_id(self):
        target = self._target("success")
        status_id = algorithmStatus.get_or_create_algorithm_status(
            self.sb,
            "pickup_matching",
            target,
        )
        self.algorithm_status_ids.append(status_id)
        run_id = self._status_run_id("aaaaaaaa-aaaa-aaaa-aaaa")

        algorithmStatus.update_algorithm_status(
            self.sb,
            status_id,
            "success",
            run_id=run_id,
        )

        row = self._select_algorithm_status(status_id)
        self.assertEqual(row["status"], "success")
        self.assertEqual(row["run_id"], run_id)
        self.assertIsNotNone(row["finished_at"])
        self.assertIsNone(row["error_message"])

    def test_update_algorithm_status_failed_sets_error_message(self):
        target = self._target("failed")
        status_id = algorithmStatus.get_or_create_algorithm_status(
            self.sb,
            "pickup_matching",
            target,
        )
        self.algorithm_status_ids.append(status_id)
        run_id = self._status_run_id("bbbbbbbb-bbbb-bbbb-bbbb")
        error_message = "integration test expected failure"

        algorithmStatus.update_algorithm_status(
            self.sb,
            status_id,
            "failed",
            error_message=error_message,
            run_id=run_id,
        )

        row = self._select_algorithm_status(status_id)
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["run_id"], run_id)
        self.assertEqual(row["error_message"], error_message)
        self.assertIsNotNone(row["finished_at"])


class TestMainPipelineTouchesSupabase(SupabaseIntegrationTestCase):
    PIPELINE_DATE = "2099-01-15"
    PERSISTENT_PIPELINE_DATE = "2099-11-17"
    PERSISTENT_PARKED_DATE = "2000-01-01"
    PERSISTENT_FLIGHT_IDS = tuple(range(9_000_000_000_101, 9_000_000_000_111))
    PERSISTENT_EMAILS = tuple(
        f"pickup-ml-persistent-pipeline-{index:02d}@example.com"
        for index in range(1, 11)
    )

    def _disable_connect_patch(self):
        return mock.patch.object(self.main.cp, "connect_enabled", return_value=False)

    def _track_run_id(self, status: dict) -> str:
        run_id = status["run_id"]
        self.assertIsNotNone(run_id)
        self.run_ids.append(run_id)
        return run_id

    def _assert_matches_for_flights(self, flight_ids: list[int], expected_count: int) -> list[dict]:
        rows = self._execute(
            self.sb.table("Matches")
            .select("ride_id,flight_id,source,uber_type,date,time,earliest_time,latest_time")
            .in_("flight_id", flight_ids),
            "select pipeline Matches",
        ).data or []
        self.assertEqual(len(rows), expected_count)
        for row in rows:
            if row.get("ride_id") is not None:
                self.created_ride_ids.add(int(row["ride_id"]))
        return rows

    def _persistent_auth_users_by_email(self, emails: set[str]) -> dict[str, str]:
        found: dict[str, str] = {}
        page = 1
        per_page = 1000
        while emails - found.keys():
            try:
                users = self.sb.auth.admin.list_users(page=page, per_page=per_page)
            except Exception as exc:
                self.fail(
                    "Supabase auth admin list_users failed while locating persistent "
                    f"pipeline fixtures: {type(exc).__name__}: {exc}"
                )
            for user in users:
                email = getattr(user, "email", None)
                user_id = getattr(user, "id", None)
                if email in emails and user_id:
                    found[email] = user_id
            if len(users) < per_page:
                break
            page += 1
        return found

    def _ensure_persistent_pipeline_users(self) -> list[str]:
        emails = set(self.PERSISTENT_EMAILS)
        profiles = self._execute(
            self.sb.table("Users").select("user_id,email").in_("email", sorted(emails)),
            "select persistent pipeline Users",
        ).data or []
        user_ids_by_email = {
            row["email"]: row["user_id"]
            for row in profiles
            if row.get("email") in emails
        }

        missing_emails = emails - user_ids_by_email.keys()
        auth_users = self._persistent_auth_users_by_email(missing_emails) if missing_emails else {}
        new_profiles = []
        for index, email in enumerate(self.PERSISTENT_EMAILS, start=1):
            if email in user_ids_by_email:
                continue
            user_id = auth_users.get(email)
            if user_id is None:
                try:
                    response = self.sb.auth.admin.create_user(
                        {
                            "email": email,
                            "password": "PickupMLPersistentPipeline-2026!",
                            "email_confirm": True,
                        }
                    )
                except Exception as exc:
                    self.fail(
                        "Supabase auth admin create_user failed for a persistent "
                        f"pipeline fixture: {type(exc).__name__}: {exc}"
                    )
                user = getattr(response, "user", None)
                user_id = getattr(user, "id", None)
                if not user_id:
                    self.fail("Persistent pipeline auth user creation returned no user id.")
            user_ids_by_email[email] = user_id
            new_profiles.append(
                {
                    "user_id": user_id,
                    "firstname": f"MLPersistent{index:02d}",
                    "lastname": "Integration",
                    "school": "POMONA",
                    "email": email,
                    "sms_opt_in": False,
                    "role": None,
                    "admin_scope": None,
                }
            )

        if new_profiles:
            self._execute(
                self.sb.table("Users").insert(new_profiles),
                "insert persistent pipeline Users",
            )
        return [user_ids_by_email[email] for email in self.PERSISTENT_EMAILS]

    def _delete_persistent_pipeline_matches(self) -> None:
        flight_ids = list(self.PERSISTENT_FLIGHT_IDS)
        existing_matches = self._execute(
            self.sb.table("Matches").select("ride_id").in_("flight_id", flight_ids),
            "select persistent pipeline Matches for cleanup",
        ).data or []
        ride_ids = {
            int(row["ride_id"])
            for row in existing_matches
            if row.get("ride_id") is not None
        }
        self._execute(
            self.sb.table("Matches").delete().in_("flight_id", flight_ids),
            "cleanup persistent pipeline Matches",
        )
        if not ride_ids:
            return

        remaining = self._execute(
            self.sb.table("Matches").select("ride_id").in_("ride_id", sorted(ride_ids)),
            "check persistent pipeline Rides for remaining Matches",
        ).data or []
        retained_ride_ids = {
            int(row["ride_id"])
            for row in remaining
            if row.get("ride_id") is not None
        }
        orphaned_ride_ids = sorted(ride_ids - retained_ride_ids)
        if orphaned_ride_ids:
            self._execute(
                self.sb.table("Rides").delete().in_("ride_id", orphaned_ride_ids),
                "cleanup persistent pipeline Rides",
            )

    def _persistent_pipeline_specs(self, user_ids: list[str]) -> list[dict]:
        windows = [
            ("LAX", "09:00:00", "11:00:00"),
            ("LAX", "09:05:00", "10:55:00"),
            ("LAX", "09:10:00", "10:50:00"),
            ("LAX", "09:15:00", "10:45:00"),
            ("LAX", "09:20:00", "10:40:00"),
            ("LAX", "09:25:00", "10:35:00"),
            ("LAX", "09:30:00", "10:30:00"),
            ("ONT", "13:00:00", "15:00:00"),
            ("ONT", "13:15:00", "14:45:00"),
            ("LAX", "18:00:00", "19:00:00"),
        ]
        rows = []
        for index, (flight_id, user_id, window) in enumerate(
            zip(self.PERSISTENT_FLIGHT_IDS, user_ids, windows),
            start=1,
        ):
            airport, earliest_time, latest_time = window
            rows.append(
                {
                    "flight_id": flight_id,
                    "user_id": user_id,
                    "date": self.PERSISTENT_PIPELINE_DATE,
                    "earliest_time": earliest_time,
                    "latest_time": latest_time,
                    "airport": airport,
                    "to_airport": True,
                    "terminal": "1",
                    "matching_status": "submitted",
                    "original_unmatched": False,
                    "flight_no": 9100 + index,
                    "airline_iata": "ZZ",
                    "bag_no": 1,
                    "bag_no_large": 0,
                    "bag_no_personal": 0,
                }
            )
        return rows

    def _activate_persistent_pipeline_flights(self, user_ids: list[str]) -> list:
        rows = self._persistent_pipeline_specs(user_ids)
        existing = self._execute(
            self.sb.table("Flights")
            .select("flight_id,user_id")
            .in_("flight_id", list(self.PERSISTENT_FLIGHT_IDS)),
            "select persistent pipeline Flights",
        ).data or []
        expected_users = set(user_ids)
        collisions = [row for row in existing if row.get("user_id") not in expected_users]
        if collisions:
            self.fail(
                "Reserved persistent pipeline flight ids are already owned by "
                f"non-test users: {[row['flight_id'] for row in collisions]}"
            )

        self._execute(
            self.sb.table("Flights").upsert(rows, on_conflict="flight_id"),
            "activate persistent pipeline Flights",
        )
        return [
            self._rider(
                user_id=row["user_id"],
                flight_id=row["flight_id"],
                date=row["date"],
                earliest_time=row["earliest_time"],
                latest_time=row["latest_time"],
                airport=row["airport"],
                to_airport=row["to_airport"],
                terminal=row["terminal"],
                matching_status=row["matching_status"],
                label=f"Persistent {index:02d}",
            )
            for index, row in enumerate(rows, start=1)
        ]

    def _reset_persistent_pipeline_flights(self, target: str) -> None:
        self._delete_persistent_pipeline_matches()
        statuses = self._execute(
            self.sb.table("AlgorithmStatus").select("id,run_id").eq("target", target),
            "select persistent pipeline AlgorithmStatus for cleanup",
        ).data or []
        run_ids = [row["run_id"] for row in statuses if row.get("run_id")]
        if run_ids:
            self._execute(
                self.sb.table("MatchingRuns").delete().in_("run_id", run_ids),
                "cleanup persistent pipeline MatchingRuns",
            )
        if statuses:
            self._execute(
                self.sb.table("AlgorithmStatus").delete().in_(
                    "id", [row["id"] for row in statuses]
                ),
                "cleanup persistent pipeline AlgorithmStatus",
            )
        self._execute(
            self.sb.table("Flights")
            .update(
                {
                    "date": self.PERSISTENT_PARKED_DATE,
                    "matching_status": "submitted",
                    "original_unmatched": False,
                }
            )
            .in_("flight_id", list(self.PERSISTENT_FLIGHT_IDS)),
            "park persistent pipeline Flights",
        )

    def _assert_cross_midnight_pipeline_window(
        self,
        *,
        label: str,
        riders: list,
        expected_date: str,
        expected_time: str,
        expected_earliest_time: str,
        expected_latest_time: str,
    ) -> None:
        target = self._target(label)

        self._run_pipeline_with_riders(
            riders,
            target=target,
            TERMINAL_MODE="slack",
            SAME_FLIGHT_PRIORITY=False,
            patchers=[self._disable_connect_patch()],
        )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "success")
        self.assertIsNone(status["error_message"])
        run_id = self._track_run_id(status)

        matching_run = self._matching_run(run_id)
        self.assertIsNotNone(matching_run)
        self.assertEqual(matching_run["status"], "committed")
        self.assertEqual(matching_run["commit_result"]["groups_inserted"], 1)
        self.assertEqual(matching_run["commit_result"]["matches_inserted"], len(riders))

        matches = self._assert_matches_for_flights([r.flight_id for r in riders], len(riders))
        self.assertEqual(len({row["ride_id"] for row in matches}), 1)
        self.assertEqual({row["date"] for row in matches}, {expected_date})
        self.assertEqual({row["time"] for row in matches}, {expected_time})
        self.assertEqual({row["earliest_time"] for row in matches}, {expected_earliest_time})
        self.assertEqual({row["latest_time"] for row in matches}, {expected_latest_time})

    def test_pipeline_success_with_matches_commits_and_marks_algorithm_status_success(self):
        target = self._target("pipeline-success")
        rider_a = self._create_pipeline_rider(
            label="pipeline-success-a",
            flight_offset=401,
            date=self.PIPELINE_DATE,
        )
        rider_b = self._create_pipeline_rider(
            label="pipeline-success-b",
            flight_offset=402,
            date=self.PIPELINE_DATE,
        )

        self._run_pipeline_with_riders(
            [rider_a, rider_b],
            target=target,
            patchers=[self._disable_connect_patch()],
        )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "success")
        self.assertIsNotNone(status["finished_at"])
        self.assertIsNone(status["error_message"])
        run_id = self._track_run_id(status)

        matching_run = self._matching_run(run_id)
        self.assertIsNotNone(matching_run)
        self.assertEqual(matching_run["status"], "committed")
        self.assertEqual(matching_run["commit_result"]["groups_inserted"], 1)
        self.assertEqual(matching_run["commit_result"]["matches_inserted"], 2)
        self.assertEqual(matching_run["commit_result"]["connect_cleanup_flights"], 0)

        matches = self._assert_matches_for_flights([rider_a.flight_id, rider_b.flight_id], 2)
        self.assertEqual({row["flight_id"] for row in matches}, {rider_a.flight_id, rider_b.flight_id})
        self.assertEqual(len({row["ride_id"] for row in matches}), 1)

        flights = self._execute(
            self.sb.table("Flights")
            .select("flight_id,matching_status,original_unmatched")
            .in_("flight_id", [rider_a.flight_id, rider_b.flight_id]),
            "verify pipeline matched Flights",
        ).data or []
        self.assertTrue(all(row["matching_status"] == "matched" for row in flights))
        self.assertTrue(all(not row["original_unmatched"] for row in flights))

    def test_persistent_ten_flight_pipeline_forms_connect_ont_and_unmatched(self):
        target = self._target("persistent-ten-flight-pipeline")
        flight_ids = list(self.PERSISTENT_FLIGHT_IDS)
        lax_connect_ids = set(flight_ids[:7])
        ont_group_ids = set(flight_ids[7:9])
        unmatched_id = flight_ids[9]

        try:
            self._delete_persistent_pipeline_matches()
            user_ids = self._ensure_persistent_pipeline_users()
            riders = self._activate_persistent_pipeline_flights(user_ids)

            self._run_pipeline_with_riders(
                riders,
                target=target,
                TERMINAL_MODE="slack",
                SAME_FLIGHT_PRIORITY=False,
                CONNECT_ARRIVAL=[],
                CONNECT_DEPARTURE=["LAX", "ONT"],
                CONNECT_SIZE1=[6, 12],
                CONNECT_SIZE2=[],
            )

            status = self._algorithm_status_for_target(target)
            self.assertEqual(status["status"], "success")
            self.assertIsNone(status["error_message"])
            run_id = self._track_run_id(status)

            matching_run = self._matching_run(run_id)
            self.assertIsNotNone(matching_run)
            self.assertEqual(matching_run["status"], "committed")
            commit_result = matching_run["commit_result"]
            self.assertEqual(commit_result["groups_inserted"], 2)
            self.assertEqual(commit_result["matches_inserted"], 9)
            self.assertEqual(commit_result["matched_flights_updated"], 9)
            self.assertEqual(commit_result["unmatched_flights_updated"], 1)
            self.assertEqual(commit_result["connect_cleanup_flights"], 0)

            matches = self._assert_matches_for_flights(flight_ids, 9)
            matches_by_flight = {int(row["flight_id"]): row for row in matches}
            self.assertEqual(set(matches_by_flight), lax_connect_ids | ont_group_ids)
            self.assertNotIn(unmatched_id, matches_by_flight)

            lax_ride_ids = {
                int(matches_by_flight[flight_id]["ride_id"])
                for flight_id in lax_connect_ids
            }
            ont_ride_ids = {
                int(matches_by_flight[flight_id]["ride_id"])
                for flight_id in ont_group_ids
            }
            self.assertEqual(len(lax_ride_ids), 1)
            self.assertEqual(len(ont_ride_ids), 1)
            self.assertTrue(lax_ride_ids.isdisjoint(ont_ride_ids))

            ride_ids = sorted(lax_ride_ids | ont_ride_ids)
            rides = self._execute(
                self.sb.table("Rides").select("ride_id,ride_type").in_("ride_id", ride_ids),
                "verify persistent pipeline Rides",
            ).data or []
            rides_by_id = {int(row["ride_id"]): row for row in rides}
            self.assertEqual(rides_by_id[next(iter(lax_ride_ids))]["ride_type"], "Connect")
            self.assertIsNone(rides_by_id[next(iter(ont_ride_ids))]["ride_type"])

            flights = self._execute(
                self.sb.table("Flights")
                .select("flight_id,matching_status,original_unmatched")
                .in_("flight_id", flight_ids),
                "verify persistent pipeline Flights",
            ).data or []
            self.assertEqual(len(flights), 10)
            flights_by_id = {int(row["flight_id"]): row for row in flights}
            for flight_id in lax_connect_ids | ont_group_ids:
                self.assertEqual(flights_by_id[flight_id]["matching_status"], "matched")
                self.assertFalse(flights_by_id[flight_id]["original_unmatched"])
            self.assertEqual(flights_by_id[unmatched_id]["matching_status"], "unmatched")
            self.assertTrue(flights_by_id[unmatched_id]["original_unmatched"])
        finally:
            self._reset_persistent_pipeline_flights(target)

        remaining_matches = self._execute(
            self.sb.table("Matches").select("flight_id").in_("flight_id", flight_ids),
            "verify persistent pipeline Matches cleanup",
        ).data or []
        self.assertEqual(remaining_matches, [])
        parked_flights = self._execute(
            self.sb.table("Flights")
            .select("date,matching_status,original_unmatched")
            .in_("flight_id", flight_ids),
            "verify persistent pipeline Flights parked",
        ).data or []
        self.assertEqual(len(parked_flights), 10)
        self.assertTrue(all(row["date"] == self.PERSISTENT_PARKED_DATE for row in parked_flights))
        self.assertTrue(all(row["matching_status"] == "submitted" for row in parked_flights))
        self.assertTrue(all(not row["original_unmatched"] for row in parked_flights))

    def test_pipeline_cross_midnight_large_window_persists_normalized_match_window(self):
        rider_a = self._create_pipeline_rider(
            label="pipeline-cross-broad-a",
            flight_offset=421,
            date=self.PIPELINE_DATE,
            earliest_time="22:00:00",
            latest_time="03:00:00",
            to_airport=True,
        )
        rider_b = self._create_pipeline_rider(
            label="pipeline-cross-broad-b",
            flight_offset=422,
            date=self.PIPELINE_DATE,
            earliest_time="23:00:00",
            latest_time="02:30:00",
            to_airport=True,
        )

        self._assert_cross_midnight_pipeline_window(
            label="pipeline-cross-broad",
            riders=[rider_a, rider_b],
            expected_date="2099-01-16",
            expected_time="02:15:00",
            expected_earliest_time="23:00:00",
            expected_latest_time="02:30:00",
        )

    def test_pipeline_cross_midnight_tight_window_persists_normalized_match_window(self):
        rider_a = self._create_pipeline_rider(
            label="pipeline-cross-tight-a",
            flight_offset=431,
            date=self.PIPELINE_DATE,
            earliest_time="23:45:00",
            latest_time="00:30:00",
            to_airport=True,
        )
        rider_b = self._create_pipeline_rider(
            label="pipeline-cross-tight-b",
            flight_offset=432,
            date=self.PIPELINE_DATE,
            earliest_time="23:55:00",
            latest_time="00:20:00",
            to_airport=True,
        )

        self._assert_cross_midnight_pipeline_window(
            label="pipeline-cross-tight",
            riders=[rider_a, rider_b],
            expected_date="2099-01-16",
            expected_time="00:05:00",
            expected_earliest_time="23:55:00",
            expected_latest_time="00:20:00",
        )

    def test_pipeline_cross_midnight_three_person_mixed_window_persists_normalized_match_window(self):
        rider_a = self._create_pipeline_rider(
            label="pipeline-cross-mixed-a",
            flight_offset=441,
            date=self.PIPELINE_DATE,
            earliest_time="22:30:00",
            latest_time="02:30:00",
            to_airport=True,
        )
        rider_b = self._create_pipeline_rider(
            label="pipeline-cross-mixed-b",
            flight_offset=442,
            date=self.PIPELINE_DATE,
            earliest_time="23:15:00",
            latest_time="01:45:00",
            to_airport=True,
        )
        rider_c = self._create_pipeline_rider(
            label="pipeline-cross-mixed-c",
            flight_offset=443,
            date=self.PIPELINE_DATE,
            earliest_time="23:40:00",
            latest_time="01:10:00",
            to_airport=True,
        )

        self._assert_cross_midnight_pipeline_window(
            label="pipeline-cross-mixed",
            riders=[rider_a, rider_b, rider_c],
            expected_date="2099-01-16",
            expected_time="00:55:00",
            expected_earliest_time="23:40:00",
            expected_latest_time="01:10:00",
        )

    def test_pipeline_no_candidate_riders_marks_success_without_commit(self):
        target = self._target("pipeline-empty")

        self._run_pipeline_with_riders(
            [],
            target=target,
            patchers=[self._disable_connect_patch()],
        )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "success")
        self.assertIsNotNone(status["finished_at"])
        self.assertIsNone(status["error_message"])
        run_id = self._track_run_id(status)
        self.assertIsNone(self._matching_run(run_id))

    def test_pipeline_single_unmatched_rider_marks_flight_original_unmatched(self):
        target = self._target("pipeline-unmatched")
        rider = self._create_pipeline_rider(
            label="pipeline-unmatched",
            flight_offset=501,
            date=self.PIPELINE_DATE,
        )

        self._run_pipeline_with_riders(
            [rider],
            target=target,
            patchers=[self._disable_connect_patch()],
        )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "success")
        run_id = self._track_run_id(status)

        matching_run = self._matching_run(run_id)
        self.assertIsNotNone(matching_run)
        self.assertEqual(matching_run["commit_result"]["groups_inserted"], 0)
        self.assertEqual(matching_run["commit_result"]["matches_inserted"], 0)
        self.assertEqual(matching_run["commit_result"]["unmatched_flights_updated"], 1)
        self._assert_matches_for_flights([rider.flight_id], 0)

        flight = self._execute(
            self.sb.table("Flights")
            .select("matching_status,original_unmatched")
            .eq("flight_id", rider.flight_id),
            "verify pipeline unmatched Flight",
        ).data[0]
        self.assertEqual(flight["matching_status"], "unmatched")
        self.assertTrue(flight["original_unmatched"])

    def test_pipeline_connect_enabled_no_merge_commits_without_cleanup(self):
        target = self._target("pipeline-connect-no-merge")
        rider_a = self._create_pipeline_rider(
            label="pipeline-connect-no-merge-a",
            flight_offset=601,
            date=self.PIPELINE_DATE,
        )
        rider_b = self._create_pipeline_rider(
            label="pipeline-connect-no-merge-b",
            flight_offset=602,
            date=self.PIPELINE_DATE,
        )

        self._run_pipeline_with_riders(
            [rider_a, rider_b],
            target=target,
            CONNECT_ARRIVAL=["LAX"],
            CONNECT_DEPARTURE=[],
            CONNECT_SIZE1=[6, 12],
            CONNECT_SIZE2=[],
        )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "success")
        run_id = self._track_run_id(status)
        matching_run = self._matching_run(run_id)
        self.assertIsNotNone(matching_run)
        self.assertEqual(matching_run["commit_result"]["connect_cleanup_flights"], 0)
        self.assertEqual(matching_run["commit_result"]["connect_cleanup_rides_touched"], 0)

        matches = self._assert_matches_for_flights([rider_a.flight_id, rider_b.flight_id], 2)
        ride_id = int(matches[0]["ride_id"])
        ride = self._execute(
            self.sb.table("Rides").select("ride_type").eq("ride_id", ride_id),
            "verify no-merge ride type",
        ).data[0]
        self.assertIsNone(ride["ride_type"])

    def test_pipeline_connect_merge_replaces_existing_match_and_old_ride(self):
        target = self._target("pipeline-connect-merge")
        old_rider = self._create_pipeline_rider(
            label="pipeline-connect-old",
            flight_offset=701,
            matching_status="matched",
            date=self.PIPELINE_DATE,
        )
        new_rider = self._create_pipeline_rider(
            label="pipeline-connect-new",
            flight_offset=702,
            date=self.PIPELINE_DATE,
        )

        old_ride = self._execute(
            self.sb.table("Rides").insert({"ride_date": self.PIPELINE_DATE, "ride_type": None}),
            "insert pipeline old Rides row",
        ).data[0]
        old_ride_id = int(old_ride["ride_id"])
        self.created_ride_ids.add(old_ride_id)
        self._execute(
            self.sb.table("Matches").insert(
                {
                    "ride_id": old_ride_id,
                    "user_id": old_rider.user_id,
                    "flight_id": old_rider.flight_id,
                    "date": self.PIPELINE_DATE,
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
            "insert pipeline old Matches row",
        )

        self._run_pipeline_with_riders(
            [new_rider],
            target=target,
            CONNECT_ARRIVAL=["LAX"],
            CONNECT_DEPARTURE=[],
            CONNECT_SIZE1=[2, 6],
            CONNECT_SIZE2=[],
        )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "success")
        run_id = self._track_run_id(status)
        matching_run = self._matching_run(run_id)
        self.assertIsNotNone(matching_run)
        self.assertEqual(matching_run["commit_result"]["connect_cleanup_rides_touched"], 1)

        self.assertEqual(
            self._execute(
                self.sb.table("Rides").select("ride_id").eq("ride_id", old_ride_id),
                "verify pipeline old ride deleted",
            ).data or [],
            [],
        )

        matches = self._assert_matches_for_flights([old_rider.flight_id, new_rider.flight_id], 2)
        self.assertEqual({row["flight_id"] for row in matches}, {old_rider.flight_id, new_rider.flight_id})
        new_ride_ids = {int(row["ride_id"]) for row in matches}
        self.assertEqual(len(new_ride_ids), 1)
        self.assertNotIn(old_ride_id, new_ride_ids)

        new_ride = self._execute(
            self.sb.table("Rides").select("ride_type").eq("ride_id", sorted(new_ride_ids)[0]),
            "verify pipeline Connect ride",
        ).data[0]
        self.assertEqual(new_ride["ride_type"], "Connect")

    def test_pipeline_failure_before_commit_marks_failed_without_commit_side_effects(self):
        target = self._target("pipeline-pre-commit-failure")
        rider_a = self._create_pipeline_rider(
            label="pipeline-pre-failure-a",
            flight_offset=801,
            date=self.PIPELINE_DATE,
        )
        rider_b = self._create_pipeline_rider(
            label="pipeline-pre-failure-b",
            flight_offset=802,
            date=self.PIPELINE_DATE,
        )
        expected_error = "integration pre-commit failure"

        with self.assertRaisesRegex(RuntimeError, expected_error):
            self._run_pipeline_with_riders(
                [rider_a, rider_b],
                target=target,
                patchers=[
                    self._disable_connect_patch(),
                    mock.patch.object(self.main, "match_bucket", side_effect=RuntimeError(expected_error)),
                ],
            )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "failed")
        self.assertIn(expected_error, status["error_message"])
        run_id = self._track_run_id(status)
        self.assertIsNone(self._matching_run(run_id))
        self._assert_matches_for_flights([rider_a.flight_id, rider_b.flight_id], 0)

        flights = self._execute(
            self.sb.table("Flights")
            .select("flight_id,matching_status,original_unmatched")
            .in_("flight_id", [rider_a.flight_id, rider_b.flight_id]),
            "verify pre-commit failure Flights unchanged",
        ).data or []
        self.assertTrue(all(row["matching_status"] == "submitted" for row in flights))
        self.assertTrue(all(not row["original_unmatched"] for row in flights))

    def test_pipeline_failure_during_commit_marks_failed_and_rpc_rolls_back(self):
        target = self._target("pipeline-commit-failure")
        rider = self._create_pipeline_rider(
            label="pipeline-commit-failure",
            flight_offset=901,
            date=self.PIPELINE_DATE,
        )

        def fail_during_real_rpc(sb, *, run_id: str, payload: dict):
            bad_member = {
                "user_id": rider.user_id,
                "flight_id": rider.flight_id,
                "date": self.PIPELINE_DATE,
                "time": "10:30:00",
                "earliest_time": "10:00:00",
                "latest_time": "12:00:00",
                "source": "ml",
                "is_verified": False,
                "is_subsidized": False,
                "uber_type": "X",
            }
            bad_group = {
                "ride_date": self.PIPELINE_DATE,
                "ride_type": None,
                "airport": "LAX",
                "to_airport": False,
                "is_subsidized": False,
                "members": [bad_member],
            }
            bad_payload = {
                "run_id": run_id,
                "groups": [bad_group, bad_group],
                "matched_flight_ids": [rider.flight_id],
                "unmatched_flight_ids": [],
                "connect_cleanup_flight_ids": [],
            }
            return sb.rpc(
                "commit_matching_run",
                {"p_run_id": run_id, "p_payload": bad_payload},
            ).execute().data

        with self.assertRaises(Exception):
            self._run_pipeline_with_riders(
                [rider],
                target=target,
                patchers=[
                    self._disable_connect_patch(),
                    mock.patch.object(self.main, "commit_matching_run", side_effect=fail_during_real_rpc),
                ],
            )

        status = self._algorithm_status_for_target(target)
        self.assertEqual(status["status"], "failed")
        self.assertTrue(status["error_message"])
        run_id = self._track_run_id(status)
        self.assertIsNone(self._matching_run(run_id))
        self._assert_matches_for_flights([rider.flight_id], 0)

        rides = self._execute(
            self.sb.table("Rides").select("ride_id").eq("ride_date", self.PIPELINE_DATE),
            "verify commit failure rolled back Rides",
        ).data or []
        self.assertEqual(rides, [])

        flight = self._execute(
            self.sb.table("Flights")
            .select("matching_status,original_unmatched")
            .eq("flight_id", rider.flight_id),
            "verify commit failure Flight unchanged",
        ).data[0]
        self.assertEqual(flight["matching_status"], "submitted")
        self.assertFalse(flight["original_unmatched"])

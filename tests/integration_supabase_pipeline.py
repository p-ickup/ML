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

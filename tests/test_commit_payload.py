"""Tests for atomic matching commit payload construction."""

import unittest

import commit_payload
from ruleMatching import Match
from tests.helpers import make_rider, patch_config


class TestCommitPayloadValidation(unittest.TestCase):
    def test_rejects_duplicate_matched_flight(self):
        a = make_rider(1)
        m1 = Match(riders=[a], suggested_time_iso="2026-05-12T11:00:00", terminal="1")
        m2 = Match(riders=[a], suggested_time_iso="2026-05-12T11:30:00", terminal="1")

        with self.assertRaises(commit_payload.MatchingCommitError):
            commit_payload.build_matching_commit_payload(
                run_id="11111111-1111-1111-1111-111111111111",
                matches=[m1, m2],
                all_riders=[a],
                connect_for_cleanup=[],
            )

    def test_rejects_matched_and_unmatched_overlap(self):
        payload = {
            "run_id": "11111111-1111-1111-1111-111111111111",
            "groups": [
                {
                    "ride_date": "2026-05-12",
                    "airport": "LAX",
                    "to_airport": True,
                    "is_subsidized": False,
                    "members": [
                        {
                            "user_id": "11111111-1111-1111-1111-111111111111",
                            "flight_id": 1,
                            "date": "2026-05-12",
                            "time": "11:00:00",
                            "earliest_time": "10:00:00",
                            "latest_time": "12:00:00",
                            "source": "ml",
                        }
                    ],
                }
            ],
            "unmatched_flight_ids": [1],
        }

        with self.assertRaises(commit_payload.MatchingCommitError):
            commit_payload.validate_matching_commit_payload(payload)


class TestBuildMatchingCommitPayload(unittest.TestCase):
    def test_builds_groups_and_unmatched_ids(self):
        with patch_config(COVERED_DATES_EXPLICIT=False):
            a = make_rider(1)
            b = make_rider(2)
            c = make_rider(3)
            a.user_id = "11111111-1111-1111-1111-111111111111"
            b.user_id = "22222222-2222-2222-2222-222222222222"
            c.user_id = "33333333-3333-3333-3333-333333333333"
            m = Match(riders=[a, b], suggested_time_iso="2026-05-12T11:45:00", terminal="1")
            m.group_subsidy = True

            payload = commit_payload.build_matching_commit_payload(
                run_id="11111111-1111-1111-1111-111111111111",
                matches=[m],
                all_riders=[a, b, c],
                connect_for_cleanup=[],
            )

        self.assertEqual(payload["matched_flight_ids"], [1, 2])
        self.assertEqual(payload["unmatched_flight_ids"], [3])
        self.assertEqual(len(payload["groups"]), 1)
        self.assertEqual(len(payload["groups"][0]["members"]), 2)
        self.assertEqual(payload["groups"][0]["airport"], "LAX")
        self.assertTrue(payload["groups"][0]["to_airport"])
        self.assertTrue(payload["groups"][0]["is_subsidized"])
        self.assertNotIn("voucher", payload["groups"][0]["members"][0])
        self.assertNotIn("contingency_voucher", payload["groups"][0]["members"][0])

    def test_cross_midnight_window_uses_next_day_end(self):
        a = make_rider(1, date="2026-05-12", earliest_time="23:00:00", latest_time="02:00:00")
        b = make_rider(2, date="2026-05-12", earliest_time="23:30:00", latest_time="01:00:00")

        earliest_time, latest_time = commit_payload.compute_group_time_window([a, b])

        self.assertEqual(earliest_time, "23:30:00")
        self.assertEqual(latest_time, "01:00:00")

    def test_payload_members_use_cross_midnight_group_window(self):
        with patch_config(COVERED_DATES_EXPLICIT=False):
            riders = [
                make_rider(1, date="2026-05-12", earliest_time="22:30:00", latest_time="02:30:00"),
                make_rider(2, date="2026-05-12", earliest_time="23:15:00", latest_time="01:45:00"),
                make_rider(3, date="2026-05-12", earliest_time="23:40:00", latest_time="01:10:00"),
            ]
            for idx, rider in enumerate(riders, start=1):
                rider.user_id = f"00000000-0000-0000-0000-00000000000{idx}"
            match = Match(
                riders=riders,
                suggested_time_iso="2026-05-13T00:55:00",
                terminal="1",
            )

            payload = commit_payload.build_matching_commit_payload(
                run_id="11111111-1111-1111-1111-111111111111",
                matches=[match],
                all_riders=riders,
                connect_for_cleanup=[],
            )

        members = payload["groups"][0]["members"]
        self.assertEqual({member["earliest_time"] for member in members}, {"23:40:00"})
        self.assertEqual({member["latest_time"] for member in members}, {"01:10:00"})
        self.assertEqual({member["date"] for member in members}, {"2026-05-13"})
        self.assertEqual({member["time"] for member in members}, {"00:55:00"})


class TestCommitMatchingRun(unittest.TestCase):
    def _payload(self):
        return {
            "run_id": "11111111-1111-1111-1111-111111111111",
            "groups": [
                {
                    "ride_date": "2026-05-12",
                    "airport": "LAX",
                    "to_airport": True,
                    "is_subsidized": False,
                    "members": [
                        {
                            "user_id": "11111111-1111-1111-1111-111111111111",
                            "flight_id": 1,
                            "date": "2026-05-12",
                            "time": "11:00:00",
                            "earliest_time": "10:00:00",
                            "latest_time": "12:00:00",
                            "source": "ml",
                        }
                    ],
                }
            ],
            "unmatched_flight_ids": [],
        }

    def test_calls_rpc_with_run_id_and_payload(self):
        class FakeRpc:
            data = {"run_id": "11111111-1111-1111-1111-111111111111"}

            def execute(self):
                return self

        class FakeSupabase:
            def __init__(self):
                self.calls = []

            def rpc(self, name, params):
                self.calls.append((name, params))
                return FakeRpc()

        payload = self._payload()
        fake = FakeSupabase()

        result = commit_payload.commit_matching_run(
            fake,
            run_id="11111111-1111-1111-1111-111111111111",
            payload=payload,
        )

        self.assertEqual(result["run_id"], "11111111-1111-1111-1111-111111111111")
        self.assertEqual(fake.calls[0][0], "commit_matching_run")
        self.assertEqual(fake.calls[0][1]["p_run_id"], "11111111-1111-1111-1111-111111111111")
        self.assertEqual(fake.calls[0][1]["p_payload"], payload)

    def test_retries_transient_rpc_error_with_same_run_id(self):
        class FakeRpc:
            def __init__(self, parent):
                self.parent = parent

            def execute(self):
                self.parent.executions += 1
                if self.parent.executions == 1:
                    raise RuntimeError("service unavailable")
                self.data = {
                    "run_id": "11111111-1111-1111-1111-111111111111",
                    "idempotent_replay": False,
                }
                return self

        class FakeSupabase:
            def __init__(self):
                self.calls = []
                self.executions = 0

            def rpc(self, name, params):
                self.calls.append((name, params))
                return FakeRpc(self)

        payload = self._payload()
        fake = FakeSupabase()

        result = commit_payload.commit_matching_run(
            fake,
            run_id="11111111-1111-1111-1111-111111111111",
            payload=payload,
            retry_delay_seconds=0,
        )

        self.assertFalse(result["idempotent_replay"])
        self.assertEqual(fake.executions, 2)
        self.assertEqual(fake.calls[0][1]["p_run_id"], fake.calls[1][1]["p_run_id"])
        self.assertEqual(fake.calls[0][1]["p_payload"], fake.calls[1][1]["p_payload"])

    def test_does_not_retry_non_transient_rpc_error(self):
        class FakeRpc:
            def __init__(self, parent):
                self.parent = parent

            def execute(self):
                self.parent.executions += 1
                raise RuntimeError("duplicate key value violates unique constraint")

        class FakeSupabase:
            def __init__(self):
                self.executions = 0

            def rpc(self, _name, _params):
                return FakeRpc(self)

        fake = FakeSupabase()

        with self.assertRaises(RuntimeError):
            commit_payload.commit_matching_run(
                fake,
                run_id="11111111-1111-1111-1111-111111111111",
                payload=self._payload(),
                retry_delay_seconds=0,
            )

        self.assertEqual(fake.executions, 1)


if __name__ == "__main__":
    unittest.main()

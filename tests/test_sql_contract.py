"""Contract checks between the Python commit client and checked-in SQL RPC."""

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
COMMIT_RPC_SQL = REPO_ROOT / "documentation" / "sql" / "001_commit_matching_run.sql"


class TestCommitRpcSqlContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = COMMIT_RPC_SQL.read_text()
        cls.normalized_sql = re.sub(r"\s+", " ", cls.sql.lower())

    def test_rpc_signature_matches_python_client_parameters(self):
        self.assertRegex(
            self.normalized_sql,
            r"create or replace function public\.commit_matching_run"
            r"\s*\(\s*p_run_id uuid,\s*p_payload jsonb\s*\)",
        )

    def test_rpc_remains_service_role_only(self):
        self.assertIn(
            "revoke all on function public.commit_matching_run(uuid, jsonb) "
            "from authenticated;",
            self.normalized_sql,
        )
        self.assertIn(
            "grant execute on function public.commit_matching_run(uuid, jsonb) "
            "to service_role;",
            self.normalized_sql,
        )

    def test_rpc_persists_ml_matching_statuses(self):
        self.assertIn("set matching_status = 'matched'", self.normalized_sql)
        self.assertIn("set matching_status = 'unmatched'", self.normalized_sql)

    def test_rpc_rejects_missing_required_group_voucher(self):
        self.assertIn(
            "if v_group_voucher_id is null then raise exception "
            "'no available group voucher",
            self.normalized_sql,
        )

    def test_rpc_rejects_missing_required_contingency_voucher(self):
        self.assertIn(
            "if v_contingency_voucher_id is null then raise exception "
            "'no available contingency voucher",
            self.normalized_sql,
        )

    def test_rpc_uses_group_subsidy_as_match_source_of_truth(self):
        self.assertIn(
            "coalesce((member_item->>'is_verified')::boolean, false), "
            "v_group_is_subsidized, nullif(member_item->>'uber_type', '')",
            self.normalized_sql,
        )
        self.assertNotIn(
            "coalesce((member_item->>'is_subsidized')::boolean, false)",
            self.normalized_sql,
        )

    def test_rpc_keeps_run_ledger_and_transactional_write_set(self):
        for required_fragment in (
            'insert into public."matchingruns"',
            'insert into public."rides"',
            'insert into public."matches"',
            'update public."vouchers"',
            'update public."flights"',
        ):
            self.assertIn(required_fragment, self.normalized_sql)


if __name__ == "__main__":
    unittest.main()

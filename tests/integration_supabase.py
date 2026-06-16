"""Entrypoint for live Supabase integration tests.

Run from the repository root:

    python3 -m unittest tests.integration_supabase

The concrete tests live in:

- tests.integration_supabase_commit
- tests.integration_supabase_pipeline
"""

from __future__ import annotations

import unittest

from tests import integration_supabase_commit, integration_supabase_pipeline


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromModule(integration_supabase_commit))
    suite.addTests(loader.loadTestsFromModule(integration_supabase_pipeline))
    return suite


if __name__ == "__main__":
    unittest.main()

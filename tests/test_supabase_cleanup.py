"""Tests for deterministic integration-client cleanup."""

import unittest

from tests.integration_supabase_base import close_supabase_client


class Closeable:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


class AsyncNamedCloseable:
    def __init__(self):
        self.closed = 0

    def aclose(self):
        self.closed += 1


class Component:
    def __init__(self, session):
        self.session = session
        self._client = session


class FakeSupabase:
    def __init__(self):
        self.auth = Closeable()
        self._postgrest = AsyncNamedCloseable()
        shared_session = Closeable()
        self._storage = Component(shared_session)
        self._functions = Component(shared_session)
        self.shared_session = shared_session


class TestCloseSupabaseClient(unittest.TestCase):
    def test_closes_initialized_clients_and_shared_sessions_once(self):
        client = FakeSupabase()

        close_supabase_client(client)

        self.assertEqual(client.auth.closed, 1)
        self.assertEqual(client._postgrest.closed, 1)
        self.assertEqual(client.shared_session.closed, 1)

    def test_none_is_safe(self):
        close_supabase_client(None)


if __name__ == "__main__":
    unittest.main()

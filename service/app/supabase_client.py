import os
from functools import lru_cache

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")


class SupabaseNotConfigured(RuntimeError):
    """Raised when a DB-backed request is made without SUPABASE_URL/SUPABASE_SECRET_KEY set."""


def is_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_SECRET_KEY)


@lru_cache(maxsize=1)
def get_client() -> Client:
    if not is_configured():
        raise SupabaseNotConfigured(
            "SUPABASE_URL and SUPABASE_SECRET_KEY are not set. Add them to your "
            "environment or to a .env file next to this service (see .env.example), "
            "or pass 'riders' directly in the /match request body instead."
        )
    return create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

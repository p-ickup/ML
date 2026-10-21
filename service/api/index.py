"""Vercel Python entrypoint — re-exports the FastAPI ASGI app as-is.

Vercel's Python runtime detects the `app` ASGI callable here and routes every
request to it per vercel.json's rewrite rule; FastAPI handles its own
internal routing (/, /health, /ready, /match) exactly as it does locally or
in the Docker image.
"""

from app.main import app

__all__ = ["app"]

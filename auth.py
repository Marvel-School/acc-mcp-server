import os
import requests
import time
import logging
import threading

logger = logging.getLogger(__name__)

# --- Configuration ---
APS_CLIENT_ID = os.environ.get("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET")

# Fail fast — crash the container immediately if credentials are missing.
# This prevents a "healthy" container that silently fails on every API call.
if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
    raise SystemExit(
        "FATAL: APS_CLIENT_ID and APS_CLIENT_SECRET environment variables are required. "
        "Copy .env.example to .env and fill in your Autodesk credentials."
    )

# OAuth Scopes — viewables:read is required for Model Derivative API
APS_SCOPES = "data:read data:write data:create bucket:read viewables:read account:read account:write"

# Viewer-only scope — the browser-side Forge Viewer should never receive the full admin token.
VIEWER_SCOPES = "viewables:read"

# Token caches with dedicated locks
_token_cache = {"access_token": None, "expires_at": 0}
_token_lock = threading.Lock()

_viewer_token_cache = {"access_token": None, "expires_at": 0}
_viewer_token_lock = threading.Lock()


def _fetch_token(scope: str, cache: dict, lock: threading.Lock, force_refresh: bool) -> str:
    """Shared helper: fetch or return a cached 2-legged OAuth token.

    The lock wraps the entire read-check-write sequence so two threads
    cannot both see an expired token and both fire a token request.

    Args:
        scope:         OAuth scope string.
        cache:         Mutable dict with 'access_token' and 'expires_at' keys.
        lock:          threading.Lock guarding this cache.
        force_refresh: If True, ignores the cache and requests a new token.

    Returns:
        Access token string.

    Raises:
        requests.exceptions.RequestException: If the token request fails.
    """
    with lock:
        if not force_refresh and time.time() < cache["expires_at"]:
            logger.debug(
                "TOKEN cache hit (expires in %.0fs)",
                cache["expires_at"] - time.time(),
            )
            return cache["access_token"]

        logger.info("TOKEN requesting new token (scopes: %s)", scope[:40])

        resp = requests.post(
            "https://developer.api.autodesk.com/authentication/v2/token",
            data={
                "client_id": APS_CLIENT_ID,
                "client_secret": APS_CLIENT_SECRET,
                "grant_type": "client_credentials",
                "scope": scope,
            },
            timeout=15,
        )

        if resp.status_code != 200:
            logger.error(f"Token request failed ({resp.status_code}): {resp.text}")
            resp.raise_for_status()

        token_data = resp.json()
        expires_in = token_data["expires_in"]
        cache["access_token"] = token_data["access_token"]
        cache["expires_at"] = time.time() + expires_in - 60
        logger.info("TOKEN acquired (expires in %ds)", expires_in)
        return cache["access_token"]


def get_token(force_refresh: bool = False) -> str:
    """Retrieves a 2-legged OAuth access token, using a cached value when possible.

    Thread-safe: concurrent callers will not trigger redundant token requests.
    """
    return _fetch_token(APS_SCOPES, _token_cache, _token_lock, force_refresh)


def get_viewer_token(force_refresh: bool = False) -> str:
    """Retrieves a 2-legged OAuth access token scoped to viewables:read only.

    This token is safe to send to the browser-side Forge Viewer because it
    cannot read, write, or administer any project data.
    Thread-safe: concurrent callers will not trigger redundant token requests.
    """
    return _fetch_token(VIEWER_SCOPES, _viewer_token_cache, _viewer_token_lock, force_refresh)

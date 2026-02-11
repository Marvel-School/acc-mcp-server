import os
import requests
import time
import logging

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

# Token cache
_token_cache = {"access_token": None, "expires_at": 0}


def get_token(force_refresh: bool = False) -> str:
    """
    Retrieves a 2-legged OAuth access token, using a cached value when possible.

    Args:
        force_refresh: If True, ignores the cache and requests a new token immediately.

    Returns:
        Access token string.

    Raises:
        requests.exceptions.RequestException: If the token request fails.
    """
    # Return cached token if still valid and not forcing refresh
    if not force_refresh and time.time() < _token_cache["expires_at"]:
        logger.debug(f"Using cached token (expires in {int(_token_cache['expires_at'] - time.time())}s)")
        return _token_cache["access_token"]

    if force_refresh:
        logger.info("Force refreshing token (requested by caller)")

    logger.info(f"Authenticating with scopes: {APS_SCOPES}")

    resp = requests.post(
        "https://developer.api.autodesk.com/authentication/v2/token",
        data={
            "client_id": APS_CLIENT_ID,
            "client_secret": APS_CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": APS_SCOPES,
        },
        timeout=15,
    )

    if resp.status_code != 200:
        logger.error(f"Token request failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    token_data = resp.json()
    _token_cache["access_token"] = token_data["access_token"]
    _token_cache["expires_at"] = time.time() + token_data["expires_in"] - 60
    logger.info("Token acquired successfully.")
    return _token_cache["access_token"]

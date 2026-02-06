import os
import requests
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
APS_CLIENT_ID = os.environ.get("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET")
ACC_ADMIN_EMAIL = os.environ.get("ACC_ADMIN_EMAIL")

# OAuth Scopes - CRITICAL: viewables:read is required for Model Derivative API
APS_SCOPES = "data:read data:write data:create bucket:read viewables:read"

# API Base URLs
BASE_URL_ACC = "https://developer.api.autodesk.com/construction"
BASE_URL_HQ_US = "https://developer.api.autodesk.com/hq/v1/accounts"
BASE_URL_HQ_EU = "https://developer.api.autodesk.com/hq/v1/regions/eu/accounts"
BASE_URL_GRAPHQL = "https://developer.api.autodesk.com/aec/graphql"

# Global token cache
token_cache = {"access_token": None, "expires_at": 0}

def get_token() -> str:
    """Retrieves or refreshes the 2-legged access token."""
    global token_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        logger.error("APS credentials missing.")
        raise ValueError("Error: APS credentials missing.")

    if time.time() < token_cache["expires_at"]:
        logger.debug(f"Using cached token (expires in {int(token_cache['expires_at'] - time.time())}s)")
        return token_cache["access_token"]

    logger.info("🔄 Authenticating with scopes: " + APS_SCOPES)
    url = "https://developer.api.autodesk.com/authentication/v2/token"

    # Using POST Body for credentials to avoid 400 Bad Request
    data = {
        "client_id": APS_CLIENT_ID,
        "client_secret": APS_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": APS_SCOPES  # Use the global constant
    }

    try:
        resp = requests.post(url, data=data)
        
        # Loud Fail: Log the exact error from Autodesk if 400
        if resp.status_code == 400:
            logger.error(f"❌ Token Refresh Failed (400): {resp.text}")
            resp.raise_for_status()

        resp.raise_for_status()
        token_data = resp.json()
        token_cache["access_token"] = token_data["access_token"]
        token_cache["expires_at"] = time.time() + token_data["expires_in"] - 60
        return token_cache["access_token"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get token: {e}")
        raise


def clear_token_cache():
    """Force clears the token cache to request a fresh token on next call."""
    global token_cache
    token_cache = {"access_token": None, "expires_at": 0}
    logger.info("🗑️ Token cache cleared. Next API call will request fresh token.")

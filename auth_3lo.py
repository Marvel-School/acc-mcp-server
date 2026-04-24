"""
3-Legged OAuth (Authorization Code flow) for Autodesk APS.

This module adds per-user authentication alongside the 2-legged OAuth
service-account flow in auth.py. The two flows are independent:
  - auth.py       — client_credentials (service account). Used by all
                    existing tools today. Unchanged.
  - auth_3lo.py   — authorization_code (per user). Tokens are stored
                    per MCP session and are available to any future
                    tool that opts in via get_user_token(session_id).

Flow:
  1. Tool autodesk_login on nav_mcp returns an authorization URL.
  2. User visits the URL in a browser, signs in with their Autodesk
     account, and grants consent.
  3. Autodesk redirects to APS_REDIRECT_URI (which must resolve to the
     /callback route exposed in server.py) carrying a one-time code.
  4. /callback exchanges the code for (access_token, refresh_token) and
     deposits them into _pending_tokens, keyed by MCP session_id (which
     was round-tripped through the OAuth `state` parameter).
  5. On the next autodesk_login call the tool migrates the pending
     entry into _session_tokens, enriches it with the user's display
     name and email via the APS userinfo endpoint, and reports success.
  6. get_user_token(session_id) returns a valid access token for any
     authenticated session, automatically refreshing it when expired.

STORAGE WARNING — in-memory only. Both _session_tokens and
_pending_tokens live in process memory and are lost on container
restart. Users must re-authenticate after any redeploy, scale event,
or Azure App Service recycle. A persistent store (Redis, database) is
required for production-grade durability.

APS docs: https://aps.autodesk.com/en/docs/oauth/v2/tutorials/get-3-legged-token/
"""

import os
import time
import logging
import threading
import urllib.parse
from typing import Optional

import httpx

from auth import APS_CLIENT_ID, APS_CLIENT_SECRET

logger = logging.getLogger(__name__)

# --- Configuration ---------------------------------------------------------
APS_REDIRECT_URI = os.environ.get(
    "APS_REDIRECT_URI",
    "https://autodesk-agent-dev.azurewebsites.net/callback",
)
APS_3LO_SCOPES = "data:read data:write data:create viewables:read account:read"

APS_AUTH_URL = "https://developer.api.autodesk.com/authentication/v2/authorize"
APS_TOKEN_URL = "https://developer.api.autodesk.com/authentication/v2/token"
APS_USERINFO_URL = "https://api.userprofile.autodesk.com/userinfo"

# --- In-memory session store -----------------------------------------------
# Keyed by MCP session_id. Entry shape:
#   {
#     "access_token":  str,
#     "refresh_token": str | None,
#     "expires_at":    float,   # unix seconds
#     "user_name":     str,
#     "user_email":    str,
#   }
_session_tokens: dict[str, dict] = {}
_session_lock = threading.Lock()

# Bridge dict — the /callback route has no MCP Context, so it cannot
# write directly into session state. It drops token data here keyed by
# session_id (carried through OAuth `state`) and the next tool call
# migrates the entry into _session_tokens.
_pending_tokens: dict[str, dict] = {}


# --- Public API ------------------------------------------------------------

def build_auth_url(session_id: str) -> str:
    """Build the Autodesk authorization URL with session_id in the state."""
    params = {
        "response_type": "code",
        "client_id": APS_CLIENT_ID,
        "redirect_uri": APS_REDIRECT_URI,
        "scope": APS_3LO_SCOPES,
        "state": session_id,
    }
    return f"{APS_AUTH_URL}?{urllib.parse.urlencode(params)}"


async def exchange_code(code: str) -> dict:
    """Exchange an authorization code for access and refresh tokens."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            APS_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": APS_REDIRECT_URI,
            },
            auth=(APS_CLIENT_ID, APS_CLIENT_SECRET),
        )
        resp.raise_for_status()
        return resp.json()


async def refresh_access_token(session_id: str) -> Optional[str]:
    """Refresh an expired access token using the stored refresh token.

    Updates the entry in _session_tokens in place. Returns the new
    access token, or None if the session has no refresh token or the
    refresh request fails (e.g. refresh token revoked).
    """
    with _session_lock:
        entry = _session_tokens.get(session_id)
        if entry is None or not entry.get("refresh_token"):
            return None
        refresh_token = entry["refresh_token"]

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                APS_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "scope": APS_3LO_SCOPES,
                },
                auth=(APS_CLIENT_ID, APS_CLIENT_SECRET),
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as e:
        logger.warning(
            "TOKEN 3LO refresh failed for session=%s: %s",
            session_id[:8], e,
        )
        return None

    new_access = data["access_token"]
    new_refresh = data.get("refresh_token") or refresh_token
    new_expires_at = time.time() + data.get("expires_in", 3600)

    with _session_lock:
        entry = _session_tokens.get(session_id)
        if entry is None:
            return None
        entry["access_token"] = new_access
        entry["refresh_token"] = new_refresh
        entry["expires_at"] = new_expires_at

    logger.info("TOKEN 3LO refreshed for session=%s", session_id[:8])
    return new_access


async def get_user_info(access_token: str) -> dict:
    """Fetch display name and email from the OpenID Connect userinfo endpoint.

    Uses ``https://api.userprofile.autodesk.com/userinfo`` (the legacy
    ``/userprofile/v1/users/@me`` endpoint on developer.api.autodesk.com
    returns 410 Gone).

    The OIDC response shape is:
        {
          "sub":                "...",
          "name":               "Full Name",
          "given_name":         "First",
          "family_name":        "Last",
          "email":              "user@example.com",
          "preferred_username": "..."
        }

    Returns a dict with 'name' and 'email' keys. On any failure (network
    error, non-2xx response, malformed JSON) this function logs a warning
    and returns {"name": "Autodesk User", "email": ""} so the caller's
    auth flow is never interrupted by a profile-lookup failure.
    """
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                APS_USERINFO_URL,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            resp.raise_for_status()
            data = resp.json()
        return {
            "name": data.get("name", "") or "Autodesk User",
            "email": data.get("email", "") or "",
        }
    except Exception as e:
        logger.warning("TOKEN 3LO userinfo fetch failed: %s", e)
        return {"name": "Autodesk User", "email": ""}


async def get_user_token(session_id: str) -> Optional[str]:
    """Return a valid 3LO access token for the given MCP session.

    Auto-refreshes the token when it has expired (or is within 60 s of
    expiry) provided a refresh token is on file. Returns None when no
    authenticated session exists for this session_id or the refresh
    fails.
    """
    with _session_lock:
        entry = _session_tokens.get(session_id)
        if entry is None:
            return None
        access = entry["access_token"]
        expires_at = entry["expires_at"]
        has_refresh = bool(entry.get("refresh_token"))

    if access and time.time() < expires_at - 60:
        return access

    if has_refresh:
        return await refresh_access_token(session_id)

    return None


def store_pending_tokens(session_id: str, token_data: dict) -> None:
    """Write freshly exchanged tokens to the /callback → tool bridge.

    Called by the /callback route (which has no MCP Context). The next
    autodesk_login tool call for this session will migrate the entry
    into _session_tokens.
    """
    with _session_lock:
        _pending_tokens[session_id] = token_data


async def migrate_pending_tokens(session_id: str) -> bool:
    """Promote a pending-token entry into the per-session store.

    Fetches the user's display name and email via the APS userinfo
    endpoint so autodesk_login can greet the user by name. Removes the
    entry from _pending_tokens on success so it cannot be migrated twice.

    Returns:
        True if a pending entry was found and migrated; False if nothing
        was pending for this session_id.
    """
    with _session_lock:
        pending = _pending_tokens.pop(session_id, None)
    if pending is None:
        return False

    try:
        info = await get_user_info(pending["access_token"])
    except Exception as e:
        logger.warning(
            "TOKEN 3LO userinfo failed for session=%s: %s",
            session_id[:8], e,
        )
        info = {"name": "Autodesk User", "email": ""}

    with _session_lock:
        _session_tokens[session_id] = {
            "access_token": pending["access_token"],
            "refresh_token": pending.get("refresh_token"),
            "expires_at": pending["expires_at"],
            "user_name": info["name"],
            "user_email": info["email"],
        }
    logger.info(
        "TOKEN 3LO session created for %s (session=%s)",
        info["name"], session_id[:8],
    )
    return True


def logout(session_id: str) -> bool:
    """Remove a session's tokens from the store.

    Also clears any pending entry so a stale /callback deposit does not
    silently re-log the user in on their next tool call.

    Returns:
        True if an authenticated session was removed; False if no such
        session existed.
    """
    with _session_lock:
        removed = _session_tokens.pop(session_id, None) is not None
        _pending_tokens.pop(session_id, None)
    if removed:
        logger.info("TOKEN 3LO session removed (session=%s)", session_id[:8])
    return removed


def is_authenticated(session_id: str) -> bool:
    """Return True if a non-expired 3LO token exists for this session."""
    with _session_lock:
        entry = _session_tokens.get(session_id)
        if entry is None:
            return False
        return bool(entry["access_token"]) and time.time() < entry["expires_at"]


def get_session_user(session_id: str) -> Optional[dict]:
    """Return cached {name, email} for a session, or None if unauthenticated.

    Used by autodesk_login to greet the user without issuing a fresh
    userinfo call on every invocation.
    """
    with _session_lock:
        entry = _session_tokens.get(session_id)
        if entry is None:
            return None
        return {
            "name": entry.get("user_name", ""),
            "email": entry.get("user_email", ""),
        }

"""
3-Legged OAuth (Authorization Code flow) for Autodesk APS.

This module adds per-user authentication alongside the 2-legged OAuth
service-account flow in auth.py. The two flows are independent:
  - auth.py       — client_credentials (service account). Used by all
                    existing tools today. Unchanged.
  - auth_3lo.py   — authorization_code (per user). Tokens are stored
                    in Azure Table Storage (see token_store.py) keyed
                    by user email and shared across the nav, admin and
                    bim FastMCP instances.

Flow:
  1. Tool autodesk_login on nav_mcp returns an authorization URL.
  2. User visits the URL in a browser, signs in with their Autodesk
     account, and grants consent.
  3. Autodesk redirects to APS_REDIRECT_URI (which must resolve to the
     /callback route exposed in server.py) carrying a one-time code.
  4. /callback exchanges the code for (access_token, refresh_token) and
     deposits them into _pending_tokens (in-memory, per-process bridge),
     keyed by MCP session_id (round-tripped through the OAuth `state`
     parameter).
  5. On the next autodesk_login call (or any tool call), the pending
     entry is migrated into Azure Table Storage: the token row is
     written under the user's email and a session_id → email pointer
     is created. From that point on the token is durable and visible
     to every MCP server in the deployment.
  6. get_user_token(session_id) returns a valid access token for any
     authenticated session, automatically refreshing it when expired.

The only remaining in-memory state is _pending_tokens, which lives just
long enough to bridge the gap between the OAuth /callback handler and
the next tool call from the same session.

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
from token_store import (
    get_token_by_email,
    store_token_by_email,
    delete_token_by_email,
    link_session_to_email,
    get_email_for_session,
    unlink_session,
    list_all_user_emails,
)

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

# --- Pending-token bridge --------------------------------------------------
# The /callback route has no MCP Context, so it cannot write directly into
# session state. It drops token data here keyed by session_id (carried
# through OAuth `state`) and the next tool call promotes the entry into
# persistent storage via migrate_pending_tokens.
#
# This is deliberately in-memory: the bridge is only used for the few
# seconds between the browser redirect and the user's next tool call
# within the same process.
_pending_tokens: dict[str, dict] = {}
_pending_lock = threading.Lock()


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


async def refresh_access_token(email: str, refresh_token: str) -> Optional[dict]:
    """Refresh an expired access token. Returns the new token row on
    success, or None if the refresh fails (refresh token revoked etc.).

    The caller is responsible for persisting the result via
    store_token_by_email — this function does not write storage itself.
    """
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
        logger.warning("TOKEN 3LO refresh failed for email=%s: %s", email, e)
        return None

    return {
        "access_token":  data["access_token"],
        "refresh_token": data.get("refresh_token") or refresh_token,
        "expires_at":    time.time() + data.get("expires_in", 3600),
    }


async def get_user_info(access_token: str) -> dict:
    """Fetch display name, email, and user_id from the OpenID Connect
    userinfo endpoint.

    Uses ``https://api.userprofile.autodesk.com/userinfo`` (the legacy
    ``/userprofile/v1/users/@me`` endpoint on developer.api.autodesk.com
    returns 410 Gone).

    Returns a dict with 'name', 'email', and 'id' keys. On any failure
    (network error, non-2xx response, malformed JSON) this function logs
    a warning and returns sentinel defaults so the caller's auth flow is
    never interrupted by a profile-lookup failure.
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
            "name":  data.get("name", "") or "Autodesk User",
            "email": data.get("email", "") or "",
            "id":    data.get("sub", "") or "",
        }
    except Exception as e:
        logger.warning("TOKEN 3LO userinfo fetch failed: %s", e)
        return {"name": "Autodesk User", "email": "", "id": ""}


# TODO: REMOVE WHEN STAGE 2 LANDS
async def _resolve_session_to_email(session_id: str) -> Optional[str]:
    """Resolve a session_id to a user email, with single-user fallback.

    SINGLE-USER FALLBACK for the experimental environment.
    Each FastMCP instance (nav, admin, bim) assigns its own session_id
    on connect, so a login on nav-server is invisible to admin-server.
    If exactly ONE user is logged in across the whole deployment, we
    assume any incoming session belongs to them and back-fill the
    sessiontouser pointer so future lookups are O(1) again.

    This is safe in single-user experimental but MUST be removed before
    multi-user production deployment — it would otherwise hand any
    drive-by session a fully authenticated identity.
    """
    email = await get_email_for_session(session_id)
    if email:
        return email

    all_emails = await list_all_user_emails()
    if len(all_emails) == 1:
        email = all_emails[0]
        await link_session_to_email(session_id, email)
        logger.warning(
            "TOKEN single-user fallback: linking session=%s to %s "
            "(remove this fallback before multi-user prod)",
            session_id[:8], email,
        )
        return email
    return None


async def get_user_token(session_id: str) -> Optional[str]:
    """Return a valid 3LO access token for the given MCP session.

    Resolves session_id → email via the session pointer table (with
    single-user fallback while on experimental), then loads the token
    row keyed by that email. Auto-refreshes the token when it has
    expired (or is within 60 s of expiry) provided a refresh token is
    on file. Returns None when:
      - no user has any token stored (single-user fallback can't trigger), or
      - the token row is missing, or
      - refresh fails (token revoked).

    Propagates TokenStorageUnavailable when Azure Table Storage is
    unreachable so the caller can render a "try again in a moment"
    message rather than a misleading "please log in".
    """
    email = await _resolve_session_to_email(session_id)
    if not email:
        return None

    entry = await get_token_by_email(email)
    if entry is None:
        return None

    if entry["access_token"] and time.time() < entry["expires_at"] - 60:
        return entry["access_token"]

    refresh = entry.get("refresh_token")
    if not refresh:
        return None

    refreshed = await refresh_access_token(email, refresh)
    if refreshed is None:
        # Refresh token has been revoked (or is otherwise unusable). Drop
        # the row entirely so the user is forced through a fresh
        # authorization flow on next attempt.
        await delete_token_by_email(email)
        await unlink_session(session_id)
        return None

    await store_token_by_email(email, {
        **refreshed,
        "user_name":  entry.get("user_name", ""),
        "user_id":    entry.get("user_id", ""),
        "user_email": email,
    })
    logger.info("TOKEN 3LO refreshed for email=%s", email)
    return refreshed["access_token"]


def store_pending_tokens(session_id: str, token_data: dict) -> None:
    """Write freshly exchanged tokens to the /callback → tool bridge.

    Called by the /callback route (which has no MCP Context). The next
    tool call for this session will migrate the entry into persistent
    storage via migrate_pending_tokens.
    """
    with _pending_lock:
        _pending_tokens[session_id] = token_data


async def migrate_pending_tokens(session_id: str) -> bool:
    """Promote a pending-token entry into persistent storage.

    Fetches the user's display name, email, and user_id via the APS
    userinfo endpoint, writes the token row keyed by email, and creates
    the session_id → email pointer. Removes the bridge entry on success
    so it cannot be migrated twice.

    Returns:
        True if a pending entry was found and migrated; False if nothing
        was pending for this session_id.
    """
    with _pending_lock:
        pending = _pending_tokens.pop(session_id, None)
    if pending is None:
        return False

    info = await get_user_info(pending["access_token"])
    email = info["email"]

    if not email:
        # Without an email we cannot key the token row. Treat as a hard
        # auth failure — the user will need to retry the login flow.
        logger.warning(
            "TOKEN 3LO migrate failed: userinfo returned no email "
            "(session=%s)", session_id[:8],
        )
        return False

    await store_token_by_email(email, {
        "access_token":  pending["access_token"],
        "refresh_token": pending.get("refresh_token"),
        "expires_at":    pending["expires_at"],
        "user_name":     info["name"],
        "user_id":       info["id"],
    })
    await link_session_to_email(session_id, email)

    logger.info(
        "TOKEN 3LO session created for %s <%s> (session=%s)",
        info["name"], email, session_id[:8],
    )
    return True


async def logout(session_id: str) -> bool:
    """End the current MCP session's link to its Autodesk identity.

    Removes the session_id → email pointer but leaves the token row in
    autodesktokens intact. This is intentional: a user logging out of
    one MCP session must not invalidate other concurrent sessions for
    the same user. To fully invalidate a token the user must revoke
    consent at Autodesk's end.

    Also clears any pending entry so a stale /callback deposit does not
    silently re-log the user in on their next tool call.

    Returns:
        True if a session pointer was removed; False if no such session
        existed.
    """
    with _pending_lock:
        _pending_tokens.pop(session_id, None)

    email = await get_email_for_session(session_id)
    if not email:
        return False

    await unlink_session(session_id)
    logger.info(
        "TOKEN 3LO session logged out (session=%s, email=%s)",
        session_id[:8], email,
    )
    return True


async def is_authenticated(session_id: str) -> bool:
    """Return True if a non-expired 3LO token exists for this session."""
    email = await _resolve_session_to_email(session_id)
    if not email:
        return False
    entry = await get_token_by_email(email)
    if entry is None:
        return False
    return bool(entry["access_token"]) and time.time() < entry["expires_at"]


async def get_session_user(session_id: str) -> Optional[dict]:
    """Return {name, email} for a session, or None if unauthenticated.

    Used by tools to show "(created by ...)" in success messages.
    """
    email = await _resolve_session_to_email(session_id)
    if not email:
        return None
    entry = await get_token_by_email(email)
    if entry is None:
        return None
    return {
        "name":  entry.get("user_name", ""),
        "email": entry.get("user_email", email),
    }

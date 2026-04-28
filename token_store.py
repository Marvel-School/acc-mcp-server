"""
Persistent token storage backed by Azure Table Storage.

Replaces the in-memory _session_tokens dict in auth_3lo.py so that 3LO
tokens:
  1. Survive container restarts and scale events.
  2. Follow the user across MCP sessions.
  3. Are visible to ALL three FastMCP instances (nav, admin, bim) — a
     login on nav is immediately usable from admin's create_project,
     bim's inspect_file, etc.

Two tables are used:

  autodesktokens
    PartitionKey = user email (lowercase)
    RowKey       = "current"
    Stores:      access_token, refresh_token, expires_at, user_name,
                 user_id, user_email
    The token row is the source of truth — keyed by identity, not by
    transient session_id.

  sessiontouser
    PartitionKey = "session"
    RowKey       = MCP session_id
    Stores:      email
    A pointer table that lets a tool call resolve session_id → email
    without re-running OAuth. logout() removes the pointer but leaves
    the token row intact, so other concurrent sessions for the same
    user continue to work.

The Azure Tables SDK (azure-data-tables) is synchronous, so every
public function in this module wraps the blocking call in
asyncio.to_thread to keep the FastMCP event loop responsive.
"""

import os
import logging
import asyncio
from typing import Optional

from azure.data.tables import TableClient
from azure.core.exceptions import (
    ResourceNotFoundError,
    HttpResponseError,
    ServiceRequestError,
    ServiceResponseError,
)

logger = logging.getLogger(__name__)


class TokenStorageUnavailable(RuntimeError):
    """Raised when Azure Table Storage cannot be reached on a read path.

    Tool wrappers in server.py catch this and surface a user-friendly
    "try again in a moment" message rather than crashing or telling the
    user to log in (which would be misleading — they may already be).
    """

# --- Configuration ---------------------------------------------------------

_CONN_STR = os.environ.get("TOKEN_STORAGE_CONNECTION_STRING")
if not _CONN_STR:
    raise SystemExit("FATAL: TOKEN_STORAGE_CONNECTION_STRING is required")

_TOKENS_TABLE = "autodesktokens"
_SESSIONS_TABLE = "sessiontouser"
_SESSION_PARTITION = "session"
_TOKEN_ROWKEY = "current"

tokens_table = TableClient.from_connection_string(_CONN_STR, table_name=_TOKENS_TABLE)
session_table = TableClient.from_connection_string(_CONN_STR, table_name=_SESSIONS_TABLE)

logger.info(
    "STORE initialized | tables=%s,%s",
    _TOKENS_TABLE, _SESSIONS_TABLE,
)


# --- Token row (autodesktokens) -------------------------------------------

def _read_token_entity(email: str) -> Optional[dict]:
    key = email.lower()
    try:
        entity = tokens_table.get_entity(partition_key=key, row_key=_TOKEN_ROWKEY)
    except ResourceNotFoundError:
        return None
    return {
        "access_token":  entity.get("access_token"),
        "refresh_token": entity.get("refresh_token"),
        "expires_at":    float(entity.get("expires_at", 0)),
        "user_name":     entity.get("user_name", ""),
        "user_id":       entity.get("user_id", ""),
        "user_email":    entity.get("user_email", key),
    }


async def get_token_by_email(email: str) -> Optional[dict]:
    """Read the stored token row for a user, or None if absent.

    Raises TokenStorageUnavailable if Azure Table Storage is unreachable
    (network failure, service outage). ResourceNotFoundError is treated
    as a normal "no token" result and returns None silently.
    """
    try:
        result = await asyncio.to_thread(_read_token_entity, email)
    except (HttpResponseError, ServiceRequestError, ServiceResponseError) as e:
        logger.error(
            "STORE token read FAILED | email=%s | %s: %s",
            email.lower(), type(e).__name__, e,
        )
        raise TokenStorageUnavailable(str(e)) from e
    logger.debug("STORE token read | email=%s", email.lower())
    return result


def _write_token_entity(email: str, token_data: dict) -> None:
    key = email.lower()
    entity = {
        "PartitionKey":  key,
        "RowKey":        _TOKEN_ROWKEY,
        "access_token":  token_data["access_token"],
        "refresh_token": token_data.get("refresh_token"),
        "expires_at":    float(token_data["expires_at"]),
        "user_name":     token_data.get("user_name", ""),
        "user_id":       token_data.get("user_id", ""),
        "user_email":    key,
    }
    tokens_table.upsert_entity(entity)


async def store_token_by_email(email: str, token_data: dict) -> None:
    """Upsert the token row for a user. Email is normalized to lowercase."""
    await asyncio.to_thread(_write_token_entity, email, token_data)
    logger.info("STORE token written | email=%s", email.lower())


def _delete_token_entity(email: str) -> bool:
    key = email.lower()
    try:
        tokens_table.delete_entity(partition_key=key, row_key=_TOKEN_ROWKEY)
        return True
    except ResourceNotFoundError:
        return False


async def delete_token_by_email(email: str) -> bool:
    """Delete the token row. Returns True if deleted, False if absent."""
    deleted = await asyncio.to_thread(_delete_token_entity, email)
    if deleted:
        logger.info("STORE token deleted | email=%s", email.lower())
    return deleted


# --- Session pointer (sessiontouser) --------------------------------------

def _write_session_entity(session_id: str, email: str) -> None:
    entity = {
        "PartitionKey": _SESSION_PARTITION,
        "RowKey":       session_id,
        "email":        email.lower(),
    }
    session_table.upsert_entity(entity)


async def link_session_to_email(session_id: str, email: str) -> None:
    """Map an MCP session_id to a user email. Upserts."""
    await asyncio.to_thread(_write_session_entity, session_id, email)
    logger.info(
        "STORE session linked | session=%s | email=%s",
        session_id[:8], email.lower(),
    )


def _read_session_entity(session_id: str) -> Optional[str]:
    try:
        entity = session_table.get_entity(
            partition_key=_SESSION_PARTITION, row_key=session_id,
        )
    except ResourceNotFoundError:
        return None
    return entity.get("email")


async def get_email_for_session(session_id: str) -> Optional[str]:
    """Resolve an MCP session_id to a user email, or None if unmapped.

    Raises TokenStorageUnavailable if Azure Table Storage is unreachable.
    """
    try:
        return await asyncio.to_thread(_read_session_entity, session_id)
    except (HttpResponseError, ServiceRequestError, ServiceResponseError) as e:
        logger.error(
            "STORE session read FAILED | session=%s | %s: %s",
            session_id[:8], type(e).__name__, e,
        )
        raise TokenStorageUnavailable(str(e)) from e


def _delete_session_entity(session_id: str) -> bool:
    try:
        session_table.delete_entity(
            partition_key=_SESSION_PARTITION, row_key=session_id,
        )
        return True
    except ResourceNotFoundError:
        return False


async def unlink_session(session_id: str) -> bool:
    """Remove the session→email mapping. The token row itself is untouched
    so other concurrent sessions for the same user keep working.
    """
    removed = await asyncio.to_thread(_delete_session_entity, session_id)
    if removed:
        logger.info("STORE session unlinked | session=%s", session_id[:8])
    return removed

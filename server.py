import os
import re
import json
import hmac
import uuid
import asyncio
import hashlib
import logging
import pathlib
import contextvars
from collections import OrderedDict
from contextlib import asynccontextmanager
from pythonjsonlogger.json import JsonFormatter
from fastmcp import FastMCP
from fastmcp.tools.tool import ToolResult
from auth import get_token, get_viewer_token
from api import (
    find_project_globally,
    inspect_generic_file,
    resolve_file_to_urn,
    get_latest_version_urn,
    trigger_translation,
    stream_count_elements,
    get_hubs,
    get_projects,
    get_top_folders,
    get_folder_contents,
    create_acc_project,
    replicate_folders,
    get_project_users,
    get_project_user_permissions,
    get_user_projects,
    add_project_user,
    get_all_hub_users,
    soft_delete_folder,
    safe_b64encode,
)

# --- Request correlation ID (ContextVar) ------------------------------------
_request_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)


class _RequestIdFilter(logging.Filter):
    """Injects the current request_id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id.get("-")  # type: ignore[attr-defined]
        return True


# --- Structured JSON logging ------------------------------------------------
_log_handler = logging.StreamHandler()
_log_handler.setFormatter(
    JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(request_id)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    )
)
_log_handler.addFilter(_RequestIdFilter())

logging.root.handlers.clear()
logging.root.addHandler(_log_handler)
logging.root.setLevel(logging.INFO)

logger = logging.getLogger(__name__)

admin_mcp = FastMCP("ACC_Admin")
nav_mcp = FastMCP("ACC_Navigator")
bim_mcp = FastMCP("ACC_BIM")
PORT = int(os.environ.get("WEBSITES_PORT") or os.environ.get("PORT") or 8000)

# Concurrency limiter — caps parallel tool invocations across all clients.
_tool_semaphore = asyncio.Semaphore(5)

MCP_API_KEY = os.environ.get("MCP_API_KEY", "").strip()
if not MCP_API_KEY:
    raise SystemExit(
        "FATAL: MCP_API_KEY environment variable is required. "
        "Set it to a long random string and pass the same value as the X-API-Key header in client requests."
    )

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "").strip()
if _raw_origins == "*":
    ALLOWED_ORIGINS: list[str] = ["*"]
    logger.warning("ALLOWED_ORIGINS=* is insecure, do not use in production.")
elif _raw_origins:
    ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]
else:
    ALLOWED_ORIGINS = []
    logger.warning(
        "ALLOWED_ORIGINS not set — CORS is fully restricted. "
        "Set this env var to allow MCP clients to connect."
    )

_HIGHLIGHT_STATE: OrderedDict[str, dict] = OrderedDict()
_HIGHLIGHT_MAX_ENTRIES = 50
_COLOR_MAP: dict[str, list[float]] = {
    "red": [1, 0, 0, 1],
    "green": [0, 1, 0, 1],
    "blue": [0, 0, 1, 1],
    "yellow": [1, 1, 0, 1],
    "orange": [1, 0.5, 0, 1],
    "clear": [0, 0, 0, 0],
}

# Used by _format_permissions_report to filter UUID-only role names
_PERM_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Used by _format_permissions_report to map access levels to display labels
_LEVEL_LABEL = {
    "projectAdmin": "Project Admin",
    "projectMember": "Project Member",
    "executive": "Executive",
    "projectController": "Project Controller",
}


async def _resolve_hub_id(hub_name: str) -> str:
    """Resolve a hub display name to its Autodesk Hub ID.

    Uses a two-phase match: exact first, then substring fallback.
    Returns the hub ID string (e.g. 'b.xxxxxxxx-...').

    Raises:
        ValueError: If zero or multiple hubs match the given name.
    """
    hubs = await asyncio.to_thread(get_hubs)
    target = hub_name.lower().strip()

    # Phase 1: exact match (case-insensitive)
    exact = [
        h for h in hubs
        if h.get("attributes", {}).get("name", "").lower().strip() == target
    ]
    if len(exact) == 1:
        return exact[0].get("id")
    if len(exact) > 1:
        names = [h.get("attributes", {}).get("name", "?") for h in exact]
        raise ValueError(
            f"Ambiguous hub name '{hub_name}' — found {len(exact)} exact matches: "
            + ", ".join(names)
            + ". Please provide a more specific name."
        )

    # Phase 2: substring match (only when zero exact matches)
    substring = [
        h for h in hubs
        if target in h.get("attributes", {}).get("name", "").lower()
    ]
    if len(substring) == 1:
        return substring[0].get("id")

    all_names = [h.get("attributes", {}).get("name", "?") for h in hubs]
    if not substring:
        raise ValueError(
            f"No hub found matching '{hub_name}'. "
            f"Available hubs: {', '.join(all_names)}"
        )
    matched_names = [h.get("attributes", {}).get("name", "?") for h in substring]
    raise ValueError(
        f"Multiple hubs contain '{hub_name}': "
        + ", ".join(matched_names)
        + ". Please use the exact hub name."
    )


async def _resolve_project_id(hub_id: str, project_name: str) -> tuple[str, str, str]:
    """Resolve a project display name to (hub_id, project_id, resolved_name).

    Uses a two-phase match (exact then substring) within *hub_id* first.
    If nothing is found there, falls back to a global search across all
    accessible hubs with the same two-phase logic.  The returned hub_id may
    differ from the input when the project lives in a different hub.

    Raises:
        ValueError: If zero or multiple projects match the given name.
    """
    projects = await asyncio.to_thread(get_projects, hub_id)
    target = project_name.lower().strip()

    # --- 1. Search in the specified hub (exact → substring) ---
    exact = []
    substring = []
    for p in projects:
        p_name = p.get("attributes", {}).get("name", "")
        p_lower = p_name.lower().strip()
        if p_lower == target:
            exact.append((hub_id, p.get("id"), p_name))
        elif target in p_lower:
            substring.append((hub_id, p.get("id"), p_name))

    # Exact matches take priority
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        listing = "\n".join(f"  - {name} (id: {pid})" for _, pid, name in exact)
        raise ValueError(
            f"Ambiguous project name '{project_name}' — found {len(exact)} exact matches:\n"
            + listing
            + "\nPlease provide the project ID to disambiguate."
        )

    # Substring fallback (only when zero exact matches in this hub)
    if len(substring) == 1:
        return substring[0]
    if len(substring) > 1:
        listing = "\n".join(f"  - {name} (id: {pid})" for _, pid, name in substring)
        raise ValueError(
            f"Multiple projects contain '{project_name}':\n"
            + listing
            + "\nPlease use the exact project name."
        )

    # --- 2. Not found in specified hub — search all hubs (exact → substring) ---
    logger.info(
        "Project '%s' not found in hub %s — searching all hubs…",
        project_name, hub_id,
    )
    global_matches = await asyncio.to_thread(find_project_globally, project_name)

    # find_project_globally returns (exact_matches, substring_matches)
    global_exact, global_sub = global_matches

    if len(global_exact) == 1:
        g_hub_id, g_hub_name, g_proj_id, g_proj_name = global_exact[0]
        logger.info(
            "Found project '%s' in hub '%s' (cross-hub exact match)",
            g_proj_name, g_hub_name,
        )
        return (g_hub_id, g_proj_id, g_proj_name)
    if len(global_exact) > 1:
        listing = "\n".join(
            f"  - {name} in hub '{hname}' (id: {pid})"
            for _, hname, pid, name in global_exact
        )
        raise ValueError(
            f"Ambiguous project name '{project_name}' — found {len(global_exact)} "
            f"exact matches across hubs:\n"
            + listing
            + "\nPlease provide a more specific name."
        )

    # Substring fallback across all hubs
    if len(global_sub) == 1:
        g_hub_id, g_hub_name, g_proj_id, g_proj_name = global_sub[0]
        logger.info(
            "Found project '%s' in hub '%s' (cross-hub substring match)",
            g_proj_name, g_hub_name,
        )
        return (g_hub_id, g_proj_id, g_proj_name)
    if len(global_sub) > 1:
        listing = "\n".join(
            f"  - {name} in hub '{hname}' (id: {pid})"
            for _, hname, pid, name in global_sub
        )
        raise ValueError(
            f"Multiple projects contain '{project_name}' across hubs:\n"
            + listing
            + "\nPlease use the exact project name."
        )

    # Nothing found anywhere
    all_names = [p.get("attributes", {}).get("name", "?") for p in projects[:30]]
    suffix = f" (showing 30 of {len(projects)})" if len(projects) > 30 else ""
    raise ValueError(
        f"No project found matching '{project_name}' in any hub. "
        f"Projects in specified hub{suffix}: {', '.join(all_names)}"
    )


async def _resolve_folder_id(hub_id: str, project_id: str, folder_name: str) -> str:
    """Resolve a top-level folder display name to its folder ID.

    Uses a two-phase match: exact first, then substring fallback.
    Returns the folder ID on success.

    Raises:
        ValueError: If zero or multiple folders match the given name.
    """
    folders = await asyncio.to_thread(get_top_folders, hub_id, project_id)
    target = folder_name.lower().strip()

    # Phase 1: exact match (case-insensitive)
    exact = [f for f in folders if (f.get("name") or "").lower().strip() == target]
    if len(exact) == 1:
        return exact[0].get("id")
    if len(exact) > 1:
        names = [f.get("name") or "?" for f in exact]
        raise ValueError(
            f"Ambiguous folder name '{folder_name}' — found {len(exact)} exact matches: "
            + ", ".join(names)
            + ". Please provide the folder ID to disambiguate."
        )

    # Phase 2: substring match (bidirectional, only when zero exact matches)
    substring = [
        f for f in folders
        if target in (f.get("name") or "").lower() or (f.get("name") or "").lower() in target
    ]
    if len(substring) == 1:
        return substring[0].get("id")

    all_names = [f.get("name") or "?" for f in folders]
    if not substring:
        raise ValueError(
            f"No folder found matching '{folder_name}'. "
            f"Available folders: {', '.join(all_names)}"
        )
    matched_names = [f.get("name") or "?" for f in substring]
    raise ValueError(
        f"Multiple folders contain '{folder_name}': "
        + ", ".join(matched_names)
        + ". Please use the exact folder name."
    )


@nav_mcp.tool()
async def find_project(name_query: str) -> str:
    """
    Search for an ACC/BIM 360 project by name across ALL accessible hubs.

    Use this to verify a project exists and discover which hub it belongs to.
    Other tools accept a project name directly, so calling this first is
    optional but useful when the exact name or hub is unknown.
    The search is case-insensitive and matches substrings.

    Args:
        name_query: Full or partial project name (e.g. "Marvel", "Grasbaan").

    Returns:
        Project name, project ID, and hub name on success; error message
        otherwise. When multiple projects match, lists all candidates.
    """
    async with _tool_semaphore:
        try:
            exact, substring = await asyncio.to_thread(find_project_globally, name_query)

            # Prefer exact matches; fall back to substring only when no exact match
            effective = exact or substring
            match_type = "exact" if exact else "substring"

            if not effective:
                hubs = await asyncio.to_thread(get_hubs)
                hub_names = [h.get("attributes", {}).get("name", "?") for h in hubs]
                return (
                    f"No project found matching '{name_query}' in any accessible hub.\n"
                    f"Searched hubs: {', '.join(hub_names) if hub_names else '(none)'}"
                )

            if len(effective) == 1:
                hub_id, hub_name, project_id, project_name = effective[0]
                return (
                    f"Found Project ({match_type} match):\n"
                    f"  Name:       {project_name}\n"
                    f"  Project ID: {project_id}\n"
                    f"  Hub:        {hub_name} ({hub_id})\n\n"
                    f"You can now use this project name with other tools."
                )

            # Multiple matches — list them all so the user can pick.
            lines = [f"Found {len(effective)} projects matching '{name_query}' ({match_type}):\n"]
            for hub_id, hub_name, project_id, project_name in effective:
                lines.append(
                    f"  - {project_name} (id: {project_id}) — hub: {hub_name}"
                )
            lines.append("\nPlease use the exact project name to narrow down.")
            return "\n".join(lines)
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"find_project failed: {e}")
            return f"Failed to search for project: {e}"


@nav_mcp.tool()
async def list_hubs() -> str:
    """
    Lists all Autodesk Hubs (BIM 360 / ACC) accessible to the service account.
    Use this to find the hub name needed for list_projects, create_project,
    and other hub-scoped tools.
    """
    async with _tool_semaphore:
        try:
            hubs = await asyncio.to_thread(get_hubs)
            if not hubs:
                return "No hubs found. Check your Autodesk account permissions."

            report = "Found Hubs:\n"
            for hub in hubs:
                name = hub.get("attributes", {}).get("name", "Unknown")
                report += f"- {name}\n"
            return report
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"list_hubs failed: {e}")
            return f"Failed to list hubs: {e}"


@nav_mcp.tool()
async def list_projects(hub_name: str, fields: str = "") -> str:
    """
    Lists all projects in a hub.

    Optionally request extra metadata by passing a comma-separated list of fields
    (e.g. "status,projectValue,postalCode,city,constructionType").

    Args:
        hub_name: The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        fields:   Comma-separated extra fields (optional). Leave empty for default view.
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)

            field_list = [f.strip() for f in fields.split(",") if f.strip()] if fields else None
            projects = await asyncio.to_thread(get_projects, hub_id, 50, field_list)
            if not projects:
                return f"No projects found in hub '{hub_name}'."

            report = f"Found {len(projects)} Projects:\n"
            for p in projects:
                attrs = p.get("attributes", {})
                name = attrs.get("name", "Unknown")
                report += f"- {name}\n"

                for key in (field_list or []):
                    val = attrs.get(key)
                    if val is not None:
                        report += f"    {key}: {val}\n"

            _tw = getattr(projects, "truncation_warning", "")
            if _tw:
                report += f"\n{_tw}"

            return report
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"list_projects failed: {e}")
            return f"Failed to list projects: {e}"



@nav_mcp.tool()
async def list_top_folders(hub_name: str, project_name: str) -> str:
    """
    Lists top-level folders in a project (e.g. 'Project Files', 'Plans').
    This is the starting point for navigating a project's file structure.

    Args:
        hub_name:     The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        project_name: The Project name (e.g. "Grasbaan"). Use list_projects to find names.
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, resolved_name = await _resolve_project_id(hub_id, project_name)

            folders = await asyncio.to_thread(get_top_folders, hub_id, project_id)
            if not folders:
                return "No top-level folders found."

            report = "Top Folders:\n"
            for f in folders:
                name = f.get("name") or f.get("attributes", {}).get("displayName", "Unknown")
                fid = f.get("id") or ""
                fid_display = f"...{fid[-12:]}" if len(fid) > 12 else fid
                report += f"- {name} (ID: {fid_display})\n"
            return report
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"list_top_folders failed: {e}")
            return f"Failed to list folders: {e}"


@nav_mcp.tool()
async def list_folder_contents(hub_name: str, project_name: str, folder_name: str = "Project Files") -> str:
    """
    Lists files and subfolders inside a top-level folder.
    Use list_top_folders first to discover available folder names.

    Args:
        hub_name:     The Hub name (e.g. "TBI Holding").
        project_name: The Project name (e.g. "Grasbaan").
        folder_name:  The top-level folder name (e.g. "Project Files"). Defaults to "Project Files".
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, resolved_name = await _resolve_project_id(hub_id, project_name)
            folder_id = await _resolve_folder_id(hub_id, project_id, folder_name)

            items = await asyncio.to_thread(get_folder_contents, project_id, folder_id)
            if not items:
                return "Folder is empty."

            report = f"Folder Contents ({len(items)} items):\n"
            for item in items:
                name = item.get("name") or item.get("attributes", {}).get("displayName", "Unknown")
                iid = item.get("id") or ""
                iid_display = f"...{iid[-12:]}" if len(iid) > 12 else iid
                item_type = item.get("itemType") or item.get("type", "unknown")
                icon = "[folder]" if item_type in ("folder", "folders") else "[file]"
                report += f"  {icon} {name} (ID: {iid_display})\n"
            return report
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"list_folder_contents failed: {e}")
            return f"Failed to list folder contents: {e}"


@bim_mcp.tool()
async def inspect_file(hub_name: str, project_name: str, file_name: str) -> str:
    """
    Inspect a file's translation/processing status in the Model Derivative service.

    Returns translation status (Ready / Processing / Failed / Not Translated).

    Args:
        hub_name:     The Hub name (e.g. "TBI Holding").
        project_name: The Project name (e.g. "Grasbaan").
        file_name:    Filename (e.g. "MyFile.rvt").
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, _ = await _resolve_project_id(hub_id, project_name)

            return await asyncio.to_thread(inspect_generic_file, project_id, file_name)
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"inspect_file failed: {e}")
            return f"Failed to inspect file: {e}"



@bim_mcp.tool()
async def reprocess_file(hub_name: str, project_name: str, file_name: str) -> str:
    """
    Trigger a fresh Model Derivative translation job for a file.

    Use this when a file shows "No Property Database", translation errors,
    or when count_elements returns 0 unexpectedly.

    Args:
        hub_name:     The Hub name (e.g. "TBI Holding").
        project_name: The Project name (e.g. "Grasbaan").
        file_name:    Filename (e.g. "MyFile.rvt").

    Note: Translation typically takes 5-10 minutes. Use inspect_file to check progress.
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, _ = await _resolve_project_id(hub_id, project_name)

            lineage_urn = await asyncio.to_thread(resolve_file_to_urn, project_id, file_name)
            if not lineage_urn or not lineage_urn.startswith("urn:"):
                return f"Error: Could not resolve '{file_name}' to a valid lineage URN."

            version_urn = await asyncio.to_thread(get_latest_version_urn, project_id, lineage_urn)

            result = await asyncio.to_thread(trigger_translation, version_urn)
            errors = result.get("errors") or []
            if errors:
                error_detail = "; ".join(
                    e.get("detail", str(e)) if isinstance(e, dict) else str(e)
                    for e in errors
                )
                logger.error(f"reprocess_file: Autodesk returned errors: {error_detail}")
                return f"Translation request was rejected by Autodesk: {error_detail}"

            status = result.get("result", "unknown")
            return (
                f"\u2705 Translation job started for '{file_name}' (status: {status}).\n"
                f"Please wait 5-10 minutes, then use inspect_file or count_elements to verify."
            )
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"reprocess_file failed: {e}")
            return f"Failed to trigger reprocessing: {e}"


@bim_mcp.tool()
async def count_elements(hub_name: str, project_name: str, file_name: str, category_name: str) -> str:
    """
    Count elements in a Revit/IFC model that match a category name.

    Uses a streaming regex scanner — works on models of any size without
    loading the full JSON into memory. The search is case-insensitive and
    automatically tries singular forms (e.g. "Walls" also matches "Wall").

    Args:
        hub_name:      The Hub name (e.g. "TBI Holding").
        project_name:  The Project name (e.g. "Grasbaan").
        file_name:     Filename (e.g. "MyFile.rvt").
        category_name: Category to count (e.g. "Walls", "Doors", "Windows", "Floors").
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, _ = await _resolve_project_id(hub_id, project_name)

            lineage_urn = await asyncio.to_thread(resolve_file_to_urn, project_id, file_name)
            if not lineage_urn or not lineage_urn.startswith("urn:"):
                return f"Error: Could not resolve '{file_name}' to a valid URN."

            version_urn = await asyncio.to_thread(get_latest_version_urn, project_id, lineage_urn)

            count = await asyncio.to_thread(stream_count_elements, version_urn, category_name)
            return f"Found {count} elements matching '{category_name}' (including singular variations)."
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"count_elements failed: {e}")
            return f"Failed to count elements: {e}"



@admin_mcp.tool()
async def create_project(hub_name: str, name: str, project_type: str = "ACC") -> str:
    """
    Creates a new project in the specified Hub.

    Args:
        hub_name:     The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        name:         The name of the new project.
        project_type: 'ACC' or 'BIM360' (Default: ACC).
    """
    async with _tool_semaphore:
        try:
            real_hub_id = await _resolve_hub_id(hub_name)

            result = await asyncio.to_thread(create_acc_project, real_hub_id, name, project_type)
            new_id = result.get("id") or result.get("projectId")

            if new_id:
                return f"\u2705 Project '{name}' created successfully. Project ID: {new_id}"
            else:
                return "API succeeded, but couldn't parse the new Project ID from the response."
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"create_project failed: {e}")
            return f"Failed to create project: {e}"


@admin_mcp.tool()
async def list_project_users(hub_name: str, project_name: str) -> str:
    """
    Lists users assigned to a project (requires Admin permissions).

    Args:
        hub_name:     The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        project_name: The Project name (e.g. "Grasbaan"). Use list_projects to find names.
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, resolved_name = await _resolve_project_id(hub_id, project_name)

            users = await asyncio.to_thread(get_project_users, project_id, 1)
            if not users:
                return f"No users found in project '{resolved_name}' (or insufficient permissions)."

            report = f"Project Members ({len(users)}):\n"
            for u in users:
                name = u.get("name", u.get("email", "Unknown"))
                email = u.get("email", "")
                report += f"- {name} ({email})\n"

            _tw = getattr(users, "truncation_warning", "")
            if _tw:
                report += (
                    f"\nShowing first 100 users — use check_project_permissions "
                    f"for the full list."
                )

            return report
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"list_project_users failed: {e}")
            return f"Failed to list project users: {e}"


@admin_mcp.tool()
async def add_user(hub_name: str, project_name: str, email: str) -> str:
    """
    Adds a user to a project by email.

    Args:
        hub_name:     The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        project_name: The Project name (e.g. "Grasbaan"). Use list_projects to find names.
        email:        The user's email address.
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, resolved_name = await _resolve_project_id(hub_id, project_name)

            await asyncio.to_thread(add_project_user, project_id, email, ["docs"])
            return f"\u2705 User '{email}' added to project '{resolved_name}'."
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"add_user failed: {e}")
            return f"Failed to add user: {e}"


@admin_mcp.tool()
async def audit_hub_users(hub_name: str) -> str:
    """
    Scans the entire hub to list all users and the products they are assigned
    (e.g. Build, Docs, Takeoff). Aggregates across up to 20 projects.

    Args:
        hub_name: The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)

            users, skipped = await asyncio.to_thread(get_all_hub_users, hub_id)
            if not users and not skipped:
                return f"No users found across projects in hub '{hub_name}'."

            MAX_USERS = 100
            truncated = len(users) > MAX_USERS
            display_users = users[:MAX_USERS]

            report = f"Hub User Audit ({len(users)} unique users):\n\n"
            for u in display_users:
                products = ", ".join(u["products"]) if u["products"] else "none"
                report += f"- {u['name']} ({u['email']})\n    Products: {products}\n"

            if truncated:
                report += f"\n\u26a0\ufe0f Showing first {MAX_USERS} of {len(users)} users. Use a more specific query to narrow results."

            if skipped:
                report += (
                    f"\n\n\u26a0\ufe0f NOTE: {len(skipped)} project(s) were skipped due to permission errors: "
                    + ", ".join(skipped)
                )

            _tw = getattr(users, "truncation_warning", "")
            if _tw:
                report += f"\n\n{_tw}"

            return report
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"audit_hub_users failed: {e}")
            return f"Failed to audit hub users: {e}"


def _format_permissions_report(raw_users: list, project_name: str) -> str:
    """Transform raw API user records into a formatted permissions report.

    Deduplicates by email, merges roles/access-levels, and groups output
    into admins vs members.
    """

    def _safe(s: str) -> bool:
        return bool(s) and not _PERM_UUID_RE.match(s.strip())

    def _role_label(entry: dict) -> str:
        names = sorted(entry["roles"])
        if names:
            return ", ".join(names)
        for lvl in entry["levels"]:
            if lvl in _LEVEL_LABEL:
                return _LEVEL_LABEL[lvl]
        return "Member"

    def _fmt_entry(entry: dict) -> str:
        return (
            f"* **Name**: {entry['name']} | "
            f"**Company**: {entry['company']} | "
            f"**Role**: {_role_label(entry)}"
        )

    user_map: dict = {}
    for u in raw_users:
        key = u.get("email") or u.get("name") or "unknown"
        if key not in user_map:
            user_map[key] = {
                "name": u.get("name") or "Unknown",
                "company": u.get("companyName") or "\u2014",
                "roles": set(),
                "levels": set(),
            }
        for rn in u.get("roleNames", []):
            if _safe(rn):
                user_map[key]["roles"].add(rn)
        user_map[key]["levels"].update(u.get("accessLevels", []))

    deduped = list(user_map.values())
    admins = [e for e in deduped if "projectAdmin" in e["levels"]]
    members = [e for e in deduped if e not in admins]

    lines = [
        f"## Project Members: {project_name}",
        f"Total: {len(deduped)} ({len(admins)} admins, {len(members)} members)",
        "",
        "**Admins:**",
    ]
    lines += [_fmt_entry(e) for e in admins] or ["* (none)"]
    lines += ["", "**Members:**"]
    lines += [_fmt_entry(e) for e in members] or ["* (none)"]

    _tw = getattr(raw_users, "truncation_warning", "")
    if _tw:
        lines += ["", _tw]

    return "\n".join(lines)


@admin_mcp.tool()
async def check_project_permissions(hub_name: str, project_name: str) -> str:
    """
    Audit the real-time permissions, roles, and company affiliations for every
    user in an ACC project. Data is fetched live — no in-process caching —
    so the result always reflects the current state in ACC.

    Useful for answering questions like:
    - "Who is the project admin?"
    - "Which users belong to company X?"
    - "Does user Y have Build access?"

    Args:
        hub_name:     Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        project_name: Project name (exact or close match, case-insensitive).
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, project_id, resolved_name = await _resolve_project_id(hub_id, project_name)

            users = await asyncio.to_thread(get_project_user_permissions, project_id)
            if not users:
                return (
                    f"No users found for project '{resolved_name}' "
                    f"(or insufficient admin permissions)."
                )

            return _format_permissions_report(users, resolved_name)

        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"check_project_permissions failed: {e}")
            return f"Failed to check permissions: {e}"


@admin_mcp.tool()
async def find_user_projects(user_name: str) -> str:
    """
    Lists every ACC project a specific user has access to.

    Automatically searches ALL accessible hubs in parallel — no hub
    parameter is needed. Resolves the user by display name (substring,
    case-insensitive) or exact email address. Results are fetched live
    from ACC on every call (never cached).

    Output is grouped by hub with per-hub counts and a cross-hub total.

    Args:
        user_name: User's full name (or part of it) or their email address.
    """
    async with _tool_semaphore:
        try:
            hubs = await asyncio.to_thread(get_hubs)
            if not hubs:
                return "No hubs found. Check your Autodesk account permissions."

            # Build (account_id, hub_display) pairs for valid hubs.
            hub_pairs: list[tuple[str, str]] = []
            for hub in hubs:
                hub_id = hub.get("id")
                hub_display = hub.get("attributes", {}).get("name", "Unknown")
                if not hub_id:
                    continue
                account_id = hub_id[2:] if hub_id.startswith("b.") else hub_id
                hub_pairs.append((account_id, hub_display))

            # Query all hubs in parallel.
            tasks = [
                asyncio.to_thread(get_user_projects, account_id, user_name)
                for account_id, _ in hub_pairs
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            lines: list[str] = []
            total_projects = 0
            display_name = None
            email = None
            hub_errors: list[str] = []

            for (account_id, hub_display), result in zip(hub_pairs, results):
                if isinstance(result, Exception):
                    logger.warning("find_user_projects: hub '%s' failed: %s", hub_display, result)
                    hub_errors.append(hub_display)
                    continue

                # Capture user identity from the first successful result
                if display_name is None:
                    display_name = result["user_name"]
                    email = result["user_email"]

                projects = result["projects"]
                if not projects:
                    continue

                total_projects += len(projects)
                lines.append(f"\n**Hub: {hub_display}** ({len(projects)} projects)")
                for p in projects:
                    lines.append(f"* {p['name']} (Role: {p['role']})")

                _tw = result.get("warning", "")
                if _tw:
                    lines.append(_tw)

            if display_name is None:
                return f"User '{user_name}' not found in any accessible hub."

            header = f"**User**: {display_name}"
            if email:
                header += f"\n**Email**: {email}"
            header += f"\n**Total projects across all hubs**: {total_projects}"

            if hub_errors:
                header += (
                    f"\n\n\u26a0\ufe0f Could not search {len(hub_errors)} hub(s): "
                    + ", ".join(hub_errors)
                )

            if total_projects == 0:
                return header + "\n\nNo project assignments found (or insufficient admin permissions)."

            return header + "\n" + "\n".join(lines)

        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"find_user_projects failed: {e}")
            return f"Failed to find user projects: {e}"


@admin_mcp.tool()
async def apply_folder_template(hub_name: str, source_project_name: str, dest_project_name: str) -> str:
    """
    Copies the folder structure from a Source Project to a Destination Project.
    Executes immediately — no preview or confirmation step.

    Source and destination may live in different hubs; cross-hub resolution
    is automatic. Only folders are copied, not files. Folders that already
    exist in the destination are skipped (no duplicates).

    If a project name exists in multiple hubs, resolution will fail with an
    ambiguity error listing the candidates. In that case, ask the user to
    confirm which hub they mean and use find_project to verify first.

    The completion message always shows the resolved hub for both source
    and destination so the user can verify the correct projects were used.

    Args:
        hub_name:             The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        source_project_name:  Name of the template project to copy FROM.
        dest_project_name:    Name of the target project to copy TO.
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)

            # Resolve both projects — cross-hub fallback is automatic.
            src_hub_id, src_id, src_name = await _resolve_project_id(hub_id, source_project_name)
            dst_hub_id, dst_id, dst_name = await _resolve_project_id(hub_id, dest_project_name)

            # Look up hub display names for the completion message.
            hubs = await asyncio.to_thread(get_hubs)
            hub_names = {h.get("id"): h.get("attributes", {}).get("name", "?") for h in hubs}
            src_hub_name = hub_names.get(src_hub_id, src_hub_id)
            dst_hub_name = hub_names.get(dst_hub_id, dst_hub_id)

            # Execute immediately — no preview.
            summary = await asyncio.to_thread(replicate_folders, src_hub_id, src_id, dst_id)

            # Extract folder count from summary string (e.g. "Successfully copied 12 folders …")
            count_match = re.search(r"(\d+)\s+folders", summary)
            count = count_match.group(1) if count_match else "unknown"

            return (
                f"\u2705 Folder structure copied successfully.\n"
                f"Source: {src_name} (hub: {src_hub_name})\n"
                f"Destination: {dst_name} (hub: {dst_hub_name})\n"
                f"Folders created: {count}\n\n"
                f"If some folders already existed they were skipped."
            )
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"apply_folder_template failed: {e}")
            return f"Failed to apply folder template: {e}"


@admin_mcp.tool()
async def delete_folder(hub_name: str, project_name: str, folder_name: str) -> str:
    """
    Deletes (hides) a specific top-level folder within a project.
    Useful for cleaning up mistakes or removing unwanted template folders.

    Args:
        hub_name:      The Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        project_name:  The name of the project containing the folder.
        folder_name:   The name of the folder to delete (e.g. '01_WIP').
    """
    async with _tool_semaphore:
        try:
            hub_id = await _resolve_hub_id(hub_name)
            hub_id, found_id, _ = await _resolve_project_id(hub_id, project_name)

            result = await asyncio.to_thread(soft_delete_folder, hub_id, found_id, folder_name)
            if result.startswith("Successfully"):
                return f"\u2705 {result}"
            return result
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"delete_folder failed: {e}")
            return f"Failed to delete folder: {e}"


_VIEWER_HTML_PATH = pathlib.Path(__file__).parent / "viewer.html"
_VIEWER_HTML_CONTENT = _VIEWER_HTML_PATH.read_text(encoding="utf-8")
_VIEWER_HASH = hashlib.md5(_VIEWER_HTML_CONTENT.encode()).hexdigest()[:8]

VIEWER_URI = f"ui://preview-design/viewer-{_VIEWER_HASH}.html"

_ALLOWED_DOMAINS = [
    "https://*.autodesk.com",
    "https://*.autodesk360.com",
    "https://*.amazonaws.com",
    "https://cdn.jsdelivr.net",
]

_CSP_HEADER = (
    "default-src 'none'; "
    f"script-src 'unsafe-inline' 'unsafe-eval' {_ALLOWED_DOMAINS[0]} {_ALLOWED_DOMAINS[3]}; "
    f"style-src 'unsafe-inline' {_ALLOWED_DOMAINS[0]}; "
    f"connect-src {' '.join(_ALLOWED_DOMAINS)} wss://*.autodesk.com; "
    f"img-src {_ALLOWED_DOMAINS[0]} blob: data:; "
    f"font-src {_ALLOWED_DOMAINS[0]}; "
    "worker-src blob:; "
    "frame-src 'none'"
)


@bim_mcp.resource(
    VIEWER_URI,
    mime_type="text/html;profile=mcp-app",
    meta={
        "headers": {
            "Content-Security-Policy": _CSP_HEADER,
        }
    },
)
def viewer_resource() -> str:
    """Serves the APS Viewer HTML app."""
    return _VIEWER_HTML_CONTENT


@bim_mcp.tool()
async def preview_model(urn: str) -> ToolResult:
    """
    Opens a 3D preview of a translated model in the Autodesk Viewer.
    Accepts a version URN (base64-encoded or raw) and renders it in an embedded viewer.

    Args:
        urn: The version URN of the model to preview (e.g. from inspect_file or count_elements).
    """
    async with _tool_semaphore:
        try:
            encoded_urn = safe_b64encode(urn) if urn.startswith("urn:") else urn
            access_token = await asyncio.to_thread(get_viewer_token)

            structured = {
                "urn": encoded_urn,
                "config": {
                    "accessToken": access_token,
                    "env": "AutodeskProduction",
                    "api": "derivativeV2_EU",
                },
            }

            return ToolResult(
                content=f"Loading 3D preview for model URN: {urn[:60]}...",
                structured_content=structured,
            )
        except ValueError as e:
            return ToolResult(content=str(e))
        except Exception as e:
            logger.error(f"preview_model failed: {e}")
            return ToolResult(content=f"Failed to preview model: {e}")


@bim_mcp.tool()
async def highlight_elements(urn: str, ids: list[int], color: str = "red") -> str:
    """
    Highlight specific elements in the 3D viewer by coloring them.

    Use this after preview_model to visually emphasize elements (e.g. all Walls,
    specific doors, structural issues). The viewer polls for changes automatically.

    Args:
        urn:   The base64-encoded model URN (same as used in preview_model).
        ids:   List of dbId integers to highlight (from selection context or count_elements).
        color: Color name: "red", "green", "blue", "yellow", "orange", or "clear" to reset.
    """
    async with _tool_semaphore:
        try:
            if color == "clear" or not ids:
                _HIGHLIGHT_STATE[urn] = {"ids": [], "color": []}
                return f"Cleared all highlights for model."

            rgba = _COLOR_MAP.get(color, _COLOR_MAP["red"])
            if urn not in _HIGHLIGHT_STATE and len(_HIGHLIGHT_STATE) >= _HIGHLIGHT_MAX_ENTRIES:
                _HIGHLIGHT_STATE.popitem(last=False)  # evict oldest entry (FIFO)
            _HIGHLIGHT_STATE[urn] = {"ids": ids, "color": rgba}
            return f"Highlighted {len(ids)} element(s) in {color}."
        except ValueError as e:
            return str(e)
        except Exception as e:
            logger.error(f"highlight_elements failed: {e}")
            return f"Failed to highlight elements: {e}"


@bim_mcp.resource("highlight://{urn}")
async def get_highlights(urn: str) -> str:
    """Returns the current highlight state for a model URN."""
    return json.dumps(_HIGHLIGHT_STATE.get(urn, {"ids": [], "color": []}))


from mcp.types import ListToolsRequest, ReadResourceRequest

_UI_META = {"ui": {"resourceUri": VIEWER_URI}}

_CSP_META = {
    "ui": {
        "csp": {
            "resourceDomains": _ALLOWED_DOMAINS + ["blob:", "data:"],
            "connectDomains": _ALLOWED_DOMAINS + ["wss://*.autodesk.com"],
            "frameDomains": [],
        }
    }
}


def _inject_meta_via_handler() -> None:
    """Inject UI _meta into preview_model on tools/list responses."""
    low_level = bim_mcp._mcp_server
    original_handler = low_level.request_handlers.get(ListToolsRequest)
    if not original_handler:
        logger.warning("list_tools handler not found — _meta injection skipped")
        return

    async def _wrapped_handler(req):
        result = await original_handler(req)
        inner = getattr(result, "root", result)
        for tool in getattr(inner, "tools", []):
            if tool.name == "preview_model":
                tool.meta = _UI_META
        return result

    low_level.request_handlers[ListToolsRequest] = _wrapped_handler
    logger.info("Patch applied: _meta on preview_model")


def _inject_ui_extension_capability() -> None:
    """Advertise io.modelcontextprotocol/ui in the initialize handshake."""
    low_level = bim_mcp._mcp_server
    _orig_get_caps = low_level.get_capabilities  # bound method

    def _patched_get_caps(notification_options, experimental_capabilities):
        caps = _orig_get_caps(notification_options, experimental_capabilities)
        if caps.__pydantic_extra__ is None:
            caps.__pydantic_extra__ = {}
        caps.__pydantic_extra__.setdefault("extensions", {})[
            "io.modelcontextprotocol/ui"
        ] = {}
        return caps

    low_level.get_capabilities = _patched_get_caps
    logger.info("Patch applied: UI extension capability")


def _inject_csp_into_resource() -> None:
    """Inject CSP _meta into the viewer resource on read."""
    low_level = bim_mcp._mcp_server
    original_handler = low_level.request_handlers.get(ReadResourceRequest)
    if not original_handler:
        logger.warning("read_resource handler not found — CSP injection skipped")
        return

    async def _wrapped_handler(req):
        result = await original_handler(req)
        inner = getattr(result, "root", result)
        for content in getattr(inner, "contents", []):
            if str(content.uri) == VIEWER_URI:
                content.meta = _CSP_META
        return result

    low_level.request_handlers[ReadResourceRequest] = _wrapped_handler
    logger.info("Patch applied: CSP on viewer resource")


_viewer_patches_applied = 0

# Patch 1/3: Inject _meta UI hints into the preview_model tool definition
# so MCP clients know to render the tool result in an embedded viewer panel.
# Without this patch: the viewer still works but clients won't auto-open the
# viewer UI — users would need to manually open the structured content.
try:
    _inject_meta_via_handler()
    _viewer_patches_applied += 1
except Exception as e:
    logger.warning(
        "_inject_meta_via_handler failed — preview_model tool will not "
        "carry _meta UI hints, so the viewer may not open automatically. "
        "Error: %s", e,
    )

# Patch 2/3: Advertise the io.modelcontextprotocol/ui extension capability
# in the MCP initialize handshake so clients know this server supports UI.
# Without this patch: clients that gate UI features on this capability flag
# will not offer the embedded viewer at all.
try:
    _inject_ui_extension_capability()
    _viewer_patches_applied += 1
except Exception as e:
    logger.warning(
        "_inject_ui_extension_capability failed — MCP UI capability will "
        "not be advertised to clients. The viewer may not open "
        "automatically. Error: %s", e,
    )

# Patch 3/3: Inject Content-Security-Policy _meta into the viewer HTML
# resource so the browser allows loading the Autodesk Forge Viewer scripts.
# Without this patch: the viewer HTML loads but the browser blocks external
# scripts, resulting in a blank viewer panel.
try:
    _inject_csp_into_resource()
    _viewer_patches_applied += 1
except Exception as e:
    logger.warning(
        "_inject_csp_into_resource failed — viewer HTML resource will not "
        "carry CSP _meta, so the Forge Viewer scripts may be blocked by "
        "the browser. Error: %s", e,
    )

if _viewer_patches_applied == 3:
    logger.info("Viewer UI patches applied successfully (3/3)")
else:
    logger.warning(
        "Viewer UI patches: %d/3 applied — see warnings above",
        _viewer_patches_applied,
    )


if __name__ == "__main__":
    import uvicorn
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from starlette.requests import Request
    from starlette.responses import PlainTextResponse, JSONResponse
    from starlette.routing import Mount, Route
    from starlette.types import ASGIApp, Receive, Scope, Send

    class APIKeyMiddleware:
        """Rejects requests without a valid X-API-Key header.

        GET /health is exempt so Azure health probes pass without credentials.
        Assigns a short correlation ID to every request for log tracing.
        """

        def __init__(self, app: ASGIApp) -> None:
            self.app = app

        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] == "http":
                _request_id.set(uuid.uuid4().hex[:8])
                request = Request(scope)
                if not (request.method == "GET" and request.url.path == "/health"):
                    if not hmac.compare_digest(
                        request.headers.get("x-api-key", "").encode(),
                        MCP_API_KEY.encode(),
                    ):
                        response = PlainTextResponse("Unauthorized", status_code=401)
                        await response(scope, receive, send)
                        return
            await self.app(scope, receive, send)

    admin_asgi = admin_mcp.http_app(path="/", transport="streamable-http")
    nav_asgi = nav_mcp.http_app(path="/", transport="streamable-http")
    bim_asgi = bim_mcp.http_app(path="/", transport="streamable-http")

    async def health_check(request: Request) -> JSONResponse:
        """Shallow health check for Azure load balancer.

        Verifies credentials are configured and reports whether at least
        one successful Autodesk API call has been made (hub cache populated).
        Does NOT make live API calls — always returns fast.
        """
        from api import _cached_hub_id

        has_aps_creds = bool(
            os.environ.get("APS_CLIENT_ID", "").strip()
            and os.environ.get("APS_CLIENT_SECRET", "").strip()
        )
        has_api_key = bool(MCP_API_KEY)
        autodesk_connected = _cached_hub_id is not None

        if has_aps_creds and has_api_key:
            return JSONResponse({
                "status": "ok",
                "version": "1.0.0",
                "autodesk_connected": autodesk_connected,
            })

        reason = "Missing APS credentials" if not has_aps_creds else "Missing MCP_API_KEY"
        return JSONResponse(
            {
                "status": "degraded",
                "version": "1.0.0",
                "autodesk_connected": False,
                "reason": reason,
            },
            status_code=503,
        )

    @asynccontextmanager
    async def master_lifespan(app):
        async with admin_asgi.lifespan(app):
            async with nav_asgi.lifespan(app):
                async with bim_asgi.lifespan(app):
                    yield

    master_app = Starlette(
        lifespan=master_lifespan,
        routes=[
            Route("/health", health_check),
            Mount("/mcp/admin", app=admin_asgi),
            Mount("/mcp/nav", app=nav_asgi),
            Mount("/mcp/bim", app=bim_asgi),
        ],
        middleware=[
            Middleware(
                CORSMiddleware,
                allow_origins=ALLOWED_ORIGINS,
                allow_methods=["*"],
                allow_headers=["*"],
            ),
            Middleware(APIKeyMiddleware),
        ],
    )

    logger.info(f"Starting multi-endpoint MCP server on port {PORT}...")
    logger.info(f"  Admin:     http://0.0.0.0:{PORT}/mcp/admin")
    logger.info(f"  Navigator: http://0.0.0.0:{PORT}/mcp/nav")
    logger.info(f"  BIM:       http://0.0.0.0:{PORT}/mcp/bim")
    uvicorn.run(
        master_app,
        host="0.0.0.0",
        port=PORT,
        log_config=None,
        access_log=True,
    )

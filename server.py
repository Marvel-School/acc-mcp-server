import os
import re
import json
import asyncio
import hashlib
import logging
import pathlib
from fastmcp import FastMCP
from fastmcp.tools.tool import ToolResult
from auth import get_token
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

mcp = FastMCP("Autodesk ACC Agent")
PORT = int(os.environ.get("PORT", 8000))

_HIGHLIGHT_STATE: dict[str, dict] = {}
_COLOR_MAP: dict[str, list[float]] = {
    "red": [1, 0, 0, 1],
    "green": [0, 1, 0, 1],
    "blue": [0, 0, 1, 1],
    "yellow": [1, 1, 0, 1],
    "orange": [1, 0.5, 0, 1],
    "clear": [0, 0, 0, 0],
}


@mcp.tool()
async def find_project(name_query: str) -> str:
    """
    Search for an ACC/BIM 360 project by name across ALL accessible hubs.

    Use this FIRST to obtain a project_id before calling any other tool.
    The search is case-insensitive and matches substrings.

    Args:
        name_query: Full or partial project name (e.g. "Marvel", "Grasbaan").

    Returns:
        Project name, project_id, and hub_id on success; error message otherwise.
    """
    try:
        result = await asyncio.to_thread(find_project_globally, name_query)
        if result is None:
            return f"Project '{name_query}' not found in any accessible hub. Please check the name and try again."
        hub_id, project_id, project_name = result
        return (
            f"Found Project:\n"
            f"  Name:       {project_name}\n"
            f"  Project ID: {project_id}\n"
            f"  Hub ID:     {hub_id}\n\n"
            f"You can now use this project_id with other tools."
        )
    except Exception as e:
        logger.error(f"find_project failed: {e}")
        return f"Error searching for project: {e}"


@mcp.tool()
async def list_hubs() -> str:
    """
    Lists all Autodesk Hubs (BIM 360 / ACC) accessible to the service account.
    Use this to find the hub_id needed for list_projects or create_project.
    """
    try:
        hubs = await asyncio.to_thread(get_hubs)
        if not hubs:
            return "No hubs found. Check your Autodesk account permissions."

        report = "Found Hubs:\n"
        for hub in hubs:
            name = hub.get("attributes", {}).get("name", "Unknown")
            hub_id = hub.get("id")
            report += f"- {name} (ID: {hub_id})\n"
        return report
    except Exception as e:
        logger.error(f"list_hubs failed: {e}")
        return f"Failed to list hubs: {e}"


@mcp.tool()
async def list_projects(hub_id: str, fields: str = "") -> str:
    """
    Lists all projects in a hub.

    Optionally request extra metadata by passing a comma-separated list of fields
    (e.g. "status,projectValue,postalCode,city,constructionType").

    Args:
        hub_id: The Hub ID (starts with 'b.'). Use list_hubs to find this.
        fields: Comma-separated extra fields (optional). Leave empty for default view.
    """
    try:
        field_list = [f.strip() for f in fields.split(",") if f.strip()] if fields else None
        projects = await asyncio.to_thread(get_projects, hub_id, 50, field_list)
        if not projects:
            return f"No projects found in hub {hub_id}."

        report = f"Found {len(projects)} Projects:\n"
        for p in projects:
            attrs = p.get("attributes", {})
            name = attrs.get("name", "Unknown")
            pid = p.get("id")
            report += f"- {name} (ID: {pid})\n"

            for key in (field_list or []):
                val = attrs.get(key)
                if val is not None:
                    report += f"    {key}: {val}\n"

        return report
    except Exception as e:
        logger.error(f"list_projects failed: {e}")
        return f"Failed to list projects: {e}"



@mcp.tool()
async def list_top_folders(hub_id: str, project_id: str) -> str:
    """
    Lists top-level folders in a project (e.g. 'Project Files', 'Plans').
    This is the starting point for navigating a project's file structure.

    Args:
        hub_id:     The Hub ID (starts with 'b.').
        project_id: The Project ID (from find_project or list_projects).
    """
    try:
        folders = await asyncio.to_thread(get_top_folders, hub_id, project_id)
        if not folders:
            return "No top-level folders found."

        report = "Top Folders:\n"
        for f in folders:
            name = f.get("name") or f.get("attributes", {}).get("displayName", "Unknown")
            fid = f.get("id")
            report += f"- {name} (ID: {fid})\n"
        return report
    except Exception as e:
        logger.error(f"list_top_folders failed: {e}")
        return f"Failed to list folders: {e}"


@mcp.tool()
async def list_folder_contents(project_id: str, folder_id: str) -> str:
    """
    Lists files and subfolders inside a folder.
    Use list_top_folders first, then drill down with this tool.

    Args:
        project_id: The Project ID.
        folder_id:  The Folder ID (from list_top_folders or a previous list_folder_contents call).
    """
    try:
        items = await asyncio.to_thread(get_folder_contents, project_id, folder_id)
        if not items:
            return "Folder is empty."

        report = f"Folder Contents ({len(items)} items):\n"
        for item in items:
            name = item.get("name") or item.get("attributes", {}).get("displayName", "Unknown")
            iid = item.get("id")
            item_type = item.get("itemType") or item.get("type", "unknown")
            icon = "[folder]" if item_type in ("folder", "folders") else "[file]"
            report += f"  {icon} {name} (ID: {iid})\n"
            tip = item.get("tipVersionUrn")
            if tip:
                report += f"         Version URN: {tip}\n"
        return report
    except Exception as e:
        logger.error(f"list_folder_contents failed: {e}")
        return f"Failed to list folder contents: {e}"


@mcp.tool()
async def inspect_file(project_id: str, file_id: str) -> str:
    """
    Inspect a file's translation/processing status in the Model Derivative service.

    Accepts a filename OR a URN — no need to look up the URN manually.
    Returns translation status (Ready / Processing / Failed / Not Translated).

    Args:
        project_id: The project ID (from find_project).
        file_id:    Filename (e.g. "MyFile.rvt"), lineage URN, or version URN.
    """
    try:
        return await asyncio.to_thread(inspect_generic_file, project_id, file_id)
    except Exception as e:
        logger.error(f"inspect_file failed: {e}")
        return f"Error inspecting file: {e}"



@mcp.tool()
async def reprocess_file(project_id: str, file_id: str) -> str:
    """
    Trigger a fresh Model Derivative translation job for a file.

    Use this when a file shows "No Property Database", translation errors,
    or when count_elements returns 0 unexpectedly.

    Args:
        project_id: The project ID (from find_project).
        file_id:    Filename (e.g. "MyFile.rvt"), lineage URN, or version URN.

    Note: Translation typically takes 5-10 minutes. Use inspect_file to check progress.
    """
    try:
        lineage_urn = await asyncio.to_thread(resolve_file_to_urn, project_id, file_id)
        if not lineage_urn or not lineage_urn.startswith("urn:"):
            return f"Error: Could not resolve '{file_id}' to a valid lineage URN."

        version_urn = await asyncio.to_thread(get_latest_version_urn, project_id, lineage_urn)
        if not version_urn or not version_urn.startswith("urn:"):
            return f"Error: Could not resolve lineage URN to a version URN."

        result = await asyncio.to_thread(trigger_translation, version_urn)
        status = result.get("result", "unknown")
        return (
            f"Translation job started for '{file_id}' (status: {status}).\n"
            f"Please wait 5-10 minutes, then use inspect_file or count_elements to verify."
        )
    except Exception as e:
        logger.error(f"reprocess_file failed: {e}")
        return f"Error triggering reprocess: {e}"


@mcp.tool()
async def count_elements(project_id: str, file_id: str, category_name: str) -> str:
    """
    Count elements in a Revit/IFC model that match a category name.

    Uses a streaming regex scanner — works on models of any size without
    loading the full JSON into memory. The search is case-insensitive and
    automatically tries singular forms (e.g. "Walls" also matches "Wall").

    Args:
        project_id:    The project ID (from find_project).
        file_id:       Filename (e.g. "MyFile.rvt"), lineage URN, or version URN.
        category_name: Category to count (e.g. "Walls", "Doors", "Windows", "Floors").
    """
    try:
        lineage_urn = await asyncio.to_thread(resolve_file_to_urn, project_id, file_id)
        if not lineage_urn or not lineage_urn.startswith("urn:"):
            return f"Error: Could not resolve '{file_id}' to a valid URN."

        version_urn = await asyncio.to_thread(get_latest_version_urn, project_id, lineage_urn)
        if not version_urn or not version_urn.startswith("urn:"):
            return f"Error: Could not resolve to a version URN."

        count = await asyncio.to_thread(stream_count_elements, version_urn, category_name)
        return f"Found {count} elements matching '{category_name}' (including singular variations)."
    except ValueError as ve:
        return str(ve)
    except Exception as e:
        logger.error(f"count_elements failed: {e}")
        return f"Error scanning model: {e}"



@mcp.tool()
async def create_project(hub_id_or_name: str, name: str, project_type: str = "ACC") -> str:
    """
    Creates a new project in the specified Hub.

    Smart feature: accepts a Hub ID ('b.xxx') OR a Hub Name.
    If a name is provided, it automatically resolves it to the correct hub_id.

    Args:
        hub_id_or_name: Hub ID (starts with 'b.') OR Hub name (e.g. "TBI Holding").
        name:           The name of the new project.
        project_type:   'ACC' or 'BIM360' (Default: ACC).
    """
    try:
        real_hub_id = hub_id_or_name

        if not real_hub_id.startswith("b."):
            hubs = await asyncio.to_thread(get_hubs)
            found = None
            for h in hubs:
                h_name = h.get("attributes", {}).get("name", "")
                if h_name.lower() == hub_id_or_name.lower():
                    found = h.get("id")
                    break
            if found:
                real_hub_id = found
            else:
                return f"Could not find a hub named '{hub_id_or_name}'. Run list_hubs to see valid names."

        result = await asyncio.to_thread(create_acc_project, real_hub_id, name, project_type)
        new_id = result.get("id") or result.get("projectId")

        if new_id:
            return f"Project '{name}' successfully created! Project ID: {new_id}"
        else:
            return f"API succeeded, but couldn't parse ID. Raw response: {result}"
    except Exception as e:
        logger.error(f"create_project failed: {e}")
        return f"Failed to create project: {e}"


@mcp.tool()
async def list_project_users(hub_id: str, project_id: str) -> str:
    """
    Lists users assigned to a project (requires Admin permissions).

    Args:
        hub_id:     The Hub ID (starts with 'b.').
        project_id: The Project ID.
    """
    try:
        users = await asyncio.to_thread(get_project_users, hub_id, project_id)
        if not users:
            return f"No users found in project {project_id} (or insufficient permissions)."

        report = f"Project Members ({len(users)}):\n"
        for u in users[:30]:
            name = u.get("name", u.get("email", "Unknown"))
            email = u.get("email", "")
            report += f"- {name} ({email})\n"

        if len(users) > 30:
            report += f"\n(Showing 30 of {len(users)} users)"
        return report
    except Exception as e:
        logger.error(f"list_project_users failed: {e}")
        return f"Failed to list project users: {e}"


@mcp.tool()
async def add_user(hub_id: str, project_id: str, email: str) -> str:
    """
    Adds a user to a project by email.

    Args:
        hub_id:     The Hub ID (starts with 'b.'). Needed for admin auth.
        project_id: The Project ID.
        email:      The user's email address.
    """
    try:
        result = await asyncio.to_thread(add_project_user, hub_id, project_id, email, ["docs"])
        user_id = result.get("id", "unknown")
        return f"User '{email}' added to project. User ID: {user_id}"
    except Exception as e:
        logger.error(f"add_user failed: {e}")
        return f"Failed to add user: {e}"


@mcp.tool()
async def audit_hub_users(hub_id: str) -> str:
    """
    Scans the entire hub to list all users and the products they are assigned
    (e.g. Build, Docs, Takeoff). Aggregates across up to 20 projects.

    Args:
        hub_id: The Hub ID (starts with 'b.'). Use list_hubs to find this.
    """
    try:
        users = await asyncio.to_thread(get_all_hub_users, hub_id)
        if not users:
            return f"No users found across projects in hub {hub_id}."

        report = f"Hub User Audit ({len(users)} unique users):\n\n"
        for u in users:
            products = ", ".join(u["products"]) if u["products"] else "none"
            report += f"- {u['name']} ({u['email']})\n    Products: {products}\n"

        return report
    except Exception as e:
        logger.error(f"audit_hub_users failed: {e}")
        return f"Failed to audit hub users: {e}"


@mcp.tool()
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
    try:
        # Resolve hub name → hub_id
        hubs = await asyncio.to_thread(get_hubs)
        hub_target = hub_name.lower().strip()
        hub_id = None
        for h in hubs:
            if h.get("attributes", {}).get("name", "").lower() == hub_target:
                hub_id = h.get("id")
                break
        if not hub_id:
            return (
                f"Hub '{hub_name}' not found. "
                f"Run list_hubs to see valid names."
            )

        # Resolve project name → project_id
        projects = await asyncio.to_thread(get_projects, hub_id)
        proj_target = project_name.lower().strip()
        project_id = None
        resolved_name = None
        for p in projects:
            p_name = p.get("attributes", {}).get("name", "")
            if proj_target in p_name.lower():
                project_id = p.get("id")
                resolved_name = p_name
                break
        if not project_id:
            return (
                f"Project '{project_name}' not found in hub '{hub_name}'. "
                f"Run list_projects to see valid names."
            )

        # Fetch live permissions — explicitly uncached
        users = await asyncio.to_thread(get_project_user_permissions, project_id)
        if not users:
            return (
                f"No users found for project '{resolved_name}' "
                f"(or insufficient admin permissions)."
            )

        # UUID pattern — any value matching this is an internal system ID, not human text.
        _UUID = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.IGNORECASE,
        )

        def _safe(s: str) -> bool:
            """True when s is a non-empty, non-UUID display string."""
            return bool(s) and not _UUID.match(s.strip())

        # Deduplicate by email (falls back to name).
        # If ACC returns multiple rows per user (one per product/role), merge them.
        _LEVEL_LABEL = {
            "projectAdmin": "Project Admin",
            "projectMember": "Project Member",
            "executive": "Executive",
            "projectController": "Project Controller",
        }

        user_map: dict = {}
        for u in users:
            key = u.get("email") or u.get("name") or "unknown"
            if key not in user_map:
                user_map[key] = {
                    "name": u.get("name") or "Unknown",
                    "company": u.get("companyName") or "—",
                    "roles": set(),
                    "levels": set(),
                }
            for rn in u.get("roleNames", []):
                if _safe(rn):
                    user_map[key]["roles"].add(rn)
            user_map[key]["levels"].update(u.get("accessLevels", []))

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

        deduped = list(user_map.values())
        admins = [e for e in deduped if "projectAdmin" in e["levels"]]
        members = [e for e in deduped if e not in admins]

        lines = [
            f"## Project Members: {resolved_name}",
            f"Total: {len(deduped)} ({len(admins)} admins, {len(members)} members)",
            "",
            "**Admins:**",
        ]
        lines += [_fmt_entry(e) for e in admins] or ["* (none)"]
        lines += ["", "**Members:**"]
        lines += [_fmt_entry(e) for e in members] or ["* (none)"]

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"check_project_permissions failed: {e}")
        return f"Failed to fetch permissions: {e}"


@mcp.tool()
async def find_user_projects(hub_name: str, user_name: str) -> str:
    """
    Lists every ACC project a specific user has access to within a hub.

    Resolves the user by display name (substring, case-insensitive) or exact
    email address, then returns their project assignments in real-time.
    No results are cached — each call reflects the live state in ACC.

    Args:
        hub_name:  Hub name (e.g. "TBI Holding"). Use list_hubs to find names.
        user_name: User's full name (or part of it) or their email address.
    """
    try:
        # Resolve hub name → raw account_id (strip 'b.' prefix)
        hubs = await asyncio.to_thread(get_hubs)
        hub_target = hub_name.lower().strip()
        hub_id = None
        for h in hubs:
            if h.get("attributes", {}).get("name", "").lower() == hub_target:
                hub_id = h.get("id")
                break
        if not hub_id:
            return (
                f"Hub '{hub_name}' not found. "
                f"Run list_hubs to see valid names."
            )

        account_id = hub_id[2:] if hub_id.startswith("b.") else hub_id

        # Fetch live user-project assignments — no caching
        result = await asyncio.to_thread(get_user_projects, account_id, user_name)

        display_name = result["user_name"]
        email = result["user_email"]
        projects = result["projects"]

        header = f"**User**: {display_name} | **Hub**: {hub_name}"
        if email:
            header += f"\n**Email**: {email}"
        header += f"\n**Total projects**: {len(projects)}\n"

        if not projects:
            return header + "\nNo project assignments found (or insufficient admin permissions)."

        lines = [header]
        for p in projects:
            lines.append(f"* {p['name']} (Role: {p['role']})")

        return "\n".join(lines)

    except ValueError as ve:
        return str(ve)
    except Exception as e:
        logger.error(f"find_user_projects failed: {e}")
        return f"Failed to fetch user projects: {e}"


@mcp.tool()
async def apply_folder_template(hub_id: str, source_project_name: str, dest_project_name: str) -> str:
    """
    Copies the folder structure from a Source Project to a Destination Project.
    Useful for setting up new projects from a template. Only folders are copied, not files.

    Args:
        hub_id:               The Hub ID (starts with 'b.').
        source_project_name:  Name of the template project to copy FROM.
        dest_project_name:    Name of the target project to copy TO.
    """
    try:
        projects = await asyncio.to_thread(get_projects, hub_id)

        def _resolve(name: str) -> tuple:
            target = name.lower().strip()
            for p in projects:
                p_name = p.get("attributes", {}).get("name", "")
                if p_name.lower() == target:
                    return p.get("id"), p_name
            return None, None

        src_id, src_name = _resolve(source_project_name)
        if not src_id:
            return f"Source project '{source_project_name}' not found. Run list_projects to see valid names."

        dst_id, dst_name = _resolve(dest_project_name)
        if not dst_id:
            return f"Destination project '{dest_project_name}' not found. Run list_projects to see valid names."

        summary = await asyncio.to_thread(replicate_folders, hub_id, src_id, dst_id)
        return f"Template applied from '{src_name}' to '{dst_name}': {summary}"
    except Exception as e:
        logger.error(f"apply_folder_template failed: {e}")
        return f"Failed to replicate folder structure: {e}"


@mcp.tool()
async def delete_folder(hub_id: str, project_name: str, folder_name: str) -> str:
    """
    Deletes (hides) a specific top-level folder within a project.
    Useful for cleaning up mistakes or removing unwanted template folders.

    Args:
        hub_id:        The Hub ID (starts with 'b.').
        project_name:  The name of the project containing the folder.
        folder_name:   The name of the folder to delete (e.g. '01_WIP').
    """
    try:
        projects = await asyncio.to_thread(get_projects, hub_id)
        target = project_name.lower().strip()
        found_id = None
        for p in projects:
            p_name = p.get("attributes", {}).get("name", "")
            if p_name.lower() == target:
                found_id = p.get("id")
                break

        if not found_id:
            return f"Could not find a project named '{project_name}'. Run list_projects to see valid names."

        return await asyncio.to_thread(soft_delete_folder, hub_id, found_id, folder_name)
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


@mcp.resource(
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


@mcp.tool()
async def preview_model(urn: str) -> ToolResult:
    """
    Opens a 3D preview of a translated model in the Autodesk Viewer.
    Accepts a version URN (base64-encoded or raw) and renders it in an embedded viewer.

    Args:
        urn: The version URN of the model to preview (e.g. from inspect_file or count_elements).
    """
    try:
        encoded_urn = safe_b64encode(urn) if urn.startswith("urn:") else urn
        access_token = await asyncio.to_thread(get_token)

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
    except Exception as e:
        logger.error(f"preview_model failed: {e}")
        return ToolResult(content=f"Failed to preview model: {e}")


@mcp.tool()
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
    if color == "clear" or not ids:
        _HIGHLIGHT_STATE[urn] = {"ids": [], "color": []}
        return f"Cleared all highlights for model."

    rgba = _COLOR_MAP.get(color, _COLOR_MAP["red"])
    _HIGHLIGHT_STATE[urn] = {"ids": ids, "color": rgba}
    return f"Highlighted {len(ids)} element(s) in {color}."


@mcp.resource("highlight://{urn}")
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
    low_level = mcp._mcp_server
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
    low_level = mcp._mcp_server
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
    low_level = mcp._mcp_server
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


_inject_meta_via_handler()
_inject_ui_extension_capability()
_inject_csp_into_resource()


if __name__ == "__main__":
    logger.info(f"Starting MCP Server on port {PORT}...")
    import uvicorn

    app = getattr(mcp, "http_app", None)
    if callable(app):
        app = app()
    elif app is None:
        app = getattr(mcp, "_fastapi_app", mcp)

    uvicorn.run(app, host="0.0.0.0", port=PORT)

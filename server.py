import os
import logging
from fastmcp import FastMCP
from api import (
    find_project_globally,
    inspect_generic_file,
    resolve_file_to_urn,
    get_latest_version_urn,
    trigger_translation,
    stream_count_elements,
    get_hubs,
    create_acc_project,
)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# FastMCP
mcp = FastMCP("Autodesk ACC Agent")
PORT = int(os.environ.get("PORT", 8000))


# ==========================================================================
# TOOLS
# ==========================================================================


@mcp.tool()
def find_project(name_query: str) -> str:
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
        result = find_project_globally(name_query)
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
def inspect_file(project_id: str, file_id: str) -> str:
    """
    Inspect a file's translation/processing status in the Model Derivative service.

    Accepts a filename OR a URN — no need to look up the URN manually.
    Returns translation status (Ready / Processing / Failed / Not Translated).

    Args:
        project_id: The project ID (from find_project).
        file_id:    Filename (e.g. "MyFile.rvt"), lineage URN, or version URN.

    Returns:
        Human-readable translation status report.
    """
    try:
        return inspect_generic_file(project_id, file_id)
    except Exception as e:
        logger.error(f"inspect_file failed: {e}")
        return f"Error inspecting file: {e}"


@mcp.tool()
def reprocess_file(project_id: str, file_id: str) -> str:
    """
    Trigger a fresh Model Derivative translation job for a file.

    Use this when a file shows "No Property Database", translation errors,
    or when count_elements returns 0 unexpectedly.

    Args:
        project_id: The project ID (from find_project).
        file_id:    Filename (e.g. "MyFile.rvt"), lineage URN, or version URN.

    Returns:
        Confirmation that the job was started, or an error message.
        Translation typically takes 5-10 minutes. Use inspect_file to check progress.
    """
    try:
        # Resolve filename/URN -> lineage URN
        lineage_urn = resolve_file_to_urn(project_id, file_id)
        if not lineage_urn or not lineage_urn.startswith("urn:"):
            return f"Error: Could not resolve '{file_id}' to a valid lineage URN."

        # Lineage URN -> version URN
        version_urn = get_latest_version_urn(project_id, lineage_urn)
        if not version_urn or not version_urn.startswith("urn:"):
            return f"Error: Could not resolve lineage URN to a version URN."

        # Trigger translation
        result = trigger_translation(version_urn)
        if isinstance(result, str):
            return result

        status = result.get("result", "unknown")
        return (
            f"Translation job started for '{file_id}' (status: {status}).\n"
            f"Please wait 5-10 minutes, then use inspect_file or count_elements to verify."
        )
    except Exception as e:
        logger.error(f"reprocess_file failed: {e}")
        return f"Error triggering reprocess: {e}"


@mcp.tool()
def count_elements(project_id: str, file_id: str, category_name: str) -> str:
    """
    Count elements in a Revit/IFC model that match a category name.

    Uses a streaming regex scanner — works on models of any size without
    loading the full JSON into memory. The search is case-insensitive and
    automatically tries singular forms (e.g. "Walls" also matches "Wall").

    Args:
        project_id:    The project ID (from find_project).
        file_id:       Filename (e.g. "MyFile.rvt"), lineage URN, or version URN.
        category_name: Category to count (e.g. "Walls", "Doors", "Windows", "Floors").

    Returns:
        The number of matching elements, or an error message.
    """
    try:
        # Resolve filename/URN -> lineage URN
        lineage_urn = resolve_file_to_urn(project_id, file_id)
        if not lineage_urn or not lineage_urn.startswith("urn:"):
            return f"Error: Could not resolve '{file_id}' to a valid URN."

        # Lineage URN -> version URN
        version_urn = get_latest_version_urn(project_id, lineage_urn)
        if not version_urn or not version_urn.startswith("urn:"):
            return f"Error: Could not resolve to a version URN."

        # Stream and count
        count = stream_count_elements(version_urn, category_name)
        return f"Found {count} elements matching '{category_name}' (including singular variations)."
    except ValueError as ve:
        return str(ve)
    except Exception as e:
        logger.error(f"count_elements failed: {e}")
        return f"Error scanning model: {e}"


@mcp.tool()
def list_hubs() -> str:
    """
    Lists all Autodesk Hubs (BIM 360 / ACC) accessible to the Agent.
    Use this to find the 'hub_id' needed for creating projects.
    """
    try:
        hubs = get_hubs()
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
def create_project(hub_id: str, name: str, project_type: str = "BIM360") -> str:
    """
    Creates a new project in the specified Hub.

    Args:
        hub_id:       The Hub ID (starts with 'b.'). Use 'list_hubs' to find this.
        name:         The name of the new project.
        project_type: 'ACC' or 'BIM360' (Default: BIM360).
    """
    try:
        result = create_acc_project(hub_id, name, project_type)
        new_id = result.get("data", {}).get("id")
        return f"Project '{name}' created successfully! ID: {new_id}"
    except Exception as e:
        logger.error(f"create_project failed: {e}")
        return f"Failed to create project: {e}"


# ==========================================================================
# ENTRYPOINT
# ==========================================================================

if __name__ == "__main__":
    logger.info(f"Starting MCP Server on port {PORT}...")
    import uvicorn

    app = getattr(mcp, "http_app", None)
    if callable(app):
        app = app()
    elif app is None:
        app = getattr(mcp, "_fastapi_app", mcp)

    uvicorn.run(app, host="0.0.0.0", port=PORT)

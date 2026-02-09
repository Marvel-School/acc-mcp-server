import os
import requests
import json
import logging
import time
from difflib import SequenceMatcher
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import quote
from fastmcp import FastMCP
from auth import get_token, BASE_URL_ACC
from api import (
    make_api_request,
    make_graphql_request,
    get_user_id_by_email,
    get_acting_user_id,
    clean_id,
    ensure_b_prefix,
    encode_urn,
    get_cached_hub_id,
    resolve_to_version_id,
    fetch_project_users,
    trigger_data_extraction,
    check_request_job_status,
    get_data_download_url,
    safe_b64encode,
    get_viewer_domain,
    search_project_folder,
    fetch_paginated_data,
    get_project_issues,
    get_project_assets,
    get_account_users,
    invite_user_to_project,
    get_account_user_details,
    get_hubs_aec,
    get_projects_aec,
    get_hubs_rest,
    get_projects_rest,
    get_top_folders as fetch_top_folders,
    get_folder_contents as fetch_folder_contents,
    find_design_files,
    resolve_project,
    find_project_globally,
    resolve_file_to_urn,
    get_latest_version_urn,
    get_model_manifest,
    get_model_metadata,
    inspect_generic_file,
    stream_count_elements,
    trigger_translation
)

# Initialize Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastMCP
mcp = FastMCP("Autodesk ACC Agent")
PORT = int(os.environ.get("PORT", 8000))

# ==========================================
# DISCOVERY TOOLS
# ==========================================

@mcp.tool()
def list_hubs() -> str:
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, str): return data
    output = "🏢 **Found Hubs:**\n"
    for h in data.get("data", []): output += f"- {h['attributes']['name']} (ID: {h['id']})\n"
    return output

@mcp.tool()
def list_projects(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 20) -> str:
    """
    Finds projects.
    AI INSTRUCTIONS:
    1. If the user asks for a specific project (e.g. "Find the Marvel project"), pass that name to 'name_filter'.
    2. If the user asks for "all projects", leave arguments empty.
    3. Use this tool FIRST to find a 'project_id' before calling other tools.
    """
    if not hub_id:
        hub_id = get_cached_hub_id()
        if not hub_id: return "Error: No Hubs found."
        
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
    
    # Use shared pagination logic (Style 'url' for Data Management API)
    all_projs = fetch_paginated_data(url, style='url')
    
    # Client-side filtering with Fuzzy Logic (Substring + difflib)
    if name_filter:
        nf = name_filter.lower()
        clean_nf = nf.replace(" ", "")
        filtered_projs = []
        
        for p in all_projs:
            p_name = p.get('attributes', {}).get('name', '')
            pn = p_name.lower()
            clean_pn = pn.replace(" ", "")
            
            # 1. Strict Substring Match (Fastest & Safest)
            if nf in pn or clean_nf in clean_pn:
                filtered_projs.append(p)
                continue
                
            # 2. Fuzzy Match (For Typos)
            similarity = SequenceMatcher(None, nf, pn).ratio()
            if similarity > 0.6:
                filtered_projs.append(p)
                
        all_projs = filtered_projs
    
    # Sort by name
    all_projs.sort(key=lambda x: x['attributes'].get('name', ''))

    output = f"📂 **Found {len(all_projs)} Projects:**\n"
    for p in all_projs[:limit]: 
        output += f"- **{p['attributes']['name']}**\n  ID: `{p['id']}`\n"
        
    if len(all_projs) > limit:
        output += f"\n*(Displaying {limit} of {len(all_projs)} results. Use 'name_filter' to refine.)*"

    return output

@mcp.tool()
def list_aec_hubs() -> str:
    """
    Lists all hubs using Data Management REST API.
    Supports 2-legged OAuth (Service Accounts).
    """
    result = get_hubs_rest()

    if isinstance(result, str):
        return f"❌ Error: {result}"

    if not result:
        return "No hubs found."

    output = "🏢 **Hubs (via REST API):**\n"
    for hub in result:
        output += f"- {hub.get('name', 'Unknown')} (ID: `{hub.get('id')}`)\n"

    return output

@mcp.tool()
def list_aec_projects(hub_id: str) -> str:
    """
    Lists all projects for a hub using Data Management REST API.
    Supports 2-legged OAuth (Service Accounts).
    """
    result = get_projects_rest(hub_id)

    if isinstance(result, str):
        return f"❌ Error: {result}"

    if not result:
        return f"No projects found for hub {hub_id}."

    output = f"📂 **Projects for Hub {hub_id} (via REST API):**\n"
    for project in result:
        output += f"- **{project.get('name', 'Unknown')}**\n  ID: `{project.get('id')}`\n"

    return output

@mcp.tool()
def find_project(name_query: str) -> str:
    """
    Universal project finder that searches across ALL accessible hubs.
    No need to specify hub_id - searches everywhere automatically.

    Args:
        name_query: Project name to search for (case-insensitive substring match)

    Example: find_project("Marvel") will find "Marvel Office Building"
    """
    result = find_project_globally(name_query)

    if result is None:
        return f"❌ Project '{name_query}' not found in any accessible hub.\n\nPlease check the project name and try again."

    hub_id, project_id, project_name = result

    return f"✅ **Found Project:**\n\n**Name:** {project_name}\n**Project ID:** `{project_id}`\n**Hub ID:** `{hub_id}`\n\nYou can now use this project_id with other tools."

# ==========================================
# FILE & FOLDER TOOLS
# ==========================================

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    """
    Lists top-level folders in a project.
    Uses Data Management REST API with EMEA region support.
    """
    hub_id = get_cached_hub_id()
    if not hub_id:
        return "❌ Error: No Hubs found."

    result = fetch_top_folders(hub_id, project_id)

    if isinstance(result, str):
        return f"❌ Error: {result}"

    if not result:
        return "No top folders found."

    output = "📁 **Top Folders:**\n"
    for folder in result:
        output += f"- {folder.get('name', 'Unknown')} (ID: `{folder.get('id')}`)\n"

    return output

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
    """
    Lists contents of a folder (files and subfolders).
    Uses Data Management REST API with EMEA region support.
    Extracts tip version URNs for files (needed for Model Derivative API).
    """
    result = fetch_folder_contents(project_id, folder_id)

    if isinstance(result, str):
        return f"❌ Error: {result}"

    if not result:
        return "📂 Folder is empty."

    output = f"📂 **Folder Contents ({len(result)} items):**\n"
    for item in result[:limit]:
        name = item.get("name", "Unnamed")
        item_type = item.get("itemType", "unknown")
        item_id = item.get("id")

        # Use appropriate icon
        icon = "📁" if item_type == "folder" else "📄"

        output += f"{icon} **{name}**\n"
        output += f"   ID: `{item_id}`\n"

        # Include tip version URN for files (needed for Model Derivative API)
        if item_type == "file" and item.get("tipVersionUrn"):
            output += f"   Version URN: `{item.get('tipVersionUrn')}`\n"

    if len(result) > limit:
        output += f"\n*(Showing {limit} of {len(result)} items)*"

    return output

@mcp.tool()
def get_download_url(project_id: str, file_id: str) -> str:
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        target_version_id = resolve_to_version_id(project_id, file_id)
        r = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encode_urn(target_version_id)}", headers=headers)
        if r.status_code != 200: return f"Error finding file version: {r.text}"

        storage_urn = r.json()["data"]["relationships"]["storage"]["data"]["id"]
        parts = storage_urn.split("/")
        bucket_key, object_key = parts[-2].split(":")[-1], parts[-1]

        oss_url = f"https://developer.api.autodesk.com/oss/v2/buckets/{bucket_key}/objects/{object_key}/signeds3download"
        r = requests.get(oss_url, headers=headers, params={"minutesExpiration": 60})
        return f"⬇️ **[Click to Download File]({r.json()['url']})**" if r.status_code == 200 else f"Error: {r.text}"
    except Exception as e: return str(e)

@mcp.tool()
def find_files(project_name: Optional[str] = None, project_id: Optional[str] = None, extension: str = "rvt") -> str:
    """
    Universal file finder that works across any project.
    Automatically searches recursively through folders (max depth 3).

    Args:
        project_name: Project name to search for (triggers automatic project lookup)
        project_id: Direct project ID (use if you already know it)
        extension: File extension to find (e.g., "rvt", "dwg", "nwc"). Can be comma-separated.

    You must provide either project_name OR project_id.
    """
    # Step 1: Resolve project if name is provided
    if project_name:
        logger.info(f"Looking up project: {project_name}")
        result = find_project_globally(project_name)

        if result is None:
            return f"❌ Project '{project_name}' not found. Please check the name and try again."

        hub_id, resolved_project_id, resolved_project_name = result
        logger.info(f"Found project: {resolved_project_name}")

    elif project_id:
        # Use cached hub_id if project_id is provided directly
        hub_id = get_cached_hub_id()
        if not hub_id:
            return "❌ Error: Could not determine hub_id. Please use project_name instead."
        resolved_project_id = project_id
        resolved_project_name = project_id
    else:
        return "❌ Error: You must provide either 'project_name' or 'project_id'."

    # Step 2: Search for files
    files = find_design_files(hub_id, resolved_project_id, extension)

    if isinstance(files, str):
        return f"❌ Error: {files}"

    if not files:
        return f"❌ No files with extension '.{extension}' found in project."

    # Step 3: Format output
    output = f"🔍 **Found {len(files)} Files in '{resolved_project_name}':**\n\n"

    for file in files:
        name = file.get("name", "Unknown")
        item_id = file.get("item_id", "")
        version_id = file.get("version_id", "")
        folder_path = file.get("folder_path", "Unknown")

        output += f"📄 **{name}**\n"
        output += f"   Location: `{folder_path}`\n"
        output += f"   Item ID: `{item_id}`\n"
        if version_id:
            output += f"   Version URN: `{version_id}`\n"
        output += "\n"

    return output

@mcp.tool()
def inspect_file(project_id: str, file_id: str) -> str:
    """
    Smart file inspector with automatic name and URN resolution.
    Accepts file by **ID OR Name** - no need to find the URN manually!

    Args:
        project_id: The project ID
        file_id: Can be ANY of the following:
                 - Filename: "MyFile.rvt", "Grasbaan102026.rvt"
                 - Lineage URN: urn:adsk.wipp:dm.lineage:...
                 - Version URN: urn:adsk.wipp:fs.file:vf...

    Examples:
        inspect_file(proj_id, "Grasbaan102026.rvt")  ← Just use the filename!
        inspect_file(proj_id, "urn:adsk.wipp:dm.lineage:...")  ← Or use the URN

    The tool automatically:
    - Searches for files by name if a filename is provided
    - Converts Lineage URNs to Version URNs
    - Returns translation status (Ready, Processing, Failed, Not Translated, etc.)
    """
    return inspect_generic_file(project_id, file_id)

# ==========================================
# 3D MODEL TOOLS
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
    p_id = ensure_b_prefix(project_id)
    query = """query GetElementGroups($projectId: ID!) { 
        elementGroupsByProject(projectId: $projectId) { 
            results { id name alternativeIdentifiers { fileVersionUrn } } 
        } 
    }"""
    data = make_graphql_request(query, {"projectId": p_id})
    should_retry = False
    if not data or isinstance(data, str): should_retry = True
    elif isinstance(data, dict) and not data.get("elementGroupsByProject"): should_retry = True

    if should_retry:
        data = make_graphql_request(query, {"projectId": clean_id(project_id)})

    if isinstance(data, str): return data
    if not data: return "❌ No design data returned."

    container = data.get("elementGroupsByProject") or {}
    groups = container.get("results", [])
    if not groups: return "No 3D designs found (Check 'Project Files' using find_models instead)."
    
    output = "🏗️ **Designs Found:**\n"
    for g in groups:
        identifiers = g.get("alternativeIdentifiers") or {}
        urn = identifiers.get("fileVersionUrn", g['id'])
        output += f"- **{g['name']}**\n  ID: `{urn}`\n" 
    return output

@mcp.tool()
def get_model_viewer_link(project_id: str, item_id: str) -> str:
    version_id = resolve_to_version_id(project_id, item_id)
    domain = get_viewer_domain(version_id)
    return f"https://{domain}/docs/files/projects/{clean_id(project_id)}?entityId={quote(version_id, safe='')}"

@mcp.tool()
def find_models(project_name_or_id: str, file_types: str = "rvt,rcp,dwg,nwc") -> str:
    """
    Automatically searches for design files (RVT, DWG, etc.) in a project.
    Smart search that finds the project globally and navigates folders autonomously.

    Args:
        project_name_or_id: Project name or ID (will search all hubs automatically)
        file_types: Comma-separated list of file extensions (e.g. "rvt,dwg,nwc")
    """
    # Try to resolve the project globally
    logger.info(f"Attempting to resolve project: {project_name_or_id}")

    # First, try to use cached hub_id if the input looks like a project ID
    if project_name_or_id.startswith("b."):
        hub_id = get_cached_hub_id()
        if hub_id:
            logger.info(f"Using cached hub_id for project ID: {project_name_or_id}")
            project_id = project_name_or_id
        else:
            # Fallback to global search
            resolution = resolve_project(project_name_or_id)
            if isinstance(resolution, str):
                return f"❌ {resolution}"

            hub_id = resolution.get("hub_id")
            project_id = resolution.get("project_id")
            project_name = resolution.get("project_name")
            hub_name = resolution.get("hub_name")
            logger.info(f"Resolved to: Project '{project_name}' in Hub '{hub_name}'")
    else:
        # Input is likely a project name, do global search
        resolution = resolve_project(project_name_or_id)
        if isinstance(resolution, str):
            return f"❌ {resolution}"

        hub_id = resolution.get("hub_id")
        project_id = resolution.get("project_id")
        project_name = resolution.get("project_name")
        hub_name = resolution.get("hub_name")
        logger.info(f"Resolved to: Project '{project_name}' in Hub '{hub_name}'")

    if not hub_id or not project_id:
        return "❌ Error: Could not determine hub_id or project_id."

    # Use smart search function
    result = find_design_files(hub_id, project_id, file_types)

    if isinstance(result, str):
        return f"❌ Error: {result}"

    if not result:
        return "❌ No models found matching those extensions."

    output = f"🔍 **Found {len(result)} Models:**\n"
    for file in result:
        name = file.get("name", "Unknown")
        file_id = file.get("item_id", "")
        tip_urn = file.get("version_id") or file_id
        folder_path = file.get("folder_path", "Unknown")

        # Generate viewer link only if we have a valid URN
        if tip_urn:
            domain = get_viewer_domain(tip_urn)
            viewer_link = f"https://{domain}/docs/files/projects/{clean_id(project_id)}?entityId={quote(tip_urn, safe='')}"

            output += f"- **{name}**\n"
            output += f"  📁 Location: `{folder_path}`\n"
            output += f"  [Open in Viewer]({viewer_link})\n"
            output += f"  ID: `{file_id}`\n"
            if tip_urn != file_id:
                output += f"  Version URN: `{tip_urn}`\n"
        else:
            output += f"- **{name}**\n"
            output += f"  📁 Location: `{folder_path}`\n"
            output += f"  ID: `{file_id}`\n"

    return output

@mcp.tool()
def search_files(project_id: str, query: str) -> str:
    """
    Search for files or folders within a project by name.
    """
    items = search_project_folder(project_id, query)
    if not items:
        return f"🔍 No files found matching '{query}' in Project {project_id}."

    output = f"🔍 **Search Results for '{query}':**\n"
    for i in items:
        name = i["attributes"]["displayName"]
        item_id = i['id']
        item_type = "📁" if i.get("type") == "folders" else "📄"
        output += f"{item_type} **{name}** (ID: `{item_id}`)\n"
    return output

@mcp.tool()
def inspect_model(project_id: str, file_id: str, show_tree: bool = False) -> str:
    """
    Comprehensive model inspection using Model Derivative API.
    Shows translation status, available formats, and optionally the object tree.

    Args:
        project_id: The project ID
        file_id: The file/item ID or version URN
        show_tree: If True, includes detailed object tree summary (default: False)
    """
    # Step 1: Resolve to version URN
    version_urn = get_latest_version_urn(project_id, file_id)

    if not version_urn or not version_urn.startswith("urn:"):
        return f"❌ Error: Could not resolve file ID to a valid version URN. Please check the file ID."

    logger.info(f"Inspecting model with version URN: {version_urn}")

    # Step 2: Get manifest (translation status)
    manifest = get_model_manifest(version_urn)

    if isinstance(manifest, str):
        return f"❌ Manifest Error: {manifest}"

    # Parse manifest
    status = manifest.get("status", "unknown")
    progress = manifest.get("progress", "unknown")

    output = "🔍 **Model Inspection Report**\n\n"
    output += f"**Version URN:** `{version_urn}`\n"
    output += f"**Translation Status:** {status}\n"
    output += f"**Progress:** {progress}\n\n"

    # Show available derivatives
    derivatives = manifest.get("derivatives", [])
    if derivatives:
        output += "**Available Formats:**\n"
        for deriv in derivatives:
            output_type = deriv.get("outputType", "unknown")
            deriv_status = deriv.get("status", "unknown")
            output += f"- {output_type}: {deriv_status}\n"
        output += "\n"

    # Step 3: Get metadata if requested
    if show_tree and status == "success":
        metadata = get_model_metadata(version_urn)

        if isinstance(metadata, str):
            output += f"⚠️ Metadata: {metadata}\n"
        else:
            # Parse object tree for summary
            try:
                objects = metadata.get("data", {}).get("objects", [])
                categories = {}

                def traverse(nodes):
                    for node in nodes:
                        if "objects" in node:
                            cat_name = node.get("name", "Unknown")
                            categories[cat_name] = categories.get(cat_name, 0) + len(node["objects"])
                        if "objects" in node and isinstance(node["objects"], list):
                            traverse(node["objects"])

                if objects:
                    traverse(objects)

                output += "**Object Tree Summary:**\n"
                for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True)[:15]:
                    output += f"- {cat}: {count} items\n"
            except Exception as e:
                output += f"⚠️ Could not parse object tree: {str(e)}\n"

    elif show_tree and status != "success":
        output += "⚠️ Object tree not available until translation is complete.\n"

    return output

@mcp.tool()
def get_model_tree(project_id: str, file_id: str) -> str:
    version_id = resolve_to_version_id(project_id, file_id)
    if not version_id or not version_id.startswith("urn:"):
         return f"❌ Error: I tried to resolve '{file_id}' to an ID but failed. Please try 'Find files' first to get the correct ID."

    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    urn = safe_b64encode(version_id)
    resp = requests.get(f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn}/metadata", headers=headers)

    if resp.status_code == 404: return "❌ File found, but it has no metadata. (Has it been fully processed in the viewer?)"
    if resp.status_code != 200: return f"❌ Autodesk API Error ({resp.status_code}): {resp.text}"

    data = resp.json().get("data", {}).get("metadata", [])
    if not data: return "❌ No 3D views found."
    guid = data[0]["guid"]

    resp_tree = requests.get(f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn}/metadata/{guid}", headers=headers, params={"forceget": "true"})

    if resp_tree.status_code == 202: return "⏳ The Model Tree is currently processing. Please try again in 1 minute."
    if resp_tree.status_code != 200: return f"❌ Failed to retrieve object tree (Status: {resp_tree.status_code})."

    try:
        objects = resp_tree.json().get("data", {}).get("objects", [])
        categories = {}
        def traverse(nodes):
            for node in nodes:
                if "objects" in node:
                    categories[node.get("name", "Unknown")] = categories.get(node.get("name", "Unknown"), 0) + len(node["objects"])
        if objects: traverse(objects[0].get("objects", []))

        output = f"🏗️ **Model Structure (View GUID: `{guid}`):**\n"
        for cat, count in sorted(categories.items(), key=lambda item: item[1], reverse=True)[:15]:
            output += f"- **{cat}**: {count} items\n"
        return output
    except Exception as e: return f"❌ Parsing Error: {str(e)}"

@mcp.tool()
def count_elements(project_id: str, file_id: str, category_name: str) -> str:
    """
    Counts elements in a model that match a specific category.
    Uses streaming regex scanner to process metadata without loading full JSON - no memory limits!
    Accepts file by ID OR Name!

    Args:
        project_id: The project ID
        file_id: Filename (e.g., "MyFile.rvt") OR URN (Lineage/Version)
        category_name: Category to search for (e.g., "Walls", "Doors", "Windows")

    Examples:
        count_elements(proj_id, "Grasbaan102026.rvt", "Walls")  ← Use filename!
        count_elements(proj_id, "urn:adsk.wipp...", "Doors")     ← Or URN

    Note: Search is case-insensitive and uses streaming pattern matching.
    """
    try:
        logger.info(f"Initiating streaming scan for category: {category_name}")

        # Step 1: Resolve file_id to lineage URN
        lineage_urn = resolve_file_to_urn(project_id, file_id)

        if not lineage_urn or not lineage_urn.startswith("urn:"):
            return f"❌ Error: Could not resolve '{file_id}' to a valid URN."

        # Step 2: Get version URN
        version_urn = get_latest_version_urn(project_id, lineage_urn)

        if not version_urn or not version_urn.startswith("urn:"):
            return f"❌ Error: Could not resolve to version URN."

        # Step 3: Stream and count using regex pattern matching
        count = stream_count_elements(version_urn, category_name)

        # Return success message
        logger.info(f"✅ Streaming scan complete. Found {count} elements.")
        return f"✅ Smart Scan Complete. Found {count} items matching '{category_name}' (or singular variations)."

    except ValueError as ve:
        # These are user-friendly errors from the API functions
        logger.error(f"Stream scan failed: {str(ve)}")
        return str(ve)

    except Exception as e:
        logger.error(f"Stream scan failed: {str(e)}")
        return f"❌ Error: Failed to scan model data: {str(e)}"

@mcp.tool()
def reprocess_file(project_id: str, file_id: str) -> str:
    """
    Triggers a fresh Model Derivative translation job for a file.
    Use this when a file exists but shows "No Property Database" or other translation errors.

    Args:
        project_id: The project ID
        file_id: Filename (e.g., "MyFile.rvt") OR URN (Lineage/Version)

    Examples:
        reprocess_file(proj_id, "Grasbaan102026.rvt")  ← Use filename!
        reprocess_file(proj_id, "urn:adsk.wipp...")     ← Or URN

    Note: Translation takes 5-10 minutes. Check status with inspect_file after waiting.
    """
    try:
        # Step 1: Smart Resolve (Handles Name -> URN lookup automatically)
        # This ensures we have a valid URN (urn:adsk.wipp...) before proceeding
        logger.info(f"Resolving file '{file_id}' for reprocessing...")
        lineage_urn = resolve_file_to_urn(project_id, file_id)

        if not lineage_urn or not lineage_urn.startswith("urn:"):
            return f"❌ Error: Could not resolve '{file_id}' to a valid lineage URN. Please check the file ID."

        logger.info(f"  Resolved to lineage URN: {lineage_urn[:80]}...")

        # Step 2: Get Latest Version (The translation requires the VERSION URN)
        version_urn = get_latest_version_urn(project_id, lineage_urn)

        if not version_urn or not version_urn.startswith("urn:"):
            return f"❌ Error: Could not resolve lineage URN to version URN."

        logger.info(f"  Resolved to version URN: {version_urn[:80]}...")

        # Step 3: Trigger Translation
        logger.info(f"Triggering translation for version: {version_urn}")
        result = trigger_translation(version_urn)

        # Check if we got an error string
        if isinstance(result, str):
            return result

        # Success - result is a dictionary
        job_status = result.get("result", "unknown")

        if job_status == "success":
            return f"✅ Translation Job Started for '{file_id}'.\nStatus: {job_status}\n\nPlease wait 5-10 minutes, then try counting elements again."
        elif job_status == "created":
            return f"✅ Translation Job Started for '{file_id}'.\nStatus: {job_status}\n\nPlease wait 5-10 minutes, then try counting elements again."
        else:
            return f"✅ Translation Job Started for '{file_id}'.\nStatus: {job_status}\n\nPlease wait 5-10 minutes, then try counting elements again."

    except Exception as e:
        logger.error(f"Reprocess failed: {str(e)}")
        return f"❌ Reprocess Failed: {str(e)}"

# ==========================================
# ADMIN TOOLS
# ==========================================

@mcp.tool()
def create_project(
    project_name: str, 
    project_type: str = "Commercial", 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None, 
    address: Optional[str] = None, 
    city: Optional[str] = None, 
    country: Optional[str] = None, 
    job_number: Optional[str] = None
) -> str:
    """
    Creates a new project.
    AI INSTRUCTIONS:
    1. Extract the project name from the user's request.
    2. If the user provides a type, start/end date, or address, include them.
    3. If details are missing, do NOT ask the user. Use the defaults provided in the function.
    """
    # 1. Get Authentication
    try:
        token = get_token() 
    except Exception as e:
        return f"❌ Auth Error: {e}"

    # 2. Get Account ID
    raw_hub_id = get_cached_hub_id()
    if not raw_hub_id: 
        logger.error("Could not find Hub/Account ID via Data Management API.")
        return "❌ Error: Could not find Hub/Account ID. Check if your app is added to the BIM 360/ACC Account Admin."
    
    account_id = clean_id(raw_hub_id) 
    logger.info(f"Targeting Account ID: {account_id}")

    # Resolve Acting Admin ID for 2-legged Auth (Required).
    admin_id = get_acting_user_id(account_id)
    if not admin_id:
        logger.warning("⚠️ Could not resolve Admin User ID. Request might fail with 401.")
    else:
        logger.info(f"Acting as Admin User ID: {admin_id}")

    # 3. Determine Endpoint (ACC Admin V1)
    url = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{account_id}/projects"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Add x-user-id header if resolved
    if admin_id:
        headers["x-user-id"] = admin_id

    # 4. Generate Data (ACC Admin API matches the docs: camelCase, 'type' for project category)
    today = datetime.now()
    next_year = today + timedelta(days=365)
    
    final_job_num = job_number if job_number else f"JN-{int(time.time())}"

    # Minimal payload required by ACC Admin API.
    # Uses environment variables for defaults if not provided in arguments.
    payload = {
        "name": project_name,
        "type": project_type,  
        "timezone": os.environ.get("DEFAULT_PROJECT_TIMEZONE", "Europe/Amsterdam"), 
        "jobNumber": final_job_num,
        "addressLine1": address or os.environ.get("DEFAULT_PROJECT_ADDRESS_LINE1", "123 Generic St"), 
        "city": city or os.environ.get("DEFAULT_PROJECT_CITY", "Metropolis"),
        "postalCode": os.environ.get("DEFAULT_PROJECT_POSTAL_CODE", "0000AA"),       
        "country": country or os.environ.get("DEFAULT_PROJECT_COUNTRY", "Netherlands")
    }

    logger.info(f"🚀 Creating Project '{project_name}' via ACC Admin API (Minimal)...")
    logger.info(f"Payload: {payload}")
    
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in [201, 202]:
        data = response.json()
        new_id = data.get("id")
        
        if response.status_code == 202:
             logger.info(f"INFO - Project creation initiated (Async). ID: {new_id}")
             msg = f"✅ **Success!** Project '{project_name}' creation initiated (Async)."
        else:
             msg = f"✅ **Success!** Project '{project_name}' created."
             
        return f"{msg}\nID: `{new_id}`\nJob #: {final_job_num}\nLoc: {payload.get('city')}, {payload.get('country')}"
    elif response.status_code == 409:
        return f"⚠️ A project with the name '{project_name}' already exists."
    else:
        logger.error(f"Failed to create project: {response.text}")
        return f"❌ Failed to create project. (Status: {response.status_code})\nError Details: {response.text}"

# ==========================================
# QUALITY & ASSETS TOOLS
# ==========================================

@mcp.tool()
def list_issues(project_id: str, status_filter: str = "open") -> str:
    """
    Lists issues in a project.
    AI INSTRUCTIONS:
    1. You MUST have a 'project_id' first. If you don't, call list_projects.
    2. Map user terms to filters: "Active"->"open", "Fixed"->"closed", "Everything"->"all".
    """
    # Normalize filter arguments.
    pass_status = status_filter
    if not status_filter or status_filter == "none" or status_filter == "all":
        pass_status = None 
    
    # Defaults to 'open' if specified.
    if status_filter == "open": pass_status = "open"

    return str(get_project_issues(project_id, pass_status))

@mcp.tool()
def list_assets(project_id: str, category_filter: str = "all") -> str:
    """Lists project assets. category_filter is optional."""
    # Normalize category filter.
    cat = category_filter if category_filter and category_filter not in ["all", "none"] else None
    return str(get_project_assets(project_id, cat))

# ==========================================
# USER MANAGEMENT TOOLS
# ==========================================

@mcp.tool()
def list_users() -> str:
    """List all users in the account. No arguments."""
    return str(get_account_users(""))

@mcp.tool()
def list_project_users(project_id: str) -> str:
    """
    Lists valid users assigned to a specific project.
    AI INSTRUCTIONS: Use this instead of 'list_users' when the user asks about a specific project.
    """
    try:
        users = fetch_project_users(project_id)
        if not users:
            return f"ℹ️ No users found in project {project_id} (or Access Denied)."
        
        output = f"👥 **Project Members ({len(users)}):**\n"
        for u in users[:20]: # Limit to avoid huge context
            # Handle potential missing keys safely
            name = u.get("name", u.get("email", "Unknown"))
            role = u.get("jobTitle", "Member") 
            # Note: ACC Admin API returns 'products' list which implies access
            output += f"- {name} ({role})\n"
            
        if len(users) > 20:
            output += f"\n*(Showing 20 of {len(users)} users)*"
            
        return output
    except Exception as e:
        return f"❌ Error listing project users: {str(e)}"

@mcp.tool()
def manage_project_users(json_payload: str) -> str:
    """
    Add a user to a project.
    
    IMPORTANT INSTRUCTIONS FOR AI:
    1. When a user says "Add [EMAIL] to [PROJECT]", you must first find the 'project_id'.
    2. Then, construct a JSON string internally: '{"project_id": "...", "email": "..."}'.
    3. Pass ONLY this JSON string as the 'json_payload' argument.
    4. Do NOT ask the user to format JSON. Do it silently.
    """
    try:
        data = json.loads(json_payload)
        p_id = data.get("project_id")
        email = data.get("email")
        
        if not p_id or not email:
            return "Error: JSON must contain 'project_id' and 'email'."
            
        # Call the API function
        return str(invite_user_to_project(p_id, email))
        
    except json.JSONDecodeError:
        return "Error: Invalid JSON format. Please provide a valid JSON string."
    except Exception as e:
        return f"Error processing request: {str(e)}"


@mcp.tool()
def run_data_export(data_types: str = "all") -> str:
    """
    Triggers a full account data export via Data Connector.
    Arguments:
      data_types: Comma-separated list of services. 
                  Options: admin, issues, locations, submittals, cost, rfis.
                  Default: "all" (exports everything).
    
    AI INSTRUCTIONS:
    1. If user says "Export everything", pass "all".
    2. If user specifies (e.g., "just costs"), pass "cost".
    3. Tell the user this is an async job and give them the Job ID to check later.
    """
    services = None
    if data_types and data_types.lower() != "all":
        services = [s.strip().lower() for s in data_types.split(",")]
        
    result = trigger_data_extraction(services)
    
    if "error" in result:
        return f"❌ Error starting export: {result['error']}"
        
    job_id = result.get("id")
    return f"✅ **Data Export Started!**\nJob ID: `{job_id}`\n\nThis process happens in the background. You can check progress using 'check_export_status' with this ID, or look for an email from Autodesk when complete."

@mcp.tool()
def check_export_status(request_id: str) -> str:
    """
    Checks status of Data Connector request.
    If complete, returns a DOWNLOAD LINK.
    """
    result = check_request_job_status(request_id)

    if "error" in result:
        return f"❌ Error: {result['error']}"

    status = result.get("status", "").upper()
    job_id = result.get("job_id")

    if status == "SUCCESS" and job_id:
        link = get_data_download_url(job_id)
        if link:
            return f"✅ **Export Complete!**\n\n⬇️ [Click here to Download ZIP]({link})\n*(Link expires in 60 seconds)*"
        else:
            return "✅ Export complete, but failed to generate download link."

    elif status == "FAILED":
        return "❌ Export Job Failed."

    return f"⏳ Export Processing... (Job ID: {job_id})"

@mcp.tool()
def check_admin_status() -> str:
    """
    Diagnostic: Use HQ API to find the Admin's configured permissions.
    """
    email = os.environ.get("ACC_ADMIN_EMAIL")
    if not email:
        return "❌ `ACC_ADMIN_EMAIL` is not set in environment."
        
    result = get_account_user_details(email)
    return f"🔍 **Admin User Details ({email}):**\n```json\n{json.dumps(result, indent=2)}\n```"

if __name__ == "__main__":
    logger.info(f"Starting MCP Server on port {PORT}...")
    import uvicorn
    from starlette.middleware.base import BaseHTTPMiddleware

    # Create ASGI app from FastMCP
    # mcp.http_app is the underlying FastAPI/Starlette app
    if hasattr(mcp, "http_app") and mcp.http_app:
        app = mcp.http_app() if callable(mcp.http_app) else mcp.http_app
    elif hasattr(mcp, "_fastapi_app"):
         app = mcp._fastapi_app
    else:
         logger.warning("Could not find http_app or _fastapi_app on mcp object. Using mcp as app.")
         app = mcp

    # Middleware to fix 406 Not Acceptable error
    async def fix_accept_header(request, call_next):
        if "text/event-stream" not in request.headers.get("accept", ""):
            headers = dict(request.scope["headers"])
            current_accept = request.headers.get("accept", "*/*")
            headers[b"accept"] = f"{current_accept}, text/event-stream".encode()
            request.scope["headers"] = [(k, v) for k, v in headers.items()]
        
        response = await call_next(request)
        return response

    # Add middleware safely
    if hasattr(app, "add_middleware"):
        app.add_middleware(BaseHTTPMiddleware, dispatch=fix_accept_header)
    else:
        logger.error(f"Cannot add middleware. App type: {type(app)}")

    uvicorn.run(app, host="0.0.0.0", port=PORT)

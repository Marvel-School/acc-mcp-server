import os
import requests
import logging
import time
import base64
from functools import lru_cache
from urllib.parse import quote
from typing import Optional, Dict, Any, List, Union
from auth import get_token, BASE_URL_ACC, BASE_URL_HQ_US, BASE_URL_HQ_EU, BASE_URL_GRAPHQL, ACC_ADMIN_EMAIL

logger = logging.getLogger(__name__)

# OAuth Scopes - CRITICAL: viewables:read is required for Model Derivative API
APS_SCOPES = "data:read data:write data:create bucket:read viewables:read"

# --- UTILS ---
def clean_id(id_str: Optional[str]) -> str:
    return id_str.replace("b.", "") if id_str else ""

def ensure_b_prefix(id_str: Optional[str]) -> str:
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: Optional[str]) -> str:
    return quote(urn, safe='') if urn else ""

def safe_b64encode(value: Optional[str]) -> str:
    if not value: return ""
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")
    return encoded.rstrip("=")

def get_viewer_domain(urn: str) -> str:
    if "wipemea" in urn or "emea" in urn:
        return "acc.autodesk.eu"
    return "acc.autodesk.com"

# --- REQUEST WRAPPERS ---
def make_api_request(url: str):
    """Generic wrapper for GET requests with error handling."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        if resp.status_code >= 400:
            logger.warning(f"API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"
        return resp.json()
    except Exception as e:
        logger.error(f"Request Exception: {str(e)}")
        return f"Error: {str(e)}"

def make_graphql_request(query: str, variables: Optional[Dict[str, Any]] = None):
    """
    Wrapper for AEC GraphQL requests.
    Critical: GraphQL returns 200 OK even with errors - must check response.data.errors.
    """
    try:
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-ads-region": "EMEA"
        }
        payload = {"query": query, "variables": variables or {}}
        resp = requests.post(BASE_URL_GRAPHQL, headers=headers, json=payload)

        if resp.status_code != 200:
            logger.warning(f"GraphQL HTTP Error {resp.status_code}: {resp.text}")
            return f"GraphQL Error {resp.status_code}: {resp.text}"

        json_data = resp.json()

        # Critical: GraphQL returns 200 even on errors - check errors field
        if "errors" in json_data:
            error_messages = [err.get("message", str(err)) for err in json_data["errors"]]
            error_str = "; ".join(error_messages)
            logger.error(f"GraphQL Query Errors: {error_str}")
            return f"GraphQL Query Error: {error_str}"

        return json_data.get("data", {})
    except Exception as e:
        logger.error(f"GraphQL Exception: {str(e)}")
        return f"GraphQL Exception: {str(e)}"

def get_hubs_aec() -> Union[List[Dict[str, Any]], str]:
    """
    Fetches all hubs using AEC Data Model GraphQL API.
    Returns list of hubs or error string.
    """
    query = """
    query {
        hubs {
            results {
                id
                name
            }
        }
    }
    """
    result = make_graphql_request(query)

    if isinstance(result, str):
        return result

    if not result or "hubs" not in result:
        return "No hubs data returned from GraphQL API"

    return result["hubs"].get("results", [])

def get_projects_aec(hub_id: str) -> Union[List[Dict[str, Any]], str]:
    """
    Fetches all projects for a given hub using AEC Data Model GraphQL API.
    Returns list of projects or error string.
    """
    query = """
    query($hubId: ID!) {
        projects(hubId: $hubId) {
            results {
                id
                name
                hubId
            }
        }
    }
    """
    variables = {"hubId": hub_id}
    result = make_graphql_request(query, variables)

    if isinstance(result, str):
        return result

    if not result or "projects" not in result:
        return f"No projects data returned for hub {hub_id}"

    return result["projects"].get("results", [])

def get_hubs_rest() -> Union[List[Dict[str, Any]], str]:
    """
    Fetches all hubs using Data Management REST API.
    Supports 2-legged OAuth (Service Accounts).
    """
    try:
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA"
        }
        url = "https://developer.api.autodesk.com/project/v1/hubs"
        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            logger.error(f"REST API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

        data = resp.json()
        hubs = data.get("data", [])

        # Extract relevant fields
        result = []
        for hub in hubs:
            result.append({
                "id": hub.get("id"),
                "name": hub.get("attributes", {}).get("name")
            })

        return result

    except Exception as e:
        logger.error(f"REST Hub Exception: {str(e)}")
        return f"Error: {str(e)}"

def get_projects_rest(hub_id: str) -> Union[List[Dict[str, Any]], str]:
    """
    Fetches all projects for a given hub using Data Management REST API.
    Supports 2-legged OAuth (Service Accounts).
    """
    try:
        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"

        # Use pagination to get all projects
        all_projects = fetch_paginated_data(url, style='url', impersonate=False)

        if isinstance(all_projects, str):
            return all_projects

        # Extract relevant fields
        result = []
        for project in all_projects:
            result.append({
                "id": project.get("id"),
                "name": project.get("attributes", {}).get("name"),
                "hubId": hub_id
            })

        return result

    except Exception as e:
        logger.error(f"REST Project Exception: {str(e)}")
        return f"Error: {str(e)}"

def find_project_globally(name_query: str) -> Optional[tuple]:
    """
    Universal project finder that searches across ALL accessible hubs.
    Case-insensitive substring matching.

    Args:
        name_query: Project name to search for (case-insensitive)

    Returns:
        Tuple of (hub_id, project_id, project_name) if found, None otherwise
    """
    try:
        logger.info(f"[Universal Explorer] Searching globally for project: {name_query}")

        # Step 1: Get all accessible hubs (strict EMEA region)
        hubs = get_hubs_rest()

        if isinstance(hubs, str):
            logger.error(f"Failed to get hubs: {hubs}")
            return None

        if not hubs:
            logger.error("No hubs found. Check if your app has access to any accounts.")
            return None

        search_term = name_query.lower().strip()

        # Step 2: Search through every hub
        for hub in hubs:
            hub_id = hub.get("id")
            hub_name = hub.get("name", "Unknown")

            # Skip if hub_id is None
            if not hub_id:
                logger.warning(f"Skipping hub with no ID: {hub_name}")
                continue

            logger.info(f"  Searching in hub: {hub_name}")

            # Step 3: Get projects for this hub
            projects = get_projects_rest(hub_id)

            if isinstance(projects, str):
                logger.warning(f"  Failed to get projects for hub {hub_name}: {projects}")
                continue

            # Step 4: Check for matching project (case-insensitive substring match)
            for project in projects:
                project_id = project.get("id", "")
                project_name = project.get("name", "")

                # Case-insensitive substring match
                if search_term in project_name.lower():
                    logger.info(f"✅ Found project '{project_name}' in hub '{hub_name}'")
                    return (hub_id, project_id, project_name)

        # Not found in any hub
        logger.warning(f"Project '{name_query}' not found in any accessible hub")
        return None

    except Exception as e:
        logger.error(f"Project Search Exception: {str(e)}")
        return None

def resolve_project(project_name_or_id: str) -> Union[Dict[str, Any], str]:
    """
    Legacy wrapper for find_project_globally() that returns dict format.
    Kept for backward compatibility with existing tools.

    Args:
        project_name_or_id: Project name or ID to search for

    Returns:
        Dict with hub_id, project_id, and project_name, or error string
    """
    result = find_project_globally(project_name_or_id)

    if result is None:
        return f"Project '{project_name_or_id}' not found in any accessible hub. Please check the name or ID."

    hub_id, project_id, project_name = result
    return {
        "hub_id": hub_id,
        "project_id": project_id,
        "project_name": project_name,
        "hub_name": "Unknown"  # Legacy field
    }

def get_top_folders(hub_id: str, project_id: str) -> Union[List[Dict[str, Any]], str]:
    """
    Fetches top-level folders for a project using Data Management REST API.
    Supports 2-legged OAuth (Service Accounts).
    """
    try:
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA"
        }

        # Ensure proper ID formatting
        hub_id_clean = ensure_b_prefix(hub_id)
        project_id_clean = ensure_b_prefix(project_id)

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id_clean}/projects/{project_id_clean}/topFolders"
        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            logger.error(f"Top Folders API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

        data = resp.json()
        folders = data.get("data", [])

        # Extract relevant fields
        result = []
        for folder in folders:
            result.append({
                "id": folder.get("id"),
                "name": folder.get("attributes", {}).get("displayName"),
                "type": folder.get("type")
            })

        return result

    except Exception as e:
        logger.error(f"Top Folders Exception: {str(e)}")
        return f"Error: {str(e)}"

def get_folder_contents(project_id: str, folder_id: str) -> Union[List[Dict[str, Any]], str]:
    """
    Fetches contents of a folder using Data Management REST API.
    Returns files and subfolders with proper URN extraction for files.
    Supports 2-legged OAuth (Service Accounts).
    """
    try:
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA"
        }

        # Ensure proper ID formatting
        project_id_clean = ensure_b_prefix(project_id)
        folder_id_encoded = encode_urn(folder_id)

        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id_clean}/folders/{folder_id_encoded}/contents"
        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            logger.error(f"Folder Contents API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

        data = resp.json()
        items = data.get("data", [])

        # Parse items with detailed information
        result = []
        for item in items:
            item_type = item.get("type")
            attributes = item.get("attributes", {})

            parsed_item = {
                "id": item.get("id"),
                "name": attributes.get("displayName"),
                "type": item_type
            }

            # For files, extract the tip version URN (needed for Model Derivative API)
            if item_type == "items":
                relationships = item.get("relationships", {})
                tip = relationships.get("tip", {})
                tip_data = tip.get("data", {})
                tip_id = tip_data.get("id")

                if tip_id:
                    parsed_item["tipVersionUrn"] = tip_id
                    parsed_item["itemType"] = "file"
                else:
                    parsed_item["itemType"] = "file"
            elif item_type == "folders":
                parsed_item["itemType"] = "folder"

            result.append(parsed_item)

        return result

    except Exception as e:
        logger.error(f"Folder Contents Exception: {str(e)}")
        return f"Error: {str(e)}"

def find_design_files(hub_id: str, project_id: str, extensions: str = "rvt") -> Union[List[Dict[str, Any]], str]:
    """
    Universal recursive file finder using BFS with depth limit.
    Automatically navigates to 'Project Files' folder and searches nested subfolders.

    Args:
        hub_id: The hub/account ID
        project_id: The project ID
        extensions: Comma-separated file extensions (e.g., "rvt,dwg,nwc")

    Returns:
        List of files with format: {name, item_id, version_id, folder_path}
    """
    try:
        # Step 1: Get top folders
        logger.info(f"[Universal Explorer] Searching for files with extensions: {extensions}")
        top_folders = get_top_folders(hub_id, project_id)

        if isinstance(top_folders, str):
            return f"Failed to get top folders: {top_folders}"

        # Step 2: Find "Project Files" folder
        project_files_folder = None
        for folder in top_folders:
            if folder.get("name") == "Project Files":
                project_files_folder = folder.get("id")
                logger.info(f"  Found 'Project Files' folder: {project_files_folder}")
                break

        # Fallback: if "Project Files" not found, try the first folder
        if not project_files_folder and top_folders:
            project_files_folder = top_folders[0].get("id")
            logger.warning(f"  'Project Files' not found, using first folder")

        if not project_files_folder:
            return "No folders found in project"

        # Step 3: Initialize BFS queue with depth tracking
        folder_queue = [{"id": project_files_folder, "name": "Project Files", "depth": 0}]
        matching_files = []
        ext_list = [ext.strip().lower() for ext in extensions.split(",")]

        # Safety limits
        max_folders_to_scan = 50
        max_depth = 3
        folders_scanned = 0

        # Step 4: BFS Loop with depth limit
        while folder_queue and folders_scanned < max_folders_to_scan:
            current_folder = folder_queue.pop(0)
            folder_id = current_folder["id"]
            folder_name = current_folder["name"]
            depth = current_folder["depth"]

            folders_scanned += 1
            logger.info(f"  Scanning folder {folders_scanned}/{max_folders_to_scan} (depth {depth}): '{folder_name}'")

            # Get folder contents
            contents = get_folder_contents(project_id, folder_id)

            if isinstance(contents, str):
                logger.warning(f"  Failed to get contents of folder '{folder_name}': {contents}")
                continue

            # Process items in this folder
            for item in contents:
                item_type = item.get("itemType")

                # If it's a file, check if it matches our extensions
                if item_type == "file":
                    name = item.get("name", "")
                    # Check if file ends with any of the specified extensions
                    if any(name.lower().endswith(f".{ext}") for ext in ext_list):
                        matching_files.append({
                            "name": name,
                            "item_id": item.get("id"),
                            "version_id": item.get("tipVersionUrn"),
                            "folder_path": folder_name
                        })
                        logger.info(f"    Found: {name}")

                # If it's a subfolder and we haven't reached max depth, add to queue
                elif item_type == "folder" and depth < max_depth:
                    subfolder_name = item.get("name", "Unknown")
                    folder_queue.append({
                        "id": item.get("id"),
                        "name": f"{folder_name}/{subfolder_name}",
                        "depth": depth + 1
                    })
                    logger.info(f"    Queued subfolder: {subfolder_name} (depth {depth + 1})")

        # Log summary
        if folders_scanned >= max_folders_to_scan:
            logger.warning(f"Reached maximum folder scan limit ({max_folders_to_scan})")

        logger.info(f"✅ Search complete: Scanned {folders_scanned} folders, found {len(matching_files)} matching files")
        return matching_files

    except Exception as e:
        logger.error(f"Design File Search Exception: {str(e)}")
        return f"Error: {str(e)}"

# --- CACHE ---
# Cache for hub_id to avoid repeated calls
hub_cache = {"id": None}

def get_cached_hub_id():
    """Fetches Hub ID once and remembers it."""
    if hub_cache["id"]:
        return hub_cache["id"]
        
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, dict) and data.get("data"):
        hub_id = data["data"][0]["id"]
        hub_cache["id"] = hub_id
        return hub_id
    return None

# --- ROBUST USER SEARCH (Client-Side Filtering with Pagination) ---
def get_user_id_by_email(account_id: str, email: str) -> Optional[str]:
    """
    Finds a user ID by pulling the user list.
    Handles pagination to ensure all users are checked.
    Tries ACC Admin API first, then falls back to HQ API (US then EU).
    """
    token = get_token()
    c_id = clean_id(account_id)
    headers = {"Authorization": f"Bearer {token}"}
    target_email = email.lower().strip()
    
    # Define strategies: (Name, URL_Pattern, Is_ACC_Format)
    strategies = [
        ("ACC Admin", f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users", True),
        ("HQ US", f"{BASE_URL_HQ_US}/{c_id}/users", False),
        ("HQ EU", f"{BASE_URL_HQ_EU}/{c_id}/users", False)
    ]
    
    for name, url_base, is_acc in strategies:
        try:
            logger.info(f"Trying User Search via {name} ({url_base})...")
            offset = 0
            limit = 100
            
            while True:
                params = {"limit": limit, "offset": offset}
                resp = requests.get(url_base, headers=headers, params=params)
                
                if resp.status_code == 404:
                    logger.warning(f"❌ {name} returned 404 (Not Found). Account might be in a different region or using a different API.")
                    break # Trigger fallback to next strategy
                
                if resp.status_code != 200:
                    logger.warning(f"⚠️ {name} Search failed: {resp.status_code} {resp.text}")
                    break # Stop search on this endpoint if API error occurs.
                
                data = resp.json()
                results = []
                
                if is_acc:
                    results = data.get("results", [])
                else:
                    # HQ API typically returns a list, but handle dict wrapper just in case
                    if isinstance(data, list):
                        results = data
                    elif isinstance(data, dict):
                        results = data.get("results", [])
                
                if not results:
                    if offset == 0:
                        logger.info(f"Endpoint {name} returned empty list.")
                    break
                    
                for u in results:
                    u_email = u.get("email", "")
                    if u_email and u_email.lower().strip() == target_email:
                        logger.info(f"✅ Found user in {name}: {target_email} -> {u.get('id')}")
                        return u.get("id")
                
                if len(results) < limit:
                    break # Last page
                    
                offset += limit
                
            # If loop finishes without returning, user was not found in this region.
            if resp.status_code == 200:
                logger.info(f"Scanned {name} and did NOT find user. Stopping search.")
                return None 
        
        except Exception as e:
            logger.error(f"{name} Search Exception: {e}")
            # Continue to next strategy

    return None

@lru_cache(maxsize=16)
def get_acting_user_id(account_id: Optional[str] = None, requester_email: Optional[str] = None) -> Optional[str]:
    """
    Robustly resolves a User ID for 2-legged auth impersonation.
    Cached to prevent repeated API hits for the same account/email.
    """
    # Resolve account_id if missing
    if not account_id:
        account_id = get_cached_hub_id()
        
    if not account_id:
         logger.warning("No Account ID available for resolving Acting User.")
         return None

    try:
        # 0. Check for explicit Admin ID in env (Fastest path)
        env_admin_id = os.environ.get("ACC_ADMIN_ID")
        if env_admin_id:
            return env_admin_id

        # 1. Try Requesting User (Context-specific)
        if requester_email:
            logger.info(f"Looking up specific requester: {requester_email}")
            uid = get_user_id_by_email(account_id, requester_email)
            if uid: return uid

        # 2. Try Fallback Service Account (Global Admin)
        if ACC_ADMIN_EMAIL:
            # Resolves Admin ID from configured email using LRU cache optimization.
            logger.info(f"Resolving Admin ID for configured email: {ACC_ADMIN_EMAIL}")
            uid = get_user_id_by_email(account_id, ACC_ADMIN_EMAIL)
            if uid: 
                return uid
            else:
                logger.error(f"FATAL: Configured ACC_ADMIN_EMAIL '{ACC_ADMIN_EMAIL}' could not be found in Account {account_id}. Admin actions will fail.")
        else:
            logger.warning("ACC_ADMIN_EMAIL (or ACC_ADMIN_ID) is not set in environment. 2-legged Admin actions require this for x-user-id impersonation.")

    except Exception as e:
        logger.error(f"Unexpected error resolving Acting User ID: {e}")
    
    return None

def resolve_to_version_id(project_id: str, item_id: str) -> str:
    """Helper to resolve a File Item ID (urn:adsk.wipp...) to its latest Version ID."""
    try:
        if "fs.file" in item_id or "version=" in item_id:
            return item_id
            
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        
        # If item_id is a plain Item ID (urn:adsk.wipp:dm.lineage:...), resolved to latest tip
        if item_id.startswith("urn:adsk.wipp:dm.lineage"):
             url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(item_id)}/tip"
             r = requests.get(url, headers=headers)
             if r.status_code == 200:
                 return r.json()["data"]["id"] # Returns the specific Version URN
    except Exception as e:
        logger.error(f"Version Resolution Error: {e}")
    
    return item_id

def search_project_folder(project_id: str, query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Searches for files/folders in a project using the Data Management API search.
    """
    try:
        # 1. Get Hub ID (Cached)
        hub_id = get_cached_hub_id()
        if not hub_id: return []

        # 2. Find 'Project Files' root folder
        p_id = ensure_b_prefix(project_id)
        # We need to fetch top folders to find the root
        url_top = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{p_id}/topFolders"
        data_top = make_api_request(url_top)
        
        if isinstance(data_top, str) or not data_top.get("data"):
            logger.warning("Could not find top folders for search.")
            return []
            
        # Prioritize 'Project Files' or just use the first valid folder
        target_folder = next((f["id"] for f in data_top["data"] if f["attributes"]["name"] == "Project Files"), None)
        if not target_folder:
            target_folder = data_top["data"][0]["id"]
            
        # 3. Perform Search
        # Filter syntax: filter[attributes.displayName-contains]=query
        safe_folder = encode_urn(target_folder)
        url_search = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/folders/{safe_folder}/search"
        params = {
            "filter[attributes.displayName-contains]": query,
            "page[limit]": limit
        }
        
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        resp = requests.get(url_search, headers=headers, params=params)
        if resp.status_code != 200:
            logger.error(f"Search API Error {resp.status_code}: {resp.text}")
            return []
            
        return resp.json().get("data", [])
        
    except Exception as e:
        logger.error(f"Search Exception: {e}")
        return []

# --- NEW: FEATURES API (Issues/Assets) ---

def fetch_paginated_data(url: str, limit: int = 100, style: str = "url", impersonate: bool = False) -> Union[List[Dict[str, Any]], str]:
    """
    Generic pagination helper for ACC APIs.
    styles: 
      - 'url': uses data['links']['next']['href'] (Data Management API)
      - 'offset': uses 'offset' and 'limit' query params (Admin/Issues API)
    """
    all_items = []
    page_count = 0
    MAX_PAGES = 50
    current_url = url
    offset = 0
    first_request = True # Flag for "Fail Loudly"
    
    while current_url and page_count < MAX_PAGES:
        try:
            token = get_token()
            headers = {"Authorization": f"Bearer {token}", "x-ads-region": "EMEA"}
            
            # Auto-inject x-user-id for 2-legged flows (Required for Admin/Issues/Assets)
            if impersonate:
                try:
                    hub_id = get_cached_hub_id()
                    if hub_id:
                        admin_uid = get_acting_user_id(clean_id(hub_id))
                        if admin_uid:
                            headers["x-user-id"] = admin_uid
                except Exception:
                    pass # Proceed without header if resolution fails

            # Add params for offset style
            params = {}
            if style == 'offset':
                params = {"offset": offset, "limit": limit}

            resp = requests.get(current_url, headers=headers, params=params if style == 'offset' else None)
            
            # ESCALATION LOGIC: If 401, we might be missing x-user-id or using a non-admin.
            # Issues API strictly forbids Service-to-Service (missing header).
            if resp.status_code == 401 and impersonate:
                logger.warning(f"⚠️ 401 Unauthorized at {current_url}. Escalating: Verifying Admin Context.")
                
                try:
                    # 1. Resolve the definitive Admin ID
                    hub_id = get_cached_hub_id()
                    admin_id = get_acting_user_id(clean_id(hub_id)) if hub_id else None
                    
                    current_header_id = headers.get("x-user-id")
                    
                    # 2. Check if we are already using it
                    if admin_id and current_header_id == admin_id:
                         logger.error("❌ Already failed as Admin. Cannot escalate further.")
                         # Allow it to fall through to the error handler
                    elif admin_id:
                         # 3. Apply Escalation
                         logger.info(f"🔄 Retrying with Admin ID: {admin_id}")
                         headers["x-user-id"] = admin_id
                         resp = requests.get(current_url, headers=headers, params=params if style == 'offset' else None)
                    else:
                         logger.warning("❌ Could not resolve Admin ID for escalation.")
                         
                except Exception as e:
                    logger.error(f"Escalation failed: {e}")
            
            if resp.status_code in [403, 404]:
                logger.warning(f"Endpoint returned {resp.status_code} (Module inactive?).")
                # Treat as empty result, not hard error
                break
                
            if resp.status_code != 200:
                logger.error(f"Pagination Error {resp.status_code} at {current_url}: {resp.text}")
                
                 # CRITICAL FIX: If this is the very first attempt, FAIL LOUDLY.
                if first_request:
                     return f"❌ API Error {resp.status_code}: {resp.text}"
                
                break
                
            data = resp.json()
            first_request = False # Mark first attempt complete
            
            # Determine list key
            batch = []
            if isinstance(data, list):
                batch = data
            elif isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    batch = data["data"]
                elif "results" in data and isinstance(data["results"], list):
                    batch = data["results"]
            
            all_items.extend(batch)

            # Navigate
            if style == 'url':
                links = data.get("links", {}) if isinstance(data, dict) else {}
                next_obj = links.get("next")
                if isinstance(next_obj, dict):
                    current_url = next_obj.get("href")
                else:
                    current_url = None
            elif style == 'offset':
                if len(batch) < limit:
                     current_url = None
                else:
                     offset += limit
            
            page_count += 1
            time.sleep(0.5) # Rate limit protection

        except Exception as e:
            logger.error(f"Pagination Loop Exception: {e}")
            break
            
    return all_items

def get_project_issues(project_id: str, status: Optional[str] = None) -> Union[List[Dict[str, Any]], str]:
    p_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/issues/v1/projects/{p_id}/issues"
    # Issues API uses offset/limit
    items = fetch_paginated_data(url, limit=50, style='offset', impersonate=True)
    
    if isinstance(items, str): return items

    if status:
        items = [i for i in items if i.get("status", "").lower() == status.lower()]
        
    return items

def get_project_assets(project_id: str, category: Optional[str] = None) -> Union[List[Dict[str, Any]], str]:
    p_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/assets/v2/projects/{p_id}/assets" # Assets V2
    items = fetch_paginated_data(url, limit=50, style='offset', impersonate=True)
    
    if isinstance(items, str): return items

    if category:
        items = [i for i in items if category.lower() in i.get("category", {}).get("name", "").lower()]
        
    return items

# --- ADMIN API (Users) ---

def get_account_users(search_term: str = "") -> List[Dict[str, Any]]:
    """
    Fetches users from the Account Admin.
    Defaults to HQ US API as fallbacks proved necessary for this account.
    """
    hub_id = get_cached_hub_id()
    if not hub_id: return []
    account_id = clean_id(hub_id)
    
    # Use HQ US endpoint as the primary strategy for this account context.
    url = f"{BASE_URL_HQ_US}/{account_id}/users"
    
    # HQ API typically uses offset-based pagination.
    # Attempt to fetch with 'url' style first, falling back to 'offset' if needed.
    all_users = fetch_paginated_data(url, limit=100, style='url')
    
    if isinstance(all_users, str) or not all_users:
         # Retry with offset style explicitly
         all_users = fetch_paginated_data(url, limit=100, style='offset')
    
    if isinstance(all_users, str): return [] # Fail silently/empty for User Search to avoid crash

    if search_term and search_term.lower() != "all":
        term = search_term.lower()
        all_users = [
            u for u in all_users 
            if term in u.get("name", "").lower() or term in u.get("email", "").lower()
        ]
        
    return all_users

def invite_user_to_project(project_id: str, email: str, products: Optional[List[Dict[str, str]]] = None) -> str:
    """
    Invites a user to a project.
    Always ensures 'products' list exists (defaults to 'docs' -> 'member').
    """
    try:
        # 1. Get Token & Headers
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-ads-region": "EMEA"
        }
        
        # 1b. Resolve Account context for impersonation
        hub_id = get_cached_hub_id()
        account_id = clean_id(hub_id)

        # 2. Add Impersonation (Required for Admin Write operations)
        acting_user = get_acting_user_id(account_id)
        if acting_user:
            headers["x-user-id"] = acting_user
            
        # 3. Clean ID
        p_id = clean_id(project_id)
        
        # 4. SAFETY NET: Force Default Product if missing
        if not products:
            products = [{
                "key": "docs",
                "access": "member"
            }]
            
        # 5. Construct Payload
        payload = {
            "email": email,
            "products": products
        }
        
        url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{p_id}/users"
        
        # 6. Execute
        response = requests.post(url, headers=headers, json=payload)
        
        # 7. Handle Common Responses
        if response.status_code == 200 or response.status_code == 201:
            return f"✅ Success: Added {email} to project {p_id} with access: {[p['key'] for p in products]}."
            
        elif response.status_code == 409:
            # Handle "User already exists" gracefully
            return f"ℹ️ User {email} is already active in project {p_id}."
            
        elif response.status_code == 400:
            return f"❌ Bad Request (400): {response.text} (Check payload format)"
            
        else:
            return f"❌ Error {response.status_code}: {response.text}"
            
    except Exception as e:
        return f"❌ Exception in invite_user_to_project: {str(e)}"

def fetch_project_users(project_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches users specific to a project using ACC Admin API."""
    p_id = clean_id(project_id)
    url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{p_id}/users"
    # Admin API requires impersonation
    return fetch_paginated_data(url, limit=100, style='offset', impersonate=True)

# --- DATA CONNECTOR API ---

def _get_admin_headers(account_id: str):
    """Helper to get headers with Admin Impersonation."""
    token = get_token()
    headers = { 
        "Authorization": f"Bearer {token}", 
        "Content-Type": "application/json",
        "x-ads-region": "EMEA"
    }
    admin_id = get_acting_user_id(account_id)
    if admin_id:
        headers["x-user-id"] = admin_id
    return headers

def trigger_data_extraction(services: Optional[List[str]] = None) -> dict:
    """Triggers Data Export (Admin Context)."""
    hub_id = get_cached_hub_id()
    if not hub_id: return {"error": "No Hub ID found."}
    account_id = clean_id(hub_id)
    
    headers = _get_admin_headers(account_id)
    if "x-user-id" not in headers:
        return {"error": "Could not resolve Account Admin ID. Data Connector requires Admin Impersonation."}
    
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{account_id}/requests"
    
    payload = {
        "description": f"MCP Agent Export - {time.strftime('%Y-%m-%d')}",
        "schedule": { "interval": "OneTime" }
    }
    if services:
        payload["serviceGroups"] = services
        
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code in [200, 201]:
        return response.json()
    return {"error": f"Failed {response.status_code}: {response.text}"}

def check_request_job_status(request_id: str) -> dict:
    """Gets the JOB status associated with a REQUEST."""
    hub_id = get_cached_hub_id()
    if not hub_id: return {"error": "No Hub ID found."}
    account_id = clean_id(hub_id)
    
    headers = _get_admin_headers(account_id)
    
    # 1. Get Jobs for this Request
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{account_id}/requests/{request_id}/jobs"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        return {"error": f"Failed to get jobs: {response.text}"}
        
    data = response.json()
    jobs = data.get("results", [])
    if not jobs:
        return {"status": "QUEUED", "job_id": None}
        
    # Get latest job
    latest_job = jobs[0]
    return {
        "status": latest_job.get("completionStatus", "PROCESSING"), # success, failed
        "job_id": latest_job.get("id"),
        "progress": latest_job.get("progress", 0)
    }

def get_data_download_url(job_id: str) -> Optional[str]:
    """Gets signed URL for the ZIP file."""
    hub_id = get_cached_hub_id()
    account_id = clean_id(hub_id)
    headers = _get_admin_headers(account_id)
    
    # We request the master ZIP file specifically
    filename = "autodesk_data_extract.zip"
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{account_id}/jobs/{job_id}/data/{filename}"
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("signedUrl")
    return None

def get_account_user_details(email: str) -> dict:
    """
    Fetches full details for a specific user from HQ API (EMEA).
    Does NOT use impersonation.
    """
    hub_id = get_cached_hub_id()
    if not hub_id: return {"error": "No Hub ID found."}
    account_id = clean_id(hub_id)

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "x-ads-region": "EMEA"
    }

    # HQ API User Search
    url = f"https://developer.api.autodesk.com/hq/v1/accounts/{account_id}/users"
    params = {"filter[email]": email}

    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
             return {"error": f"HQ API returned {resp.status_code}: {resp.text}"}

        data = resp.json()
        # HQ API usually returns list
        if isinstance(data, list) and len(data) > 0:
            return data[0]

        return {"error": "User not found via HQ Search."}

    except Exception as e:
        return {"error": str(e)}

# --- MODEL DERIVATIVE API ---

def get_latest_version_urn(project_id: str, item_id: str) -> Optional[str]:
    """
    Resolves a File Item ID (Lineage URN) to its latest Version URN.
    Uses Data Management API to get the tip version from item relationships.

    Args:
        project_id: The project ID
        item_id: The item ID (Lineage URN like urn:adsk.wipp:dm.lineage:...)

    Returns:
        Version URN (urn:adsk.wipp:fs.file:vf...) or None on error
    """
    try:
        # If already a version URN, return as-is
        if "fs.file" in item_id or "version=" in item_id:
            logger.info(f"Already a version URN: {item_id}")
            return item_id

        # Prepare request with EMEA region
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA"
        }

        # Ensure proper ID formatting
        project_id_clean = ensure_b_prefix(project_id)
        item_id_encoded = encode_urn(item_id)

        # Call Data Management API to get item details
        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id_clean}/items/{item_id_encoded}"
        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            logger.error(f"Failed to resolve item {item_id}: {resp.status_code} - {resp.text}")
            return None

        # Extract tip version URN from relationships
        data = resp.json()
        tip_urn = data.get("data", {}).get("relationships", {}).get("tip", {}).get("data", {}).get("id")

        if tip_urn:
            logger.info(f"Resolved lineage URN to version: {tip_urn[:60]}...")
            return tip_urn
        else:
            logger.warning("No tip version found in item relationships")
            return None

    except Exception as e:
        logger.error(f"Version Resolution Exception: {str(e)}")
        return None

def get_model_manifest(version_urn: str) -> Union[Dict[str, Any], str]:
    """
    Fetches the Model Derivative manifest for a file version.
    Shows translation status and available formats.

    Args:
        version_urn: The version URN (e.g., urn:adsk.wipp:fs.file:vf.xxx)

    Returns:
        Manifest data or error string
    """
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}

        # Base64 encode the URN (URL-safe, no padding)
        encoded_urn = safe_b64encode(version_urn)

        url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/manifest"
        resp = requests.get(url, headers=headers)

        if resp.status_code == 404:
            return "Model not found or not yet translated. Please check if the file has been processed in the viewer."

        if resp.status_code != 200:
            logger.error(f"Manifest API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

        return resp.json()

    except Exception as e:
        logger.error(f"Manifest Exception: {str(e)}")
        return f"Error: {str(e)}"

def get_model_metadata(version_urn: str, guid: Optional[str] = None) -> Union[Dict[str, Any], str]:
    """
    Fetches the Model Derivative metadata (object tree) for a file version.
    Shows the hierarchical structure of objects in the model.

    Args:
        version_urn: The version URN
        guid: Optional specific view GUID. If not provided, uses first available view.

    Returns:
        Metadata object tree or error string
    """
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}

        # Base64 encode the URN
        encoded_urn = safe_b64encode(version_urn)

        # If no GUID provided, fetch the manifest to get available views
        if not guid:
            manifest_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/metadata"
            resp = requests.get(manifest_url, headers=headers)

            if resp.status_code == 404:
                return "Model metadata not found. File may not be fully processed."

            if resp.status_code != 200:
                logger.error(f"Metadata API Error {resp.status_code}: {resp.text}")
                return f"Error {resp.status_code}: {resp.text}"

            data = resp.json().get("data", {}).get("metadata", [])
            if not data:
                return "No 3D views found in this model."

            guid = data[0]["guid"]
            logger.info(f"Using first available view GUID: {guid}")

        # Fetch the object tree for this view
        tree_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/metadata/{guid}"
        resp_tree = requests.get(tree_url, headers=headers, params={"forceget": "true"})

        if resp_tree.status_code == 202:
            return "Model metadata is still processing. Please try again in a moment."

        if resp_tree.status_code != 200:
            logger.error(f"Object Tree API Error {resp_tree.status_code}: {resp_tree.text}")
            return f"Error {resp_tree.status_code}: {resp_tree.text}"

        return resp_tree.json()

    except Exception as e:
        logger.error(f"Metadata Exception: {str(e)}")
        return f"Error: {str(e)}"

def resolve_file_to_urn(project_id: str, identifier: str) -> str:
    """
    Smart file resolver that accepts either a URN or a filename.
    Automatically searches for files by name if a URN is not provided.

    Args:
        project_id: The project ID
        identifier: Either a URN (urn:adsk...) or a filename (e.g., "MyFile.rvt")

    Returns:
        File URN (Lineage or Version)

    Raises:
        ValueError: If filename doesn't match any files
    """
    try:
        # Check 1: It's already a URN
        if identifier.startswith("urn:adsk"):
            logger.info(f"  Identifier is already a URN")
            return identifier

        # Check 2: It's a filename - search for it
        logger.info(f"  Identifier appears to be a filename, searching: {identifier}")

        # Get hub_id for the search
        hub_id = get_cached_hub_id()
        if not hub_id:
            raise ValueError("Could not determine hub_id for file search")

        # Search for files with common extensions
        # Try multiple extensions to increase match likelihood
        extensions_to_try = ["rvt", "dwg", "nwc", "rcp", "ifc", "nwd"]

        # Extract extension from identifier if present
        if "." in identifier:
            file_ext = identifier.split(".")[-1].lower()
            # Put the matching extension first
            if file_ext in extensions_to_try:
                extensions_to_try.remove(file_ext)
                extensions_to_try.insert(0, file_ext)

        # Try first extension (most likely match)
        search_extension = extensions_to_try[0]
        files = find_design_files(hub_id, project_id, search_extension)

        if isinstance(files, str):
            raise ValueError(f"Error searching for files: {files}")

        # Search for matching filename (case-insensitive)
        identifier_lower = identifier.lower()

        for file in files:
            file_name = file.get("name", "").lower()

            # Exact match or contains match
            if identifier_lower == file_name or identifier_lower in file_name:
                item_id = file.get("item_id")
                if item_id:
                    logger.info(f"  ✅ Found file: {file.get('name')} (ID: {item_id[:60]}...)")
                    return item_id

        # If not found with first extension, try others
        for ext in extensions_to_try[1:]:
            logger.info(f"  Trying extension: {ext}")
            files = find_design_files(hub_id, project_id, ext)

            if not isinstance(files, str):
                for file in files:
                    file_name = file.get("name", "").lower()
                    if identifier_lower == file_name or identifier_lower in file_name:
                        item_id = file.get("item_id")
                        if item_id:
                            logger.info(f"  ✅ Found file: {file.get('name')} (ID: {item_id[:60]}...)")
                            return item_id

        # Not found
        raise ValueError(f"Could not find file matching '{identifier}' in project")

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"File Resolution Exception: {str(e)}")
        raise ValueError(f"Error resolving file identifier: {str(e)}")

def inspect_generic_file(project_id: str, file_id: str) -> str:
    """
    Smart file inspector with automatic name and URN resolution.
    Accepts either a filename, Lineage URN, or Version URN.

    Args:
        project_id: The project ID
        file_id: Either a filename (e.g., "MyFile.rvt"), Lineage URN, or Version URN

    Returns:
        Status message string
    """
    try:
        logger.info(f"[Smart Inspector] Inspecting file: {file_id[:60] if len(file_id) > 60 else file_id}...")

        # Step 1: Resolve filename to URN if needed
        try:
            resolved_id = resolve_file_to_urn(project_id, file_id)
        except ValueError as e:
            return f"❌ {str(e)}"

        # Step 2: Auto-Resolve - Check if this is a Lineage URN
        version_urn = None

        if "lineage" in resolved_id:
            logger.info("  Detected Lineage URN - resolving to latest version...")
            version_urn = get_latest_version_urn(project_id, resolved_id)

            if not version_urn:
                return "❌ Error: Could not resolve Lineage URN to Version URN. File may not exist or has no versions."

        elif "fs.file" in resolved_id or "version=" in resolved_id:
            logger.info("  Detected Version URN - proceeding directly")
            version_urn = resolved_id
        else:
            # Assume it might be a lineage URN without obvious markers
            logger.info("  URN type unclear - attempting resolution...")
            version_urn = get_latest_version_urn(project_id, resolved_id)

            if not version_urn:
                # If resolution fails, try using it as-is
                version_urn = resolved_id

        # Step 2: Encode the URN (Base64, strip padding)
        encoded_urn = safe_b64encode(version_urn)

        # Step 3: Call Model Derivative API to get manifest
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/manifest"

        resp = requests.get(url, headers=headers)

        # Step 4: Handle different status codes
        if resp.status_code == 404:
            return "⚠️ Not Translated. Translation required. The file exists but hasn't been processed by Model Derivative API yet."

        if resp.status_code == 202:
            return "⏳ Processing - Translation in progress. Please check again later."

        if resp.status_code != 200:
            logger.error(f"Manifest API Error {resp.status_code}: {resp.text}")
            return f"❌ Error {resp.status_code}: Unable to inspect file."

        # Parse the manifest
        manifest = resp.json()
        status = manifest.get("status", "unknown").lower()
        progress = manifest.get("progress", "unknown")

        if status == "success":
            return f"✅ Ready for Extraction (Translation complete, progress: {progress})"
        elif status == "inprogress":
            return f"⏳ Processing (Translation {progress}% complete)"
        elif status == "failed":
            return "❌ Translation Failed - Check file format or try re-uploading"
        elif status == "timeout":
            return "⏱️ Translation Timeout - File may be too large or complex"
        else:
            return f"Status: {status} (Progress: {progress})"

    except Exception as e:
        logger.error(f"File Inspection Exception: {str(e)}")
        return f"❌ Error: {str(e)}"

def fetch_object_tree(project_id: str, file_identifier: str) -> Union[Dict[str, Any], str]:
    """
    Fetches the complete object tree (hierarchy) for a translated model.
    Accepts filename, Lineage URN, or Version URN.

    Args:
        project_id: The project ID
        file_identifier: Filename (e.g., "MyFile.rvt"), Lineage URN, or Version URN

    Returns:
        Object tree data dict or error string
    """
    try:
        logger.info(f"[Data Extraction] Fetching object tree for: {file_identifier[:60] if len(file_identifier) > 60 else file_identifier}...")

        # Step 1: Resolve filename to URN if needed
        try:
            resolved_id = resolve_file_to_urn(project_id, file_identifier)
        except ValueError as e:
            return f"❌ {str(e)}"

        # Step 2: Auto-resolve if this is a Lineage URN
        resolved_urn = resolved_id
        if "lineage" in resolved_id:
            logger.info("  Resolving Lineage URN to Version URN...")
            resolved_urn = get_latest_version_urn(project_id, resolved_id)
            if not resolved_urn:
                return "❌ Error: Could not resolve URN to version"

        # Step 2: Encode the URN (Base64, no padding)
        encoded_urn = safe_b64encode(resolved_urn)

        # Get auth token
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA"
        }

        # Step 3: Get metadata to find the view GUID
        metadata_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/metadata"
        resp = requests.get(metadata_url, headers=headers)

        if resp.status_code == 404:
            return "❌ Model not found or not yet translated. Run inspect_file first to check translation status."

        if resp.status_code != 200:
            logger.error(f"Metadata API Error {resp.status_code}: {resp.text}")
            return f"❌ Error {resp.status_code}: {resp.text}"

        # Extract the first view GUID
        metadata = resp.json()
        views = metadata.get("data", {}).get("metadata", [])

        if not views:
            return "❌ No 3D views found in this model. It may not contain extractable geometry."

        guid = views[0].get("guid")
        view_name = views[0].get("name", "Unknown")
        logger.info(f"  Using view: {view_name} (GUID: {guid})")

        # Step 4: Get the object tree for this view (with safe GZIP handling)
        tree_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/metadata/{guid}"

        # Prepare headers with explicit GZIP support and EMEA region
        tree_headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip, deflate",  # Explicitly request compression
            "x-ads-region": "EMEA"  # EMEA region support
        }

        logger.info(f"  Requesting object tree from Model Derivative API...")

        # Use stream=True for large payloads and proper GZIP handling
        resp_tree = requests.get(
            tree_url,
            headers=tree_headers,
            params={"forceget": "true"},
            stream=True,
            timeout=120  # 2 minute timeout for large models
        )

        if resp_tree.status_code == 202:
            return "⏳ Object tree is still being processed. Please wait a moment and try again."

        if resp_tree.status_code != 200:
            logger.error(f"Object Tree API Error {resp_tree.status_code}: {resp_tree.text}")
            return f"❌ Error {resp_tree.status_code}: Failed to fetch object tree"

        # Debug: Log response details
        content_encoding = resp_tree.headers.get('Content-Encoding', 'none')
        content_length = resp_tree.headers.get('Content-Length', 'unknown')
        logger.info(f"  Response encoding: {content_encoding}, Content-Length: {content_length}")

        # Step 5: Safely parse the JSON response
        try:
            # requests automatically handles GZIP decompression when using .json()
            tree_data = resp_tree.json()

            if not tree_data:
                logger.error("Received empty JSON response")
                return "❌ Error: Received empty response from API"

            # Extract and log statistics
            data_section = tree_data.get("data", {})
            objects = data_section.get("objects", [])
            object_count = len(objects)

            # Calculate approximate data size for logging
            if objects:
                total_nodes = 0

                def count_nodes(nodes):
                    nonlocal total_nodes
                    for node in nodes:
                        total_nodes += 1
                        if "objects" in node and isinstance(node["objects"], list):
                            count_nodes(node["objects"])

                count_nodes(objects)
                logger.info(f"✅ Successfully fetched object tree:")
                logger.info(f"   - Root objects: {object_count}")
                logger.info(f"   - Total nodes: {total_nodes}")
            else:
                logger.warning("Object tree has no objects - model may be empty")

            return tree_data

        except ValueError as json_err:
            logger.error(f"JSON parsing failed: {str(json_err)}")
            # DO NOT log response content - it may be huge
            return f"❌ Error: Failed to parse model data. The response may be corrupted or invalid JSON."

        except Exception as parse_err:
            logger.error(f"Unexpected parsing error: {str(parse_err)}")
            return f"❌ Error: Unexpected error while processing model data: {str(parse_err)}"

    except requests.exceptions.Timeout:
        logger.error("Request timed out while fetching object tree")
        return "❌ Error: Request timed out. The model may be too large or the server is slow to respond."

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request Exception: {str(req_err)}")
        return f"❌ Error: Network error while fetching object tree: {str(req_err)}"

    except Exception as e:
        logger.error(f"Object Tree Exception: {str(e)}")
        return f"❌ Error: {str(e)}"


def get_view_guid_only(version_urn: str) -> str:
    """
    Fetches view GUID with automatic token healing.
    Retries ONCE on 401 errors by forcing a token refresh.

    Args:
        version_urn: The version URN (e.g., urn:adsk.wipp:fs.file:vf...)

    Returns:
        View GUID (str)

    Raises:
        ValueError: If model not found, no 3D views available, or auth fails after retry
    """
    logger.info(f"[Lightweight GUID Fetch - EMEA Region] Getting view GUID for model...")

    # Step 1: Encode the RAW version URN (Do not strip query params)
    # For ACC/BIM360, the URN must include the version suffix (e.g., ?version=X)
    logger.info(f"  Using Full URN: {version_urn[:80]}...")
    urn_b64 = safe_b64encode(version_urn)
    metadata_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn_b64}/metadata"

    # RETRY LOOP (The Fix) - Up to 2 attempts
    for attempt in range(2):
        try:
            # Attempt 0: Normal token (cached if available)
            # Attempt 1: Force refresh token
            is_retry = (attempt == 1)
            if is_retry:
                logger.info("⚠️ 401 received on first attempt. Force-refreshing token and retrying...")

            # Get token (force refresh on second attempt)
            token = get_token(force_refresh=is_retry)

            # Prepare headers with EMEA region
            headers = {
                "Authorization": f"Bearer {token}",
                "x-ads-region": "EMEA"  # CRITICAL: Must route to European data center
            }

            logger.info(f"  Attempt {attempt + 1}/2: Fetching Metadata from EMEA...")
            logger.info(f"  Request URL: {metadata_url}")
            logger.info(f"  Request Headers: Authorization=Bearer *****, x-ads-region={headers.get('x-ads-region')}")

            # Make the request
            resp = requests.get(metadata_url, headers=headers, timeout=30)

            # If 401 and it's the first attempt, CONTINUE to next loop iteration (retry)
            if resp.status_code == 401 and attempt == 0:
                logger.warning("  Received 401 on first attempt. Will retry with fresh token...")
                continue

            # If 401 on second attempt after token refresh, raise error
            if resp.status_code == 401:
                error_msg = "❌ Critical Auth Failure: 401 Unauthorized even after token refresh. Verify 'viewables:read' scope is configured."
                logger.error(error_msg)
                logger.error(f"  Response: {resp.text}")
                logger.error(f"  Required scopes: data:read data:write data:create bucket:read viewables:read")
                raise ValueError(error_msg)

            # Handle 404 - model not found or not translated
            if resp.status_code == 404:
                error_msg = "❌ Model not found (404). Translation might be missing or file doesn't exist in Model Derivative."
                logger.error(error_msg)
                logger.error(f"  Response: {resp.text}")
                raise ValueError(error_msg)

            # Raise for any other non-200 status
            if resp.status_code != 200:
                error_msg = f"Metadata API Error {resp.status_code}: {resp.text}"
                logger.error(error_msg)
                raise ValueError(f"❌ {error_msg}")

            # Success - parse the response
            data = resp.json()
            views = data.get("data", {}).get("metadata", [])

            if not views:
                error_msg = "❌ No 3D views found in model metadata. Model may not contain extractable geometry."
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Extract GUID and view name
            guid = views[0].get("guid")
            view_name = views[0].get("name", "Unknown")

            logger.info(f"✅ Found view: {view_name} (GUID: {guid})")
            return guid

        except ValueError:
            # Re-raise ValueError (already has user-friendly message)
            if attempt == 1:
                raise
            # On first attempt, continue to retry
            continue

        except Exception as e:
            # Only raise on final attempt
            if attempt == 1:
                error_msg = f"❌ Error fetching view GUID: {str(e)}"
                logger.error(error_msg)
                raise ValueError(error_msg)

    # Should never reach here, but just in case
    raise ValueError("❌ Unexpected error in retry loop.")


def query_model_elements(project_id: str, file_identifier: str, category_name: str) -> Union[int, str]:
    """
    Queries a model for elements matching a category using server-side filtering.
    Uses the Model Derivative Query API to avoid downloading the full object tree.

    Args:
        project_id: The project ID
        file_identifier: Filename (e.g., "MyFile.rvt"), Lineage URN, or Version URN
        category_name: Category to search for (e.g., "Walls", "Doors", "Windows")

    Returns:
        Count of matching elements (int) or error message (str)
    """
    try:
        logger.info(f"[Server-Side Query] Querying model for category: {category_name}")

        # Step 1: Resolve filename to URN if needed
        try:
            resolved_id = resolve_file_to_urn(project_id, file_identifier)
        except ValueError as e:
            return f"❌ {str(e)}"

        # Step 2: Auto-resolve if this is a Lineage URN
        resolved_urn = resolved_id
        if "lineage" in resolved_id:
            logger.info("  Resolving Lineage URN to Version URN...")
            resolved_urn = get_latest_version_urn(project_id, resolved_id)
            if not resolved_urn:
                return "❌ Error: Could not resolve URN to version"

        # Step 3: Get view GUID (lightweight - no object tree download)
        try:
            guid = get_view_guid_only(resolved_urn)
        except ValueError as e:
            # get_view_guid_only raises ValueError with user-friendly message
            return str(e)

        # Step 4: Encode the URN for the query endpoint (use raw URN)
        # For ACC/BIM360, the URN must include the version suffix
        encoded_urn = safe_b64encode(resolved_urn)

        # Get auth token
        token = get_token()

        # Step 5: Execute server-side query
        query_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/metadata/{guid}/properties:query"

        query_headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA",
            "Content-Type": "application/json"
        }

        # Build query payload - filter by name containing category
        query_payload = {
            "query": {
                "$contains": ["name", category_name]
            }
        }

        logger.info(f"  Executing server-side query: {query_payload}")

        resp_query = requests.post(
            query_url,
            headers=query_headers,
            json=query_payload,
            timeout=60
        )

        # Handle specific error cases
        if resp_query.status_code == 400:
            logger.warning(f"Query API returned 400: {resp_query.text}")
            return "⚠️ Query Failed. The model might not support advanced querying yet."

        if resp_query.status_code == 404:
            return "❌ Model metadata not found. The model may not be fully translated yet."

        if resp_query.status_code != 200:
            logger.error(f"Query API Error {resp_query.status_code}: {resp_query.text}")
            return f"❌ Query Error {resp_query.status_code}: {resp_query.text}"

        # Step 6: Extract count from results
        query_data = resp_query.json()
        collection = query_data.get("data", {}).get("collection", [])

        count = len(collection)
        logger.info(f"✅ Server-side query complete. Found {count} matching elements.")

        return count

    except requests.exceptions.Timeout:
        logger.error("Query request timed out")
        return "❌ Error: Query request timed out. The model may be too large or the server is slow to respond."

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request Exception during query: {str(req_err)}")
        return f"❌ Error: Network error during query: {str(req_err)}"

    except Exception as e:
        logger.error(f"Query Exception: {str(e)}")
        return f"❌ Error: {str(e)}"

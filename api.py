import os
import requests
import logging
import time
import base64
import re
from functools import lru_cache
from urllib.parse import quote
from typing import Optional, Dict, Any, List, Union
from auth import get_token, BASE_URL_ACC, BASE_URL_HQ_US, BASE_URL_HQ_EU, BASE_URL_GRAPHQL, ACC_ADMIN_EMAIL

logger = logging.getLogger(__name__)


# ==========================================================================
# UTILITIES
# ==========================================================================

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


# ==========================================================================
# CENTRALIZED REQUEST HELPER (DRY)
# ==========================================================================

def _make_request(method: str, url: str, *, extra_headers: Optional[Dict] = None,
                  retry_on_401: bool = True, **kwargs) -> requests.Response:
    """
    Centralized HTTP request helper.
    - Auto-adds Authorization header from cached/fresh token.
    - Auto-adds x-ads-region: EMEA to every request.
    - Auto-retries ONCE on 401 by forcing a token refresh.

    Args:
        method: HTTP method (GET, POST, PUT, PATCH, DELETE).
        url: Full request URL.
        extra_headers: Additional headers merged on top of the defaults.
        retry_on_401: If True, retry once with a fresh token on 401.
        **kwargs: Passed directly to requests.request (json, data, params, timeout, stream, etc.)

    Returns:
        requests.Response object (caller decides how to handle status codes).
    """
    max_attempts = 2 if retry_on_401 else 1
    last_resp: requests.Response = None  # type: ignore[assignment]

    for attempt in range(max_attempts):
        token = get_token(force_refresh=(attempt > 0))
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA",
        }
        if extra_headers:
            headers.update(extra_headers)

        last_resp = requests.request(method, url, headers=headers, **kwargs)

        if last_resp.status_code == 401 and attempt == 0 and retry_on_401:
            logger.warning(f"401 on {method} {url[:80]} — refreshing token and retrying...")
            continue

        return last_resp

    return last_resp


# ==========================================================================
# GENERIC REQUEST WRAPPERS
# ==========================================================================

def make_api_request(url: str):
    """Generic GET wrapper with error handling."""
    try:
        resp = _make_request("GET", url)
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
    Critical: GraphQL returns 200 OK even with errors — must check response.data.errors.
    """
    try:
        resp = _make_request(
            "POST", BASE_URL_GRAPHQL,
            extra_headers={"Content-Type": "application/json"},
            json={"query": query, "variables": variables or {}}
        )

        if resp.status_code != 200:
            logger.warning(f"GraphQL HTTP Error {resp.status_code}: {resp.text}")
            return f"GraphQL Error {resp.status_code}: {resp.text}"

        json_data = resp.json()

        if "errors" in json_data:
            error_messages = [err.get("message", str(err)) for err in json_data["errors"]]
            error_str = "; ".join(error_messages)
            logger.error(f"GraphQL Query Errors: {error_str}")
            return f"GraphQL Query Error: {error_str}"

        return json_data.get("data", {})
    except Exception as e:
        logger.error(f"GraphQL Exception: {str(e)}")
        return f"GraphQL Exception: {str(e)}"


# ==========================================================================
# HUB / PROJECT DISCOVERY
# ==========================================================================

def get_hubs_aec() -> Union[List[Dict[str, Any]], str]:
    """Fetches all hubs using AEC Data Model GraphQL API."""
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
    """Fetches all projects for a hub using AEC Data Model GraphQL API."""
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
    result = make_graphql_request(query, {"hubId": hub_id})
    if isinstance(result, str):
        return result
    if not result or "projects" not in result:
        return f"No projects data returned for hub {hub_id}"
    return result["projects"].get("results", [])

def get_hubs_rest() -> Union[List[Dict[str, Any]], str]:
    """Fetches all hubs using Data Management REST API (2-legged OAuth)."""
    try:
        resp = _make_request("GET", "https://developer.api.autodesk.com/project/v1/hubs")

        if resp.status_code != 200:
            logger.error(f"REST API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

        return [
            {"id": h.get("id"), "name": h.get("attributes", {}).get("name")}
            for h in resp.json().get("data", [])
        ]
    except Exception as e:
        logger.error(f"REST Hub Exception: {str(e)}")
        return f"Error: {str(e)}"

def get_projects_rest(hub_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches all projects for a hub using Data Management REST API (2-legged OAuth)."""
    try:
        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
        all_projects = fetch_paginated_data(url, style='url', impersonate=False)

        if isinstance(all_projects, str):
            return all_projects

        return [
            {"id": p.get("id"), "name": p.get("attributes", {}).get("name"), "hubId": hub_id}
            for p in all_projects
        ]
    except Exception as e:
        logger.error(f"REST Project Exception: {str(e)}")
        return f"Error: {str(e)}"


# ==========================================================================
# PROJECT FINDER
# ==========================================================================

def find_project_globally(name_query: str) -> Optional[tuple]:
    """
    Universal project finder — searches across ALL accessible hubs.
    Case-insensitive substring matching.

    Returns:
        Tuple of (hub_id, project_id, project_name) if found, None otherwise.
    """
    try:
        logger.info(f"[Universal Explorer] Searching globally for project: {name_query}")
        hubs = get_hubs_rest()

        if isinstance(hubs, str):
            logger.error(f"Failed to get hubs: {hubs}")
            return None
        if not hubs:
            logger.error("No hubs found. Check if your app has access to any accounts.")
            return None

        search_term = name_query.lower().strip()

        for hub in hubs:
            hub_id = hub.get("id")
            hub_name = hub.get("name", "Unknown")
            if not hub_id:
                continue

            logger.info(f"  Searching in hub: {hub_name}")
            projects = get_projects_rest(hub_id)

            if isinstance(projects, str):
                logger.warning(f"  Failed to get projects for hub {hub_name}: {projects}")
                continue

            for project in projects:
                project_name = project.get("name", "")
                if search_term in project_name.lower():
                    logger.info(f"Found project '{project_name}' in hub '{hub_name}'")
                    return (hub_id, project.get("id", ""), project_name)

        logger.warning(f"Project '{name_query}' not found in any accessible hub")
        return None
    except Exception as e:
        logger.error(f"Project Search Exception: {str(e)}")
        return None

def resolve_project(project_name_or_id: str) -> Union[Dict[str, Any], str]:
    """Legacy wrapper for find_project_globally() returning dict format."""
    result = find_project_globally(project_name_or_id)
    if result is None:
        return f"Project '{project_name_or_id}' not found in any accessible hub. Please check the name or ID."
    hub_id, project_id, project_name = result
    return {
        "hub_id": hub_id,
        "project_id": project_id,
        "project_name": project_name,
        "hub_name": "Unknown"
    }


# ==========================================================================
# FOLDER OPERATIONS
# ==========================================================================

def get_top_folders(hub_id: str, project_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches top-level folders for a project."""
    try:
        hub_id_clean = ensure_b_prefix(hub_id)
        project_id_clean = ensure_b_prefix(project_id)

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id_clean}/projects/{project_id_clean}/topFolders"
        resp = _make_request("GET", url)

        if resp.status_code != 200:
            logger.error(f"Top Folders API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

        return [
            {
                "id": f.get("id"),
                "name": f.get("attributes", {}).get("displayName"),
                "type": f.get("type")
            }
            for f in resp.json().get("data", [])
        ]
    except Exception as e:
        logger.error(f"Top Folders Exception: {str(e)}")
        return f"Error: {str(e)}"

def get_folder_contents(project_id: str, folder_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches contents of a folder with proper URN extraction for files."""
    try:
        project_id_clean = ensure_b_prefix(project_id)
        folder_id_encoded = encode_urn(folder_id)

        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id_clean}/folders/{folder_id_encoded}/contents"
        resp = _make_request("GET", url)

        if resp.status_code != 200:
            logger.error(f"Folder Contents API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

        result = []
        for item in resp.json().get("data", []):
            item_type = item.get("type")
            attributes = item.get("attributes", {})

            parsed_item = {
                "id": item.get("id"),
                "name": attributes.get("displayName"),
                "type": item_type
            }

            if item_type == "items":
                tip_id = (item.get("relationships", {})
                          .get("tip", {}).get("data", {}).get("id"))
                if tip_id:
                    parsed_item["tipVersionUrn"] = tip_id
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
    Navigates to 'Project Files' folder and searches nested subfolders.
    """
    try:
        logger.info(f"[Universal Explorer] Searching for files with extensions: {extensions}")
        top_folders = get_top_folders(hub_id, project_id)

        if isinstance(top_folders, str):
            return f"Failed to get top folders: {top_folders}"

        # Find "Project Files" folder (fallback to first folder)
        project_files_folder = None
        for folder in top_folders:
            if folder.get("name") == "Project Files":
                project_files_folder = folder.get("id")
                break
        if not project_files_folder and top_folders:
            project_files_folder = top_folders[0].get("id")
        if not project_files_folder:
            return "No folders found in project"

        # BFS with depth limit
        folder_queue = [{"id": project_files_folder, "name": "Project Files", "depth": 0}]
        matching_files = []
        ext_list = [ext.strip().lower() for ext in extensions.split(",")]
        max_folders_to_scan = 50
        max_depth = 3
        folders_scanned = 0

        while folder_queue and folders_scanned < max_folders_to_scan:
            current_folder = folder_queue.pop(0)
            folder_id = current_folder["id"]
            folder_name = current_folder["name"]
            depth = current_folder["depth"]
            folders_scanned += 1

            logger.info(f"  Scanning folder {folders_scanned}/{max_folders_to_scan} (depth {depth}): '{folder_name}'")
            contents = get_folder_contents(project_id, folder_id)

            if isinstance(contents, str):
                logger.warning(f"  Failed to get contents of folder '{folder_name}': {contents}")
                continue

            for item in contents:
                item_type = item.get("itemType")

                if item_type == "file":
                    name = item.get("name", "")
                    if any(name.lower().endswith(f".{ext}") for ext in ext_list):
                        matching_files.append({
                            "name": name,
                            "item_id": item.get("id"),
                            "version_id": item.get("tipVersionUrn"),
                            "folder_path": folder_name
                        })
                        logger.info(f"    Found: {name}")

                elif item_type == "folder" and depth < max_depth:
                    subfolder_name = item.get("name", "Unknown")
                    folder_queue.append({
                        "id": item.get("id"),
                        "name": f"{folder_name}/{subfolder_name}",
                        "depth": depth + 1
                    })

        if folders_scanned >= max_folders_to_scan:
            logger.warning(f"Reached maximum folder scan limit ({max_folders_to_scan})")

        logger.info(f"Search complete: Scanned {folders_scanned} folders, found {len(matching_files)} matching files")
        return matching_files
    except Exception as e:
        logger.error(f"Design File Search Exception: {str(e)}")
        return f"Error: {str(e)}"


# ==========================================================================
# HUB CACHE
# ==========================================================================

hub_cache = {"id": None}

def get_cached_hub_id():
    """Fetches Hub ID once and caches it."""
    if hub_cache["id"]:
        return hub_cache["id"]
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, dict) and data.get("data"):
        hub_id = data["data"][0]["id"]
        hub_cache["id"] = hub_id
        return hub_id
    return None


# ==========================================================================
# USER SEARCH / RESOLUTION
# ==========================================================================

def get_user_id_by_email(account_id: str, email: str) -> Optional[str]:
    """
    Finds a user ID by email using pagination.
    Tries ACC Admin API first, then falls back to HQ API (US then EU).
    """
    token = get_token()
    c_id = clean_id(account_id)
    headers = {"Authorization": f"Bearer {token}"}
    target_email = email.lower().strip()

    strategies = [
        ("ACC Admin", f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users", True),
        ("HQ US", f"{BASE_URL_HQ_US}/{c_id}/users", False),
        ("HQ EU", f"{BASE_URL_HQ_EU}/{c_id}/users", False)
    ]

    for name, url_base, is_acc in strategies:
        try:
            logger.info(f"Trying User Search via {name}...")
            offset = 0
            limit = 100

            while True:
                params = {"limit": limit, "offset": offset}
                resp = requests.get(url_base, headers=headers, params=params)

                if resp.status_code == 404:
                    logger.warning(f"{name} returned 404. Trying next strategy.")
                    break
                if resp.status_code != 200:
                    logger.warning(f"{name} Search failed: {resp.status_code} {resp.text}")
                    break

                data = resp.json()
                if is_acc:
                    results = data.get("results", [])
                else:
                    results = data if isinstance(data, list) else data.get("results", [])

                if not results:
                    break

                for u in results:
                    u_email = u.get("email", "")
                    if u_email and u_email.lower().strip() == target_email:
                        logger.info(f"Found user in {name}: {target_email} -> {u.get('id')}")
                        return u.get("id")

                if len(results) < limit:
                    break
                offset += limit

            if resp.status_code == 200:
                logger.info(f"Scanned {name} — user not found. Stopping search.")
                return None

        except Exception as e:
            logger.error(f"{name} Search Exception: {e}")

    return None

@lru_cache(maxsize=16)
def get_acting_user_id(account_id: Optional[str] = None, requester_email: Optional[str] = None) -> Optional[str]:
    """
    Resolves a User ID for 2-legged auth impersonation.
    Cached to prevent repeated API calls.
    """
    if not account_id:
        account_id = get_cached_hub_id()
    if not account_id:
        logger.warning("No Account ID available for resolving Acting User.")
        return None

    try:
        env_admin_id = os.environ.get("ACC_ADMIN_ID")
        if env_admin_id:
            return env_admin_id

        if requester_email:
            logger.info(f"Looking up specific requester: {requester_email}")
            uid = get_user_id_by_email(account_id, requester_email)
            if uid:
                return uid

        if ACC_ADMIN_EMAIL:
            logger.info(f"Resolving Admin ID for configured email: {ACC_ADMIN_EMAIL}")
            uid = get_user_id_by_email(account_id, ACC_ADMIN_EMAIL)
            if uid:
                return uid
            else:
                logger.error(f"Configured ACC_ADMIN_EMAIL '{ACC_ADMIN_EMAIL}' not found in Account {account_id}.")
        else:
            logger.warning("ACC_ADMIN_EMAIL (or ACC_ADMIN_ID) not set. 2-legged Admin actions require this.")

    except Exception as e:
        logger.error(f"Unexpected error resolving Acting User ID: {e}")

    return None

def resolve_to_version_id(project_id: str, item_id: str) -> str:
    """Resolves a File Item ID to its latest Version ID."""
    try:
        if "fs.file" in item_id or "version=" in item_id:
            return item_id

        p_id = ensure_b_prefix(project_id)

        if item_id.startswith("urn:adsk.wipp:dm.lineage"):
            url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(item_id)}/tip"
            resp = _make_request("GET", url)
            if resp.status_code == 200:
                return resp.json()["data"]["id"]
    except Exception as e:
        logger.error(f"Version Resolution Error: {e}")

    return item_id


# ==========================================================================
# PAGINATION AND SEARCH
# ==========================================================================

def fetch_paginated_data(url: str, limit: int = 100, style: str = "url",
                         impersonate: bool = False) -> Union[List[Dict[str, Any]], str]:
    """
    Generic pagination helper for ACC APIs.
    Styles:
      - 'url': follows data.links.next.href (Data Management API)
      - 'offset': uses offset/limit query params (Admin/Issues API)
    """
    all_items = []
    page_count = 0
    MAX_PAGES = 50
    current_url = url
    offset = 0
    first_request = True

    while current_url and page_count < MAX_PAGES:
        try:
            token = get_token()
            headers = {"Authorization": f"Bearer {token}", "x-ads-region": "EMEA"}

            if impersonate:
                try:
                    hub_id = get_cached_hub_id()
                    if hub_id:
                        admin_uid = get_acting_user_id(clean_id(hub_id))
                        if admin_uid:
                            headers["x-user-id"] = admin_uid
                except Exception:
                    pass

            params = {}
            if style == 'offset':
                params = {"offset": offset, "limit": limit}

            resp = requests.get(current_url, headers=headers,
                                params=params if style == 'offset' else None)

            # Escalation on 401 with impersonation
            if resp.status_code == 401 and impersonate:
                logger.warning(f"401 Unauthorized at {current_url}. Escalating with Admin context.")
                try:
                    hub_id = get_cached_hub_id()
                    admin_id = get_acting_user_id(clean_id(hub_id)) if hub_id else None
                    current_header_id = headers.get("x-user-id")

                    if admin_id and current_header_id == admin_id:
                        logger.error("Already failed as Admin. Cannot escalate further.")
                    elif admin_id:
                        logger.info(f"Retrying with Admin ID: {admin_id}")
                        headers["x-user-id"] = admin_id
                        resp = requests.get(current_url, headers=headers,
                                            params=params if style == 'offset' else None)
                    else:
                        logger.warning("Could not resolve Admin ID for escalation.")
                except Exception as e:
                    logger.error(f"Escalation failed: {e}")

            if resp.status_code in [403, 404]:
                logger.warning(f"Endpoint returned {resp.status_code} (Module inactive?).")
                break

            if resp.status_code != 200:
                logger.error(f"Pagination Error {resp.status_code} at {current_url}: {resp.text}")
                if first_request:
                    return f"API Error {resp.status_code}: {resp.text}"
                break

            data = resp.json()
            first_request = False

            batch = []
            if isinstance(data, list):
                batch = data
            elif isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    batch = data["data"]
                elif "results" in data and isinstance(data["results"], list):
                    batch = data["results"]

            all_items.extend(batch)

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
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"Pagination Loop Exception: {e}")
            break

    return all_items

def search_project_folder(project_id: str, query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Searches for files/folders in a project using Data Management API search."""
    try:
        hub_id = get_cached_hub_id()
        if not hub_id:
            return []

        p_id = ensure_b_prefix(project_id)
        url_top = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{p_id}/topFolders"
        data_top = make_api_request(url_top)

        if isinstance(data_top, str) or not data_top.get("data"):
            logger.warning("Could not find top folders for search.")
            return []

        target_folder = next(
            (f["id"] for f in data_top["data"] if f["attributes"]["name"] == "Project Files"),
            data_top["data"][0]["id"]
        )

        safe_folder = encode_urn(target_folder)
        url_search = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/folders/{safe_folder}/search"

        resp = _make_request("GET", url_search, params={
            "filter[attributes.displayName-contains]": query,
            "page[limit]": limit
        })

        if resp.status_code != 200:
            logger.error(f"Search API Error {resp.status_code}: {resp.text}")
            return []

        return resp.json().get("data", [])
    except Exception as e:
        logger.error(f"Search Exception: {e}")
        return []


# ==========================================================================
# FEATURES API (Issues / Assets)
# ==========================================================================

def get_project_issues(project_id: str, status: Optional[str] = None) -> Union[List[Dict[str, Any]], str]:
    p_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/issues/v1/projects/{p_id}/issues"
    items = fetch_paginated_data(url, limit=50, style='offset', impersonate=True)
    if isinstance(items, str):
        return items
    if status:
        items = [i for i in items if i.get("status", "").lower() == status.lower()]
    return items

def get_project_assets(project_id: str, category: Optional[str] = None) -> Union[List[Dict[str, Any]], str]:
    p_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/assets/v2/projects/{p_id}/assets"
    items = fetch_paginated_data(url, limit=50, style='offset', impersonate=True)
    if isinstance(items, str):
        return items
    if category:
        items = [i for i in items if category.lower() in i.get("category", {}).get("name", "").lower()]
    return items


# ==========================================================================
# ADMIN OPERATIONS (Users)
# ==========================================================================

def get_account_users(search_term: str = "") -> List[Dict[str, Any]]:
    """Fetches users from the Account Admin (HQ US endpoint)."""
    hub_id = get_cached_hub_id()
    if not hub_id:
        return []
    account_id = clean_id(hub_id)
    url = f"{BASE_URL_HQ_US}/{account_id}/users"

    all_users = fetch_paginated_data(url, limit=100, style='url')
    if isinstance(all_users, str) or not all_users:
        all_users = fetch_paginated_data(url, limit=100, style='offset')
    if isinstance(all_users, str):
        return []

    if search_term and search_term.lower() != "all":
        term = search_term.lower()
        all_users = [
            u for u in all_users
            if term in u.get("name", "").lower() or term in u.get("email", "").lower()
        ]
    return all_users

def invite_user_to_project(project_id: str, email: str,
                           products: Optional[List[Dict[str, str]]] = None) -> str:
    """Invites a user to a project. Defaults to 'docs' -> 'member' access."""
    try:
        hub_id = get_cached_hub_id()
        account_id = clean_id(hub_id)
        acting_user = get_acting_user_id(account_id)

        extra = {"Content-Type": "application/json"}
        if acting_user:
            extra["x-user-id"] = acting_user

        p_id = clean_id(project_id)

        if not products:
            products = [{"key": "docs", "access": "member"}]

        url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{p_id}/users"
        resp = _make_request("POST", url, extra_headers=extra, json={"email": email, "products": products})

        if resp.status_code in [200, 201]:
            return f"Success: Added {email} to project {p_id} with access: {[p['key'] for p in products]}."
        elif resp.status_code == 409:
            return f"User {email} is already active in project {p_id}."
        elif resp.status_code == 400:
            return f"Bad Request (400): {resp.text}"
        else:
            return f"Error {resp.status_code}: {resp.text}"
    except Exception as e:
        return f"Exception in invite_user_to_project: {str(e)}"

def fetch_project_users(project_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches users specific to a project using ACC Admin API."""
    p_id = clean_id(project_id)
    url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{p_id}/users"
    return fetch_paginated_data(url, limit=100, style='offset', impersonate=True)

def get_account_user_details(email: str) -> dict:
    """Fetches full details for a specific user from HQ API (EMEA)."""
    hub_id = get_cached_hub_id()
    if not hub_id:
        return {"error": "No Hub ID found."}
    account_id = clean_id(hub_id)

    try:
        url = f"https://developer.api.autodesk.com/hq/v1/accounts/{account_id}/users"
        resp = _make_request("GET", url, params={"filter[email]": email})

        if resp.status_code != 200:
            return {"error": f"HQ API returned {resp.status_code}: {resp.text}"}

        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return {"error": "User not found via HQ Search."}
    except Exception as e:
        return {"error": str(e)}


# ==========================================================================
# DATA CONNECTOR API
# ==========================================================================

def _get_admin_headers(account_id: str) -> Dict[str, str]:
    """Returns headers with Admin Impersonation for Data Connector calls."""
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
    if not hub_id:
        return {"error": "No Hub ID found."}
    account_id = clean_id(hub_id)

    headers = _get_admin_headers(account_id)
    if "x-user-id" not in headers:
        return {"error": "Could not resolve Account Admin ID. Data Connector requires Admin Impersonation."}

    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{account_id}/requests"
    payload = {
        "description": f"MCP Agent Export - {time.strftime('%Y-%m-%d')}",
        "schedule": {"interval": "OneTime"}
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
    if not hub_id:
        return {"error": "No Hub ID found."}
    account_id = clean_id(hub_id)
    headers = _get_admin_headers(account_id)

    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{account_id}/requests/{request_id}/jobs"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return {"error": f"Failed to get jobs: {response.text}"}

    jobs = response.json().get("results", [])
    if not jobs:
        return {"status": "QUEUED", "job_id": None}

    latest_job = jobs[0]
    return {
        "status": latest_job.get("completionStatus", "PROCESSING"),
        "job_id": latest_job.get("id"),
        "progress": latest_job.get("progress", 0)
    }

def get_data_download_url(job_id: str) -> Optional[str]:
    """Gets signed URL for the exported ZIP file."""
    hub_id = get_cached_hub_id()
    account_id = clean_id(hub_id)
    headers = _get_admin_headers(account_id)

    filename = "autodesk_data_extract.zip"
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{account_id}/jobs/{job_id}/data/{filename}"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("signedUrl")
    return None


# ==========================================================================
# MODEL DERIVATIVE — VERSION RESOLUTION
# ==========================================================================

def get_latest_version_urn(project_id: str, item_id: str) -> Optional[str]:
    """
    Resolves a File Item ID (Lineage URN) to its latest Version URN.
    Uses Data Management API to get the tip version.
    """
    try:
        if "fs.file" in item_id or "version=" in item_id:
            logger.info(f"Already a version URN: {item_id}")
            return item_id

        project_id_clean = ensure_b_prefix(project_id)
        item_id_encoded = encode_urn(item_id)

        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id_clean}/items/{item_id_encoded}"
        resp = _make_request("GET", url)

        if resp.status_code != 200:
            logger.error(f"Failed to resolve item {item_id}: {resp.status_code} - {resp.text}")
            return None

        tip_urn = (resp.json().get("data", {})
                   .get("relationships", {}).get("tip", {}).get("data", {}).get("id"))

        if tip_urn:
            logger.info(f"Resolved lineage URN to version: {tip_urn[:60]}...")
            return tip_urn
        else:
            logger.warning("No tip version found in item relationships")
            return None
    except Exception as e:
        logger.error(f"Version Resolution Exception: {str(e)}")
        return None


# ==========================================================================
# MODEL DERIVATIVE — MANIFEST & METADATA
# ==========================================================================

def get_model_manifest(version_urn: str) -> Union[Dict[str, Any], str]:
    """Fetches the Model Derivative manifest (translation status and formats)."""
    try:
        encoded_urn = safe_b64encode(version_urn)
        url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/manifest"
        resp = _make_request("GET", url)

        if resp.status_code == 404:
            return "Model not found or not yet translated."
        if resp.status_code != 200:
            logger.error(f"Manifest API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"
        return resp.json()
    except Exception as e:
        logger.error(f"Manifest Exception: {str(e)}")
        return f"Error: {str(e)}"

def get_model_metadata(version_urn: str, guid: Optional[str] = None) -> Union[Dict[str, Any], str]:
    """Fetches model metadata (object tree) for a file version."""
    try:
        encoded_urn = safe_b64encode(version_urn)

        if not guid:
            url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/metadata"
            resp = _make_request("GET", url)
            if resp.status_code == 404:
                return "Model metadata not found. File may not be fully processed."
            if resp.status_code != 200:
                return f"Error {resp.status_code}: {resp.text}"
            data = resp.json().get("data", {}).get("metadata", [])
            if not data:
                return "No 3D views found in this model."
            guid = data[0]["guid"]
            logger.info(f"Using first available view GUID: {guid}")

        tree_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/metadata/{guid}"
        resp_tree = _make_request("GET", tree_url, params={"forceget": "true"})

        if resp_tree.status_code == 202:
            return "Model metadata is still processing. Please try again in a moment."
        if resp_tree.status_code != 200:
            return f"Error {resp_tree.status_code}: {resp_tree.text}"
        return resp_tree.json()
    except Exception as e:
        logger.error(f"Metadata Exception: {str(e)}")
        return f"Error: {str(e)}"


# ==========================================================================
# MODEL DERIVATIVE — FILE RESOLUTION & INSPECTION
# ==========================================================================

def resolve_file_to_urn(project_id: str, identifier: str) -> str:
    """
    Smart file resolver — accepts either a URN or a filename.
    Automatically searches for files by name if a URN is not provided.

    Returns:
        File URN (Lineage or Version).

    Raises:
        ValueError: If filename doesn't match any files.
    """
    try:
        if identifier.startswith("urn:adsk"):
            logger.info(f"  Identifier is already a URN")
            return identifier

        logger.info(f"  Identifier appears to be a filename, searching: {identifier}")

        hub_id = get_cached_hub_id()
        if not hub_id:
            raise ValueError("Could not determine hub_id for file search")

        extensions_to_try = ["rvt", "dwg", "nwc", "rcp", "ifc", "nwd"]

        if "." in identifier:
            file_ext = identifier.split(".")[-1].lower()
            if file_ext in extensions_to_try:
                extensions_to_try.remove(file_ext)
                extensions_to_try.insert(0, file_ext)

        identifier_lower = identifier.lower()

        for ext in extensions_to_try:
            files = find_design_files(hub_id, project_id, ext)
            if isinstance(files, str):
                continue
            for file in files:
                file_name = file.get("name", "").lower()
                if identifier_lower == file_name or identifier_lower in file_name:
                    item_id = file.get("item_id")
                    if item_id:
                        logger.info(f"  Found file: {file.get('name')} (ID: {item_id[:60]}...)")
                        return item_id

        raise ValueError(f"Could not find file matching '{identifier}' in project")
    except ValueError:
        raise
    except Exception as e:
        logger.error(f"File Resolution Exception: {str(e)}")
        raise ValueError(f"Error resolving file identifier: {str(e)}")

def inspect_generic_file(project_id: str, file_id: str) -> str:
    """
    Smart file inspector with automatic name and URN resolution.
    Accepts filename, Lineage URN, or Version URN.
    """
    try:
        logger.info(f"[Smart Inspector] Inspecting file: {file_id[:60]}...")

        try:
            resolved_id = resolve_file_to_urn(project_id, file_id)
        except ValueError as e:
            return str(e)

        # Auto-resolve to version URN
        version_urn = None
        if "lineage" in resolved_id:
            version_urn = get_latest_version_urn(project_id, resolved_id)
            if not version_urn:
                return "Error: Could not resolve Lineage URN to Version URN."
        elif "fs.file" in resolved_id or "version=" in resolved_id:
            version_urn = resolved_id
        else:
            version_urn = get_latest_version_urn(project_id, resolved_id)
            if not version_urn:
                version_urn = resolved_id

        encoded_urn = safe_b64encode(version_urn)
        url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded_urn}/manifest"
        resp = _make_request("GET", url)

        if resp.status_code == 404:
            return "Not Translated. The file exists but hasn't been processed by Model Derivative API yet."
        if resp.status_code == 202:
            return "Processing — Translation in progress. Please check again later."
        if resp.status_code != 200:
            return f"Error {resp.status_code}: Unable to inspect file."

        manifest = resp.json()
        status = manifest.get("status", "unknown").lower()
        progress = manifest.get("progress", "unknown")

        if status == "success":
            return f"Ready for Extraction (Translation complete, progress: {progress})"
        elif status == "inprogress":
            return f"Processing (Translation {progress}% complete)"
        elif status == "failed":
            return "Translation Failed — Check file format or try re-uploading"
        elif status == "timeout":
            return "Translation Timeout — File may be too large or complex"
        else:
            return f"Status: {status} (Progress: {progress})"
    except Exception as e:
        logger.error(f"File Inspection Exception: {str(e)}")
        return f"Error: {str(e)}"


# ==========================================================================
# MODEL DERIVATIVE — STREAMING SCANNER & TRANSLATION
# ==========================================================================

def get_view_guid_only(version_urn: str) -> str:
    """
    Fetches view GUID for a model.
    Auto-retry on 401 is handled by _make_request.

    Raises:
        ValueError: If model not found, no views available, or auth fails.
    """
    logger.info(f"[GUID Fetch] Getting view GUID for: {version_urn[:80]}...")

    urn_b64 = safe_b64encode(version_urn)
    url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn_b64}/metadata"

    resp = _make_request("GET", url, timeout=30)

    if resp.status_code == 401:
        raise ValueError("Auth Failure: 401 Unauthorized even after token refresh. "
                         "Verify 'viewables:read' scope is configured.")
    if resp.status_code == 404:
        raise ValueError("Model not found (404). Translation might be missing.")
    if resp.status_code != 200:
        raise ValueError(f"Metadata API Error {resp.status_code}: {resp.text}")

    views = resp.json().get("data", {}).get("metadata", [])
    if not views:
        raise ValueError("No views found in model metadata.")

    guid = views[0].get("guid")
    view_name = views[0].get("name", "Unknown")
    logger.info(f"  Found view: {view_name} (GUID: {guid})")
    return guid

def stream_count_elements(version_urn: str, category_name: str) -> int:
    """
    Streams metadata and counts elements matching a category using regex.
    Avoids loading full JSON into memory — processes the HTTP stream in 64KB chunks.
    Includes smart singularization (e.g., "Walls" also searches for "Wall").

    Raises:
        ValueError: If model not found or metadata fetch fails.
    """
    logger.info(f"[Streaming Scanner] Counting elements matching: {category_name}")

    # Smart singularization
    terms = {category_name}
    if category_name.lower().endswith("s") and len(category_name) > 2:
        terms.add(category_name[:-1])
    logger.info(f"  Search terms: {list(terms)}")

    # Get view GUID (lightweight — no object tree download)
    guid = get_view_guid_only(version_urn)

    # Construct streaming URL
    urn_b64 = safe_b64encode(version_urn)
    url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn_b64}/metadata/{guid}"

    # Build composite regex (OR logic for all terms, case-insensitive)
    terms_pattern = b'|'.join([re.escape(t).encode() for t in terms])
    pattern = re.compile(rb':\s*"[^"]*(' + terms_pattern + rb')[^"]*"', re.IGNORECASE)

    count = 0
    buffer = b""

    # Stream the response — _make_request handles auth + EMEA + 401 retry
    resp = _make_request("GET", url, stream=True, timeout=120)

    try:
        resp.raise_for_status()

        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                buffer += chunk
                matches = pattern.findall(buffer)
                count += len(matches)
                buffer = buffer[-512:]
    except requests.exceptions.HTTPError as http_err:
        raise ValueError(f"HTTP Error while streaming metadata: {http_err}")
    except requests.exceptions.Timeout:
        raise ValueError("Timeout while streaming metadata.")
    finally:
        resp.close()

    logger.info(f"  Scan complete. Found {count} elements matching '{category_name}'.")
    return count

def trigger_translation(version_urn: str) -> Union[Dict[str, Any], str]:
    """
    Triggers a fresh Model Derivative translation job (SVF format).
    Uses x-ads-force to overwrite existing partial data.
    """
    try:
        logger.info(f"[Translation Trigger] Starting job for: {version_urn[:80]}...")

        encoded_urn = safe_b64encode(version_urn)

        payload = {
            "input": {"urn": encoded_urn},
            "output": {
                "formats": [
                    {"type": "svf", "views": ["2d", "3d"]}
                ]
            }
        }

        url = "https://developer.api.autodesk.com/modelderivative/v2/designdata/job"
        logger.info(f"  Requesting 'svf' (Classic) translation format...")

        resp = _make_request(
            "POST", url,
            extra_headers={
                "x-ads-force": "true",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=30
        )

        if resp.status_code not in [200, 201]:
            error_msg = f"Translation API Error {resp.status_code}: {resp.text}"
            logger.error(error_msg)
            return error_msg

        job_result = resp.json()
        logger.info(f"  Translation job submitted: {job_result.get('result', 'unknown')}")
        return job_result

    except requests.exceptions.Timeout:
        return "Error: Translation job request timed out."
    except requests.exceptions.RequestException as req_err:
        return f"Error: Network error during translation job: {str(req_err)}"
    except Exception as e:
        logger.error(f"Translation Trigger Exception: {str(e)}")
        return f"Error: {str(e)}"


# ==========================================================================
# HUB & PROJECT MANAGEMENT
# ==========================================================================


def get_hubs() -> list:
    """Fetches all accessible Hubs (BIM 360 / ACC) for the authenticated user."""
    url = "https://developer.api.autodesk.com/project/v1/hubs"
    resp = _make_request("GET", url, timeout=30)
    if resp.status_code != 200:
        logger.error(f"get_hubs failed ({resp.status_code}): {resp.text}")
        return []
    return resp.json().get("data", [])


def create_acc_project(hub_id: str, project_name: str, project_type: str = "BIM360") -> dict:
    """
    Creates a new project in the specified Hub.

    Args:
        hub_id: The Hub ID (must start with 'b.').
        project_name: The name of the project.
        project_type: 'BIM360' or 'ACC'.
    """
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
    payload = {
        "jsonapi": {"version": "1.0"},
        "data": {
            "type": "projects",
            "attributes": {
                "name": project_name,
                "projectType": project_type,
            },
        },
    }
    resp = _make_request("POST", url, json=payload, timeout=30)
    return resp.json()

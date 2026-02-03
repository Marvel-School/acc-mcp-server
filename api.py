import os
import requests
import logging
import time
import base64
from functools import lru_cache
from urllib.parse import quote
from typing import Optional, Dict, Any, List
from auth import get_token, BASE_URL_ACC, BASE_URL_HQ_US, BASE_URL_HQ_EU, BASE_URL_GRAPHQL, ACC_ADMIN_EMAIL

logger = logging.getLogger(__name__)

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
    """Wrapper for GraphQL requests."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.post(BASE_URL_GRAPHQL, headers=headers, json={"query": query, "variables": variables or {}})
        if resp.status_code != 200:
            logger.warning(f"GraphQL Error {resp.status_code}: {resp.text}")
            return f"GraphQL Error {resp.status_code}: {resp.text}"
        return resp.json().get("data", {})
    except Exception as e:
        logger.error(f"GraphQL Exception: {str(e)}")
        return f"GraphQL Exception: {str(e)}"

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
                        if not results and "id" in data: 
                             pass
                
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

def fetch_paginated_data(url: str, limit: int = 100, style: str = "url", impersonate: bool = False) -> List[Dict[str, Any]]:
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
            headers = {"Authorization": f"Bearer {token}"}
            
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
                links = data.get("links", {})
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

def get_project_issues(project_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
    p_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/issues/v1/projects/{p_id}/issues"
    # Issues API uses offset/limit
    items = fetch_paginated_data(url, limit=50, style='offset', impersonate=True)
    
    if isinstance(items, str): return items

    if status:
        items = [i for i in items if i.get("status", "").lower() == status.lower()]
        
    return items

def get_project_assets(project_id: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
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

def invite_user_to_project(project_id: str, email: str, products: list = None) -> str:
    """
    Invites a user to a project.
    Always ensures 'products' list exists (defaults to 'docs' -> 'member').
    """
    try:
        # 1. Get Token & Headers
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
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

def fetch_project_users(project_id: str) -> list:
    """Fetches users specific to a project using ACC Admin API."""
    p_id = clean_id(project_id)
    url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{p_id}/users"
    # Admin API requires impersonation
    return fetch_paginated_data(url, limit=100, style='offset', impersonate=True)

# --- DATA CONNECTOR API ---

def _get_admin_headers(account_id: str):
    """Helper to get headers with Admin Impersonation."""
    token = get_token()
    headers = { "Authorization": f"Bearer {token}", "Content-Type": "application/json" }
    admin_id = get_acting_user_id(account_id)
    if admin_id:
        headers["x-user-id"] = admin_id
    return headers

def trigger_data_extraction(services: List[str] = []) -> dict:
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

def get_data_download_url(job_id: str) -> str:
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

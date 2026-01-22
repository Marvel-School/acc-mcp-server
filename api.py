import os
import requests
import logging
import base64
from urllib.parse import quote
from typing import Optional, Dict, Any, List
from auth import get_token, BASE_URL_ACC, BASE_URL_HQ, BASE_URL_GRAPHQL, ACC_ADMIN_EMAIL

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
    """
    token = get_token()
    c_id = clean_id(account_id)
    headers = {"Authorization": f"Bearer {token}"}
    target_email = email.lower().strip()
    
    # Try ACC API (Modern DB)
    # Note: We prioritize ACC API here as it's the standard for new projects
    try:
        url_acc = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users"
        offset = 0
        limit = 100
        
        while True:
            params = {"limit": limit, "offset": offset}
            resp = requests.get(url_acc, headers=headers, params=params)
            
            if resp.status_code != 200:
                logger.warning(f"ACC List Search failed for offset {offset}: {resp.status_code}")
                break

            results = resp.json().get("results", [])
            if not results:
                break
                
            for u in results:
                u_email = u.get("email", "")
                if u_email and u_email.lower().strip() == target_email:
                    logger.info(f"✅ Found user in ACC List: {target_email}")
                    return u.get("id")
            
            if len(results) < limit:
                break # Last page
                
            offset += limit

    except Exception as e:
        logger.error(f"ACC List Search Exception: {e}")

    return None

def get_acting_user_id(account_id: str, requester_email: Optional[str] = None) -> Optional[str]:
    # 1. Try Requesting User
    if requester_email:
        uid = get_user_id_by_email(account_id, requester_email)
        if uid: return uid
    # 2. Try Fallback Service Account
    if ACC_ADMIN_EMAIL:
        logger.info(f"Using fallback admin email: {ACC_ADMIN_EMAIL}")
        uid = get_user_id_by_email(account_id, ACC_ADMIN_EMAIL)
        if uid: return uid
    return None

def resolve_to_version_id(project_id: str, item_id: str) -> str:
    """Helper to resolve a File Item ID (urn:adsk.wipp...) to its latest Version ID."""
    try:
        # If it looks like a file extension ID but not a URN, search for it
        if not item_id.startswith("urn:") and not item_id.startswith("b.") and "." in item_id:
            # Simple name search heuristic
            pass 

        if "fs.file" in item_id or "version=" in item_id:
            return item_id
            
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(item_id)}"
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r.json()["data"]["relationships"]["tip"]["data"]["id"]
    except Exception: pass
    return item_id

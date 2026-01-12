import os
import time
import requests
import traceback
from urllib.parse import quote
from fastmcp import FastMCP
from typing import Optional, List, Dict, Any

# --- CONFIGURATION ---
APS_CLIENT_ID = os.environ.get("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET")
# OPTIONAL: A "Service Account" email stored in Azure settings (not code).
# Used only if the requesting user is not found or has no permissions.
ACC_ADMIN_EMAIL = os.environ.get("ACC_ADMIN_EMAIL") 
PORT = int(os.environ.get("PORT", 8000))

# Initialize FastMCP
mcp = FastMCP("Autodesk ACC Agent")

# Global token cache
token_cache = {"access_token": None, "expires_at": 0}

# Base URLs
BASE_URL_DM = "https://developer.api.autodesk.com"
BASE_URL_ACC = "https://developer.api.autodesk.com/construction"
BASE_URL_GRAPHQL = "https://developer.api.autodesk.com/aec/graphql"

# --- HELPER: AUTHENTICATION ---
def get_token():
    global token_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Error: APS credentials missing.")

    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]

    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = requests.auth.HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    data = {"grant_type": "client_credentials", "scope": "data:read data:write account:read account:write bucket:read"}

    try:
        resp = requests.post(url, auth=auth, data=data)
        resp.raise_for_status()
        result = resp.json()
        token_cache["access_token"] = result["access_token"]
        token_cache["expires_at"] = time.time() + result["expires_in"] - 60
        return result["access_token"]
    except Exception as e:
        print(f"Auth Error: {e}")
        raise e

# --- HELPER: UTILS ---
def clean_id(id_str: str) -> str:
    return id_str.replace("b.", "") if id_str else ""

def ensure_b_prefix(id_str: str) -> str:
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: str) -> str:
    return quote(urn, safe='') if urn else ""

def make_api_request(url: str):
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        if resp.status_code >= 400: return f"Error {resp.status_code}: {resp.text}"
        return resp.json()
    except Exception as e: return f"Error: {str(e)}"

def make_graphql_request(query: str, variables: Dict[str, Any] = None):
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.post(BASE_URL_GRAPHQL, headers=headers, json={"query": query, "variables": variables or {}})
        if resp.status_code != 200: return f"GraphQL Error {resp.status_code}: {resp.text}"
        return resp.json().get("data", {})
    except Exception as e: return f"GraphQL Exception: {str(e)}"

# --- HELPER: SMART USER LOOKUP ---
def get_user_id_by_email(account_id: str, email: str) -> Optional[str]:
    """Search for a specific user by email using the API."""
    try:
        token = get_token()
        c_id = clean_id(account_id)
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users"
        # Search for the user specifically
        params = {"filter[email]": email, "limit": 1}
        
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                user = results[0]
                if user.get("status") == "active":
                    return user.get("id") or user.get("autodeskId")
    except Exception as e:
        print(f"Search Error for {email}: {e}")
    return None

def get_acting_user_id(account_id: str, requester_email: Optional[str] = None) -> Optional[str]:
    """
    Determines who owns the action.
    1. The user chatting (requester_email).
    2. The Fallback Admin (ACC_ADMIN_EMAIL env var).
    3. Any active admin (Last resort).
    """
    # 1. Try Requesting User
    if requester_email:
        uid = get_user_id_by_email(account_id, requester_email)
        if uid: return uid
        print(f"User {requester_email} not found in ACC.")

    # 2. Try Fallback Service Account
    if ACC_ADMIN_EMAIL:
        uid = get_user_id_by_email(account_id, ACC_ADMIN_EMAIL)
        if uid: return uid

    # 3. Last Resort: Any Admin
    try:
        token = get_token()
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{clean_id(account_id)}/users"
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"limit": 20})
        if resp.status_code == 200:
            for u in resp.json().get("results", []):
                if u.get("status") == "active": return u.get("id")
    except: pass
    
    return None

# ==========================================
# TOOLS
# ==========================================

@mcp.tool()
def create_project(
    project_name: str, 
    requester_email: Optional[str] = None,
    account_id: Optional[str] = None, 
    project_type: str = "Renovation", 
    currency: str = "EUR", 
    language: str = "en",
    timezone: str = "Europe/Amsterdam"
) -> str:
    """
    Creates a new project attributed to the user asking.
    Args:
        requester_email: The email of the logged-in user (from Copilot context).
    """
    # 1. Account ID
    if not account_id:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str) or not hubs.get("data"): return "Error: No Account/Hub found."
        account_id = clean_id(hubs["data"][0]["id"])
    
    # 2. Smart User Lookup
    c_id = clean_id(account_id)
    acting_user_id = get_acting_user_id(c_id, requester_email)
    
    if not acting_user_id:
        return "❌ Error: I cannot find a valid user in ACC to create this project (checked requester and fallback admin)."

    payload = {
        "name": project_name, "type": "production", "currency": currency,
        "timezone": timezone, "language": language, "projectType": project_type 
    }
    
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-user-id": acting_user_id}
        resp = requests.post(f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects", headers=headers, json=payload)
        
        if resp.status_code in [200, 201]:
            used_email = requester_email if requester_email else "Service Account"
            return f"✅ Success! Project '{project_name}' created by **{used_email}** (ID: {resp.json().get('id')})."
        else:
            return f"❌ Error: {resp.status_code} {resp.text}"
    except Exception as e: return f"Error: {str(e)}"

# --- READ TOOLS ---

@mcp.tool()
def list_designs(project_id: str) -> str:
    """Lists 'Element Groups' (Designs)."""
    query = """query GetElementGroupsByProject($projectId: ID!) { elementGroupsByProject(projectId: $projectId) { results { id name alternativeIdentifiers { fileVersionUrn } } } }"""
    p_id = ensure_b_prefix(project_id)
    data = make_graphql_request(query, {"projectId": p_id})
    if isinstance(data, str) and "Error" in data:
        data = make_graphql_request(query, {"projectId": clean_id(project_id)})
    if isinstance(data, str): return data
    groups = data.get("elementGroupsByProject", {}).get("results", [])
    if not groups: return "No designs found."
    output = "🏗️ **Designs Found:**\n"
    for g in groups: output += f"- **{g.get('name')}**\n  ID: `{g.get('id')}`\n"
    return output

@mcp.tool()
def query_model_elements(design_id: str, category: str, limit: int = 20) -> str:
    """Queries elements (e.g. Walls)."""
    query = """query GetElementsByCategory($elementGroupId: ID!, $filter: String!) { elementsByElementGroup(elementGroupId: $elementGroupId, filter: {query: $filter}) { results { id name properties { results { name value } } } } }"""
    data = make_graphql_request(query, {"elementGroupId": design_id, "filter": f"property.name.category=='{category}'"})
    if isinstance(data, str): return data
    elements = data.get("elementsByElementGroup", {}).get("results", [])
    if not elements: return f"No {category} found."
    output = f"🔍 **Found {len(elements)} {category}**:\n"
    for el in elements[:limit]:
        props = ", ".join([f"{p['name']}: {p['value']}" for p in el.get("properties", {}).get("results", []) if p['value']])
        output += f"- **{el.get('name')}** (ID: {el.get('id')})\n  {props[:100]}...\n"
    return output

@mcp.tool()
def get_model_viewer_link(project_id: str, urn: str) -> str:
    return f"https://acc.autodesk.com/docs/files/projects/{clean_id(project_id)}?entityId={urn}"

@mcp.tool()
def list_hubs() -> str:
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, str): return data
    output = "Found Hubs:\n"
    for h in data.get("data", []): output += f"- {h['attributes']['name']} (ID: {h['id']})\n"
    return output

@mcp.tool()
def list_projects_dm(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    if not hub_id:
        h = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(h, str) or not h.get("data"): return "No Hubs."
        hub_id = h["data"][0]["id"]
    data = make_api_request(f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects")
    if isinstance(data, str): return data
    projs = [p for p in data.get("data", []) if not name_filter or name_filter.lower() in p['attributes']['name'].lower()]
    output = f"Found {len(projs)} projects:\n"
    for p in projs[:limit]: output += f"- {p['attributes']['name']} (ID: {p['id']})\n"
    return output

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    h = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    hub_id = h["data"][0]["id"]
    data = make_api_request(f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{ensure_b_prefix(project_id)}/topFolders")
    if isinstance(data, str): return data
    output = "Root folders:\n"
    for i in data.get("data", []): output += f"- {i['attributes']['displayName']} (ID: {i['id']})\n"
    return output

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
    data = make_api_request(f"https://developer.api.autodesk.com/data/v1/projects/{ensure_b_prefix(project_id)}/folders/{folder_id}/contents")
    if isinstance(data, str): return data
    output = "Contents:\n"
    for i in data.get("data", [])[:limit]: output += f"- {i['attributes']['displayName']} (ID: {i['id']})\n"
    return output

@mcp.tool()
def get_download_url(project_id: str, id: str) -> str:
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        target = id
        if "lineage" in id or "fs.file" in id:
            r = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(id)}", headers=headers)
            if r.status_code == 200: target = r.json()["data"]["relationships"]["tip"]["data"]["id"]
        r = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encode_urn(target)}", headers=headers)
        if r.status_code != 200: return f"Error: {r.text}"
        parts = r.json()["data"]["relationships"]["storage"]["data"]["id"].split("/")
        oss = f"https://developer.api.autodesk.com/oss/v2/buckets/{parts[-2].split(':')[-1]}/objects/{parts[-1]}/signeds3download"
        r = requests.get(oss, headers=headers, params={"minutesExpiration": 60})
        return f"⬇️ **[Click to Download]({r.json()['url']})**" if r.status_code == 200 else "Error."
    except Exception as e: return str(e)

@mcp.tool()
def add_user_to_project(project_id: str, email: str, role: str = "project_member") -> str:
    c_id = clean_id(project_id)
    try:
        token = get_token()
        resp = requests.post(f"{BASE_URL_ACC}/admin/v1/projects/{c_id}/users", headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=[{"email": email, "products": [{"key": "docs", "access": role}]}])
        return f"✅ Invited {email}." if resp.status_code in [200, 201] else f"❌ Error: {resp.text}"
    except Exception as e: return str(e)

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
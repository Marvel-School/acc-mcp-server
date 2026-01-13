import os
import time
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote
from fastmcp import FastMCP
from typing import Optional, List, Dict, Any

# --- CONFIGURATION ---
APS_CLIENT_ID = os.environ.get("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET")
# Fallback Admin Email (Optional, helps with "Admin" read permissions)
ACC_ADMIN_EMAIL = os.environ.get("ACC_ADMIN_EMAIL") 
PORT = int(os.environ.get("PORT", 8000))

# Initialize FastMCP (Read-Only Mode)
mcp = FastMCP("Autodesk ACC Read-Only Agent")

# Global token cache
token_cache = {"access_token": None, "expires_at": 0}

BASE_URL_ACC = "https://developer.api.autodesk.com/construction"
BASE_URL_HQ = "https://developer.api.autodesk.com/hq/v1"
BASE_URL_GRAPHQL = "https://developer.api.autodesk.com/aec/graphql"

# --- HELPER: AUTHENTICATION ---
def get_token():
    global token_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Error: APS credentials missing.")

    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]

    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    data = {"grant_type": "client_credentials", "scope": "data:read account:read bucket:read"}

    resp = requests.post(url, auth=auth, data=data)
    resp.raise_for_status()
    token_cache["access_token"] = resp.json()["access_token"]
    token_cache["expires_at"] = time.time() + resp.json()["expires_in"] - 60
    return token_cache["access_token"]

# --- HELPER: UTILS ---
def clean_id(id_str: str) -> str:
    """Removes 'b.' prefix for APIs that don't want it."""
    return id_str.replace("b.", "") if id_str else ""

def ensure_b_prefix(id_str: str) -> str:
    """Adds 'b.' prefix for Data Management APIs."""
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: str) -> str:
    """Safely encodes IDs with special characters (like :) for URLs."""
    return quote(urn, safe='') if urn else ""

def make_api_request(url: str):
    """Generic GET request with error handling."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        if resp.status_code >= 400: return f"Error {resp.status_code}: {resp.text}"
        return resp.json()
    except Exception as e: return f"Error: {str(e)}"

def make_graphql_request(query: str, variables: Optional[Dict[str, Any]] = None):
    """Handles 3D Model queries."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.post(BASE_URL_GRAPHQL, headers=headers, json={"query": query, "variables": variables or {}})
        if resp.status_code != 200: return f"GraphQL Error {resp.status_code}: {resp.text}"
        return resp.json().get("data", {})
    except Exception as e: return f"GraphQL Exception: {str(e)}"

# ==========================================
# DISCOVERY TOOLS (Finding Projects)
# ==========================================

@mcp.tool()
def list_hubs() -> str:
    """Step 1: Lists all Hubs/Accounts accessible to the bot."""
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, str): return data
    
    output = "🏢 **Found Hubs:**\n"
    for h in data.get("data", []): 
        output += f"- {h['attributes']['name']} (ID: {h['id']})\n"
    return output

@mcp.tool()
def list_projects(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 20) -> str:
    """Step 2: Lists projects in a Hub. Optional: Filter by name."""
    # Auto-detect Hub if missing
    if not hub_id:
        h = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(h, str) or not h.get("data"): return "Error: No Hubs found."
        hub_id = h["data"][0]["id"]
        
    data = make_api_request(f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects")
    if isinstance(data, str): return data
    
    # Filter and Limit
    all_projs = data.get("data", [])
    if name_filter:
        all_projs = [p for p in all_projs if name_filter.lower() in p['attributes']['name'].lower()]
    
    output = f"📂 **Found {len(all_projs)} Projects:**\n"
    for p in all_projs[:limit]: 
        output += f"- **{p['attributes']['name']}**\n  ID: `{p['id']}`\n"
    return output

# ==========================================
# FILE & FOLDER TOOLS (Data Management)
# ==========================================

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    """Lists the root folders (Project Files / Plans)."""
    # We need the hub ID to build the URL, so we fetch it first
    h = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(h, str) or not h.get("data"): return "Error: No Hubs."
    hub_id = h["data"][0]["id"]
    
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{ensure_b_prefix(project_id)}/topFolders"
    data = make_api_request(url)
    if isinstance(data, str): return data
    
    output = "root_folders:\n"
    for i in data.get("data", []): 
        output += f"- {i['attributes']['displayName']} (ID: {i['id']})\n"
    return output

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
    """Lists files/folders inside a specific folder."""
    # Critical: Encode the folder ID to prevent crashes
    safe_folder = encode_urn(folder_id)
    safe_proj = ensure_b_prefix(project_id)
    
    url = f"https://developer.api.autodesk.com/data/v1/projects/{safe_proj}/folders/{safe_folder}/contents"
    data = make_api_request(url)
    if isinstance(data, str): return data
    
    items = data.get("data", [])
    if not items: return "📂 Folder is empty."
    
    output = f"**Contents ({len(items)} items):**\n"
    for i in items[:limit]:
        name = i.get("attributes", {}).get("displayName", "Unnamed")
        icon = "📁" if i["type"] == "folders" else "📄"
        output += f"{icon} {name} (ID: `{i['id']}`)\n"
    return output

@mcp.tool()
def get_download_url(project_id: str, file_id: str) -> str:
    """Generates a temporary download link for any file."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        
        # 1. If it's a Lineage ID (File), get the Tip Version
        target_version_id = file_id
        if "lineage" in file_id or "fs.file" in file_id:
            r = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(file_id)}", headers=headers)
            if r.status_code == 200: 
                target_version_id = r.json()["data"]["relationships"]["tip"]["data"]["id"]
        
        # 2. Get the Storage Location (OSS URN)
        r = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encode_urn(target_version_id)}", headers=headers)
        if r.status_code != 200: return f"Error finding file version: {r.text}"
        
        storage_urn = r.json()["data"]["relationships"]["storage"]["data"]["id"]
        # Parse bucket and object key from: urn:adsk.objects:os.object:wip.dm.prod/123...
        parts = storage_urn.split("/")
        bucket_key = parts[-2].split(":")[-1]
        object_key = parts[-1]
        
        # 3. Request Signed S3 URL
        oss_url = f"https://developer.api.autodesk.com/oss/v2/buckets/{bucket_key}/objects/{object_key}/signeds3download"
        r = requests.get(oss_url, headers=headers, params={"minutesExpiration": 60})
        
        if r.status_code == 200:
            return f"⬇️ **[Click to Download File]({r.json()['url']})**"
        return f"Error getting download link: {r.text}"
    except Exception as e: return str(e)

# ==========================================
# 3D MODEL TOOLS (AEC Data Model)
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
    """Lists 3D Revit/IFC designs in a project."""
    p_id = ensure_b_prefix(project_id)
    query = """query GetElementGroups($projectId: ID!) { elementGroupsByProject(projectId: $projectId) { results { id name } } }"""
    
    # Try with 'b.' prefix
    data = make_graphql_request(query, {"projectId": p_id})
    
    # Retry without 'b.' if needed
    if not data or isinstance(data, str) or not data.get("elementGroupsByProject"):
        data = make_graphql_request(query, {"projectId": clean_id(project_id)})

    if isinstance(data, str): return data
    
    groups = data.get("elementGroupsByProject", {}).get("results", [])
    if not groups: return "No 3D designs found."
    
    output = "🏗️ **Designs:**\n"
    for g in groups: output += f"- **{g['name']}** (ID: `{g['id']}`)\n"
    return output

@mcp.tool()
def get_model_viewer_link(project_id: str, design_id: str) -> str:
    """Returns a direct link to view the model in ACC."""
    # Note: entityId usually refers to the file URN, not the GraphQL ID, 
    # but for simple viewing, linking to the project is often the safest start.
    return f"https://acc.autodesk.com/docs/files/projects/{clean_id(project_id)}"

# ==========================================
# BUILD DATA TOOLS (Issues, Assets, Forms)
# ==========================================

@mcp.tool()
def list_issues(project_id: str, limit: int = 10) -> str:
    """Lists construction issues (Open, Closed, etc)."""
    url = f"https://developer.api.autodesk.com/issues/v1/projects/{clean_id(project_id)}/issues"
    data = make_api_request(url)
    if isinstance(data, str): return data
    
    results = data.get("results", [])
    if not results: return "No issues found."
    
    output = "🚧 **Project Issues:**\n"
    for i in results[:limit]:
        title = i.get("title", "No Title")
        status = i.get("status", "unknown")
        output += f"- #{i['identifier']} **{title}** ({status})\n"
    return output

@mcp.tool()
def list_assets(project_id: str, limit: int = 10) -> str:
    """Lists assets (equipment, materials) in the project."""
    url = f"https://developer.api.autodesk.com/construction/assets/v2/projects/{clean_id(project_id)}/assets"
    data = make_api_request(url)
    if isinstance(data, str): return data
    
    results = data.get("results", [])
    if not results: return "No assets found."
    
    output = "📦 **Project Assets:**\n"
    for a in results[:limit]:
        name = a.get("clientAssetId", "Unnamed Asset")
        status = a.get("status", {}).get("name", "-")
        output += f"- **{name}** (Status: {status})\n"
    return output

@mcp.tool()
def get_data_connector_status(account_id: Optional[str] = None) -> str:
    """Checks the status of Data Connector exports."""
    if not account_id:
        h = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(h, str) or not h.get("data"): return "No Hub found."
        account_id = h["data"][0]["id"]
        
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{clean_id(account_id)}/requests"
    data = make_api_request(url)
    if isinstance(data, str): return data
    
    results = data.get("data", [])[:5]
    output = "📊 **Recent Data Exports:**\n"
    for r in results:
        output += f"- {r.get('createdAt')}: **{r.get('status')}**\n"
    return output

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
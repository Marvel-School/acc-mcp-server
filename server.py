import os
import time
import requests
import base64
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

# Global Cache (Token + Hub ID)
# We store the Hub ID so we don't have to ask for it every single command.
global_cache = {
    "access_token": None, 
    "expires_at": 0,
    "hub_id": None
}

BASE_URL_GRAPHQL = "https://developer.api.autodesk.com/aec/graphql"

# --- HELPER: AUTHENTICATION & CACHING ---
def get_token():
    global global_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Error: APS credentials missing.")

    if time.time() < global_cache["expires_at"]:
        return global_cache["access_token"]

    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    data = {"grant_type": "client_credentials", "scope": "data:read account:read bucket:read"}

    resp = requests.post(url, auth=auth, data=data)
    resp.raise_for_status()
    global_cache["access_token"] = resp.json()["access_token"]
    global_cache["expires_at"] = time.time() + resp.json()["expires_in"] - 60
    return global_cache["access_token"]

def get_cached_hub_id():
    """Fetches Hub ID once and remembers it forever."""
    global global_cache
    if global_cache["hub_id"]:
        return global_cache["hub_id"]
        
    # If not cached, fetch it
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, dict) and data.get("data"):
        hub_id = data["data"][0]["id"]
        global_cache["hub_id"] = hub_id
        return hub_id
    return None

# --- HELPER: UTILS (Fixed Type Hints) ---
def clean_id(id_str: Optional[str]) -> str:
    """Removes 'b.' prefix. Safely handles None."""
    return id_str.replace("b.", "") if id_str else ""

def ensure_b_prefix(id_str: Optional[str]) -> str:
    """Adds 'b.' prefix. Safely handles None."""
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: Optional[str]) -> str:
    """Safely encodes IDs for URLs."""
    return quote(urn, safe='') if urn else ""

def safe_b64encode(value: Optional[str]) -> str:
    """Helper to create URNs for the Model Derivative API."""
    if not value: return ""
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")
    return encoded.rstrip("=")

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
    """Handles 3D Model queries with Null Safety."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.post(BASE_URL_GRAPHQL, headers=headers, json={"query": query, "variables": variables or {}})
        
        if resp.status_code != 200: 
            return f"GraphQL Error {resp.status_code}: {resp.text}"
        # FIX: Explicitly handle if 'data' is null
        return resp.json().get("data") or {} 
    except Exception as e: return f"GraphQL Exception: {str(e)}"

def get_viewer_domain(urn: str) -> str:
    """Detects if project is EU or US based on the URN."""
    if "wipemea" in urn or "emea" in urn:
        return "acc.autodesk.eu"
    return "acc.autodesk.com"

def resolve_to_version_id(project_id: str, item_id: str) -> str:
    """
    CRITICAL FIX: Converts a 'Lineage ID' (History) to a 'Version ID' (Specific File).
    The Viewer and Model Derivative APIs fail if you give them a History ID.
    """
    # If it already looks like a version (has ?version= or starts with fs.file), return it.
    if "fs.file" in item_id or "version=" in item_id:
        return item_id
        
    # If it's a Lineage (dm.lineage), we must fetch the latest version.
    try:
        print(f"🔄 Resolving Lineage ID to Latest Version: {item_id}")
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        
        url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(item_id)}"
        r = requests.get(url, headers=headers)
        
        if r.status_code == 200:
            # The 'tip' relationship points to the latest version
            version_id = r.json()["data"]["relationships"]["tip"]["data"]["id"]
            print(f"✅ Resolved to: {version_id}")
            return version_id
    except Exception as e:
        print(f"⚠️ Failed to resolve version: {e}")
    
    # Fallback: Return original if resolution failed
    return item_id

# ==========================================
# DISCOVERY TOOLS (Optimized)
# ==========================================

@mcp.tool()
def list_hubs() -> str:
    """Step 1: Lists all Hubs/Accounts accessible to the bot."""
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, str): return data
    output = "🏢 **Found Hubs:**\n"
    for h in data.get("data", []): output += f"- {h['attributes']['name']} (ID: {h['id']})\n"
    return output

@mcp.tool()
def list_projects(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 20) -> str:
    """Step 2: Lists projects in a Hub. Optional: Filter by name."""
    if not hub_id:
        # Optimization: Use Cached Hub ID
        hub_id = get_cached_hub_id()
        if not hub_id: return "Error: No Hubs found."
        
    data = make_api_request(f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects")
    if isinstance(data, str): return data
    
    all_projs = data.get("data", [])
    if name_filter:
        all_projs = [p for p in all_projs if name_filter.lower() in p['attributes']['name'].lower()]
    
    output = f"📂 **Found {len(all_projs)} Projects:**\n"
    for p in all_projs[:limit]: 
        output += f"- **{p['attributes']['name']}**\n  ID: `{p['id']}`\n"
    return output

# ==========================================
# FILE & FOLDER TOOLS
# ==========================================

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    """Lists the root folders (Project Files / Plans)."""
    hub_id = get_cached_hub_id()
    if not hub_id: return "Error: No Hubs."
    
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
        
        # Resolve to Version ID first
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

# ==========================================
# 3D MODEL TOOLS (Robust & Universal)
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
    """Lists 3D designs (Revit/IFC) and their View IDs (URNs)."""
    p_id = ensure_b_prefix(project_id)
    query = """query GetElementGroups($projectId: ID!) { 
        elementGroupsByProject(projectId: $projectId) { 
            results { id name alternativeIdentifiers { fileVersionUrn } } 
        } 
    }"""
    
    data = make_graphql_request(query, {"projectId": p_id})
    
    # Retry Logic: Split into two checks for Pylance safety
    should_retry = False
    if not data or isinstance(data, str):
        should_retry = True
    elif isinstance(data, dict) and not data.get("elementGroupsByProject"):
        should_retry = True

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
    """Returns a direct link to view ANY file (.rvt, .rcp, .pdf, .dwg)."""
    # 1. Resolve Lineage ID -> Version ID (Fixes broken links)
    version_id = resolve_to_version_id(project_id, item_id)
    
    # 2. Check region based on URN
    domain = get_viewer_domain(version_id)
    return f"https://{domain}/docs/files/projects/{clean_id(project_id)}?entityId={quote(version_id, safe='')}"

@mcp.tool()
def find_models(project_id: str, file_types: str = "rvt,rcp,dwg,nwc") -> str:
    """Searches the entire project for model files and returns View Links."""
    hub_id = get_cached_hub_id()
    if not hub_id: return "Error: No Hubs."
    p_id = ensure_b_prefix(project_id)
    
    top_data = make_api_request(f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{p_id}/topFolders")
    if isinstance(top_data, str): return top_data
    
    proj_files_folder = next((f["id"] for f in top_data.get("data", []) if f["attributes"]["name"] == "Project Files"), None)
    if not proj_files_folder: return "Error: Could not find 'Project Files' folder."

    extensions = [ext.strip().lower() for ext in file_types.split(",")]
    search_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/folders/{proj_files_folder}/search?filter[extension.type]={','.join(extensions)}"
    
    search_results = make_api_request(search_url)
    if isinstance(search_results, str): return search_results
    
    items = search_results.get("data", [])
    if not items: return "❌ No models found matching those extensions."

    output = f"🔍 **Found {len(items)} Models:**\n"
    for i in items:
        name = i["attributes"]["displayName"]
        item_id = i['id']
        domain = get_viewer_domain(item_id)
        viewer_link = f"https://{domain}/docs/files/projects/{clean_id(project_id)}?entityId={quote(item_id, safe='')}"
        output += f"- **{name}**\n  [Open in Viewer]({viewer_link}) (ID: `{item_id}`)\n"
    return output

# ==========================================
# UNIVERSAL METADATA (Robust & WIP Compatible)
# ==========================================

@mcp.tool()
def get_model_tree(project_id: str, file_id: str) -> str:
    """Reads the hierarchy of ANY 3D file (WIP or Shared)."""
    # 1. Resolve Lineage ID -> Version ID (Critical Step!)
    version_id = resolve_to_version_id(project_id, file_id)
    
    # Safety Check: Did resolution work?
    if not version_id or version_id == file_id:
        if not version_id.startswith("urn:"):
             return f"❌ Error: I need the File ID (urn:...), but I got '{file_id}'. Please ask 'Find files' first to get the correct ID."

    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    urn = safe_b64encode(version_id)
    
    resp = requests.get(f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn}/metadata", headers=headers)
    
    if resp.status_code == 404:
        return "❌ File found, but it has no metadata. (Has it been viewed in the web viewer yet?)"
    if resp.status_code != 200: 
        return f"❌ Autodesk API Error ({resp.status_code}): {resp.text}"
        
    data = resp.json().get("data", {}).get("metadata", [])
    if not data: return "❌ No 3D views found."
    guid = data[0]["guid"]
    
    # 3. Get the Actual Tree
    tree_url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn}/metadata/{guid}"
    resp_tree = requests.get(tree_url, headers=headers, params={"forceget": "true"})
    
    if resp_tree.status_code == 202:
        return "⏳ The Model Tree is currently processing. Please try again in 1 minute."
    if resp_tree.status_code != 200: 
        return f"❌ Failed to retrieve object tree (Status: {resp_tree.status_code})."
    
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

# ==========================================
# BUILD DATA TOOLS
# ==========================================

@mcp.tool()
def list_issues(project_id: str, limit: int = 10) -> str:
    data = make_api_request(f"https://developer.api.autodesk.com/issues/v1/projects/{clean_id(project_id)}/issues")
    if isinstance(data, str): return data
    output = "🚧 **Project Issues:**\n"
    for i in data.get("results", [])[:limit]: output += f"- #{i['identifier']} **{i.get('title')}** ({i.get('status')})\n"
    return output

@mcp.tool()
def list_assets(project_id: str, limit: int = 10) -> str:
    data = make_api_request(f"https://developer.api.autodesk.com/construction/assets/v2/projects/{clean_id(project_id)}/assets")
    if isinstance(data, str): return data
    output = "📦 **Project Assets:**\n"
    for a in data.get("results", [])[:limit]: output += f"- **{a.get('clientAssetId')}** (Status: {a.get('status', {}).get('name')})\n"
    return output

@mcp.tool()
def get_data_connector_status(account_id: Optional[str] = None) -> str:
    # Use Cache
    if not account_id:
        account_id = get_cached_hub_id()
        if not account_id: return "No Hub found."
    data = make_api_request(f"https://developer.api.autodesk.com/data-connector/v1/accounts/{clean_id(account_id)}/requests")
    if isinstance(data, str): return data
    output = "📊 **Recent Data Exports:**\n"
    for r in data.get("data", [])[:5]: output += f"- {r.get('createdAt')}: **{r.get('status')}**\n"
    return output

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
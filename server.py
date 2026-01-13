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
# Fallback service account (Optional, set in Azure)
ACC_ADMIN_EMAIL = os.environ.get("ACC_ADMIN_EMAIL") 
PORT = int(os.environ.get("PORT", 8000))

# Initialize FastMCP
mcp = FastMCP("Autodesk ACC Agent")

# Global token cache
token_cache = {"access_token": None, "expires_at": 0}

BASE_URL_ACC = "https://developer.api.autodesk.com/construction"
BASE_URL_HQ = "https://developer.api.autodesk.com/hq/v1" # Legacy HQ API for Deep Search
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

    resp = requests.post(url, auth=auth, data=data)
    resp.raise_for_status()
    token_cache["access_token"] = resp.json()["access_token"]
    token_cache["expires_at"] = time.time() + resp.json()["expires_in"] - 60
    return token_cache["access_token"]

# --- HELPER: UTILS ---
def clean_id(id_str: str) -> str:
    return id_str.replace("b.", "") if id_str else ""

def ensure_b_prefix(id_str: str) -> str:
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: str) -> str:
    """Crucial for Folder IDs which contain special characters like ':'"""
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

# --- HELPER: ROBUST USER SEARCH (Client-Side Filtering) ---
def get_user_id_by_email(account_id: str, email: str) -> Optional[str]:
    """Finds a user ID by pulling the user list and searching in Python."""
    token = get_token()
    c_id = clean_id(account_id)
    headers = {"Authorization": f"Bearer {token}"}
    target_email = email.lower().strip()
    
    # 1. Try HQ API (Legacy/Master DB)
    try:
        url_hq = f"{BASE_URL_HQ}/accounts/{c_id}/users"
        resp_hq = requests.get(url_hq, headers=headers, params={"limit": 100})
        if resp_hq.status_code == 200:
            data = resp_hq.json()
            user_list = data if isinstance(data, list) else data.get("results", [])
            for u in user_list:
                if u.get("email", "").lower().strip() == target_email:
                    print(f"✅ Found user in HQ List: {target_email}")
                    return u.get("uid") or u.get("id")
    except Exception as e: print(f"HQ List Search failed: {e}")

    # 2. Try ACC API (Modern DB)
    try:
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users"
        resp = requests.get(url, headers=headers, params={"limit": 100})
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            for u in results:
                if u.get("email", "").lower().strip() == target_email:
                    print(f"✅ Found user in ACC List: {target_email}")
                    return u.get("id")
    except Exception as e: print(f"ACC List Search failed: {e}")
    return None

def get_acting_user_id(account_id: str, requester_email: Optional[str] = None) -> Optional[str]:
    if requester_email:
        uid = get_user_id_by_email(account_id, requester_email)
        if uid: return uid
    if ACC_ADMIN_EMAIL:
        uid = get_user_id_by_email(account_id, ACC_ADMIN_EMAIL)
        if uid: return uid
    return None

# ==========================================
# TOOL: DEBUG PERMISSIONS
# ==========================================
@mcp.tool()
def debug_permissions(requester_email: str) -> str:
    """Run this to check why Project Creation is failing."""
    output = [f"🔍 **Diagnostics for {requester_email}**"]
    try:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str): return f"❌ Hub List Failed: {hubs}"
        
        hub_data = hubs.get("data", [])
        if not hub_data: return "❌ No Hubs found."

        hub = hub_data[0]
        acc_id = clean_id(hub["id"])
        output.append(f"✅ Found Account: **{hub['attributes']['name']}** (ID: `{acc_id}`)")
    except Exception as e: return f"❌ Critical Error reading hubs: {str(e)}"

    user_id = get_user_id_by_email(acc_id, requester_email)
    if user_id:
        output.append(f"✅ User Found: ID `{user_id}`")
    else:
        output.append(f"❌ User '{requester_email}' NOT FOUND via API.")
        return "\n".join(output)

    try:
        token = get_token()
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{acc_id}/projects"
        headers = {"Authorization": f"Bearer {token}", "x-user-id": user_id}
        resp = requests.get(url, headers=headers, params={"limit": 1})
        if resp.status_code == 200: output.append("✅ **Authorization Success:** Admin API accessible.")
        elif resp.status_code == 403: output.append(f"❌ **Permission Denied (403):** {resp.text}")
        else: output.append(f"❌ API Error {resp.status_code}: {resp.text}")
    except Exception as e: output.append(f"❌ Exception: {str(e)}")

    return "\n".join(output)

# ==========================================
# TOOL: CREATE PROJECT
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
    """Creates a new project. Safe & Secure."""
    if not account_id:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str) or not hubs.get("data"): return "Error: No Account/Hub found."
        account_id = clean_id(hubs["data"][0]["id"])
    
    c_id = clean_id(account_id)
    acting_user_id = get_acting_user_id(c_id, requester_email)
    
    if not acting_user_id:
        return (f"RAW_ERROR: Authorization Failed. Could not find user '{requester_email}' in account {c_id}.")

    payload = {"name": project_name, "type": "production"}
    
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-user-id": acting_user_id}
        resp = requests.post(f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects", headers=headers, json=payload)
        
        if resp.status_code in [200, 201]:
            return f"✅ Success! Project '{project_name}' created (ID: {resp.json().get('id')})."
        else:
            return f"RAW_ERROR: {resp.status_code} - {resp.text}"
    except Exception as e: return f"RAW_ERROR: {str(e)}"

# ==========================================
# FOLDER & FILE TOOLS (CRASH PROOF)
# ==========================================

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    """Lists root folders (Project Files, Plans) in a project."""
    h = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(h, str) or not h.get("data"): return "Error: No Hubs found."
    hub_id = h["data"][0]["id"]
    
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{ensure_b_prefix(project_id)}/topFolders"
    data = make_api_request(url)
    
    if isinstance(data, str): return f"API Error: {data}"
    
    try:
        output = "📂 **Root Folders:**\n"
        for i in data.get("data", []):
            name = i.get("attributes", {}).get("displayName", "Unnamed")
            output += f"- **{name}**\n  ID: `{i['id']}`\n"
        return output
    except Exception as e: return f"❌ Parsing Error: {str(e)}"

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
    """Lists files and folders inside a specific folder."""
    # ENCODE ID: Crucial fix for 'SystemError'
    safe_folder_id = encode_urn(folder_id)
    safe_project_id = ensure_b_prefix(project_id)
    
    url = f"https://developer.api.autodesk.com/data/v1/projects/{safe_project_id}/folders/{safe_folder_id}/contents"
    data = make_api_request(url)
    
    if isinstance(data, str): return f"API Error: {data}"
    
    try:
        items = data.get("data", [])
        if not items: return "📂 Folder is empty."
        
        output = f"📂 **Contents:**\n"
        for i in items[:limit]:
            name = i.get("attributes", {}).get("displayName", "Unnamed")
            item_type = i.get("type", "unknown")
            icon = "📁" if item_type == "folders" else "📄"
            output += f"{icon} **{name}**\n  ID: `{i['id']}`\n"
        return output
    except Exception as e: return f"❌ Parsing Error: {str(e)}"

# ==========================================
# OTHER READ TOOLS
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
    p_id = ensure_b_prefix(project_id)
    query = """query GetElementGroupsByProject($projectId: ID!) { elementGroupsByProject(projectId: $projectId) { results { id name } } }"""
    print(f"🔍 Searching designs in: {p_id}")
    data = make_graphql_request(query, {"projectId": p_id})
    
    if not data or isinstance(data, str):
        # Retry with clean ID
        data = make_graphql_request(query, {"projectId": clean_id(project_id)})
        
    if not data or isinstance(data, str): return f"❌ Error: {data}"
    
    groups = data.get("elementGroupsByProject", {}).get("results", [])
    if not groups: return "📂 No 3D designs found."
    
    output = f"🏗️ **Found {len(groups)} Designs:**\n"
    for g in groups: output += f"- **{g.get('name')}**\n  ID: `{g.get('id')}`\n"
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
def get_file_details(project_id: str, item_id: str) -> str:
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    p_id = ensure_b_prefix(project_id)
    item_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(item_id)}"
    resp = requests.get(item_url, headers=headers)
    if resp.status_code != 200: return f"Error: {resp.status_code}"
    try: tip_id = resp.json()["data"]["relationships"]["tip"]["data"]["id"]
    except KeyError: return "Error: No active version."
    return f"📄 Tip ID: {tip_id}"

@mcp.tool()
def get_download_url(project_id: str, id: str) -> str:
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        # 1. Resolve Tip Version if needed
        target = id
        if "lineage" in id or "fs.file" in id:
            r = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(id)}", headers=headers)
            if r.status_code == 200: target = r.json()["data"]["relationships"]["tip"]["data"]["id"]
        # 2. Get Storage URN
        r = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encode_urn(target)}", headers=headers)
        if r.status_code != 200: return f"Error: {r.text}"
        parts = r.json()["data"]["relationships"]["storage"]["data"]["id"].split("/")
        # 3. Get S3 Link
        oss = f"https://developer.api.autodesk.com/oss/v2/buckets/{parts[-2].split(':')[-1]}/objects/{parts[-1]}/signeds3download"
        r = requests.get(oss, headers=headers, params={"minutesExpiration": 60})
        return f"⬇️ **[Click to Download]({r.json()['url']})**" if r.status_code == 200 else "Error."
    except Exception as e: return str(e)

@mcp.tool()
def list_issues(project_id: str, limit: int = 10) -> str:
    data = make_api_request(f"https://developer.api.autodesk.com/issues/v1/projects/{clean_id(project_id)}/issues")
    if isinstance(data, str): return data
    output = "Issues:\n"
    results = data.get("results", []) if "results" in data else data.get("data", [])
    for i in results[:limit]: output += f"- #{i.get('attributes', i).get('identifier')}: {i.get('attributes', i).get('title')}\n"
    return output

@mcp.tool()
def list_assets(project_id: str, limit: int = 10) -> str:
    data = make_api_request(f"{BASE_URL_ACC}/assets/v2/projects/{clean_id(project_id)}/assets")
    if isinstance(data, str): return data
    output = "Assets:\n"
    for a in data.get("results", [])[:limit]: output += f"- {a.get('clientAssetId')} | {a.get('status', {}).get('name')}\n"
    return output

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
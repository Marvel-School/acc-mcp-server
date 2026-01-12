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
BASE_URL_HQ = "https://developer.api.autodesk.com/hq/v1" # NEW: Legacy HQ API for Deep Search
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

# --- HELPER: ROBUST USER SEARCH (FIXED) ---
def get_user_id_by_email(account_id: str, email: str) -> Optional[str]:
    """
    Searches for a user ID by email.
    Tries ACC Admin API first, then falls back to HQ (Legacy) API.
    """
    token = get_token()
    c_id = clean_id(account_id)
    headers = {"Authorization": f"Bearer {token}"}
    
    # ATTEMPT 1: Modern ACC Admin API
    try:
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users"
        params = {"filter[email]": email, "limit": 1}
        resp = requests.get(url, headers=headers, params=params)
        
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                print(f"User found in ACC API: {email}")
                return results[0].get("id")
    except Exception as e:
        print(f"ACC Search failed: {e}")

    # ATTEMPT 2: Legacy/HQ Admin API (Source of Truth for Admins)
    # This is often required for EU accounts or Account Admins.
    try:
        url_hq = f"{BASE_URL_HQ}/accounts/{c_id}/users"
        # HQ API uses 'email' param, NOT 'filter[email]'
        params_hq = {"email": email, "limit": 1}
        resp_hq = requests.get(url_hq, headers=headers, params=params_hq)
        
        if resp_hq.status_code == 200:
            results = resp_hq.json() # HQ returns list directly sometimes, or key
            if isinstance(results, list) and results:
                print(f"User found in HQ API: {email}")
                # HQ API returns 'uid' or 'id'
                return results[0].get("uid") or results[0].get("id")
    except Exception as e:
        print(f"HQ Search failed: {e}")
        
    return None

def get_acting_user_id(account_id: str, requester_email: Optional[str] = None) -> Optional[str]:
    # 1. Try Requesting User
    if requester_email:
        uid = get_user_id_by_email(account_id, requester_email)
        if uid: return uid
    # 2. Try Fallback Service Account
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
    output = []
    output.append(f"🔍 **Diagnostics for {requester_email}**")
    
    # 1. Check Hub/Account
    try:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str): 
            output.append(f"❌ Hub List Failed: {hubs}")
            return "\n".join(output)
        
        hub_data = hubs.get("data", [])
        if not hub_data:
            output.append("❌ No Hubs found.")
            return "\n".join(output)

        hub = hub_data[0]
        acc_id = clean_id(hub["id"])
        name = hub["attributes"]["name"]
        region = hub["attributes"].get("region", "Unknown")
        
        output.append(f"✅ Found Account: **{name}** (Region: {region})")
        output.append(f"   ID: `{acc_id}`")
    except Exception as e:
        return f"❌ Critical Error reading hubs: {str(e)}"

    # 2. Check User Identity (Deep Search)
    user_id = get_user_id_by_email(acc_id, requester_email)
    
    if user_id:
        output.append(f"✅ User Found: ID `{user_id}`")
    else:
        output.append(f"❌ User '{requester_email}' NOT FOUND via API.")
        output.append("   (Tried both ACC and HQ databases. Check exact spelling or region permissions.)")
        return "\n".join(output)

    # 3. Test Permission (Dry Run)
    try:
        token = get_token()
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{acc_id}/projects"
        headers = {"Authorization": f"Bearer {token}", "x-user-id": user_id}
        resp = requests.get(url, headers=headers, params={"limit": 1})
        
        if resp.status_code == 200:
            output.append("✅ **Authorization Success:** This user CAN access the Admin API.")
        elif resp.status_code == 403:
            output.append("❌ **Permission Denied (403):**")
            output.append(f"   Raw Error: {resp.text}")
        else:
            output.append(f"❌ API Error {resp.status_code}: {resp.text}")
            
    except Exception as e:
        output.append(f"❌ Exception during connection: {str(e)}")

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
    # 1. Auto-detect Account ID
    if not account_id:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str) or not hubs.get("data"): return "Error: No Account/Hub found."
        account_id = clean_id(hubs["data"][0]["id"])
    
    # 2. SMART LOOKUP (Deep Search)
    c_id = clean_id(account_id)
    acting_user_id = get_acting_user_id(c_id, requester_email)
    
    if not acting_user_id:
        return "RAW_ERROR: Authorization Failed. I could not find a valid user ID for this email in ACC/HQ."

    payload = {
        "name": project_name, 
        "type": "production",
        "currency": currency,
        "timezone": timezone, 
        "language": language
    }
    
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-user-id": acting_user_id}
        resp = requests.post(f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects", headers=headers, json=payload)
        
        if resp.status_code in [200, 201]:
            return f"✅ Success! Project '{project_name}' created by **{requester_email}** (ID: {resp.json().get('id')})."
        else:
            return f"RAW_ERROR: {resp.status_code} - {resp.text}"
    except Exception as e: return f"RAW_ERROR: {str(e)}"

# ==========================================
# OTHER TOOLS (Read Tools)
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
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
def get_file_details(project_id: str, item_id: str) -> str:
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    p_id = ensure_b_prefix(project_id)
    item_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(item_id)}"
    resp = requests.get(item_url, headers=headers)
    if resp.status_code != 200: return f"Error: {resp.status_code}"
    try: tip_id = resp.json()["data"]["relationships"]["tip"]["data"]["id"]
    except KeyError: return "Error: No active version."
    v_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encode_urn(tip_id)}"
    v_resp = requests.get(v_url, headers=headers)
    if v_resp.status_code != 200: return "Error fetching version."
    attrs = v_resp.json().get("data", {}).get("attributes", {})
    return f"📄 {attrs.get('displayName')} (v{attrs.get('versionNumber')})\nID: {tip_id}"

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

@mcp.tool()
def get_data_connector_status(account_id: Optional[str] = None) -> str:
    if not account_id:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str) or not hubs.get("data"): return "No Hub found."
        account_id = clean_id(hubs["data"][0]["id"])
    data = make_api_request(f"https://developer.api.autodesk.com/data-connector/v1/accounts/{clean_id(account_id)}/requests")
    if isinstance(data, str): return data
    results = data.get("data", [])[:5]
    if not results: return "No requests found."
    output = "Data Connector Requests:\n"
    for r in results: output += f"- {r.get('createdAt')}: {r.get('status')}\n"
    return output

@mcp.tool()
def list_assets(project_id: str, limit: int = 10) -> str:
    data = make_api_request(f"{BASE_URL_ACC}/assets/v2/projects/{clean_id(project_id)}/assets")
    if isinstance(data, str): return data
    output = "Assets:\n"
    for a in data.get("results", [])[:limit]: output += f"- {a.get('clientAssetId')} | {a.get('status', {}).get('name')}\n"
    return output

@mcp.tool()
def list_issues(project_id: str, limit: int = 10) -> str:
    data = make_api_request(f"https://developer.api.autodesk.com/issues/v1/projects/{clean_id(project_id)}/issues")
    if isinstance(data, str): return data
    output = "Issues:\n"
    results = data.get("results", []) if "results" in data else data.get("data", [])
    for i in results[:limit]: output += f"- #{i.get('attributes', i).get('identifier')}: {i.get('attributes', i).get('title')}\n"
    return output

@mcp.tool()
def get_account_projects_admin(account_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    if not account_id:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str) or not hubs.get("data"): return "No Hub found."
        account_id = clean_id(hubs["data"][0]["id"])
    data = make_api_request(f"{BASE_URL_ACC}/admin/v1/accounts/{clean_id(account_id)}/projects")
    if isinstance(data, str): return data
    results = data if isinstance(data, list) else data.get("results", [])
    if name_filter: results = [p for p in results if name_filter.lower() in p.get("name", "").lower()]
    output = f"Admin Projects ({len(results)}):\n"
    for p in results[:limit]: output += f"- {p.get('name')} (ID: {p.get('id')})\n"
    return output

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
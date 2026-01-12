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
# SAFETY FIX: Read admin email from environment to avoid typing it in chat
DEFAULT_ADMIN_EMAIL = os.environ.get("DEFAULT_ADMIN_EMAIL") 
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
    """Retrieves an Autodesk Token with Read & Write permissions."""
    global token_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Error: APS_CLIENT_ID and APS_CLIENT_SECRET are missing.")

    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]

    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = requests.auth.HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    
    # SCOPES: data:read data:write account:read account:write bucket:read
    data = {
        "grant_type": "client_credentials", 
        "scope": "data:read data:write account:read account:write bucket:read"
    }

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

# --- HELPER: ID CLEANING ---
def clean_id(id_str: str) -> str:
    if not id_str: return ""
    return id_str.replace("b.", "")

def ensure_b_prefix(id_str: str) -> str:
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: str) -> str:
    if not urn: return ""
    return quote(urn, safe='')

# --- HELPER: REST API REQUEST ---
def make_api_request(url: str):
    """Standardizes GET requests and error handling."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        
        if resp.status_code == 403:
            return "Error 403: Access Denied. Check if App is added in ACC Admin."
        if resp.status_code == 404:
            return "Error 404: Not Found. Check IDs."
        
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return f"Request Error: {str(e)}"

# --- HELPER: GRAPHQL REQUEST ---
def make_graphql_request(query: str, variables: Dict[str, Any] = None):
    """Handles AEC Data Model queries."""
    try:
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {"query": query, "variables": variables or {}}
        
        resp = requests.post(BASE_URL_GRAPHQL, headers=headers, json=payload)
        
        if resp.status_code != 200:
            return f"GraphQL Error {resp.status_code}: {resp.text}"
            
        result = resp.json()
        if "errors" in result:
            return f"GraphQL Query Errors: {result['errors']}"
            
        return result.get("data", {})
    except Exception as e:
        return f"GraphQL Exception: {str(e)}"

# --- HELPER: FIND ADMIN USER ---
def get_admin_user_id(account_id: str, email: Optional[str] = None) -> Optional[str]:
    """
    Finds a user ID to impersonate. 
    Checks inputs in this order: 
    1. Argument 'email' 
    2. Env Var 'DEFAULT_ADMIN_EMAIL' 
    3. Any active admin.
    """
    try:
        token = get_token()
        c_id = clean_id(account_id)
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users"
        headers = {"Authorization": f"Bearer {token}"}
        
        # Priority: Explicit Argument -> Environment Variable
        target_email = email or DEFAULT_ADMIN_EMAIL
        
        params = {"limit": 50} 
        resp = requests.get(url, headers=headers, params=params)
        
        if resp.status_code == 200:
            users = resp.json().get("results", [])
            
            # 1. Search for specific email if we have one
            if target_email:
                for user in users:
                    if user.get("email", "").lower() == target_email.lower():
                        return user.get("id") or user.get("autodeskId")
                print(f"Warning: User {target_email} not found. Falling back to any active user.")

            # 2. Fallback: First active user (if strict email match failed or wasn't provided)
            for user in users:
                if user.get("status") == "active": 
                    return user.get("id") or user.get("autodeskId")
    except Exception as e:
        print(f"Warning: Could not fetch admin user: {e}")
    return None

# ==========================================
# TOOLSET 1: AEC DATA MODEL (GRAPHQL)
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
    """Lists 'Element Groups' (Designs) in a project using AEC Data Model."""
    query = """
    query GetElementGroupsByProject ($projectId: ID!) {
        elementGroupsByProject(projectId: $projectId) {
            results { id name alternativeIdentifiers { fileVersionUrn } }
        }
    }
    """
    p_id = ensure_b_prefix(project_id)
    data = make_graphql_request(query, {"projectId": p_id})
    
    if isinstance(data, str) and "Error" in data:
        p_id = clean_id(project_id)
        data = make_graphql_request(query, {"projectId": p_id})
        
    if isinstance(data, str): return data
    
    groups = data.get("elementGroupsByProject", {}).get("results", [])
    if not groups:
        return "No designs found. Ensure AEC Data Model is enabled for this hub."
        
    output = "🏗️ **Designs Found:**\n"
    for g in groups:
        output += f"- **{g.get('name')}**\n  ID: `{g.get('id')}`\n"
    return output

@mcp.tool()
def query_model_elements(design_id: str, category: str, limit: int = 20) -> str:
    """Queries specific elements (e.g. Walls, Windows) inside a model."""
    query = """
    query GetElementsByCategory ($elementGroupId: ID!, $filter: String!) {
      elementsByElementGroup(elementGroupId: $elementGroupId, filter: {query: $filter}) {
        results { id name properties { results { name value } } }
      }
    }
    """
    filter_str = f"property.name.category=='{category}'"
    data = make_graphql_request(query, {"elementGroupId": design_id, "filter": filter_str})
    if isinstance(data, str): return data
    
    elements = data.get("elementsByElementGroup", {}).get("results", [])
    if not elements:
        return f"No elements found of category '{category}'."
        
    display = elements[:limit]
    output = f"🔍 **Found {len(elements)} {category}** (Top {len(display)}):\n"
    for el in display:
        name = el.get("name", "Unnamed")
        el_id = el.get("id")
        props = el.get("properties", {}).get("results", [])
        prop_str = ", ".join([f"{p['name']}: {p['value']}" for p in props if p['value']])
        if len(prop_str) > 100: prop_str = prop_str[:100] + "..."
        output += f"- **{name}** (ID: {el_id})\n  Props: {prop_str}\n"
    return output

@mcp.tool()
def get_model_viewer_link(project_id: str, urn: str) -> str:
    """Generates a direct link to view the model in ACC (Cloud Viewer)."""
    clean_p_id = clean_id(project_id)
    return f"https://acc.autodesk.com/docs/files/projects/{clean_p_id}?entityId={urn}"

# ==========================================
# TOOLSET 2: DATA MANAGEMENT (REST API)
# ==========================================

@mcp.tool()
def list_hubs() -> str:
    """Lists all available Autodesk Hubs."""
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, str): return data
    output = "Found Hubs:\n"
    for hub in data.get("data", []):
        output += f"- {hub['attributes']['name']} (ID: {hub['id']})\n"
    return output

@mcp.tool()
def list_projects_dm(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    """Lists projects via Data Management (Docs)."""
    if not hub_id:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str) or not hubs.get("data"): return "No Hubs found."
        hub_id = hubs["data"][0]["id"]
    
    data = make_api_request(f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects")
    if isinstance(data, str): return data
    
    all_projects = data.get("data", [])
    if name_filter:
        all_projects = [p for p in all_projects if name_filter.lower() in p['attributes']['name'].lower()]
    
    display = all_projects[:limit]
    output = f"Found: {len(all_projects)} projects:\n"
    for p in display:
        output += f"- {p['attributes']['name']} (ID: {p['id']})\n"
    return output

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    """Gets the root folders of a project."""
    hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(hubs, str) or not hubs.get("data"): return "No Hubs found."
    hub_id = hubs["data"][0]["id"]
    p_id = ensure_b_prefix(project_id)
    
    data = make_api_request(f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{p_id}/topFolders")
    if isinstance(data, str): return data
    
    output = f"Root folders for {p_id}:\n"
    for item in data.get("data", []):
        output += f"- 📁 {item['attributes']['displayName']} (ID: {item['id']})\n"
    return output

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
    """Lists contents of a specific folder."""
    p_id = ensure_b_prefix(project_id)
    data = make_api_request(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/folders/{folder_id}/contents")
    if isinstance(data, str): return data
    
    items = data.get("data", [])
    output = f"Contents (Top {len(items[:limit])}):\n"
    for item in items[:limit]:
        icon = "📁" if "Folder" in item["type"] else "📄"
        output += f"- {icon} {item['attributes']['displayName']} (ID: {item['id']})\n"
    return output

@mcp.tool()
def get_file_details(project_id: str, item_id: str) -> str:
    """Gets details of a file."""
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    p_id = ensure_b_prefix(project_id)
    
    item_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(item_id)}"
    resp = requests.get(item_url, headers=headers)
    if resp.status_code != 200: return f"Error: {resp.status_code}"
    
    try: 
        tip_id = resp.json()["data"]["relationships"]["tip"]["data"]["id"]
    except KeyError: 
        return "Error: No active version."
        
    v_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encode_urn(tip_id)}"
    v_resp = requests.get(v_url, headers=headers)
    if v_resp.status_code != 200: return "Error fetching version."
    
    attrs = v_resp.json().get("data", {}).get("attributes", {})
    return f"📄 {attrs.get('displayName')} (v{attrs.get('versionNumber')})\nID: {tip_id}"

@mcp.tool()
def get_download_url(project_id: str, id: str) -> str:
    """Generates a download link."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        
        target_version_id = id
        if "lineage" in id or "fs.file" in id and "?version=" not in id:
            item_resp = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encode_urn(id)}", headers=headers)
            if item_resp.status_code == 200:
                try: target_version_id = item_resp.json()["data"]["relationships"]["tip"]["data"]["id"]
                except: return "Error: No version found."

        v_resp = requests.get(f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encode_urn(target_version_id)}", headers=headers)
        if v_resp.status_code != 200: return f"Error: {v_resp.text}"
        
        try:
            parts = v_resp.json()["data"]["relationships"]["storage"]["data"]["id"].split("/")
            oss_url = f"https://developer.api.autodesk.com/oss/v2/buckets/{parts[-2].split(':')[-1]}/objects/{parts[-1]}/signeds3download"
            oss_resp = requests.get(oss_url, headers=headers, params={"minutesExpiration": 60})
            if oss_resp.status_code == 200: 
                return f"⬇️ **[Click here to download]({oss_resp.json()['url']})**"
            else: 
                return f"Error link: {oss_resp.text}"
        except: return "Error storage location."
    except Exception as e: return f"Error: {str(e)}"

# ==========================================
# TOOLSET 3: MANAGEMENT (WRITE & ADMIN)
# ==========================================

@mcp.tool()
def create_project(
    project_name: str, 
    account_id: Optional[str] = None, 
    project_type: str = "Renovation", 
    currency: str = "EUR", 
    language: str = "en",
    timezone: str = "Europe/Amsterdam",
    admin_email: Optional[str] = None 
) -> str:
    """
    Creates a new project.
    SAFETY: Uses DEFAULT_ADMIN_EMAIL env var to avoid typing emails in chat (prevents blocks).
    """
    # 1. AUTO-DETECT Account ID
    if not account_id:
        hubs = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs, str) or not hubs.get("data"): return "Error: No Account/Hub found."
        account_id = clean_id(hubs["data"][0]["id"])
    
    # 2. FIND ACTING USER (Checks Argument first, then Env Var)
    c_id = clean_id(account_id)
    acting_user_id = get_admin_user_id(c_id, email=admin_email)
    
    # --- SAFETY NET ---
    if not acting_user_id:
        return (
            f"⚠️ ACTION REQUIRED: I cannot find an admin user to perform this action. "
            f"Please ensure the 'DEFAULT_ADMIN_EMAIL' environment variable is set in Azure settings, "
            f"OR ask the user for their email address."
        )

    payload = {
        "name": project_name, "type": "production", "currency": currency,
        "timezone": timezone, "language": language, "projectType": project_type 
    }
    
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-user-id": acting_user_id}
        resp = requests.post(f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects", headers=headers, json=payload)
        
        if resp.status_code in [200, 201]:
            return f"✅ Success! Project '{project_name}' created (ID: {resp.json().get('id')})."
        else:
            return f"❌ Error: {resp.status_code} {resp.text}"
    except Exception as e: return f"Error: {str(e)}"

@mcp.tool()
def add_user_to_project(project_id: str, email: str, role: str = "project_member") -> str:
    """Adds a user to a project."""
    c_id = clean_id(project_id)
    payload = [{"email": email, "products": [{"key": "docs", "access": role}]}]
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.post(f"{BASE_URL_ACC}/admin/v1/projects/{c_id}/users", headers=headers, json=payload)
        if resp.status_code in [200, 201]: return f"✅ Invitation sent to {email}."
        else: return f"❌ Error: {resp.status_code} {resp.text}"
    except Exception as e: return f"Error: {str(e)}"

@mcp.tool()
def get_data_connector_status(account_id: Optional[str] = None) -> str:
    """Checks Data Connector status."""
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
    """Lists Assets."""
    data = make_api_request(f"{BASE_URL_ACC}/assets/v2/projects/{clean_id(project_id)}/assets")
    if isinstance(data, str): return data
    output = "Assets:\n"
    for a in data.get("results", [])[:limit]: output += f"- {a.get('clientAssetId')} | {a.get('status', {}).get('name')}\n"
    return output

@mcp.tool()
def list_issues(project_id: str, limit: int = 10) -> str:
    """Lists Issues."""
    data = make_api_request(f"https://developer.api.autodesk.com/issues/v1/projects/{clean_id(project_id)}/issues")
    if isinstance(data, str): return data
    output = "Issues:\n"
    results = data.get("results", []) if "results" in data else data.get("data", [])
    for i in results[:limit]: output += f"- #{i.get('attributes', i).get('identifier')}: {i.get('attributes', i).get('title')}\n"
    return output

@mcp.tool()
def get_account_projects_admin(account_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    """Lists projects with admin details."""
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
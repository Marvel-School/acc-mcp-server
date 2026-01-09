import os
import time
import requests
import traceback
from urllib.parse import quote
from fastmcp import FastMCP
from typing import Optional

# --- CONFIGURATION ---
APS_CLIENT_ID = os.environ.get("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET")
PORT = int(os.environ.get("PORT", 8000))

# Initialize FastMCP
mcp = FastMCP("Autodesk ACC Agent")

# Global token storage
token_cache = {"access_token": None, "expires_at": 0}

# Base URLs
BASE_URL_DM = "https://developer.api.autodesk.com"
BASE_URL_ACC = "https://developer.api.autodesk.com/construction"
BASE_URL_ISSUES = "https://developer.api.autodesk.com/issues/v1/containers"

# --- HELPER: AUTHENTICATION ---
def get_token():
    """Helper to get a valid Autodesk Access Token with expanded scopes."""
    global token_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Error: APS_CLIENT_ID and APS_CLIENT_SECRET environment variables are missing.")

    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]

    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = requests.auth.HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    
    # Updated Scopes for Admin, Assets, and Issues
    data = {
        "grant_type": "client_credentials", 
        "scope": "data:read account:read bucket:read"
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

# --- HELPER: ID FORMATTING ---
def clean_id(id_str: str) -> str:
    """Removes 'b.' prefix for Admin, Assets, and Issues APIs."""
    if not id_str: return ""
    return id_str.replace("b.", "")

def ensure_b_prefix(id_str: str) -> str:
    """Ensures 'b.' prefix exists for Data Management (Files) API."""
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: str) -> str:
    """URL Encodes URNs for file endpoints."""
    if not urn: return ""
    return quote(urn, safe='')

# --- HELPER: GENERIC API REQUEST ---
def make_api_request(url: str):
    """Wrapper to handle token insertion and error checking."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        
        if resp.status_code == 403:
            return "Error 403: Access Denied. Ensure the App is added to the Project in ACC Admin."
        if resp.status_code == 404:
            return "Error 404: Resource not found. Check ID formats."
        
        resp.raise_for_status()
        return resp.json() # Returns Dict
    except Exception as e:
        return f"Request Error: {str(e)}"

# --- EXISTING TOOLS (Data Management) ---

@mcp.tool()
def list_hubs() -> str:
    """Lists all Autodesk Hubs (Accounts)."""
    try:
        data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(data, str): return data # Return error message

        output = "Found Hubs:\n"
        for hub in data.get("data", []):
            output += f"- {hub['attributes']['name']} (ID: {hub['id']})\n"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def list_projects_dm(hub_id: Optional[str] = None) -> str:
    """Lists projects via Data Management API. Good for finding Project IDs."""
    try:
        if not hub_id:
            # Auto-find first hub
            hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
            if isinstance(hubs_data, str) or not hubs_data.get("data"): return "Error: No Hubs found."
            hub_id = hubs_data["data"][0]["id"]

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
        data = make_api_request(url)
        if isinstance(data, str): return data

        output = f"Projects in Hub {hub_id}:\n"
        for proj in data.get("data", []):
            output += f"- {proj['attributes']['name']} (ID: {proj['id']})\n"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str) -> str:
    """Lists files in a folder (Data Management API)."""
    # DM API requires 'b.'
    p_id = ensure_b_prefix(project_id)
    url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/folders/{folder_id}/contents"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    output = f"Contents of folder {folder_id}:\n"
    for item in data.get("data", []):
        name = item["attributes"]["displayName"]
        item_type = item["type"]
        item_id = item["id"]
        output += f"- [{item_type}] {name} (ID: {item_id})\n"
    return output

@mcp.tool()
def get_download_url(project_id: str, version_id: str) -> str:
    """Generates a download link for a file version."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        encoded_version_id = encode_urn(version_id)
        
        # 1. Get Storage Location
        v_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encoded_version_id}"
        v_resp = requests.get(v_url, headers=headers)
        if v_resp.status_code != 200: return f"Error fetching version: {v_resp.text}"
        
        storage_urn = v_resp.json()["data"]["relationships"]["storage"]["data"]["id"]
        parts = storage_urn.split("/")
        object_key = parts[-1]
        bucket_key = parts[-2].split(":")[-1]
        
        # 2. Get Signed S3 Link
        oss_url = f"https://developer.api.autodesk.com/oss/v2/buckets/{bucket_key}/objects/{object_key}/signeds3download"
        oss_resp = requests.get(oss_url, headers=headers, params={"minutesExpiration": 60})
        
        if oss_resp.status_code == 200:
            return f"⬇️ **[Click Here to Download]({oss_resp.json()['url']})**"
        else:
            return f"Error creating download link: {oss_resp.text}"
    except Exception as e:
        return f"Error: {str(e)}"

# --- NEW TOOLS (Admin, Assets, Issues, Data Connector) ---

@mcp.tool()
def get_account_projects_admin(account_id: str) -> str:
    """
    (Admin API) Lists projects with admin details. 
    Use this to get project info, but use list_projects_dm for file navigation.
    """
    c_id = clean_id(account_id)
    url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    output = "Account Projects (Admin View):\n"
    # Admin API returns a list directly or inside 'results' depending on version/pagination
    results = data if isinstance(data, list) else data.get("results", [])
    
    for proj in results:
        name = proj.get("name", "Unknown")
        p_id = proj.get("id")
        output += f"- {name} (ID: {p_id})\n"
    return output

@mcp.tool()
def list_assets(project_id: str) -> str:
    """Lists Assets (equipment, rooms, etc.) for a project."""
    c_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/assets/v2/projects/{c_id}/assets"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    output = f"Assets for Project {c_id}:\n"
    results = data.get("results", [])
    if not results: return "No assets found."

    for asset in results:
        # Handling different asset structures
        ref = asset.get("clientAssetId", "No Ref")
        cat = asset.get("category", {}).get("name", "Uncategorized")
        status = asset.get("status", {}).get("name", "Unknown")
        output += f"- [{cat}] {ref} | Status: {status} (ID: {asset.get('id')})\n"
    return output

@mcp.tool()
def list_issues(project_id: str) -> str:
    """Lists open Issues in a project."""
    c_id = clean_id(project_id)
    # Issues V1 API
    url = f"https://developer.api.autodesk.com/issues/v1/projects/{c_id}/issues"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    output = f"Issues for Project {c_id}:\n"
    results = data.get("results", []) # Issues API uses 'data' or 'results' depending on version
    # Fallback if V1 returns standard JSON API structure
    if not results and "data" in data: results = data["data"]

    if not results: return "No issues found."

    for issue in results:
        attrs = issue.get("attributes", issue) # Handle V1 vs generic structure
        title = attrs.get("title", "No Title")
        status = attrs.get("status", "Unknown")
        ident = attrs.get("identifier", "No ID")
        output += f"- #{ident}: {title} [{status}]\n"
    return output

@mcp.tool()
def get_data_connector_status(account_id: str) -> str:
    """Checks the status of Data Connector extractions."""
    c_id = clean_id(account_id)
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{c_id}/requests"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    output = "Data Connector Requests:\n"
    results = data.get("data", [])
    if not results: return "No data connector requests found."

    for req in results[:5]: # Show last 5
        desc = req.get("description", "No Description")
        status = req.get("status", "Unknown")
        date = req.get("createdAt", "")
        output += f"- {date}: {desc} | Status: {status}\n"
    return output

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
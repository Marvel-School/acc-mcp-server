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
    """Removes 'b.' prefix (Required for Admin/Assets/Issues/Write Tools)."""
    if not id_str: return ""
    return id_str.replace("b.", "")

def ensure_b_prefix(id_str: str) -> str:
    """Ensures 'b.' prefix exists (Required for Files/Docs)."""
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: str) -> str:
    """URL Encode for file versions."""
    if not urn: return ""
    return quote(urn, safe='')

# --- HELPER: REST API REQUEST ---
def make_api_request(url: str):
    """Executes GET request and checks for errors."""
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

# --- HELPER: GRAPHQL REQUEST (AEC DATA MODEL) ---
def make_graphql_request(query: str, variables: Dict[str, Any] = None):
    """Executes a GraphQL query against the AEC Data Model API."""
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

# --- HELPER: FIND ADMIN USER (IMPROVED WITH EMAIL SEARCH) ---
def get_admin_user_id(account_id: str, email: Optional[str] = None) -> Optional[str]:
    """
    Finds a user ID to impersonate for project creation.
    If 'email' is provided, it searches for that specific user.
    Otherwise, it looks for the first active user.
    """
    try:
        token = get_token()
        c_id = clean_id(account_id)
        url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/users"
        headers = {"Authorization": f"Bearer {token}"}
        
        # If email is provided, we fetch more users to increase chances of finding them
        params = {"limit": 50} 
        
        resp = requests.get(url, headers=headers, params=params)
        
        if resp.status_code == 200:
            users = resp.json().get("results", [])
            
            # 1. Try to find the specific email if provided
            if email:
                for user in users:
                    if user.get("email", "").lower() == email.lower():
                        return user.get("id") or user.get("autodeskId")
                print(f"Warning: User with email {email} not found in first 50 users.")

            # 2. Fallback: Find first active user if no email provided or email not found
            # (Only do fallback if email WAS NOT provided. If specific email was asked, likely better to fail or warn)
            if not email:
                for user in users:
                    if user.get("status") == "active": 
                        return user.get("id") or user.get("autodeskId")
        else:
            print(f"User Search Failed: {resp.status_code} {resp.text}")
            
    except Exception as e:
        print(f"Warning: Could not fetch admin user: {e}")
    return None

# ==========================================
# TOOLSET 1: AEC DATA MODEL
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
    """Lists 'Element Groups' (Designs/Models) in a project using AEC Data Model."""
    query = """
    query GetElementGroupsByProject ($projectId: ID!) {
        elementGroupsByProject(projectId: $projectId) {
            results {
                id
                name
                alternativeIdentifiers {
                    fileVersionUrn
                }
            }
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
        return "No designs (Element Groups) found. Ensure AEC Data Model is enabled for this hub."
        
    output = "🏗️ **Designs / Models Found:**\n"
    for g in groups:
        name = g.get("name")
        g_id = g.get("id")
        output += f"- **{name}**\n  ID: `{g_id}`\n"
    return output

@mcp.tool()
def query_model_elements(design_id: str, category: str, limit: int = 20) -> str:
    """Queries specific elements inside a model (Walls, Doors, Windows, etc.)."""
    query = """
    query GetElementsByCategory ($elementGroupId: ID!, $filter: String!) {
      elementsByElementGroup(elementGroupId: $elementGroupId, filter: {query: $filter}) {
        results {
          id
          name
          properties {
            results {
              name
              value
            }
          }
        }
      }
    }
    """
    filter_str = f"property.name.category=='{category}'"
    data = make_graphql_request(query, {"elementGroupId": design_id, "filter": filter_str})
    if isinstance(data, str): return data
    
    elements = data.get("elementsByElementGroup", {}).get("results", [])
    if not elements:
        return f"No elements found of category '{category}' in this design."
        
    display = elements[:limit]
    output = f"🔍 **Found {len(elements)} {category}** (Showing top {len(display)}):\n"
    for el in display:
        name = el.get("name", "Unnamed")
        el_id = el.get("id")
        props = el.get("properties", {}).get("results", [])
        prop_str = ", ".join([f"{p['name']}: {p['value']}" for p in props if p['value']])
        if len(prop_str) > 100: prop_str = prop_str[:100] + "..."
        output += f"- **{name}** (ID: {el_id})\n  Props: {prop_str}\n"
    if len(elements) > limit:
        output += f"\n... (and {len(elements) - limit} more)"
    return output

@mcp.tool()
def get_model_viewer_link(project_id: str, urn: str) -> str:
    """Generates a direct link to view the model in the Autodesk Construction Cloud website."""
    clean_p_id = clean_id(project_id)
    return f"https://acc.autodesk.com/docs/files/projects/{clean_p_id}?entityId={urn}"

# ==========================================
# TOOLSET 2: EXISTING READ TOOLS (REST API)
# ==========================================

@mcp.tool()
def list_hubs() -> str:
    """Lists all available Autodesk Hubs."""
    url = "https://developer.api.autodesk.com/project/v1/hubs"
    data = make_api_request(url)
    if isinstance(data, str): return data

    output = "Found Hubs:\n"
    for hub in data.get("data", []):
        name = hub['attributes']['name']
        hub_id = hub['id']
        clean_acc_id = clean_id(hub_id)
        output += f"- {name}\n  Hub ID: {hub_id}\n  Account ID (for Admin): {clean_acc_id}\n"
    return output

@mcp.tool()
def list_projects_dm(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    """Lists projects via Data Management (Docs)."""
    try:
        if not hub_id:
            hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
            if isinstance(hubs_data, str) or not hubs_data.get("data"): return "No Hubs found."
            hub_id = hubs_data["data"][0]["id"]

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
        data = make_api_request(url)
        if isinstance(data, str): return data

        all_projects = data.get("data", [])
        if name_filter:
            lower_filter = name_filter.lower()
            filtered = [p for p in all_projects if lower_filter in p['attributes']['name'].lower()]
        else:
            filtered = all_projects

        display = filtered[:limit]
        count = len(filtered)
        output = f"Found: {count} projects in Hub {hub_id} (Showing top {len(display)}):\n"
        for proj in display:
            output += f"- {proj['attributes']['name']} (ID: {proj['id']})\n"
        if count > limit:
            output += f"\n... (and {count - limit} more. Use 'name_filter' to refine.)"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    """Gets the root folders (Top Folders) of a project."""
    try:
        hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs_data, str) or not hubs_data.get("data"): return "No Hubs found."
        hub_id = hubs_data["data"][0]["id"]
        p_id = ensure_b_prefix(project_id)
        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{p_id}/topFolders"
        data = make_api_request(url)
        if isinstance(data, str): return data
        items = data.get("data", [])
        output = f"Root folders for project {p_id}:\n"
        for item in items:
            name = item["attributes"]["displayName"]
            folder_id = item["id"]
            output += f"- 📁 {name} (ID: {folder_id})\n"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
    """Lists contents of a specific folder."""
    p_id = ensure_b_prefix(project_id)
    url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/folders/{folder_id}/contents"
    data = make_api_request(url)
    if isinstance(data, str): return data

    items = data.get("data", [])
    display = items[:limit]
    output = f"Contents of folder {folder_id} (Top {len(display)} of {len(items)}):\n"
    for item in display:
        name = item["attributes"]["displayName"]
        item_type = item["type"]
        item_id = item["id"]
        icon = "📁" if "Folder" in item_type else "📄"
        output += f"- {icon} {name} (ID: {item_id})\n"
    if len(items) > limit:
        output += "\n... (List truncated for speed.)"
    return output

@mcp.tool()
def get_file_details(project_id: str, item_id: str) -> str:
    """Gets details of a file (Metadata only)."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        encoded_item_id = encode_urn(item_id)
        url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encoded_item_id}"
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200: return f"Error: {resp.status_code} {resp.text}"
        
        json_resp = resp.json()
        try:
            tip_id = json_resp["data"]["relationships"]["tip"]["data"]["id"]
        except KeyError:
             return "Error: Cannot find active version."
        
        encoded_tip_id = encode_urn(tip_id)
        v_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encoded_tip_id}"
        v_resp = requests.get(v_url, headers=headers)
        if v_resp.status_code != 200: return f"Version Error: {v_resp.status_code}"
             
        attrs = v_resp.json().get("data", {}).get("attributes", {})
        return (
            f"📄 **File Details**\n"
            f"- **Name:** {attrs.get('displayName')}\n"
            f"- **Version:** v{attrs.get('versionNumber')}\n"
            f"- **Latest Version ID:** {tip_id}\n"
        )
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_download_url(project_id: str, id: str) -> str:
    """Generates a download link. SMART: Accepts either a Version ID OR an Item ID (Lineage)."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        
        target_version_id = id
        if "lineage" in id or "fs.file" in id and "?version=" not in id:
            print(f"Smart Download: Resolving Item ID {id} to Version...")
            encoded_item_id = encode_urn(id)
            item_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/items/{encoded_item_id}"
            item_resp = requests.get(item_url, headers=headers)
            if item_resp.status_code == 200:
                try:
                    target_version_id = item_resp.json()["data"]["relationships"]["tip"]["data"]["id"]
                except KeyError:
                    return "Error: Could not find version for this item."

        encoded_version_id = encode_urn(target_version_id)
        v_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encoded_version_id}"
        v_resp = requests.get(v_url, headers=headers)
        if v_resp.status_code != 200: return f"Error fetching version: {v_resp.text}"
        
        try:
            storage_urn = v_resp.json()["data"]["relationships"]["storage"]["data"]["id"]
            parts = storage_urn.split("/")
            object_key = parts[-1]
            bucket_key = parts[-2].split(":")[-1]
            oss_url = f"https://developer.api.autodesk.com/oss/v2/buckets/{bucket_key}/objects/{object_key}/signeds3download"
            oss_resp = requests.get(oss_url, headers=headers, params={"minutesExpiration": 60})
            if oss_resp.status_code == 200:
                return f"⬇️ **[Click here to download]({oss_resp.json()['url']})**"
            else:
                return f"Error creating link: {oss_resp.text}"
        except KeyError:
            return "Error: Could not determine storage location."
    except Exception as e:
        return f"Error: {str(e)}"

# ==========================================
# TOOLSET 3: MANAGEMENT & WRITE TOOLS
# ==========================================

@mcp.tool()
def get_account_projects_admin(account_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    """(Admin API) Lists projects with admin details. SMART: Auto-detects Account ID."""
    if not account_id:
        hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs_data, str) or not hubs_data.get("data"):
            return "Cannot find Account/Hub ID to use as default."
        raw_hub_id = hubs_data["data"][0]["id"]
        account_id = clean_id(raw_hub_id)

    c_id = clean_id(account_id)
    url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects"
    data = make_api_request(url)
    if isinstance(data, str): return data
    results = data if isinstance(data, list) else data.get("results", [])
    if name_filter:
        results = [p for p in results if name_filter.lower() in p.get("name", "").lower()]
    count = len(results)
    display = results[:limit]
    output = f"Admin Projects (Found: {count}) for Account {c_id}:\n"
    for proj in display:
        name = proj.get("name", "Unknown")
        p_id = proj.get("id")
        output += f"- {name} (ID: {p_id})\n"
    if count > limit:
        output += f"... and {count - limit} more."
    return output

@mcp.tool()
def list_assets(project_id: str, limit: int = 10) -> str:
    """Lists Assets (installations/equipment)."""
    c_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/assets/v2/projects/{c_id}/assets"
    data = make_api_request(url)
    if isinstance(data, str): return data
    results = data.get("results", [])
    if not results: return "No assets found."
    display = results[:limit]
    output = f"Assets in project {c_id} (Top {len(display)}):\n"
    for asset in display:
        ref = asset.get("clientAssetId", "No Ref")
        cat = asset.get("category", {}).get("name", "-")
        status = asset.get("status", {}).get("name", "Unknown")
        output += f"- [{cat}] {ref} | Status: {status}\n"
    return output

@mcp.tool()
def list_issues(project_id: str, status_filter: str = "open", limit: int = 10) -> str:
    """Lists Issues (filtered by status)."""
    c_id = clean_id(project_id)
    url = f"https://developer.api.autodesk.com/issues/v1/projects/{c_id}/issues"
    data = make_api_request(url)
    if isinstance(data, str): return data
    results = data.get("results", [])
    if not results and "data" in data: results = data["data"]
    if status_filter != "all":
        results = [i for i in results if i.get("attributes", i).get("status", "").lower() == status_filter]
    display = results[:limit]
    output = f"Found: {len(results)} '{status_filter}' issues (Showing {len(display)}):\n"
    for issue in display:
        attrs = issue.get("attributes", issue)
        title = attrs.get("title", "No Title")
        status = attrs.get("status", "Unknown")
        ident = attrs.get("identifier", "No ID")
        output += f"- #{ident}: {title} [{status}]\n"
    return output

@mcp.tool()
def get_data_connector_status(account_id: Optional[str] = None) -> str:
    """Checks Data Connector extraction status."""
    if not account_id:
        hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs_data, str) or not hubs_data.get("data"):
            return "Cannot find Account/Hub ID to use as default."
        raw_hub_id = hubs_data["data"][0]["id"]
        account_id = clean_id(raw_hub_id)

    c_id = clean_id(account_id)
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{c_id}/requests"
    data = make_api_request(url)
    if isinstance(data, str): return data
    results = data.get("data", [])
    if not results: return "No data connector requests found."
    output = f"Last 5 Data Connector Requests (Account {c_id}):\n"
    for req in results[:5]:
        desc = req.get("description", "No description")
        status = req.get("status", "Unknown")
        date = req.get("createdAt", "")
        output += f"- {date}: {desc} | Status: {status}\n"
    return output

@mcp.tool()
def create_project(
    project_name: str, 
    admin_email: Optional[str] = None,
    account_id: Optional[str] = None, 
    project_type: str = "Renovation", 
    currency: str = "EUR", 
    language: str = "en",
    timezone: str = "Europe/Amsterdam"
) -> str:
    """
    Creates a new project in ACC.
    SAFETY: If auto-detection of the admin fails, this tool will ask for an email.
    """
    # 1. AUTO-DETECT Account ID
    if not account_id:
        hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs_data, str) or not hubs_data.get("data"):
            return "Error: Cannot find Account/Hub ID."
        raw_hub_id = hubs_data["data"][0]["id"]
        account_id = clean_id(raw_hub_id)

    c_id = clean_id(account_id)
    url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects"
    
    # 2. FIND ACTING USER (With Safety Net)
    acting_user_id = get_admin_user_id(c_id, email=admin_email)
    
    # --- SAFETY NET TRIGGER ---
    if not acting_user_id:
        return (
            f"⚠️ ACTION REQUIRED: I could not automatically find an admin user to create project '{project_name}'. "
            "Please ask the user for their email address (e.g., 'What is your email?'). "
            "Once they provide it, retry this tool using the 'admin_email' parameter."
        )

    payload = {
        "name": project_name,
        "type": "production",
        "currency": currency,
        "timezone": timezone,
        "language": language,
        "projectType": project_type 
    }
    
    try:
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-user-id": acting_user_id
        }
        
        resp = requests.post(url, headers=headers, json=payload)
        
        if resp.status_code in [200, 201]:
            new_proj = resp.json()
            p_id = new_proj.get("id")
            return f"✅ Success! Project '{project_name}' created.\n- ID: {p_id}\n- Admin: {admin_email or 'Auto-detected'}"
        else:
            return f"❌ Error creating project: {resp.status_code} {resp.text}"
            
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def add_user_to_project(project_id: str, email: str, role: str = "project_member") -> str:
    """Adds a user to a project."""
    c_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/admin/v1/projects/{c_id}/users"
    
    payload = [{
        "email": email,
        "products": [{
            "key": "docs",
            "access": role
        }]
    }]
    
    try:
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        resp = requests.post(url, headers=headers, json=payload)
        
        if resp.status_code in [200, 201]:
            return f"✅ Invitation sent to {email} for project {c_id}."
        else:
            return f"❌ Error adding user: {resp.status_code} {resp.text}"
            
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
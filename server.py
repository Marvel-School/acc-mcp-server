import os
import requests
import logging
import time
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import quote
from fastmcp import FastMCP
from auth import get_token, BASE_URL_ACC
from api import (
    make_api_request, 
    make_graphql_request, 
    get_user_id_by_email, 
    get_acting_user_id,
    clean_id, 
    ensure_b_prefix, 
    encode_urn,
    get_cached_hub_id,
    resolve_to_version_id,
    safe_b64encode,
    get_viewer_domain,
    search_project_folder,
    fetch_paginated_data,
    get_project_issues,
    get_project_assets,
    get_account_users,
    invite_user_to_project
)

# Initialize Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastMCP
mcp = FastMCP("Autodesk ACC Agent")
PORT = int(os.environ.get("PORT", 8000))

# ==========================================
# DISCOVERY TOOLS
# ==========================================

@mcp.tool()
def list_hubs() -> str:
    data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
    if isinstance(data, str): return data
    output = "🏢 **Found Hubs:**\n"
    for h in data.get("data", []): output += f"- {h['attributes']['name']} (ID: {h['id']})\n"
    return output

@mcp.tool()
def list_projects(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 20) -> str:
    """
    Lists projects in the Hub (Pagination enabled).
    Fetches ALL projects via pagination, then filters and returns up to 'limit' results.
    """
    if not hub_id:
        hub_id = get_cached_hub_id()
        if not hub_id: return "Error: No Hubs found."
        
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
    
    # Use shared pagination logic (Style 'url' for Data Management API)
    all_projs = fetch_paginated_data(url, style='url')
    
    # Client-side filtering
    if name_filter:
        all_projs = [p for p in all_projs if name_filter.lower() in p['attributes']['name'].lower()]
    
    # Sort by name
    all_projs.sort(key=lambda x: x['attributes'].get('name', ''))

    output = f"📂 **Found {len(all_projs)} Projects:**\n"
    for p in all_projs[:limit]: 
        output += f"- **{p['attributes']['name']}**\n  ID: `{p['id']}`\n"
        
    if len(all_projs) > limit:
        output += f"\n*(Displaying {limit} of {len(all_projs)} results. Use 'name_filter' to refine.)*"
        
    return output

# ==========================================
# FILE & FOLDER TOOLS
# ==========================================

@mcp.tool()
def get_top_folders(project_id: str) -> str:
    hub_id = get_cached_hub_id()
    if not hub_id: return "Error: No Hubs."
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{ensure_b_prefix(project_id)}/topFolders"
    data = make_api_request(url)
    if isinstance(data, str): return data
    output = "root_folders:\n"
    for i in data.get("data", []): output += f"- {i['attributes']['displayName']} (ID: {i['id']})\n"
    return output

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
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
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
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
# 3D MODEL TOOLS
# ==========================================

@mcp.tool()
def list_designs(project_id: str) -> str:
    p_id = ensure_b_prefix(project_id)
    query = """query GetElementGroups($projectId: ID!) { 
        elementGroupsByProject(projectId: $projectId) { 
            results { id name alternativeIdentifiers { fileVersionUrn } } 
        } 
    }"""
    data = make_graphql_request(query, {"projectId": p_id})
    should_retry = False
    if not data or isinstance(data, str): should_retry = True
    elif isinstance(data, dict) and not data.get("elementGroupsByProject"): should_retry = True

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
    version_id = resolve_to_version_id(project_id, item_id)
    domain = get_viewer_domain(version_id)
    return f"https://{domain}/docs/files/projects/{clean_id(project_id)}?entityId={quote(version_id, safe='')}"

@mcp.tool()
def find_models(project_id: str, file_types: str = "rvt,rcp,dwg,nwc") -> str:
    """
    Finds models in the Project Files folder.
    file_types: Comma-separated list of file extensions (e.g. "rvt,dwg")
    """
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

@mcp.tool()
def search_files(project_id: str, query: str) -> str:
    """
    Search for files or folders within a project by name.
    """
    items = search_project_folder(project_id, query)
    if not items:
        return f"🔍 No files found matching '{query}' in Project {project_id}."
        
    output = f"🔍 **Search Results for '{query}':**\n"
    for i in items:
        name = i["attributes"]["displayName"]
        item_id = i['id']
        item_type = "📁" if i.get("type") == "folders" else "📄"
        output += f"{item_type} **{name}** (ID: `{item_id}`)\n"
    return output

@mcp.tool()
def get_model_tree(project_id: str, file_id: str) -> str:
    version_id = resolve_to_version_id(project_id, file_id)
    if not version_id or not version_id.startswith("urn:"):
         return f"❌ Error: I tried to resolve '{file_id}' to an ID but failed. Please try 'Find files' first to get the correct ID."

    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    urn = safe_b64encode(version_id)
    resp = requests.get(f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn}/metadata", headers=headers)
    
    if resp.status_code == 404: return "❌ File found, but it has no metadata. (Has it been fully processed in the viewer?)"
    if resp.status_code != 200: return f"❌ Autodesk API Error ({resp.status_code}): {resp.text}"
        
    data = resp.json().get("data", {}).get("metadata", [])
    if not data: return "❌ No 3D views found."
    guid = data[0]["guid"]
    
    resp_tree = requests.get(f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn}/metadata/{guid}", headers=headers, params={"forceget": "true"})
    
    if resp_tree.status_code == 202: return "⏳ The Model Tree is currently processing. Please try again in 1 minute."
    if resp_tree.status_code != 200: return f"❌ Failed to retrieve object tree (Status: {resp_tree.status_code})."
    
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
# ADMIN TOOLS
# ==========================================

@mcp.tool()
def create_project(
    project_name: str, 
    project_type: str = "Commercial", 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None, 
    address: Optional[str] = None, 
    city: Optional[str] = None, 
    country: Optional[str] = None, 
    job_number: Optional[str] = None
) -> str:
    """
    Creates a new project in the ACC Account.
    Arguments like dates and address are optional; if left blank, I will auto-generate them.
    """
    # 1. Get Authentication
    try:
        token = get_token() 
    except Exception as e:
        return f"❌ Auth Error: {e}"

    # 2. Get Account ID
    raw_hub_id = get_cached_hub_id()
    if not raw_hub_id: 
        logger.error("Could not find Hub/Account ID via Data Management API.")
        return "❌ Error: Could not find Hub/Account ID. Check if your app is added to the BIM 360/ACC Account Admin."
    
    account_id = clean_id(raw_hub_id) 
    logger.info(f"Targeting Account ID: {account_id}")

    # Resolve Acting Admin ID for 2-legged Auth
    admin_id = get_acting_user_id(account_id)
    if not admin_id:
        # Without x-user-id, Admin API often returns 401/403 for 2-legged tokens
        logger.warning("⚠️ Could not resolve Admin User ID. Request might fail with 401.")
    else:
        logger.info(f"Acting as Admin User ID: {admin_id}")

    # 3. Determine Endpoint (ACC Admin V1)
    url = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{account_id}/projects"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Add x-user-id header if resolved
    if admin_id:
        headers["x-user-id"] = admin_id

    # 4. Generate Data (ACC Admin API matches the docs: camelCase, 'type' for project category)
    today = datetime.now()
    next_year = today + timedelta(days=365)
    
    final_job_num = job_number if job_number else f"JN-{int(time.time())}"

    # Minimal + Essential payload based on verified documentation (NO currency/language)
    # The API explicitly rejected 'currency' and 'language' as unknown properties.
    # Uses environment variables for defaults if not provided in arguments.
    # Updated to remove specific real-world test addresses in favor of generic defaults.
    payload = {
        "name": project_name,
        "type": project_type,  
        "timezone": os.environ.get("DEFAULT_PROJECT_TIMEZONE", "Europe/Amsterdam"), 
        "jobNumber": final_job_num,
        "addressLine1": address or os.environ.get("DEFAULT_PROJECT_ADDRESS_LINE1", "123 Generic St"), 
        "city": city or os.environ.get("DEFAULT_PROJECT_CITY", "Metropolis"),
        "postalCode": os.environ.get("DEFAULT_PROJECT_POSTAL_CODE", "0000AA"),       
        "country": country or os.environ.get("DEFAULT_PROJECT_COUNTRY", "Netherlands")
    }

    logger.info(f"🚀 Creating Project '{project_name}' via ACC Admin API (Minimal)...")
    logger.info(f"Payload: {payload}")
    
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in [201, 202]:
        data = response.json()
        new_id = data.get("id")
        
        if response.status_code == 202:
             logger.info(f"INFO - Project creation initiated (Async). ID: {new_id}")
             msg = f"✅ **Success!** Project '{project_name}' creation initiated (Async)."
        else:
             msg = f"✅ **Success!** Project '{project_name}' created."
             
        return f"{msg}\nID: `{new_id}`\nJob #: {final_job_num}\nLoc: {payload.get('city')}, {payload.get('country')}"
    elif response.status_code == 409:
        return f"⚠️ A project with the name '{project_name}' already exists."
    else:
        logger.error(f"Failed to create project: {response.text}")
        return f"❌ Failed to create project. (Status: {response.status_code})\nError Details: {response.text}"

# ==========================================
# QUALITY & ASSETS TOOLS
# ==========================================

@mcp.tool()
def list_issues(project_id: str, status_filter: str = "open") -> str:
    """Lists project issues. status_filter can be 'open', 'closed' or 'all'."""
    # Logic: handle empty string or 'none'
    pass_status = status_filter
    if not status_filter or status_filter == "none" or status_filter == "all":
        pass_status = None 
    
    # Safe fallback if user strictly meant "Copy logic exactly", but "all" allows full list
    if status_filter == "open": pass_status = "open"

    return str(get_project_issues(project_id, pass_status))

@mcp.tool()
def list_assets(project_id: str, category_filter: str = "all") -> str:
    """Lists project assets. category_filter is optional."""
    # Logic: pass filter if it exists
    cat = category_filter if category_filter and category_filter not in ["all", "none"] else None
    return str(get_project_assets(project_id, cat))

# ==========================================
# ADMIN TOOLS
# ==========================================

@mcp.tool()
def list_users() -> str:
    """List all users in the account. No arguments."""
    return str(get_account_users(""))

@mcp.tool()
def add_user(project_id: str, email: str) -> str:
    """Add a user to a project. Both arguments are required."""
    return str(invite_user_to_project(project_id, email))
        
    output = f"📦 **Found {len(items)} Assets:**\n"
    output += "| ID | Name | Category | Status |\n"
    output += "|---|---|---|---|\n"
    
    for a in items[:20]:
        client_id = a.get('clientAssetId', a.get('id', '?'))
        # Name might be in 'description' or 'clientAssetId' depending on implementation
        # Assets V2 often uses 'clientAssetId' as the main identifier/name or specific custom fields
        # But 'categoryId' maps to category.
        
        # Let's try to find a name-like field
        # Usually Assets have 'clientAssetId' (User faced ID) and sometimes 'description'
        name = a.get('description', client_id).replace("|", "-")
        if len(name) > 30: name = name[:27] + "..."
        
        cat_node = a.get('category', {})
        cat_name = cat_node.get('name', 'General')
        
        status_node = a.get('status', {})
        status_name = status_node.get('name', status_node.get('displayName', 'Unknown'))
        
        output += f"| {client_id} | {name} | {cat_name} | {status_name} |\n"
        
    if len(items) > 20:
        output += f"\n*(Displaying 20 of {len(items)} assets)*"
        
    return output

if __name__ == "__main__":
    logger.info(f"Starting MCP Server on port {PORT}...")
    import uvicorn
    from starlette.middleware.base import BaseHTTPMiddleware

    # Create ASGI app from FastMCP
    # mcp.http_app is the underlying FastAPI/Starlette app
    if hasattr(mcp, "http_app") and mcp.http_app:
        app = mcp.http_app() if callable(mcp.http_app) else mcp.http_app
    elif hasattr(mcp, "_fastapi_app"):
         app = mcp._fastapi_app
    else:
         logger.warning("Could not find http_app or _fastapi_app on mcp object. Using mcp as app.")
         app = mcp

    # Middleware to fix 406 Not Acceptable error
    async def fix_accept_header(request, call_next):
        if "text/event-stream" not in request.headers.get("accept", ""):
            headers = dict(request.scope["headers"])
            current_accept = request.headers.get("accept", "*/*")
            headers[b"accept"] = f"{current_accept}, text/event-stream".encode()
            request.scope["headers"] = [(k, v) for k, v in headers.items()]
        
        response = await call_next(request)
        return response

    # Add middleware safely
    if hasattr(app, "add_middleware"):
        app.add_middleware(BaseHTTPMiddleware, dispatch=fix_accept_header)
    else:
        logger.error(f"Cannot add middleware. App type: {type(app)}")

    uvicorn.run(app, host="0.0.0.0", port=PORT)

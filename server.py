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
    get_viewer_domain
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
    if not hub_id:
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
    if not raw_hub_id: return "❌ Error: Could not find Hub/Account ID."
    account_id = clean_id(raw_hub_id) 

    # 3. Determine Endpoint (Switching to ACC Admin V1 API)
    # The ACC Admin API (v1) generally prefers snake_case for field names.
    url = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{account_id}/projects"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 4. Generate Data
    today = datetime.now()
    next_year = today + timedelta(days=365)
    
    # Auto-generate unique Job Number
    final_job_num = job_number if job_number else f"JN-{int(time.time())}"

    # ACC Admin payload structure (snake_case)
    payload = {
        "name": project_name,
        "project_type": project_type, # snake_case key
        "currency": "EUR",              
        "timezone": "Europe/Amsterdam", 
        "language": "en",
        "job_number": final_job_num,  # snake_case key
        "address": {
            "address_line_1": address or "Teststraat 123", # snake_case key
            "city": city or "Rotterdam",
            "postal_code": "3011AA",      # snake_case key
            "country": country or "Netherlands"
        }
    }

    logger.info(f"🚀 Creating Project '{project_name}' via ACC Admin API (SnakeCase)...")
    logger.info(f"Payload: {payload}")
    
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 201:
        new_id = response.json().get("id")
        return f"✅ **Success!** Project '{project_name}' created.\nID: `{new_id}`\nJob #: {final_job_num}\nLoc: Rotterdam, NL"
    elif response.status_code == 409:
        return f"⚠️ A project with the name '{project_name}' already exists."
    else:
        logger.error(f"Failed to create project: {response.text}")
        return f"❌ Failed to create project. (Status: {response.status_code})\nError Details: {response.text}"

if __name__ == "__main__":
    logger.info(f"Starting MCP Server on port {PORT}...")
    import uvicorn
    # Create ASGI app from FastMCP
    app = mcp._fastapi_app if hasattr(mcp, "_fastapi_app") else mcp

    # Add middleware to fix 406 Not Acceptable error from Copilot
    # Copilot sends Accept: application/json but starlette/sse might require text/event-stream
    # This middleware forces the Accept header if it's missing the required type
    @app.middleware("http")
    async def fix_accept_header(request, call_next):
        if "text/event-stream" not in request.headers.get("accept", ""):
            # Create a mutable copy of the headers path
            headers = dict(request.scope["headers"])
            # Append text/event-stream to Accept header
            current_accept = request.headers.get("accept", "*/*")
            headers[b"accept"] = f"{current_accept}, text/event-stream".encode()
            request.scope["headers"] = [(k, v) for k, v in headers.items()]
        
        response = await call_next(request)
        return response

    uvicorn.run(app, host="0.0.0.0", port=PORT)

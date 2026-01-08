from mcp.server.fastmcp import FastMCP
import requests
import time
import os

# --- CONFIGURATION ---
# Read secrets from Environment Variables
APS_CLIENT_ID = os.environ.get("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET")

# Initialize Server
mcp = FastMCP("Autodesk ACC Agent")

# Global token storage
token_cache = {"access_token": None, "expires_at": 0}

def get_token():
    """Helper to get a valid Autodesk Access Token."""
    global token_cache
    
    # Validation
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Error: APS_CLIENT_ID and APS_CLIENT_SECRET environment variables are missing.")

    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]
    
    # Request new token
    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = requests.auth.HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    data = {"grant_type": "client_credentials", "scope": "data:read"}
    
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

# --- TOOLS ---

@mcp.tool()
def list_hubs() -> str:
    """Lists all Autodesk Hubs (Company Accounts) the bot has access to."""
    try:
        token = get_token()
        url = "https://developer.api.autodesk.com/project/v1/hubs"
        headers = {"Authorization": f"Bearer {token}"}
        
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            return f"Error fetching hubs: {resp.text}"
        
        data = resp.json()
        output = "Found Hubs:\n"
        for hub in data.get("data", []):
            name = hub["attributes"]["name"]
            hub_id = hub["id"]
            output += f"- {name} (ID: {hub_id})\n"
        return output
    except Exception as e:
        return f"Failed to list hubs: {str(e)}"

@mcp.tool()
def list_projects(hub_id: str) -> str:
    """Lists all projects within a specific Hub ID."""
    try:
        token = get_token()
        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
        headers = {"Authorization": f"Bearer {token}"}
        
        resp = requests.get(url, headers=headers)
        data = resp.json()
        
        output = f"Projects in Hub {hub_id}:\n"
        for proj in data.get("data", []):
            name = proj["attributes"]["name"]
            proj_id = proj["id"]
            output += f"- {name} (ID: {proj_id})\n"
        return output
    except Exception as e:
        return f"Failed to list projects: {str(e)}"

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str) -> str:
    """Lists files and subfolders in a specific folder."""
    try:
        token = get_token()
        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{folder_id}/contents"
        headers = {"Authorization": f"Bearer {token}"}
        
        resp = requests.get(url, headers=headers)
        data = resp.json()
        
        output = f"Contents of folder {folder_id}:\n"
        for item in data.get("data", []):
            name = item["attributes"]["displayName"]
            item_type = item["type"]  # 'items' (file) or 'folders'
            item_id = item["id"]
            output += f"- [{item_type}] {name} (ID: {item_id})\n"
        return output
    except Exception as e:
        return f"Failed to list folder contents: {str(e)}"

@mcp.tool()
def get_top_folders(hub_id: str, project_id: str) -> str:
    """Gets the top-level folders (Project Files, etc) for a project."""
    try:
        token = get_token()
        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{project_id}/topFolders"
        headers = {"Authorization": f"Bearer {token}"}
        
        resp = requests.get(url, headers=headers)
        data = resp.json()
        
        output = "Top Level Folders:\n"
        for folder in data.get("data", []):
            name = folder["attributes"]["displayName"]
            folder_id = folder["id"]
            output += f"- {name} (ID: {folder_id})\n"
        return output
    except Exception as e:
        return f"Failed to get top folders: {str(e)}"

if __name__ == "__main__":
    # CRITICAL FIX: DigitalOcean tells us which PORT to use via Environment Variable.
    # If we ignore this, the app will crash.
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting Server on port {port}...")
    
    # Run in HTTP mode
    mcp.run(transport="http", host="0.0.0.0", port=port)
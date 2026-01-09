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

mcp = FastMCP("Autodesk ACC Agent")

# Global token storage
token_cache = {"access_token": None, "expires_at": 0}

def get_token():
    """Helper to get a valid Autodesk Access Token."""
    global token_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Error: APS_CLIENT_ID and APS_CLIENT_SECRET environment variables are missing.")

    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]

    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = requests.auth.HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    data = {"grant_type": "client_credentials", "scope": "data:read data:write"}

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

# --- HELPER: ENCODE URN ---
def encode_urn(urn: str) -> str:
    if not urn: return ""
    return quote(urn, safe='')

# --- TOOLS ---

@mcp.tool()
def list_hubs() -> str:
    """Lists all Autodesk Hubs."""
    try:
        token = get_token()
        url = "https://developer.api.autodesk.com/project/v1/hubs"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        data = resp.json()
        output = "Found Hubs:\n"
        for hub in data.get("data", []):
            output += f"- {hub['attributes']['name']} (ID: {hub['id']})\n"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def list_projects(hub_id: Optional[str] = None) -> str:
    """Lists projects. Auto-finds Hub ID if missing."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}

        if not hub_id:
            hubs_url = "https://developer.api.autodesk.com/project/v1/hubs"
            hubs_resp = requests.get(hubs_url, headers=headers)
            hubs_data = hubs_resp.json().get("data", [])
            if not hubs_data: return "Error: No Autodesk Hubs found."
            hub_id = hubs_data[0]["id"]

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
        resp = requests.get(url, headers=headers)
        
        if resp.status_code != 200:
            return f"Error listing projects: {resp.status_code} {resp.text}"

        data = resp.json()
        output = f"Projects in Hub {hub_id}:\n"
        for proj in data.get("data", []):
            output += f"- {proj['attributes']['name']} (ID: {proj['id']})\n"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_top_folders(project_id: str, hub_id: Optional[str] = None) -> str:
    """Gets top-level folders. Auto-finds Hub ID if missing."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}

        if not hub_id:
            hubs_url = "https://developer.api.autodesk.com/project/v1/hubs"
            hubs_resp = requests.get(hubs_url, headers=headers)
            hubs_data = hubs_resp.json().get("data", [])
            if not hubs_data: return "Error: No Hubs found."
            hub_id = hubs_data[0]["id"]

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{project_id}/topFolders"
        resp = requests.get(url, headers=headers)
        
        if resp.status_code != 200:
            return f"Error getting folders: {resp.status_code} {resp.text}"

        data = resp.json()
        output = "Top Level Folders:\n"
        for folder in data.get("data", []):
            output += f"- {folder['attributes']['displayName']} (ID: {folder['id']})\n"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str) -> str:
    """Lists files in a folder."""
    try:
        token = get_token()
        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{folder_id}/contents"
        headers = {"Authorization": f"Bearer {token}"}

        resp = requests.get(url, headers=headers)
        data = resp.json()

        output = f"Contents of folder {folder_id}:\n"
        for item in data.get("data", []):
            name = item["attributes"]["displayName"]
            item_type = item["type"]
            item_id = item["id"]
            output += f"- [{item_type}] {name} (ID: {item_id})\n"
        return output
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_file_details(project_id: str, item_id: str) -> str:
    """Gets detailed metadata. Handles URL encoding automatically."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        encoded_item_id = encode_urn(item_id)
        
        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/items/{encoded_item_id}"
        resp = requests.get(url, headers=headers)
        
        if resp.status_code != 200: return f"Error: {resp.status_code}"
        
        json_resp = resp.json()
        try:
            tip_id = json_resp["data"]["relationships"]["tip"]["data"]["id"]
        except KeyError:
             return "Error: Could not find latest version."
        
        encoded_tip_id = encode_urn(tip_id)
        v_url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/versions/{encoded_tip_id}"
        v_resp = requests.get(v_url, headers=headers)
        
        if v_resp.status_code != 200: return f"Error version: {v_resp.status_code}"
             
        attrs = v_resp.json().get("data", {}).get("attributes", {})
        
        return (
            f"📄 **File Details**\n"
            f"- **Name:** {attrs.get('displayName')}\n"
            f"- **Version:** v{attrs.get('versionNumber')}\n"
            f"- **Latest Version ID:** {tip_id}\n"
            f"- **Size:** {attrs.get('storageSize')} bytes\n"
        )
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_download_url(project_id: str, version_id: str) -> str:
    """Generates a signed download URL via S3 (Fixes 403 Forbidden)."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        encoded_version_id = encode_urn(version_id)
        
        v_url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/versions/{encoded_version_id}"
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
                return f"⬇️ **[Click Here to Download]({oss_resp.json()['url']})**"
            else:
                return f"Error creating download link: {oss_resp.text}"

        except KeyError:
             return "Error: Could not determine storage location."
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    mcp.run(transport="http", host="0.0.0.0", port=port)
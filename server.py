import os
import time
import requests
import traceback
from urllib.parse import quote
from fastmcp import FastMCP
from typing import Optional

# --- CONFIGURATIE ---
APS_CLIENT_ID = os.environ.get("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.environ.get("APS_CLIENT_SECRET")
PORT = int(os.environ.get("PORT", 8000))

# Initialiseer FastMCP
mcp = FastMCP("Autodesk ACC Agent")

# Globale token cache
token_cache = {"access_token": None, "expires_at": 0}

# Base URLs
BASE_URL_DM = "https://developer.api.autodesk.com"
BASE_URL_ACC = "https://developer.api.autodesk.com/construction"

# --- HELPER: AUTHENTICATIE ---
def get_token():
    """Haalt een Autodesk Token op met alle benodigde rechten."""
    global token_cache
    if not APS_CLIENT_ID or not APS_CLIENT_SECRET:
        raise ValueError("Fout: APS_CLIENT_ID en APS_CLIENT_SECRET ontbreken.")

    if time.time() < token_cache["expires_at"]:
        return token_cache["access_token"]

    url = "https://developer.api.autodesk.com/authentication/v2/token"
    auth = requests.auth.HTTPBasicAuth(APS_CLIENT_ID, APS_CLIENT_SECRET)
    
    # Scopes voor Admin, Docs, Issues en Assets
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
        print(f"Auth Fout: {e}")
        raise e

# --- HELPER: ID CLEANING ---
def clean_id(id_str: str) -> str:
    """Verwijdert 'b.' prefix (Nodig voor Admin/Assets/Issues)."""
    if not id_str: return ""
    return id_str.replace("b.", "")

def ensure_b_prefix(id_str: str) -> str:
    """Zorgt voor 'b.' prefix (Nodig voor Files/Docs)."""
    if not id_str: return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"

def encode_urn(urn: str) -> str:
    """URL Encode voor bestandsversies."""
    if not urn: return ""
    return quote(urn, safe='')

# --- HELPER: API REQUEST ---
def make_api_request(url: str):
    """Voert de request uit en checkt op fouten."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        
        if resp.status_code == 403:
            return "Fout 403: Geen toegang. Check of de App is toegevoegd in ACC Admin."
        if resp.status_code == 404:
            return "Fout 404: Niet gevonden. Check de ID's."
        
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return f"Request Fout: {str(e)}"

# --- TOOL 0: HUBS (RESTORED) ---
@mcp.tool()
def list_hubs() -> str:
    """
    Toont alle beschikbare Autodesk Hubs.
    Handig om handmatig een Account ID te vinden.
    """
    url = "https://developer.api.autodesk.com/project/v1/hubs"
    data = make_api_request(url)
    if isinstance(data, str): return data

    output = "Gevonden Hubs:\n"
    for hub in data.get("data", []):
        name = hub['attributes']['name']
        hub_id = hub['id']
        clean_acc_id = clean_id(hub_id)
        output += f"- {name}\n  Hub ID: {hub_id}\n  Account ID (voor Admin): {clean_acc_id}\n"
    return output

# --- TOOL 1: PROJECTEN (DATA MANAGEMENT) ---
@mcp.tool()
def list_projects_dm(hub_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    """
    Haalt projecten op via Data Management.
    Gebruik 'name_filter' om specifiek te zoeken (sneller).
    """
    try:
        if not hub_id:
            hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
            if isinstance(hubs_data, str) or not hubs_data.get("data"): return "Geen Hubs gevonden."
            hub_id = hubs_data["data"][0]["id"]

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
        data = make_api_request(url)
        if isinstance(data, str): return data

        all_projects = data.get("data", [])
        
        # 1. Filteren
        if name_filter:
            lower_filter = name_filter.lower()
            filtered = [p for p in all_projects if lower_filter in p['attributes']['name'].lower()]
        else:
            filtered = all_projects

        # 2. Limiteren (Snelheidswinst!)
        display = filtered[:limit]
        count = len(filtered)

        output = f"Gevonden: {count} projecten in Hub {hub_id} (Toon top {len(display)}):\n"
        for proj in display:
            output += f"- {proj['attributes']['name']} (ID: {proj['id']})\n"
            
        if count > limit:
            output += f"\n... (en nog {count - limit} projecten. Gebruik 'name_filter' om specifieker te zoeken.)"
            
        return output
    except Exception as e:
        return f"Error: {str(e)}"

# --- TOOL 2: BESTANDEN (FILES) ---
@mcp.tool()
def list_folder_contents(project_id: str, folder_id: str, limit: int = 20) -> str:
    """Toont bestanden in een map. Limiet standaard op 20 om snelheid te houden."""
    p_id = ensure_b_prefix(project_id)
    url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/folders/{folder_id}/contents"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    items = data.get("data", [])
    display = items[:limit]
    
    output = f"Inhoud map {folder_id} (Top {len(display)} van {len(items)}):\n"
    for item in display:
        name = item["attributes"]["displayName"]
        item_type = item["type"] # items:autodesk.bim360:File of Folder
        item_id = item["id"]
        icon = "📁" if "Folder" in item_type else "📄"
        output += f"- {icon} {name} (ID: {item_id})\n"
        
    if len(items) > limit:
        output += "\n... (Lijst ingekort voor snelheid.)"
    return output

@mcp.tool()
def get_download_url(project_id: str, version_id: str) -> str:
    """Genereert een downloadlink voor een bestand."""
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        p_id = ensure_b_prefix(project_id)
        encoded_version_id = encode_urn(version_id)
        
        # Stap 1: Opslaglocatie vinden
        v_url = f"https://developer.api.autodesk.com/data/v1/projects/{p_id}/versions/{encoded_version_id}"
        v_resp = requests.get(v_url, headers=headers)
        if v_resp.status_code != 200: return f"Fout bij versie ophalen: {v_resp.text}"
        
        storage_urn = v_resp.json()["data"]["relationships"]["storage"]["data"]["id"]
        parts = storage_urn.split("/")
        object_key = parts[-1]
        bucket_key = parts[-2].split(":")[-1]
        
        # Stap 2: S3 Link maken
        oss_url = f"https://developer.api.autodesk.com/oss/v2/buckets/{bucket_key}/objects/{object_key}/signeds3download"
        oss_resp = requests.get(oss_url, headers=headers, params={"minutesExpiration": 60})
        
        if oss_resp.status_code == 200:
            return f"⬇️ **[Klik hier om te downloaden]({oss_resp.json()['url']})**"
        else:
            return f"Fout bij maken link: {oss_resp.text}"
    except Exception as e:
        return f"Error: {str(e)}"

# --- TOOL 3: ADMIN API (UPDATED SMART) ---
@mcp.tool()
def get_account_projects_admin(account_id: Optional[str] = None, name_filter: Optional[str] = None, limit: int = 10) -> str:
    """
    (Admin API) Lijst projecten met admin details.
    SMART: Als account_id ontbreekt, zoekt hij deze automatisch op.
    """
    # 1. AUTO-DETECT ID als deze ontbreekt
    if not account_id:
        hubs_data = make_api_request("https://developer.api.autodesk.com/project/v1/hubs")
        if isinstance(hubs_data, str) or not hubs_data.get("data"):
            return "Kan geen Account/Hub ID vinden om als standaard te gebruiken."
        
        # De Hub ID (bv. "b.1234...") omzetten naar Account ID ("1234...")
        raw_hub_id = hubs_data["data"][0]["id"]
        account_id = clean_id(raw_hub_id)

    # 2. ORIGINELE LOGICA
    c_id = clean_id(account_id)
    url = f"{BASE_URL_ACC}/admin/v1/accounts/{c_id}/projects"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    results = data if isinstance(data, list) else data.get("results", [])
    
    # Filteren & Limiteren
    if name_filter:
        results = [p for p in results if name_filter.lower() in p.get("name", "").lower()]
    
    count = len(results)
    display = results[:limit]

    output = f"Admin Projecten (Gevonden: {count}) voor Account {c_id}:\n"
    for proj in display:
        name = proj.get("name", "Onbekend")
        p_id = proj.get("id")
        output += f"- {name} (ID: {p_id})\n"
        
    if count > limit:
        output += f"... en {count - limit} meer."
    return output

# --- TOOL 4: ASSETS API ---
@mcp.tool()
def list_assets(project_id: str, limit: int = 10) -> str:
    """Haalt Assets (bouwdelen/installaties) op. Maximaal 10 standaard."""
    c_id = clean_id(project_id)
    url = f"{BASE_URL_ACC}/assets/v2/projects/{c_id}/assets"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    results = data.get("results", [])
    if not results: return "Geen assets gevonden."

    display = results[:limit]
    output = f"Assets in project {c_id} (Top {len(display)}):\n"

    for asset in display:
        ref = asset.get("clientAssetId", "Geen Ref")
        cat = asset.get("category", {}).get("name", "-")
        status = asset.get("status", {}).get("name", "Unknown")
        output += f"- [{cat}] {ref} | Status: {status}\n"
        
    if len(results) > limit:
        output += "\n... (Gebruik specifiekere filters in de toekomst)"
    return output

# --- TOOL 5: ISSUES API ---
@mcp.tool()
def list_issues(project_id: str, status_filter: str = "open", limit: int = 10) -> str:
    """
    Haalt issues op.
    Args:
        status_filter: 'open' (standaard), 'answered', 'closed', of 'all'.
        limit: Maximaal aantal resultaten (standaard 10).
    """
    c_id = clean_id(project_id)
    url = f"https://developer.api.autodesk.com/issues/v1/projects/{c_id}/issues"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    results = data.get("results", [])
    if not results and "data" in data: results = data["data"]

    # Filter op status
    if status_filter != "all":
        results = [i for i in results if i.get("attributes", i).get("status", "").lower() == status_filter]

    display = results[:limit]
    
    output = f"Gevonden: {len(results)} '{status_filter}' issues (Toon {len(display)}):\n"
    
    for issue in display:
        attrs = issue.get("attributes", issue)
        title = attrs.get("title", "Geen Titel")
        status = attrs.get("status", "Unknown")
        ident = attrs.get("identifier", "No ID")
        output += f"- #{ident}: {title} [{status}]\n"
        
    if len(results) > limit:
        output += "\n... (Meer issues gevonden. Vraag om details.)"
        
    return output

# --- TOOL 6: DATA CONNECTOR ---
@mcp.tool()
def get_data_connector_status(account_id: str) -> str:
    """Checkt de status van Data Connector extracties (laatste 5)."""
    c_id = clean_id(account_id)
    url = f"https://developer.api.autodesk.com/data-connector/v1/accounts/{c_id}/requests"
    
    data = make_api_request(url)
    if isinstance(data, str): return data

    results = data.get("data", [])
    if not results: return "Geen data connector requests gevonden."

    output = "Laatste 5 Data Connector Requests:\n"
    for req in results[:5]:
        desc = req.get("description", "Geen beschrijving")
        status = req.get("status", "Unknown")
        date = req.get("createdAt", "")
        output += f"- {date}: {desc} | Status: {status}\n"
    return output

if __name__ == "__main__":
    print(f"Starting MCP Server on port {PORT}...")
    mcp.run(transport="http", host="0.0.0.0", port=PORT)
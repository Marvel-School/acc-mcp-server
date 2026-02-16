"""
Autodesk Platform Services — API Client

All HTTP requests go through _make_request() which provides:
  - Auto-auth (Bearer token from auth.py)
  - Auto-EMEA (x-ads-region header)
  - Auto-retry on 401 (stale token refresh)
  - Auto-retry on 429 (rate limit with Retry-After)
  - Default 15s timeout (prevents hanging requests)
"""

import os
import re
import logging
import time
import base64
import requests
from typing import Optional, Dict, Any, List, Union
from urllib.parse import quote

from auth import get_token

logger = logging.getLogger(__name__)



def _strip_b_prefix(id_str: str) -> str:
    """Safely removes the 'b.' prefix from Hub/Project IDs."""
    return id_str[2:] if id_str.startswith("b.") else id_str


def ensure_b_prefix(id_str: Optional[str]) -> str:
    """Ensure a hub/project ID has the 'b.' prefix."""
    if not id_str:
        return ""
    return id_str if id_str.startswith("b.") else f"b.{id_str}"


def encode_urn(urn: Optional[str]) -> str:
    """URL-encode a URN for use in path segments."""
    return quote(urn, safe="") if urn else ""


def safe_b64encode(value: Optional[str]) -> str:
    """Base64url-encode a URN (no padding) for Model Derivative API."""
    if not value:
        return ""
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8").rstrip("=")



def _make_request(
    method: str,
    url: str,
    *,
    extra_headers: Optional[Dict] = None,
    retry_on_401: bool = True,
    **kwargs,
) -> requests.Response:
    """
    Centralized HTTP helper.

    Every request automatically gets:
      - Authorization: Bearer <token>
      - x-ads-region: EMEA
      - 15 s default timeout (callers can override)
      - One retry on 401 (token refresh)
      - One retry on 429 (respects Retry-After header)

    Raises:
        ValueError: On any 4xx/5xx response (with Autodesk error detail).
    """
    kwargs.setdefault("timeout", 15)
    max_attempts = 2 if retry_on_401 else 1
    last_resp: requests.Response = None  # type: ignore[assignment]

    try:
        for attempt in range(max_attempts):
            token = get_token(force_refresh=(attempt > 0))
            headers = {
                "Authorization": f"Bearer {token}",
                "x-ads-region": "EMEA",
            }
            if extra_headers:
                headers.update(extra_headers)

            last_resp = requests.request(method, url, headers=headers, **kwargs)

            if last_resp.status_code == 429:
                wait = int(last_resp.headers.get("Retry-After", 5))
                logger.warning(f"429 on {method} {url[:80]} — waiting {wait}s")
                time.sleep(wait)
                last_resp = requests.request(method, url, headers=headers, **kwargs)
                break

            if last_resp.status_code == 401 and attempt == 0 and retry_on_401:
                logger.warning(f"401 on {method} {url[:80]} — refreshing token")
                continue

            break

        try:
            last_resp.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            error_detail = last_resp.text
            try:
                error_json = last_resp.json()
                if "errors" in error_json:
                    error_detail = str(error_json["errors"])
                else:
                    error_detail = error_json.get("detail", last_resp.text)
            except ValueError:
                pass
            logger.error(f"API HTTP Error ({last_resp.status_code}): {error_detail}")
            raise ValueError(f"Autodesk API Error {last_resp.status_code}: {error_detail}") from http_err

        return last_resp

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"Request Execution Failed: {str(e)}")
        raise



def _get_admin_user_id(account_id: str) -> str:
    """Fetches the Autodesk User ID for the Account Admin, using a local dev fallback if needed."""
    admin_email = os.getenv("ACC_ADMIN_EMAIL")
    if not admin_email:
        if os.getenv("WEBSITE_SITE_NAME") or os.getenv("FUNCTIONS_WORKER_RUNTIME"):
            raise ValueError("CRITICAL: ACC_ADMIN_EMAIL environment variable is missing in production environment.")
        logger.warning("ACC_ADMIN_EMAIL missing from env. Defaulting to local dev fallback...")
        admin_email = "marvel.tiyjudy@ssc4tbi.nl"
    return get_user_id_by_email(account_id, admin_email)


def get_user_id_by_email(account_id: str, email: str) -> str:
    """Fetches the internal Autodesk User ID for an email address."""
    endpoint = f"https://developer.api.autodesk.com/hq/v1/regions/eu/accounts/{account_id}/users/search?email={email}"
    resp = _make_request("GET", endpoint)
    users = resp.json()
    if not users:
        raise ValueError(f"Could not find an Autodesk user with email: {email}")
    user_id = users[0].get("id")
    if not user_id:
        raise ValueError(f"Autodesk returned a user for {email}, but the 'id' field is missing.")
    return user_id



_hub_cache: Dict[str, Optional[str]] = {"id": None}


def _get_hub_id() -> Optional[str]:
    """Returns the first accessible hub ID (cached after first call)."""
    if _hub_cache["id"]:
        return _hub_cache["id"]
    hubs = get_hubs()
    if hubs:
        _hub_cache["id"] = hubs[0].get("id")
    return _hub_cache["id"]


def get_hubs() -> list:
    """Fetches all accessible Hubs (BIM 360 / ACC)."""
    resp = _make_request("GET", "https://developer.api.autodesk.com/project/v1/hubs")
    return resp.json().get("data", [])


def get_projects(hub_id: str, limit: int = 50, fields: Optional[list] = None) -> list:
    """List all projects in a hub (follows pagination automatically).

    Args:
        hub_id: Hub ID (starts with 'b.').
        limit:  Max projects per page (default 50, max 100 per Autodesk API).
        fields: Optional list of extra fields to include (e.g. ["status", "projectValue"]).
    """
    hub_id = ensure_b_prefix(hub_id)
    base = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects?page[limit]={limit}"
    if fields:
        base += f"&fields[projects]={','.join(fields)}"
    url: Optional[str] = base
    all_projects: list = []

    for _ in range(50):  # safety cap
        resp = _make_request("GET", url)  # type: ignore[arg-type]
        data = resp.json()
        all_projects.extend(data.get("data", []))

        next_link = data.get("links", {}).get("next")
        if isinstance(next_link, dict):
            url = next_link.get("href")
        elif isinstance(next_link, str):
            url = next_link
        else:
            url = None

        if not url:
            break

    return all_projects


def find_project_globally(name_query: str) -> Optional[tuple]:
    """
    Search for a project by name across ALL accessible hubs.
    Case-insensitive substring match.

    Returns:
        (hub_id, project_id, project_name) or None.
    """
    try:
        logger.info(f"Searching globally for project: {name_query}")
        hubs = get_hubs()
        if not hubs:
            logger.error("No hubs found.")
            return None

        search_term = name_query.lower().strip()

        for hub in hubs:
            hub_id = hub.get("id")
            hub_name = hub.get("attributes", {}).get("name", "Unknown")
            if not hub_id:
                continue

            logger.info(f"  Searching hub: {hub_name}")
            projects = get_projects(hub_id)

            for p in projects:
                p_name = p.get("attributes", {}).get("name", "")
                if search_term in p_name.lower():
                    logger.info(f"  Found: {p_name}")
                    return (hub_id, p.get("id"), p_name)

        logger.warning(f"Project '{name_query}' not found")
        return None
    except Exception as e:
        logger.error(f"Project search error: {e}")
        return None



def get_top_folders(hub_id: str, project_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches top-level folders for a project."""
    try:
        hub_clean = ensure_b_prefix(hub_id)
        proj_clean = ensure_b_prefix(project_id)

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_clean}/projects/{proj_clean}/topFolders"
        resp = _make_request("GET", url)

        return [
            {
                "id": f.get("id"),
                "name": f.get("attributes", {}).get("displayName"),
                "type": f.get("type"),
            }
            for f in resp.json().get("data", [])
        ]
    except Exception as e:
        logger.error(f"Top Folders Exception: {e}")
        return f"Error: {e}"


def get_folder_contents(project_id: str, folder_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches contents of a folder with tip-version URN extraction for files."""
    try:
        proj_clean = ensure_b_prefix(project_id)
        folder_encoded = encode_urn(folder_id)

        url = f"https://developer.api.autodesk.com/data/v1/projects/{proj_clean}/folders/{folder_encoded}/contents"
        resp = _make_request("GET", url)

        result = []
        for item in resp.json().get("data", []):
            item_type = item.get("type")
            attrs = item.get("attributes", {})

            parsed = {
                "id": item.get("id"),
                "name": attrs.get("displayName"),
                "type": item_type,
            }

            if item_type == "items":
                tip = (
                    item.get("relationships", {})
                    .get("tip", {})
                    .get("data", {})
                    .get("id")
                )
                if tip:
                    parsed["tipVersionUrn"] = tip
                parsed["itemType"] = "file"
            elif item_type == "folders":
                parsed["itemType"] = "folder"

            result.append(parsed)

        return result
    except Exception as e:
        logger.error(f"Folder Contents Exception: {e}")
        return f"Error: {e}"


def find_design_files(
    hub_id: str, project_id: str, extensions: str = "rvt"
) -> Union[List[Dict[str, Any]], str]:
    """
    BFS file search through 'Project Files' folder.
    Depth-limited to 3 levels, max 50 folders scanned.
    """
    try:
        logger.info(f"Searching for files: {extensions}")
        top_folders = get_top_folders(hub_id, project_id)

        if isinstance(top_folders, str):
            return f"Failed to get top folders: {top_folders}"

        root_id = None
        for folder in top_folders:
            if folder.get("name") == "Project Files":
                root_id = folder.get("id")
                break
        if not root_id and top_folders:
            root_id = top_folders[0].get("id")
        if not root_id:
            return "No folders found in project"

        queue = [{"id": root_id, "name": "Project Files", "depth": 0}]
        matching = []
        ext_list = [e.strip().lower() for e in extensions.split(",")]
        max_folders = 50
        max_depth = 3
        scanned = 0

        while queue and scanned < max_folders:
            current = queue.pop(0)
            scanned += 1

            logger.info(f"  Scanning {scanned}/{max_folders} (depth {current['depth']}): {current['name']}")
            contents = get_folder_contents(project_id, current["id"])

            if isinstance(contents, str):
                continue

            for item in contents:
                if item.get("itemType") == "file":
                    name = item.get("name", "")
                    if any(name.lower().endswith(f".{ext}") for ext in ext_list):
                        matching.append({
                            "name": name,
                            "item_id": item.get("id"),
                            "version_id": item.get("tipVersionUrn"),
                            "folder_path": current["name"],
                        })
                        logger.info(f"    Found: {name}")

                elif item.get("itemType") == "folder" and current["depth"] < max_depth:
                    sub_name = item.get("name", "Unknown")
                    queue.append({
                        "id": item.get("id"),
                        "name": f"{current['name']}/{sub_name}",
                        "depth": current["depth"] + 1,
                    })

        logger.info(f"Search complete: {scanned} folders, {len(matching)} files")
        return matching
    except Exception as e:
        logger.error(f"Design file search error: {e}")
        return f"Error: {e}"



def resolve_file_to_urn(project_id: str, identifier: str) -> str:
    """
    Smart resolver — accepts a filename OR a URN.
    Searches project folders by name if needed.

    Raises:
        ValueError on failure.
    """
    try:
        if identifier.startswith("urn:adsk"):
            logger.info("  Identifier is already a URN")
            return identifier

        logger.info(f"  Searching for filename: {identifier}")

        hub_id = _get_hub_id()
        if not hub_id:
            raise ValueError("Could not determine hub_id for file search")

        extensions = ["rvt", "dwg", "nwc", "rcp", "ifc", "nwd"]
        if "." in identifier:
            ext = identifier.rsplit(".", 1)[-1].lower()
            if ext in extensions:
                extensions.remove(ext)
                extensions.insert(0, ext)

        target = identifier.lower()

        for ext in extensions:
            files = find_design_files(hub_id, project_id, ext)
            if isinstance(files, str):
                continue
            for f in files:
                name = f.get("name", "").lower()
                if target == name or target in name:
                    item_id = f.get("item_id")
                    if item_id:
                        logger.info(f"  Resolved to: {item_id[:60]}...")
                        return item_id

        raise ValueError(f"Could not find file matching '{identifier}' in project")
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Error resolving file: {e}")


def get_latest_version_urn(project_id: str, item_id: str) -> Optional[str]:
    """Resolves a lineage URN to its latest version URN via the tip relationship."""
    try:
        if "fs.file" in item_id or "version=" in item_id:
            logger.info(f"Already a version URN: {item_id[:60]}...")
            return item_id

        proj_clean = ensure_b_prefix(project_id)
        item_encoded = encode_urn(item_id)

        url = f"https://developer.api.autodesk.com/data/v1/projects/{proj_clean}/items/{item_encoded}"
        resp = _make_request("GET", url)

        tip = (
            resp.json()
            .get("data", {})
            .get("relationships", {})
            .get("tip", {})
            .get("data", {})
            .get("id")
        )
        if tip:
            logger.info(f"Resolved to version: {tip[:60]}...")
        else:
            logger.warning("No tip version found in item relationships")
        return tip
    except Exception as e:
        logger.error(f"Version resolution error: {e}")
        return None


def inspect_generic_file(project_id: str, file_id: str) -> str:
    """
    Inspects a file's Model Derivative translation status.
    Accepts filename, lineage URN, or version URN.
    """
    try:
        logger.info(f"Inspecting: {file_id[:60]}...")

        try:
            resolved = resolve_file_to_urn(project_id, file_id)
        except ValueError as e:
            return str(e)

        if "lineage" in resolved:
            version_urn = get_latest_version_urn(project_id, resolved)
            if not version_urn:
                return "Error: Could not resolve lineage URN to version URN."
        elif "fs.file" in resolved or "version=" in resolved:
            version_urn = resolved
        else:
            version_urn = get_latest_version_urn(project_id, resolved)
            if not version_urn:
                version_urn = resolved

        encoded = safe_b64encode(version_urn)
        url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{encoded}/manifest"
        resp = _make_request("GET", url)

        if resp.status_code == 202:
            return "Processing — translation in progress."

        manifest = resp.json()
        status = manifest.get("status", "unknown").lower()
        progress = manifest.get("progress", "unknown")

        status_map = {
            "success": f"Ready for Extraction (translation complete, progress: {progress})",
            "inprogress": f"Processing (translation {progress}% complete)",
            "failed": "Translation Failed — check file format or try re-uploading",
            "timeout": "Translation Timeout — file may be too large or complex",
        }
        return status_map.get(status, f"Status: {status} (progress: {progress})")
    except Exception as e:
        logger.error(f"Inspection error: {e}")
        return f"Error: {e}"



def get_view_guid_only(version_urn: str) -> str:
    """
    Fetches the first available view GUID for a model.

    Raises:
        ValueError on missing views.
    """
    urn_b64 = safe_b64encode(version_urn)
    url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn_b64}/metadata"

    resp = _make_request("GET", url)

    views = resp.json().get("data", {}).get("metadata", [])
    if not views:
        raise ValueError("No views found in model metadata.")

    guid = views[0].get("guid")
    logger.info(f"  View GUID: {guid}")
    return guid


def stream_count_elements(version_urn: str, category_name: str) -> int:
    """
    Streams metadata and counts elements matching a category via regex.

    Processes the HTTP response in 64 KB chunks — no full JSON in memory.
    Smart singularization: "Walls" also matches "Wall".

    Raises:
        ValueError on HTTP errors or timeouts.
    """
    logger.info(f"[Streaming Scanner] Counting: {category_name}")

    terms = {category_name}
    if category_name.lower().endswith("s") and len(category_name) > 2:
        terms.add(category_name[:-1])
    logger.info(f"  Search terms: {list(terms)}")

    guid = get_view_guid_only(version_urn)

    urn_b64 = safe_b64encode(version_urn)
    url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn_b64}/metadata/{guid}"

    terms_pattern = b"|".join(re.escape(t).encode() for t in terms)
    pattern = re.compile(
        rb':\s*"[^"]*(' + terms_pattern + rb')[^"]*"', re.IGNORECASE
    )

    count = 0
    buffer = b""

    resp = _make_request("GET", url, stream=True, timeout=120)
    try:
        resp.raise_for_status()

        for chunk in resp.iter_content(chunk_size=65536):
            if not chunk:
                continue

            buffer += chunk
            matches = list(pattern.finditer(buffer))
            count += len(matches)

            if matches:
                buffer = buffer[matches[-1].end():]
            else:
                buffer = buffer[-512:]

    except requests.exceptions.HTTPError as e:
        raise ValueError(f"HTTP error streaming metadata: {e}")
    except requests.exceptions.Timeout:
        raise ValueError("Timeout streaming metadata.")
    finally:
        resp.close()

    logger.info(f"  Found {count} elements matching '{category_name}'.")
    return count


def trigger_translation(version_urn: str) -> Union[Dict[str, Any], str]:
    """
    Triggers a fresh Model Derivative translation job (SVF Classic).
    Uses x-ads-force to overwrite existing partial derivatives.
    """
    try:
        logger.info(f"[Translation] Starting job for: {version_urn[:80]}...")

        encoded = safe_b64encode(version_urn)
        payload = {
            "input": {"urn": encoded},
            "output": {
                "formats": [{"type": "svf", "views": ["2d", "3d"]}]
            },
        }

        resp = _make_request(
            "POST",
            "https://developer.api.autodesk.com/modelderivative/v2/designdata/job",
            extra_headers={
                "x-ads-force": "true",
                "Content-Type": "application/json",
            },
            json=payload,
        )

        result = resp.json()
        logger.info(f"  Job submitted: {result.get('result', 'unknown')}")
        return result

    except requests.exceptions.Timeout:
        return "Error: translation job request timed out."
    except requests.exceptions.RequestException as e:
        return f"Error: network error during translation: {e}"
    except Exception as e:
        logger.error(f"Translation exception: {e}")
        return f"Error: {e}"



def create_acc_project(hub_id: str, project_name: str, project_type: str = "BIM360") -> dict:
    """Creates a project using the ACC Account Admin API."""
    account_id = _strip_b_prefix(hub_id)
    user_id = _get_admin_user_id(account_id)

    endpoint = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{account_id}/projects"
    payload = {
        "name": project_name,
        "type": "Office",
        "platform": "acc" if project_type.upper() == "ACC" else "bim360",
    }

    logger.info(f"POSTing to {endpoint} with User-Id: {user_id}")
    resp = _make_request("POST", endpoint, json=payload, extra_headers={"User-Id": user_id})
    logger.info("Project creation API responded successfully.")
    return resp.json()


def get_project_users(hub_id: str, project_id: str) -> list:
    """List users in a project (requires Admin). Paginates up to 5 pages."""
    clean_project_id = _strip_b_prefix(project_id)
    endpoint = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_project_id}/users"

    all_users: list = []
    url: Optional[str] = endpoint

    for _ in range(5):  # safety cap
        resp = _make_request("GET", url)  # type: ignore[arg-type]
        data = resp.json()
        all_users.extend(data.get("results", []))

        next_url = data.get("pagination", {}).get("nextUrl")
        if next_url:
            url = next_url
        else:
            break

    return all_users


def add_project_user(hub_id: str, project_id: str, email: str, products: Optional[list] = None) -> dict:
    """
    Add a user to a project.

    Args:
        hub_id: The Hub ID (needed to resolve admin User-Id).
        project_id: The project ID.
        email: User's email address.
        products: Product keys (e.g. ["docs"]). Defaults to ["docs"].
    """
    account_id = _strip_b_prefix(hub_id)
    clean_project_id = _strip_b_prefix(project_id)

    if products is None:
        products = ["docs"]

    user_id = _get_admin_user_id(account_id)

    endpoint = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_project_id}/users"
    payload = {
        "email": email,
        "products": [{"key": p, "access": "administrator"} for p in products],
    }
    resp = _make_request("POST", endpoint, json=payload, extra_headers={"User-Id": user_id})
    return resp.json()


def get_all_hub_users(hub_id: str, max_projects: int = 20) -> list:
    """Aggregate users and their product entitlements across all projects in a hub.

    Scans up to *max_projects* projects, collects every user encountered, and
    merges their product assignments into a single record per email.

    Returns:
        List of dicts: [{"email": ..., "name": ..., "products": ["Build", ...]}]
    """
    projects = get_projects(hub_id)[:max_projects]
    logger.info(f"[Hub Audit] Scanning {len(projects)} projects in hub {hub_id}")

    user_map: Dict[str, Dict[str, Any]] = {}

    for p in projects:
        pid = p.get("id", "")
        p_name = p.get("attributes", {}).get("name", "Unknown")
        try:
            members = get_project_users(hub_id, pid)
        except Exception as e:
            logger.warning(f"  Skipping project '{p_name}': {e}")
            continue

        for member in members:
            email = (member.get("email") or "").lower()
            if not email:
                continue

            if email not in user_map:
                user_map[email] = {
                    "email": email,
                    "name": member.get("name", email),
                    "products": set(),
                }

            for prod in member.get("products", []):
                key = prod.get("key", "")
                if key:
                    user_map[email]["products"].add(key)

    result = []
    for entry in user_map.values():
        entry["products"] = sorted(entry["products"])
        result.append(entry)

    logger.info(f"[Hub Audit] Found {len(result)} unique users across {len(projects)} projects")
    return sorted(result, key=lambda u: u["email"])


def create_folder(project_id: str, parent_folder_id: str, folder_name: str) -> dict:
    """Creates a subfolder inside a parent folder using the Data Management API.

    Args:
        project_id:       Project ID (will be b.-prefixed automatically).
        parent_folder_id: The ID of the parent folder.
        folder_name:      Name for the new folder.

    Returns:
        The created folder object from the API.
    """
    proj_clean = ensure_b_prefix(project_id)
    payload = {
        "jsonapi": {"version": "1.0"},
        "data": {
            "type": "folders",
            "attributes": {
                "name": folder_name,
                "extension": {
                    "type": "folders:autodesk.bim360:Folder",
                    "version": "1.0",
                },
            },
            "relationships": {
                "parent": {
                    "data": {"type": "folders", "id": parent_folder_id}
                }
            },
        },
    }

    url = f"https://developer.api.autodesk.com/data/v1/projects/{proj_clean}/folders"
    resp = _make_request(
        "POST", url, json=payload,
        extra_headers={"Content-Type": "application/vnd.api+json"},
    )
    return resp.json()


def replicate_folders(
    hub_id: str,
    source_project_id: str,
    dest_project_id: str,
    max_depth: int = 5,
) -> str:
    """Recursively copies the folder structure from a source project to a destination project.

    Only replicates folders (not files). Starts from the 'Project Files' root.
    Returns a short summary string (not the full folder list) to avoid content-filter issues.

    Args:
        hub_id:             Hub ID (both projects must be in the same hub).
        source_project_id:  Source project ID.
        dest_project_id:    Destination project ID.
        max_depth:          Maximum folder nesting depth (default 5).

    Returns:
        Summary string with the count of created folders.
    """
    def _find_project_files_root(pid: str) -> str:
        folders = get_top_folders(hub_id, pid)
        if isinstance(folders, str):
            raise ValueError(f"Could not list top folders: {folders}")
        for f in folders:
            if (f.get("name") or "").lower() == "project files":
                return f["id"]
        if folders:
            return folders[0]["id"]
        raise ValueError(f"No top-level folders found in project {pid}")

    source_root = _find_project_files_root(source_project_id)
    dest_root = _find_project_files_root(dest_project_id)

    count = 0

    def _recurse(src_folder_id: str, dst_parent_id: str, path: str, depth: int) -> None:
        nonlocal count
        if depth > max_depth:
            return

        contents = get_folder_contents(source_project_id, src_folder_id)
        if isinstance(contents, str):
            logger.warning(f"  Could not read source folder {path}: {contents}")
            return

        for item in contents:
            if item.get("itemType") != "folder":
                continue

            name = item.get("name", "Unnamed")
            full_path = f"{path}/{name}"

            try:
                result = create_folder(dest_project_id, dst_parent_id, name)
                new_folder_id = result.get("data", {}).get("id")
                count += 1
                logger.info(f"  Created: {full_path}")

                if new_folder_id:
                    _recurse(item["id"], new_folder_id, full_path, depth + 1)
            except Exception as e:
                logger.warning(f"  Failed to create '{full_path}': {e}")

    logger.info(f"[Replicate] Copying folder structure from {source_project_id} to {dest_project_id}")
    _recurse(source_root, dest_root, "Project Files", 0)
    logger.info(f"[Replicate] Done — {count} folders created")
    return f"Successfully copied {count} folders from source to destination."


def soft_delete_folder(hub_id: str, project_id: str, folder_name: str) -> str:
    """Hides a top-level folder inside a project's 'Project Files' root.

    Searches for the folder by name (case-insensitive), then sends a PATCH
    to set ``hidden: true`` on it.

    Args:
        hub_id:     Hub ID (needed to list top folders).
        project_id: Project ID.
        folder_name: Name of the folder to hide.

    Returns:
        Success or not-found message.
    """
    top_folders = get_top_folders(hub_id, project_id)
    if isinstance(top_folders, str):
        raise ValueError(f"Could not list top folders: {top_folders}")

    root_id = None
    for f in top_folders:
        if (f.get("name") or "").lower() == "project files":
            root_id = f["id"]
            break
    if not root_id and top_folders:
        root_id = top_folders[0]["id"]
    if not root_id:
        raise ValueError("No top-level folders found in project.")

    contents = get_folder_contents(project_id, root_id)
    if isinstance(contents, str):
        raise ValueError(f"Could not read folder contents: {contents}")

    target = folder_name.lower().strip()
    folder_id = None
    matched_name = None
    for item in contents:
        if item.get("itemType") != "folder":
            continue
        name = (item.get("name") or "").strip()
        if name.lower() == target:
            folder_id = item["id"]
            matched_name = name
            break

    if not folder_id:
        return f"Folder '{folder_name}' not found under Project Files."

    proj_clean = ensure_b_prefix(project_id)
    folder_encoded = encode_urn(folder_id)
    url = f"https://developer.api.autodesk.com/data/v1/projects/{proj_clean}/folders/{folder_encoded}"
    payload = {
        "jsonapi": {"version": "1.0"},
        "data": {
            "type": "folders",
            "id": folder_id,
            "attributes": {"hidden": True},
        },
    }

    _make_request(
        "PATCH", url, json=payload,
        extra_headers={"Content-Type": "application/vnd.api+json"},
    )
    logger.info(f"Folder '{matched_name}' hidden in project {project_id}")
    return f"Successfully deleted folder '{matched_name}'."


"""
Autodesk Platform Services — API Client

All HTTP requests go through _make_request() which provides:
  - Auto-auth (Bearer token from auth.py)
  - Auto-EMEA (x-ads-region header)
  - Auto-retry on 401 (stale token refresh)
  - Auto-retry on 429 (rate limit with Retry-After)
  - Default 30s timeout (prevents hanging requests)
"""

import re
import logging
import time
import base64
import requests
from typing import Optional, Dict, Any, List, Union
from urllib.parse import quote

from auth import get_token

logger = logging.getLogger(__name__)


# ==========================================================================
# UTILITIES
# ==========================================================================

def clean_id(id_str: Optional[str]) -> str:
    """Remove 'b.' prefix from a hub/project ID."""
    return id_str.replace("b.", "") if id_str else ""


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


# ==========================================================================
# CENTRALIZED REQUEST HELPER
# ==========================================================================

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
      - 30 s default timeout (callers can override)
      - One retry on 401 (token refresh)
      - One retry on 429 (respects Retry-After header)
    """
    kwargs.setdefault("timeout", 30)
    max_attempts = 2 if retry_on_401 else 1
    last_resp: requests.Response = None  # type: ignore[assignment]

    for attempt in range(max_attempts):
        token = get_token(force_refresh=(attempt > 0))
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ads-region": "EMEA",
        }
        if extra_headers:
            headers.update(extra_headers)

        last_resp = requests.request(method, url, headers=headers, **kwargs)

        # 429 — rate limited: honour Retry-After, retry once
        if last_resp.status_code == 429:
            wait = int(last_resp.headers.get("Retry-After", 5))
            logger.warning(f"429 on {method} {url[:80]} — waiting {wait}s")
            time.sleep(wait)
            last_resp = requests.request(method, url, headers=headers, **kwargs)
            return last_resp

        # 401 — stale token: refresh and retry
        if last_resp.status_code == 401 and attempt == 0 and retry_on_401:
            logger.warning(f"401 on {method} {url[:80]} — refreshing token")
            continue

        return last_resp

    return last_resp


# ==========================================================================
# HUB & PROJECT DISCOVERY
# ==========================================================================

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
    if resp.status_code != 200:
        logger.error(f"get_hubs failed ({resp.status_code}): {resp.text}")
        return []
    return resp.json().get("data", [])


def get_projects(hub_id: str) -> list:
    """List all projects in a hub (follows pagination automatically)."""
    url: Optional[str] = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
    all_projects: list = []

    for _ in range(50):  # safety cap
        resp = _make_request("GET", url)  # type: ignore[arg-type]
        if resp.status_code != 200:
            logger.error(f"get_projects failed ({resp.status_code}): {resp.text}")
            break

        data = resp.json()
        all_projects.extend(data.get("data", []))

        # Follow pagination link
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


# ==========================================================================
# FOLDER NAVIGATION
# ==========================================================================

def get_top_folders(hub_id: str, project_id: str) -> Union[List[Dict[str, Any]], str]:
    """Fetches top-level folders for a project."""
    try:
        hub_clean = ensure_b_prefix(hub_id)
        proj_clean = ensure_b_prefix(project_id)

        url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_clean}/projects/{proj_clean}/topFolders"
        resp = _make_request("GET", url)

        if resp.status_code != 200:
            logger.error(f"Top Folders API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

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

        if resp.status_code != 200:
            logger.error(f"Folder Contents API Error {resp.status_code}: {resp.text}")
            return f"Error {resp.status_code}: {resp.text}"

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

        # Find "Project Files" (fall back to first folder)
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


# ==========================================================================
# FILE RESOLUTION
# ==========================================================================

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

        if resp.status_code != 200:
            logger.error(f"Version resolution failed {resp.status_code}: {resp.text}")
            return None

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

        # Resolve to version URN
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

        if resp.status_code == 404:
            return "Not Translated. The file exists but hasn't been processed yet."
        if resp.status_code == 202:
            return "Processing — translation in progress."
        if resp.status_code != 200:
            return f"Error {resp.status_code}: Unable to inspect file."

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


# ==========================================================================
# MODEL DERIVATIVE — STREAMING SCANNER & TRANSLATION
# ==========================================================================

def get_view_guid_only(version_urn: str) -> str:
    """
    Fetches the first available view GUID for a model.

    Raises:
        ValueError on auth failure, 404, or missing views.
    """
    urn_b64 = safe_b64encode(version_urn)
    url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn_b64}/metadata"

    resp = _make_request("GET", url)

    if resp.status_code == 401:
        raise ValueError("Auth failure (401). Verify 'viewables:read' scope.")
    if resp.status_code == 404:
        raise ValueError("Model not found (404). Translation may be missing.")
    if resp.status_code != 200:
        raise ValueError(f"Metadata API error {resp.status_code}: {resp.text}")

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

    # Smart singularization
    terms = {category_name}
    if category_name.lower().endswith("s") and len(category_name) > 2:
        terms.add(category_name[:-1])
    logger.info(f"  Search terms: {list(terms)}")

    guid = get_view_guid_only(version_urn)

    urn_b64 = safe_b64encode(version_urn)
    url = f"https://developer.api.autodesk.com/modelderivative/v2/designdata/{urn_b64}/metadata/{guid}"

    # Composite regex — matches JSON values containing any search term
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

            # Advance past fully-processed matches to prevent double-counting.
            # Only the tail after the last match is carried over.
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

        if resp.status_code not in (200, 201):
            error = f"Translation API error {resp.status_code}: {resp.text}"
            logger.error(f"  {error}")
            return error

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


# ==========================================================================
# PROJECT MANAGEMENT
# ==========================================================================

def create_acc_project(
    hub_id: str, project_name: str, project_type: str = "BIM360"
) -> dict:
    """Creates a new project in the specified Hub."""
    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
    payload = {
        "jsonapi": {"version": "1.0"},
        "data": {
            "type": "projects",
            "attributes": {
                "name": project_name,
                "projectType": project_type,
            },
        },
    }
    resp = _make_request("POST", url, json=payload)
    return resp.json()


def get_project_users(project_id: str) -> list:
    """List users in a project (requires Admin)."""
    pid = clean_id(project_id)
    url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{pid}/users"
    resp = _make_request("GET", url)
    if resp.status_code != 200:
        logger.error(f"get_project_users failed ({resp.status_code}): {resp.text}")
        return []
    return resp.json().get("results", [])


def add_project_user(project_id: str, email: str, products: list) -> dict:
    """
    Add a user to a project.

    Args:
        project_id: The project ID.
        email: User's email address.
        products: Product keys (e.g. ["projectAdministration", "docs"]).
    """
    pid = clean_id(project_id)
    url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{pid}/users"
    payload = {
        "email": email,
        "products": [{"key": p, "access": "administrator"} for p in products],
    }
    resp = _make_request("POST", url, json=payload)
    return resp.json()

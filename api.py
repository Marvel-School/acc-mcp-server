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
import threading
import requests
from collections import deque
from typing import Optional, Any
from urllib.parse import quote

from auth import get_token

logger = logging.getLogger(__name__)

# Concurrency limiter — caps parallel Autodesk API calls across all threads.
_api_semaphore = threading.Semaphore(10)

# ---------------------------------------------------------------------------
# Shared numeric constants — replaces inline magic numbers throughout.
# ---------------------------------------------------------------------------
_MAX_PROJECT_PAGES = 50
_MAX_USER_PAGES = 10
_MAX_FOLDER_SCAN = 50
_MAX_FOLDER_DEPTH = 3
_MAX_HUB_SCAN_PROJECTS = 20
_MAX_USER_SEARCH_PAGES = 20
_DEFAULT_TIMEOUT = 15
_STREAM_TIMEOUT = 120
_CHUNK_SIZE = 65536
_BUFFER_OVERLAP = 512
_PROJECT_CACHE_TTL = 300  # seconds


class ResultList(list):
    """List subclass that can carry an optional truncation warning.

    Functions with hard pagination caps set ``truncation_warning`` when the
    cap is hit so the LLM knows results may be incomplete.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.truncation_warning: str = ""



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


_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)

_SYSTEM_FOLDER_PREFIXES = (
    "quantification_",
    "correspondence-project-",
    "issue_",
    "submittals-",
    "COST Root Folder",
    "VIRTUAL_ROOT_FOLDER",
)

# ACC platform folders auto-created for every project — not user content.
_ACC_DEFAULT_FOLDERS = {
    "photos",
    "projecttb",
    "plans",
    "submittals",
    "rfis",
    "issues",
    "cost",
    "schedule",
}


def _is_system_folder(folder_name: str) -> bool:
    """Return True if *folder_name* is an Autodesk internal system folder.

    Matches:
    - Known system prefixes (quantification_, issue_, COST Root Folder, …)
    - ACC platform default folders (Photos, Plans, Issues, …)
    - Names that are purely a UUID
    - Names that contain a UUID suffix after an underscore or hyphen
    """
    if not folder_name:
        return False
    if folder_name.startswith(_SYSTEM_FOLDER_PREFIXES):
        return True
    if folder_name.lower().strip() in _ACC_DEFAULT_FOLDERS:
        return True
    if _UUID_RE.search(folder_name):
        return True
    return False


def safe_b64encode(value: Optional[str]) -> str:
    """Base64url-encode a URN (no padding) for Model Derivative API."""
    if not value:
        return ""
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8").rstrip("=")



def _make_request(
    method: str,
    url: str,
    *,
    extra_headers: Optional[dict] = None,
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
      - One retry on 5xx (500/502/503/504) with 2 s backoff

    All HTTP calls are gated by ``_api_semaphore`` (max 10 concurrent)
    to prevent a thundering herd against the Autodesk API.

    Raises:
        ValueError: On any 4xx/5xx response (with Autodesk error detail).
    """
    kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
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

            with _api_semaphore:
                last_resp = requests.request(method, url, headers=headers, **kwargs)

            # --- 429 rate limit retry ---
            if last_resp.status_code == 429:
                wait = int(last_resp.headers.get("Retry-After", 5))
                logger.warning(f"429 on {method} {url[:80]} — waiting {wait}s")
                time.sleep(wait)
                token = get_token(force_refresh=True)
                headers["Authorization"] = f"Bearer {token}"
                with _api_semaphore:
                    last_resp = requests.request(method, url, headers=headers, **kwargs)
                break

            # --- 5xx server error retry (one attempt, 2 s backoff) ---
            if last_resp.status_code in (500, 502, 503, 504) and attempt == 0:
                logger.warning(
                    "Autodesk API returned %d on %s %s — retrying in 2s",
                    last_resp.status_code, method, url[:80],
                )
                time.sleep(2)
                continue

            # --- 401 stale token retry ---
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



def _get_admin_user_id() -> str:
    """Returns the Autodesk User ID for the Account Admin from env."""
    admin_id = os.environ.get("ACC_ADMIN_ID", "").strip()
    if not admin_id:
        raise ValueError("ACC_ADMIN_ID environment variable is required for admin operations")
    return admin_id



# TTL-based hub ID cache — expires after 5 minutes so it never gets permanently
# stuck on None (e.g. after a transient 503 on the first call).
_cached_hub_id: Optional[str] = None
_hub_cache_time: float = 0.0
_HUB_CACHE_TTL: float = 300.0  # seconds
_hub_cache_lock = threading.Lock()

# Project-list cache — keyed by hub_id, stores (project_list, cached_at).
_project_cache: dict[str, tuple[ResultList, float]] = {}
_project_cache_lock = threading.Lock()


def _get_hub_id() -> str:
    """Returns the first accessible hub ID with a 5-minute TTL cache.

    Thread-safe: a lock wraps the read-check-write sequence to prevent
    concurrent callers from both triggering an API request.

    Re-fetches from the API when the cache is empty, expired, or was
    populated with None on a previous failed call.  Failures are never
    cached so the next call will retry immediately.
    """
    global _cached_hub_id, _hub_cache_time
    with _hub_cache_lock:
        now = time.monotonic()
        if _cached_hub_id and (now - _hub_cache_time) < _HUB_CACHE_TTL:
            return _cached_hub_id
        hubs = get_hubs()
        if not hubs:
            raise ValueError(
                "Could not resolve hub ID \u2014 Autodesk API returned no hubs. "
                "Check APS credentials and account access."
            )
        hub_id = hubs[0].get("id")
        if not hub_id:
            raise ValueError(
                "Could not resolve hub ID \u2014 Autodesk API returned no hubs. "
                "Check APS credentials and account access."
            )
        _cached_hub_id = hub_id
        _hub_cache_time = now
        return _cached_hub_id


def get_hubs() -> list[dict[str, Any]]:
    """Fetches all accessible Hubs (BIM 360 / ACC)."""
    resp = _make_request("GET", "https://developer.api.autodesk.com/project/v1/hubs")
    return resp.json().get("data", [])


def get_projects(
    hub_id: str,
    limit: int = 50,
    fields: Optional[list] = None,
) -> ResultList:
    """List all projects in a hub (follows pagination automatically).

    Results are cached for 5 minutes per hub to avoid redundant API calls
    (e.g. when ``find_project_globally`` iterates over multiple hubs).
    Use ``invalidate_project_cache()`` to evict a specific hub's entry.

    Args:
        hub_id: Hub ID (starts with 'b.').
        limit:  Max projects per page (default 50, max 100 per Autodesk API).
        fields: Optional list of extra fields to include (e.g. ["status", "projectValue"]).
    """
    hub_id = ensure_b_prefix(hub_id)

    # --- Check cache (only when no custom fields are requested) ---
    use_cache = fields is None or fields == []
    if use_cache:
        with _project_cache_lock:
            cached = _project_cache.get(hub_id)
            if cached is not None:
                cached_list, cached_at = cached
                if (time.monotonic() - cached_at) < _PROJECT_CACHE_TTL:
                    logger.debug("Project cache hit for hub %s (%d projects)", hub_id, len(cached_list))
                    return cached_list

    # --- Fetch from API ---
    base = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects?page[limit]={limit}"
    if fields:
        base += f"&fields[projects]={','.join(fields)}"
    url: Optional[str] = base
    all_projects: ResultList = ResultList()

    _hit_cap = True
    for _ in range(_MAX_PROJECT_PAGES):  # safety cap
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
            _hit_cap = False
            break

    if _hit_cap:
        all_projects.truncation_warning = (
            f"⚠️ Results truncated: retrieved {len(all_projects)} projects across "
            f"{_MAX_PROJECT_PAGES} pages (safety limit). There may be more projects in this hub."
        )
        logger.warning(all_projects.truncation_warning)

    # --- Store in cache (only default-field responses) ---
    if use_cache:
        with _project_cache_lock:
            _project_cache[hub_id] = (all_projects, time.monotonic())

    return all_projects


def invalidate_project_cache(hub_id: str) -> None:
    """Evict the project-list cache entry for a hub.

    Pops both the raw and b.-prefixed key variants to be safe.
    """
    prefixed = ensure_b_prefix(hub_id)
    with _project_cache_lock:
        _project_cache.pop(prefixed, None)
        _project_cache.pop(hub_id, None)


_GlobalMatches = tuple[list[tuple[str, str, str, str]], list[tuple[str, str, str, str]]]


def find_project_globally(name_query: str) -> _GlobalMatches:
    """
    Search for a project by name across ALL accessible hubs.
    Uses two-phase matching: exact first, then substring.

    Returns:
        (exact_matches, substring_matches) — each a list of
        (hub_id, hub_name, project_id, project_name) tuples.
    """
    logger.info(f"Searching globally for project: {name_query}")
    hubs = get_hubs()
    if not hubs:
        logger.error("No hubs found.")
        return ([], [])

    search_term = name_query.lower().strip()
    exact: list[tuple[str, str, str, str]] = []
    substring: list[tuple[str, str, str, str]] = []

    for hub in hubs:
        hub_id = hub.get("id")
        hub_name = hub.get("attributes", {}).get("name", "Unknown")
        if not hub_id:
            continue

        logger.info(f"  Searching hub: {hub_name}")
        projects = get_projects(hub_id)

        for p in projects:
            p_name = p.get("attributes", {}).get("name", "")
            p_lower = p_name.lower().strip()
            entry = (hub_id, hub_name, p.get("id"), p_name)
            if p_lower == search_term:
                logger.info(f"  Exact match: {p_name} in hub {hub_name}")
                exact.append(entry)
            elif search_term in p_lower:
                logger.info(f"  Substring match: {p_name} in hub {hub_name}")
                substring.append(entry)

    if not exact and not substring:
        logger.warning(f"Project '{name_query}' not found in any hub")
    return (exact, substring)



def get_top_folders(hub_id: str, project_id: str) -> list[dict[str, Any]]:
    """Fetches top-level folders for a project."""
    hub_clean = ensure_b_prefix(hub_id)
    proj_clean = ensure_b_prefix(project_id)

    url = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_clean}/projects/{proj_clean}/topFolders"
    resp = _make_request("GET", url)

    all_folders = [
        {
            "id": f.get("id"),
            "name": f.get("attributes", {}).get("displayName"),
            "type": f.get("type"),
        }
        for f in resp.json().get("data", [])
    ]
    filtered = [f for f in all_folders if not _is_system_folder(f.get("name") or "")]
    removed = len(all_folders) - len(filtered)
    if removed:
        logger.debug(
            "Filtered %d system folders from top-level folder list for project %s",
            removed, project_id,
        )
    return filtered


def get_folder_contents(project_id: str, folder_id: str) -> list[dict[str, Any]]:
    """Fetches contents of a folder with tip-version URN extraction for files."""
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

    before = len(result)
    result = [
        item for item in result
        if item.get("itemType") != "folder" or not _is_system_folder(item.get("name") or "")
    ]
    removed = before - len(result)
    if removed:
        logger.debug(
            "Filtered %d system folders from folder contents of %s",
            removed, folder_id,
        )
    return result


def find_design_files(
    hub_id: str, project_id: str, extensions: str = "rvt"
) -> ResultList:
    """
    BFS file search through 'Project Files' folder.
    Depth-limited to 3 levels, max 50 folders scanned.
    """
    logger.info(f"Searching for files: {extensions}")
    top_folders = get_top_folders(hub_id, project_id)

    root_id = None
    for folder in top_folders:
        if folder.get("name") == "Project Files":
            root_id = folder.get("id")
            break
    if not root_id and top_folders:
        root_id = top_folders[0].get("id")
    if not root_id:
        raise ValueError("No folders found in project")

    queue = deque([{"id": root_id, "name": "Project Files", "depth": 0}])
    matching: ResultList = ResultList()
    ext_list = [e.strip().lower() for e in extensions.split(",")]
    scanned = 0

    while queue and scanned < _MAX_FOLDER_SCAN:
        current = queue.popleft()
        scanned += 1

        logger.info(f"  Scanning {scanned}/{_MAX_FOLDER_SCAN} (depth {current['depth']}): {current['name']}")
        try:
            contents = get_folder_contents(project_id, current["id"])
        except Exception as e:
            logger.warning(f"  Skipping folder '{current['name']}': {e}")
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

            elif item.get("itemType") == "folder" and current["depth"] < _MAX_FOLDER_DEPTH:
                sub_name = item.get("name", "Unknown")
                if _is_system_folder(sub_name):
                    logger.debug("Skipping system folder during BFS: %s", sub_name)
                    continue
                queue.append({
                    "id": item.get("id"),
                    "name": f"{current['name']}/{sub_name}",
                    "depth": current["depth"] + 1,
                })

    logger.info(f"Search complete: {scanned} folders, {len(matching)} files")

    if scanned >= _MAX_FOLDER_SCAN and queue:
        matching.truncation_warning = (
            f"⚠️ Results truncated: only scanned {_MAX_FOLDER_SCAN} of "
            f"{_MAX_FOLDER_SCAN + len(queue)} reachable folders (depth limit: {_MAX_FOLDER_DEPTH}). "
            f"There may be more design files. Narrow your search to get complete results."
        )
        logger.warning(matching.truncation_warning)

    return matching



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

        extensions = ["rvt", "dwg", "nwc", "rcp", "ifc", "nwd"]
        if "." in identifier:
            ext = identifier.rsplit(".", 1)[-1].lower()
            if ext in extensions:
                extensions.remove(ext)
                extensions.insert(0, ext)

        target = identifier.lower()

        for ext in extensions:
            try:
                files = find_design_files(hub_id, project_id, ext)
            except Exception:
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


def get_latest_version_urn(project_id: str, item_id: str) -> str:
    """Resolves a lineage URN to its latest version URN via the tip relationship.

    Raises:
        ValueError: If the version URN cannot be resolved.
    """
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
        return tip

    raise ValueError(
        f"No tip version found in item relationships for '{item_id[:60]}'"
    )


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
        elif "fs.file" in resolved or "version=" in resolved:
            version_urn = resolved
        else:
            try:
                version_urn = get_latest_version_urn(project_id, resolved)
            except ValueError:
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

    resp = _make_request("GET", url, stream=True, timeout=_STREAM_TIMEOUT)
    try:
        resp.raise_for_status()

        for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
            if not chunk:
                continue

            buffer += chunk
            matches = list(pattern.finditer(buffer))
            count += len(matches)

            if matches:
                buffer = buffer[matches[-1].end():]
            else:
                buffer = buffer[-_BUFFER_OVERLAP:]

    except requests.exceptions.HTTPError as e:
        raise ValueError(f"HTTP error streaming metadata: {e}")
    except requests.exceptions.Timeout:
        raise ValueError("Timeout streaming metadata.")
    finally:
        resp.close()

    logger.info(f"  Found {count} elements matching '{category_name}'.")
    return count


def trigger_translation(version_urn: str) -> dict[str, Any]:
    """
    Triggers a fresh Model Derivative translation job (SVF Classic).
    Uses x-ads-force to overwrite existing partial derivatives.
    """
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



def create_acc_project(hub_id: str, project_name: str, project_type: str = "BIM360") -> dict[str, Any]:
    """Creates a project using the ACC Account Admin API.

    After a successful creation the project-list cache for this hub is
    invalidated so subsequent ``get_projects`` calls see the new project.
    """
    account_id = _strip_b_prefix(hub_id)
    user_id = _get_admin_user_id()

    endpoint = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{account_id}/projects"
    payload = {
        "name": project_name,
        "type": "Office",
        "platform": "acc" if project_type.upper() == "ACC" else "bim360",
        "timezone": os.environ.get("DEFAULT_PROJECT_TIMEZONE", "Europe/Amsterdam"),
        "addressLine1": os.environ.get("DEFAULT_PROJECT_ADDRESS_LINE1", ""),
        "city": os.environ.get("DEFAULT_PROJECT_CITY", ""),
        "country": os.environ.get("DEFAULT_PROJECT_COUNTRY", "NL"),
        "postalCode": os.environ.get("DEFAULT_PROJECT_POSTAL_CODE", ""),
    }

    logger.info(f"POSTing to {endpoint} with User-Id: {user_id}")
    resp = _make_request("POST", endpoint, json=payload, extra_headers={"User-Id": user_id})
    logger.info("Project creation API responded successfully.")

    # Invalidate the project-list cache so the new project appears on next fetch.
    invalidate_project_cache(hub_id)

    return resp.json()


def _sanitize_user_name(raw: dict) -> str:
    """Build a clean display name from an ACC user record.

    ACC sometimes appends autodeskId directly onto the name field
    (e.g. "Maxine Bruil0c1e0b3b-..."). Prefer firstName/lastName;
    fall back to name; strip any embedded hex-ID substring.
    """
    _first = (raw.get("firstName") or "").strip()
    _last = (raw.get("lastName") or "").strip()
    _raw_name = (
        f"{_first} {_last}".strip()
        if (_first or _last)
        else (raw.get("name") or "").strip()
    )
    clean = re.sub(
        r"[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}",
        "",
        _raw_name,
    )
    return re.sub(r"[a-fA-F0-9]{10,}$", "", clean).strip() or "-"


def _paginate_project_users(
    project_id: str,
    max_pages: int,
    include_permissions: bool,
) -> ResultList:
    """Shared pagination helper for project user endpoints.

    Fetches users from the ACC Admin API with automatic pagination,
    name sanitisation, and truncation warnings.

    Args:
        project_id:          Project ID (b. prefix stripped automatically).
        max_pages:           Maximum pages to fetch (100 users/page).
        include_permissions: If True, enrich each record with role/product/
                             access-level details.  If False, return the raw
                             API record with only the name sanitised.
    """
    clean_id = _strip_b_prefix(project_id)
    endpoint = (
        f"https://developer.api.autodesk.com/construction/admin/v1"
        f"/projects/{clean_id}/users?limit=100"
    )

    all_users: ResultList = ResultList()
    url: Optional[str] = endpoint

    _hit_cap = True
    for _ in range(max_pages):
        resp = _make_request("GET", url)  # type: ignore[arg-type]
        data = resp.json()

        for raw in data.get("results", []):
            display_name = _sanitize_user_name(raw)

            if include_permissions:
                company = raw.get("company") or {}
                products_raw = raw.get("products") or []
                roles_raw = raw.get("roles") or []
                all_users.append({
                    "name": display_name,
                    "email": (raw.get("email") or "").lower(),
                    "companyId": company.get("id", ""),
                    "companyName": company.get("name", "") or "-",
                    "roleIds": [r.get("id", "") for r in roles_raw],
                    "roleNames": [r.get("name", "") for r in roles_raw],
                    "products": [
                        {"key": p.get("key", ""), "access": p.get("access", "")}
                        for p in products_raw
                    ],
                    "accessLevels": raw.get("accessLevels") or [],
                })
            else:
                raw["name"] = display_name
                all_users.append(raw)

        next_url = data.get("pagination", {}).get("nextUrl")
        if next_url:
            url = next_url
        else:
            _hit_cap = False
            break

    if _hit_cap:
        all_users.truncation_warning = (
            f"⚠️ Results truncated: retrieved {len(all_users)} users across "
            f"{max_pages} pages (safety limit). There may be more users in this project."
        )
        logger.warning(all_users.truncation_warning)

    return all_users


def get_project_users(project_id: str, max_pages: int = _MAX_USER_PAGES) -> ResultList:
    """List users in a project (requires Admin).

    Thin wrapper around _paginate_project_users — returns simplified records
    with sanitised names.
    """
    return _paginate_project_users(project_id, max_pages, include_permissions=False)


def get_project_user_permissions(project_id: str) -> ResultList:
    """Fetches all project members with full permission and role details.

    NEVER cached — every call executes a live HTTP request to ACC to return
    the absolute current truth.

    Returns:
        List of user dicts with keys: name, email, companyId, companyName,
        roleIds, roleNames, products, accessLevels.
    """
    clean_id = _strip_b_prefix(project_id)
    result = _paginate_project_users(project_id, _MAX_USER_PAGES, include_permissions=True)
    logger.info(
        f"[Permissions] Fetched {len(result)} users for project {clean_id} (live, uncached)"
    )
    return result


def add_project_user(project_id: str, email: str, products: list[str] | None = None) -> dict[str, Any]:
    """
    Add a user to a project.

    Args:
        project_id: The project ID.
        email: User's email address.
        products: Product keys (e.g. ["docs"]). Defaults to ["docs"].
    """
    clean_project_id = _strip_b_prefix(project_id)

    if products is None:
        products = ["docs"]

    user_id = _get_admin_user_id()

    endpoint = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{clean_project_id}/users"
    payload = {
        "email": email,
        "products": [{"key": p, "access": "administrator"} for p in products],
    }
    resp = _make_request("POST", endpoint, json=payload, extra_headers={"User-Id": user_id})
    return resp.json()


def get_all_hub_users(hub_id: str, max_projects: int = _MAX_HUB_SCAN_PROJECTS) -> tuple[ResultList, list[str]]:
    """Aggregate users and their product entitlements across all projects in a hub.

    Scans up to *max_projects* projects, collects every user encountered, and
    merges their product assignments into a single record per email.

    Returns:
        Tuple of (users, skipped_projects) where:
          - users: List of dicts [{"email", "name", "products": [...]}]
          - skipped_projects: List of project names that failed (permission errors etc.)
    """
    all_hub_projects = get_projects(hub_id)
    truncated_projects = len(all_hub_projects) > max_projects
    projects = all_hub_projects[:max_projects]
    logger.info(f"[Hub Audit] Scanning {len(projects)} projects in hub {hub_id}")

    user_map: dict[str, dict[str, Any]] = {}

    skipped_projects: list[str] = []
    warnings: list[str] = []

    # Propagate project-list truncation warning
    _proj_warn = getattr(all_hub_projects, "truncation_warning", "")
    if _proj_warn:
        warnings.append(_proj_warn)

    if truncated_projects:
        w = (
            f"⚠️ Results truncated: only scanned {max_projects} of "
            f"{len(all_hub_projects)} projects in this hub. "
            f"Some users may not be included."
        )
        logger.warning(w)
        warnings.append(w)

    for p in projects:
        pid = p.get("id", "")
        p_name = p.get("attributes", {}).get("name", "Unknown")
        try:
            members = get_project_users(pid)
        except Exception as e:
            logger.warning(f"  Skipping project '{p_name}': {e}")
            skipped_projects.append(p_name)
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

    result: ResultList = ResultList()
    for entry in user_map.values():
        entry["products"] = sorted(entry["products"])
        result.append(entry)
    result.sort(key=lambda u: u["email"])

    if warnings:
        result.truncation_warning = "\n".join(warnings)

    logger.info(f"[Hub Audit] Found {len(result)} unique users across {len(projects)} projects")
    return result, skipped_projects


def get_user_projects(account_id: str, user_name_or_email: str) -> dict:
    """
    Resolves a user by display name or email and returns every project they
    have access to within the account.

    NEVER cached — every call executes live HTTP requests so that permission
    changes made in ACC are reflected immediately.

    Step 1 — User resolution (two strategies, no caching):
      Email  → GET /hq/v1/regions/eu/accounts/{id}/users/search?email={email}
      Name   → GET /hq/v1/regions/eu/accounts/{id}/users (paginated, local filter)

    Step 2 — Project fetch (unified ACC endpoint, supports ACC + BIM 360):
      GET /construction/admin/v1/accounts/{id}/users/{id}/projects?limit=100
      Header: Region: EMEA  (replaces the legacy /regions/eu/ URL path segment)

    Args:
        account_id:          Raw ACC account ID (no 'b.' prefix).
        user_name_or_email:  Display name substring (case-insensitive) or exact email.

    Returns:
        {
            "user_name":  str,
            "user_email": str,
            "projects":   [{"name": str, "role": str}],
        }

    Raises:
        ValueError: If no matching user is found or an API call fails.
    """
    # Strip 'b.' prefix — hq/v1 Account Admin endpoints require the raw UUID.
    # Applied here so the function is self-defensive regardless of the caller.
    clean_account_id = _strip_b_prefix(account_id)

    query = user_name_or_email.strip()

    # --- Step 1: Resolve user → uid ----------------------------------------
    _name_search_warning = ""
    if "@" in query:
        # Fast path: direct email search
        search_url = (
            f"https://developer.api.autodesk.com/hq/v1/regions/eu"
            f"/accounts/{clean_account_id}/users/search?email={query.lower()}"
        )
        candidates: list = _make_request("GET", search_url).json()
    else:
        # Slow path: paginate through all account users, filter by name
        candidates = []
        target = query.lower()
        limit = 100
        offset = 0
        _hit_name_cap = True
        for _ in range(_MAX_USER_SEARCH_PAGES):  # safety cap: 2 000 users max
            list_url = (
                f"https://developer.api.autodesk.com/hq/v1/regions/eu"
                f"/accounts/{clean_account_id}/users?limit={limit}&offset={offset}"
            )
            page: list = _make_request("GET", list_url).json()
            for u in page:
                full = (
                    u.get("name")
                    or f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
                )
                api_name = full.lower()
                api_email = (u.get("email") or "").lower()
                # Normalize search term: strip commas so "Last, First" matches
                # "First Last", then require every token to appear in the name.
                search_clean = target.replace(",", "").split()
                name_match = bool(search_clean) and all(
                    part in api_name for part in search_clean
                )
                email_match = target.replace(",", "").strip() in api_email
                if name_match or email_match:
                    candidates.append(u)
            if len(page) < limit:
                _hit_name_cap = False
                break
            offset += limit

        if _hit_name_cap:
            _name_search_warning = (
                f"⚠️ User search truncated: scanned {_MAX_USER_SEARCH_PAGES * limit:,} users "
                f"({_MAX_USER_SEARCH_PAGES}-page safety limit). The matching user may exist beyond "
                f"this range. Try searching by exact email instead."
            )
            logger.warning(_name_search_warning)

    if not candidates:
        raise ValueError(f"No ACC user found matching '{user_name_or_email}'")

    match = candidates[0]
    # Use 'id' — the internal hub User UUID required by the
    # /users/{id}/projects endpoint.  The 'uid' / 'autodeskId' fields are the
    # Autodesk-global identifier and will cause a 404 on that endpoint.
    target_uid = match.get("id")
    display_name = (
        match.get("name")
        or f"{match.get('first_name', '')} {match.get('last_name', '')}".strip()
        or user_name_or_email
    )
    email = (match.get("email") or "").lower()

    if not target_uid:
        raise ValueError(
            f"Resolved user '{display_name}' but could not determine their internal hub UUID"
        )

    logger.info(f"[UserProjects] Resolved '{display_name}' → id={target_uid}")

    # --- Step 2: Fetch projects assigned to this user -----------------------
    # Unified ACC construction/admin/v1 endpoint — supports both ACC and BIM 360.
    # Region is passed as a header instead of being embedded in the URL path.
    proj_url = (
        f"https://developer.api.autodesk.com/construction/admin/v1"
        f"/accounts/{clean_account_id}/users/{target_uid}/projects?limit=100"
    )
    try:
        raw = _make_request(
            "GET",
            proj_url,
            extra_headers={"Region": "EMEA"},
        ).json()
        # Unified endpoint returns projects inside a 'results' array.
        raw_projects = raw.get("results", [])

        projects = []
        for p in raw_projects:
            role = (
                "Project Admin"
                if p.get("access_level") == "project_admin"
                else "Member"
            )
            projects.append({"name": p.get("name", "—"), "role": role})

        logger.info(
            f"[UserProjects] Found {len(projects)} projects for '{display_name}' (live, uncached)"
        )
        result_dict: dict = {"user_name": display_name, "user_email": email, "projects": projects}
        if _name_search_warning:
            result_dict["warning"] = _name_search_warning
        return result_dict

    except ValueError as api_err:
        logger.warning(
            f"[UserProjects] Could not fetch projects for id={target_uid}: {api_err}"
        )
        raise ValueError(
            f"User found (ID: {target_uid}), but Autodesk returned an error when "
            f"fetching their projects. They may not have any assigned projects, "
            f"or API access is restricted."
        ) from api_err


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

    Source and destination projects may be in different hubs.  The caller
    should pass the actual hub ID where the source project lives (obtained
    from ``_resolve_project_id``).

    Only replicates folders (not files). Starts from the 'Project Files' root.
    Returns a short summary string (not the full folder list) to avoid content-filter issues.

    Args:
        hub_id:             Hub ID of the source project.
        source_project_id:  Source project ID.
        dest_project_id:    Destination project ID.
        max_depth:          Maximum folder nesting depth (default 5).

    Returns:
        Summary string with the count of created folders.
    """
    def _find_project_files_root(pid: str) -> str:
        folders = get_top_folders(hub_id, pid)
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

        try:
            contents = get_folder_contents(source_project_id, src_folder_id)
        except Exception as e:
            logger.warning(f"  Could not read source folder {path}: {e}")
            return

        for item in contents:
            if item.get("itemType") != "folder":
                continue

            name = item.get("name", "Unnamed")
            if _is_system_folder(name):
                logger.debug("Skipping system folder during replication: %s", name)
                continue
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


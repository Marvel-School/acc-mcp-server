import os
import shutil
import fnmatch
import time
import uvicorn
# CHANGE 1: Use the robust 'fastmcp' library, not the basic 'mcp.server' one
from fastmcp import FastMCP

# Initialize the MCP server
mcp = FastMCP("FileSystem-v2")

# CONFIGURATION
# ---------------------------------------------------------
WORKING_DIRECTORY = os.path.abspath("./my_sandbox_files")

if not os.path.exists(WORKING_DIRECTORY):
    try:
        os.makedirs(WORKING_DIRECTORY)
    except OSError as e:
        print(f"Warning: Could not create directory {WORKING_DIRECTORY}: {e}")

# HELPER: Security Check
# ---------------------------------------------------------
def validate_path(rel_path: str) -> str:
    clean_rel = rel_path.lstrip(os.sep)
    abs_path = os.path.abspath(os.path.join(WORKING_DIRECTORY, clean_rel))
    if not abs_path.startswith(WORKING_DIRECTORY):
        raise ValueError(f"Access denied: Path '{rel_path}' is outside the working directory.")
    return abs_path

# TOOLS
# ---------------------------------------------------------

@mcp.tool()
def list_directory(path: str = ".") -> str:
    """Lists files and directories. Appends [DIR] to directory names."""
    try:
        safe_path = validate_path(path)
        items = os.listdir(safe_path)
        formatted_items = []
        for item in items:
            full_item_path = os.path.join(safe_path, item)
            if os.path.isdir(full_item_path):
                formatted_items.append(f"[DIR] {item}")
            else:
                formatted_items.append(item)
        return "\n".join(sorted(formatted_items)) or "(Empty Directory)"
    except Exception as e:
        return f"Error listing directory: {str(e)}"

@mcp.tool()
def read_file(path: str) -> str:
    """Reads the full content of a file."""
    try:
        safe_path = validate_path(path)
        if not os.path.isfile(safe_path):
            return f"Error: '{path}' is not a file."
        with open(safe_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        return f"Error reading file: {str(e)}"

@mcp.tool()
def write_file(path: str, content: str) -> str:
    """Writes content to a file. Overwrites existing files."""
    try:
        safe_path = validate_path(path)
        with open(safe_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return f"Successfully wrote to {path}"
    except Exception as e:
        return f"Error writing file: {str(e)}"

@mcp.tool()
def search_files(pattern: str) -> str:
    """Recursively searches for files matching a pattern (e.g., '*.py')."""
    matches = []
    try:
        for root, dirnames, filenames in os.walk(WORKING_DIRECTORY):
            for filename in fnmatch.filter(filenames, pattern):
                abs_path = os.path.join(root, filename)
                rel_path = os.path.relpath(abs_path, WORKING_DIRECTORY)
                matches.append(rel_path)
        if not matches:
            return "No files found matching that pattern."
        return "\n".join(matches)
    except Exception as e:
        return f"Error searching files: {str(e)}"

@mcp.tool()
def get_file_info(path: str) -> str:
    """Returns metadata about a file: size, creation time, modified time."""
    try:
        safe_path = validate_path(path)
        if not os.path.exists(safe_path):
            return "File does not exist."
        stats = os.stat(safe_path)
        return (f"File: {path}\n"
                f"Size: {round(stats.st_size / 1024, 2)} KB\n"
                f"Created: {time.ctime(stats.st_ctime)}\n"
                f"Modified: {time.ctime(stats.st_mtime)}")
    except Exception as e:
        return f"Error getting info: {str(e)}"

@mcp.tool()
def move_file(source: str, destination: str) -> str:
    """Moves or renames a file."""
    try:
        safe_src = validate_path(source)
        safe_dest = validate_path(destination)
        if not os.path.exists(safe_src):
            return f"Error: Source '{source}' does not exist."
        shutil.move(safe_src, safe_dest)
        return f"Successfully moved '{source}' to '{destination}'"
    except Exception as e:
        return f"Error moving file: {str(e)}"

# RUN THE SERVER
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting MCP Server on 0.0.0.0:{port}...")
    
    # CHANGE 2: Create the ASGI app explicitly and run with Uvicorn
    # This bypasses the 'mcp.run()' issues and gives us full control over the port.
    server_app = mcp.http_app()
    uvicorn.run(server_app, host="0.0.0.0", port=port)
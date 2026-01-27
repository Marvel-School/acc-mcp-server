from fastmcp import FastMCP
import inspect

mcp = FastMCP("test")
print(f"Attributes: {dir(mcp)}")

try:
    print(f"Has _fastapi_app: {hasattr(mcp, '_fastapi_app')}")
    if hasattr(mcp, "_fastapi_app"):
         print(f"Type of _fastapi_app: {type(mcp._fastapi_app)}")
         print(f"Dir of _fastapi_app: {dir(mcp._fastapi_app)}")
except Exception as e:
    print(e)

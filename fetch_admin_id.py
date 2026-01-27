import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging to stdout
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# Attempt to import necessary modules
try:
    from auth import get_token, ACC_ADMIN_EMAIL
    from api import get_cached_hub_id, clean_id, get_user_id_by_email
except ImportError as e:
    print(f"Import Error: {e}")
    print("Ensure you are running this from the project root and requirements are installed.")
    sys.exit(1)

def main():
    print("--- Admin ID Finder for Azure Config ---")
    
    # Check if we have credentials available in the environment
    client_id = os.environ.get("APS_CLIENT_ID")
    client_secret = os.environ.get("APS_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        print("❌ Error: Environment variables APS_CLIENT_ID or APS_CLIENT_SECRET are missing.")
        print("Please export them before running this script, e.g.:")
        print("  $env:APS_CLIENT_ID='...'")
        print("  $env:APS_CLIENT_SECRET='...'")
        return

    # Determine email to look up
    email = ACC_ADMIN_EMAIL
    if not email:
        print("⚠️  acc_admin_email is not set in environment.")
        email = input("Please enter the Admin Email address to look up: ").strip()
        if not email:
            print("No email provided. Exiting.")
            return
    else:
        print(f"Using configured email: {email}")

    print("Authenticating...")
    try:
        # get_token will raise if fails
        token = get_token()
        print("✅ Authentication successful (Token obtained).")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return

    print("Finding Account ID...")
    raw_hub_id = get_cached_hub_id()
    if not raw_hub_id:
        print("❌ Could not find any Hubs/Accounts for this App.")
        return
    
    account_id = clean_id(raw_hub_id)
    print(f"✅ Found Account ID: {account_id}")

    print(f"Searching for User ID for '{email}'...")
    uid = get_user_id_by_email(account_id, email)
    
    if uid:
        print("\n" + "="*40)
        print("✅ SUCCESS! Found User ID.")
        print("="*40)
        print(f"ACC_ADMIN_ID={uid}")
        print("="*40)
        print("Copy the line above into your Azure Configuration.")
    else:
        print("\n" + "="*40)
        print("❌ FAILURE. User not found.")
        print("="*40)
        print(f"Could not find user '{email}' in Account '{account_id}'.")
        print("Verify the email address is correct and the user is an active member of the account.")

if __name__ == "__main__":
    main()

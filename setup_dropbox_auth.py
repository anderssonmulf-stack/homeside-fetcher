#!/usr/bin/env python3
"""
Dropbox OAuth Setup Script

This script guides you through the OAuth flow to obtain a refresh token
for long-lived Dropbox access. Run this once to set up authentication.

Usage:
    python setup_dropbox_auth.py

Prerequisites:
    1. Create a Dropbox app at https://www.dropbox.com/developers/apps
    2. Choose "Scoped access" and "App folder" access type
    3. Note your App key and App secret
    4. In Permissions tab, enable: files.metadata.read, files.content.read, files.content.write

The script will:
    1. Ask for your App key and App secret
    2. Open a browser for authorization
    3. Exchange the auth code for tokens
    4. Save the refresh token to your .env file
"""

import os
import sys
import webbrowser
from urllib.parse import urlencode

# Check for dropbox package
try:
    import dropbox
    from dropbox import DropboxOAuth2FlowNoRedirect
except ImportError:
    print("ERROR: dropbox package not installed")
    print("Run: pip install dropbox")
    sys.exit(1)


def get_existing_credentials():
    """Read existing credentials from .env file if present."""
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    credentials = {}

    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    credentials[key.strip()] = value.strip()

    return credentials


def update_env_file(key: str, value: str):
    """Update a single key in the .env file, preserving other content."""
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    lines = []
    key_found = False

    # Read existing content
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            lines = f.readlines()

    # Update or add the key
    new_lines = []
    for line in lines:
        if line.strip().startswith(f'{key}='):
            new_lines.append(f'{key}={value}\n')
            key_found = True
        else:
            new_lines.append(line)

    # Add key if not found
    if not key_found:
        # Add a newline before if the file doesn't end with one
        if new_lines and not new_lines[-1].endswith('\n'):
            new_lines.append('\n')
        new_lines.append(f'{key}={value}\n')

    # Write back
    with open(env_path, 'w') as f:
        f.writelines(new_lines)


def main():
    print("=" * 60)
    print("Dropbox OAuth Setup")
    print("=" * 60)
    print()
    print("This script will help you set up Dropbox access for the")
    print("energy data import system.")
    print()

    # Check for existing credentials
    existing = get_existing_credentials()

    # Get App Key
    default_key = existing.get('DROPBOX_APP_KEY', '')
    if default_key:
        app_key = input(f"Enter Dropbox App Key [{default_key}]: ").strip()
        if not app_key:
            app_key = default_key
    else:
        app_key = input("Enter Dropbox App Key: ").strip()

    if not app_key:
        print("ERROR: App Key is required")
        sys.exit(1)

    # Get App Secret
    default_secret = existing.get('DROPBOX_APP_SECRET', '')
    if default_secret:
        masked = default_secret[:4] + '****' if len(default_secret) > 4 else '****'
        app_secret = input(f"Enter Dropbox App Secret [{masked}]: ").strip()
        if not app_secret:
            app_secret = default_secret
    else:
        app_secret = input("Enter Dropbox App Secret: ").strip()

    if not app_secret:
        print("ERROR: App Secret is required")
        sys.exit(1)

    # Check if we already have a refresh token
    existing_refresh = existing.get('DROPBOX_REFRESH_TOKEN', '')
    if existing_refresh:
        print()
        print("Found existing refresh token in .env")
        reauth = input("Do you want to re-authenticate? [y/N]: ").strip().lower()
        if reauth != 'y':
            print("Keeping existing token.")
            # Just update app key/secret if changed
            update_env_file('DROPBOX_APP_KEY', app_key)
            update_env_file('DROPBOX_APP_SECRET', app_secret)
            print("Updated DROPBOX_APP_KEY and DROPBOX_APP_SECRET in .env")
            test_connection(app_key, app_secret, existing_refresh)
            return

    print()
    print("Starting OAuth flow...")
    print()

    # Use PKCE flow for better security (no redirect URI needed)
    auth_flow = DropboxOAuth2FlowNoRedirect(
        consumer_key=app_key,
        consumer_secret=app_secret,
        token_access_type='offline',  # This gives us a refresh token
        use_pkce=True
    )

    # Get authorization URL
    auth_url = auth_flow.start()

    print("1. Opening browser for authorization...")
    print()
    print(f"   If browser doesn't open, visit this URL manually:")
    print(f"   {auth_url}")
    print()

    # Try to open browser
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    print("2. After authorizing, you'll see an authorization code.")
    print()

    auth_code = input("3. Enter the authorization code here: ").strip()

    if not auth_code:
        print("ERROR: Authorization code is required")
        sys.exit(1)

    # Exchange code for tokens
    print()
    print("4. Exchanging code for tokens...")

    try:
        oauth_result = auth_flow.finish(auth_code)
    except Exception as e:
        print(f"ERROR: Failed to exchange authorization code: {e}")
        sys.exit(1)

    refresh_token = oauth_result.refresh_token

    if not refresh_token:
        print("ERROR: No refresh token received (did you request offline access?)")
        sys.exit(1)

    print()
    print("Success! Received refresh token.")
    print()

    # Save to .env
    print("5. Saving credentials to .env file...")

    update_env_file('DROPBOX_APP_KEY', app_key)
    update_env_file('DROPBOX_APP_SECRET', app_secret)
    update_env_file('DROPBOX_REFRESH_TOKEN', refresh_token)

    print()
    print("Saved to .env:")
    print(f"   DROPBOX_APP_KEY={app_key}")
    print(f"   DROPBOX_APP_SECRET={'*' * len(app_secret)}")
    print(f"   DROPBOX_REFRESH_TOKEN={'*' * 20}...")
    print()

    # Test connection
    test_connection(app_key, app_secret, refresh_token)


def test_connection(app_key: str, app_secret: str, refresh_token: str):
    """Test the Dropbox connection."""
    print("6. Testing connection...")
    print()

    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret
        )

        account = dbx.users_get_current_account()
        print(f"   Connected as: {account.name.display_name}")
        print(f"   Email: {account.email}")
        print()

        # Check/create folders
        print("7. Checking folder structure...")

        folders = ['/incoming', '/processed', '/failed', '/requests']
        for folder in folders:
            try:
                dbx.files_create_folder_v2(folder)
                print(f"   Created: {folder}")
            except dropbox.exceptions.ApiError as e:
                if 'conflict' in str(e):
                    print(f"   Exists: {folder}")
                else:
                    print(f"   Error: {folder} - {e}")

        print()
        print("=" * 60)
        print("Setup complete!")
        print("=" * 60)
        print()
        print("The energy importer can now access your Dropbox App folder.")
        print()
        print("Folder structure:")
        print("   /incoming  - Place energy data files here")
        print("   /processed - Successfully imported files moved here")
        print("   /failed    - Files with errors moved here")
        print("   /requests  - Request file for work server")
        print()

    except dropbox.exceptions.AuthError as e:
        print(f"   ERROR: Authentication failed: {e}")
        print()
        print("   Please check your credentials and try again.")
        sys.exit(1)
    except Exception as e:
        print(f"   ERROR: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

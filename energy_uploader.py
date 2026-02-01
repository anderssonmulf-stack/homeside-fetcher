#!/usr/bin/env python3
"""
Energy File Uploader

Simple script to upload energy files to Dropbox.
Works on both Windows and Linux.

Usage:
    python energy_uploader.py file1.txt file2.txt ...
    python energy_uploader.py *.txt
    python energy_uploader.py --watch /path/to/folder  # Watch folder for new files

Environment variables:
    DROPBOX_ACCESS_TOKEN - Dropbox API token (required)

Or create a config file 'energy_uploader.conf' in the same folder:
    DROPBOX_ACCESS_TOKEN=your_token_here
"""

import os
import sys
import time
import argparse
from pathlib import Path

try:
    import dropbox
except ImportError:
    print("ERROR: dropbox package not installed")
    print("Install with: pip install dropbox")
    sys.exit(1)


def load_config():
    """Load config from environment or config file."""
    token = os.getenv('DROPBOX_ACCESS_TOKEN')

    if not token:
        # Try config file in script directory
        config_path = Path(__file__).parent / 'energy_uploader.conf'
        if config_path.exists():
            with open(config_path) as f:
                for line in f:
                    if line.startswith('DROPBOX_ACCESS_TOKEN='):
                        token = line.split('=', 1)[1].strip()
                        break

    return token


def upload_file(dbx: dropbox.Dropbox, local_path: str) -> bool:
    """Upload a file to Dropbox /incoming folder."""
    filename = os.path.basename(local_path)
    dropbox_path = f'/incoming/{filename}'

    try:
        with open(local_path, 'rb') as f:
            content = f.read()

        # Upload (overwrite if exists)
        dbx.files_upload(
            content,
            dropbox_path,
            mode=dropbox.files.WriteMode.overwrite
        )
        print(f"Uploaded: {filename} -> {dropbox_path}")
        return True

    except dropbox.exceptions.ApiError as e:
        print(f"ERROR uploading {filename}: {e}")
        return False
    except FileNotFoundError:
        print(f"ERROR: File not found: {local_path}")
        return False


def ensure_folders(dbx: dropbox.Dropbox):
    """Ensure required folders exist."""
    for folder in ['/incoming', '/processed', '/failed']:
        try:
            dbx.files_get_metadata(folder)
        except dropbox.exceptions.ApiError:
            try:
                dbx.files_create_folder_v2(folder)
                print(f"Created folder: {folder}")
            except:
                pass


def watch_folder(dbx: dropbox.Dropbox, folder: str, interval: int = 60):
    """Watch a folder and upload new .txt files."""
    print(f"Watching {folder} for new .txt files (checking every {interval}s)")
    print("Press Ctrl+C to stop")

    processed = set()

    # Initial scan
    for f in Path(folder).glob('*.txt'):
        processed.add(str(f))
    print(f"Found {len(processed)} existing file(s), will ignore these")

    try:
        while True:
            for f in Path(folder).glob('*.txt'):
                path = str(f)
                if path not in processed:
                    print(f"\nNew file detected: {f.name}")
                    if upload_file(dbx, path):
                        processed.add(path)
                        # Optionally move to a 'sent' subfolder
                        # sent_folder = Path(folder) / 'sent'
                        # sent_folder.mkdir(exist_ok=True)
                        # f.rename(sent_folder / f.name)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopped watching")


def main():
    parser = argparse.ArgumentParser(description='Upload energy files to Dropbox')
    parser.add_argument('files', nargs='*', help='Files to upload')
    parser.add_argument('--watch', metavar='FOLDER', help='Watch folder for new files')
    parser.add_argument('--interval', type=int, default=60, help='Watch interval in seconds (default: 60)')
    args = parser.parse_args()

    # Get token
    token = load_config()
    if not token:
        print("ERROR: DROPBOX_ACCESS_TOKEN not set")
        print("")
        print("Set environment variable:")
        print("  export DROPBOX_ACCESS_TOKEN=your_token_here")
        print("")
        print("Or create energy_uploader.conf:")
        print("  DROPBOX_ACCESS_TOKEN=your_token_here")
        sys.exit(1)

    # Connect
    try:
        dbx = dropbox.Dropbox(token)
        account = dbx.users_get_current_account()
        print(f"Connected to Dropbox as: {account.name.display_name}")
    except dropbox.exceptions.AuthError:
        print("ERROR: Invalid Dropbox token")
        sys.exit(1)

    # Ensure folders exist
    ensure_folders(dbx)

    # Watch mode
    if args.watch:
        watch_folder(dbx, args.watch, args.interval)
        return

    # Upload mode
    if not args.files:
        print("No files specified")
        print("Usage: python energy_uploader.py file1.txt file2.txt ...")
        print("       python energy_uploader.py --watch /path/to/folder")
        sys.exit(1)

    success = 0
    for filepath in args.files:
        if upload_file(dbx, filepath):
            success += 1

    print(f"\nUploaded {success}/{len(args.files)} file(s)")


if __name__ == '__main__':
    main()

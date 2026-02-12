#!/usr/bin/env python3
"""
Remove Customer Script

Fully removes a customer from the Homeside Fetcher system:
1. Deletes all InfluxDB data (house_id tag)
2. Deletes profile JSON
3. Deletes signals JSON (if exists)
4. Removes .env credential lines (HOUSE_{id}_*)
5. Re-syncs Dropbox meter CSV (removes meter from request file)
6. Orchestrator auto-stops the subprocess within 60 seconds

Usage:
    python3 remove_customer.py HEM_FJV_Villa_149_TEST
    python3 remove_customer.py HEM_FJV_Villa_149_TEST --force      # Skip confirmation
    python3 remove_customer.py HEM_FJV_Villa_149_TEST --dry-run    # Show what would be deleted
"""

import os
import sys
import re
import argparse
from datetime import datetime, timezone


# Project root (where this script lives)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PROFILES_DIR = os.path.join(PROJECT_ROOT, 'profiles')
ENV_FILE = os.path.join(PROJECT_ROOT, '.env')


def find_artifacts(customer_id: str) -> dict:
    """Find all artifacts that exist for a customer."""
    artifacts = {}

    # Profile JSON
    profile_path = os.path.join(PROFILES_DIR, f'{customer_id}.json')
    artifacts['profile'] = {
        'path': profile_path,
        'exists': os.path.exists(profile_path),
    }

    # Signals JSON
    signals_path = os.path.join(PROFILES_DIR, f'{customer_id}_signals.json')
    artifacts['signals'] = {
        'path': signals_path,
        'exists': os.path.exists(signals_path),
    }

    # .env credential lines
    env_lines = find_env_lines(customer_id)
    artifacts['env'] = {
        'path': ENV_FILE,
        'exists': len(env_lines) > 0,
        'lines': env_lines,
    }

    return artifacts


def find_env_lines(customer_id: str) -> list:
    """Find all .env lines related to this customer (HOUSE_{id}_* and preceding comment)."""
    if not os.path.exists(ENV_FILE):
        return []

    with open(ENV_FILE, 'r') as f:
        lines = f.readlines()

    prefix = f'HOUSE_{customer_id}_'
    matched = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(prefix):
            # Also include preceding comment line if it exists
            if i > 0 and lines[i - 1].strip().startswith('#'):
                matched.append((i - 1, lines[i - 1].rstrip()))
            matched.append((i, stripped))
    return matched


def remove_env_lines(customer_id: str) -> int:
    """Remove all HOUSE_{customer_id}_* lines and their preceding comment from .env."""
    if not os.path.exists(ENV_FILE):
        return 0

    with open(ENV_FILE, 'r') as f:
        lines = f.readlines()

    prefix = f'HOUSE_{customer_id}_'

    # Find line indices to remove
    remove_indices = set()
    for i, line in enumerate(lines):
        if line.strip().startswith(prefix):
            remove_indices.add(i)
            # Also remove preceding comment line
            if i > 0 and lines[i - 1].strip().startswith('#'):
                remove_indices.add(i - 1)

    if not remove_indices:
        return 0

    # Write back without removed lines
    with open(ENV_FILE, 'w') as f:
        for i, line in enumerate(lines):
            if i not in remove_indices:
                f.write(line)

    return len(remove_indices)


def delete_influxdb_data(customer_id: str) -> bool:
    """Delete all InfluxDB data for this house_id."""
    try:
        from influxdb_client import InfluxDBClient
    except ImportError:
        print("  influxdb_client not installed, skipping InfluxDB deletion")
        return False

    # Read settings from .env or use defaults
    env_settings = {}
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    env_settings[key.strip()] = value.strip()

    url = env_settings.get('INFLUXDB_URL', 'http://localhost:8086')
    token = env_settings.get('INFLUXDB_TOKEN', '')
    org = env_settings.get('INFLUXDB_ORG', 'homeside')
    bucket = env_settings.get('INFLUXDB_BUCKET', 'heating')

    if not token:
        print("  No INFLUXDB_TOKEN found in .env")
        return False

    client = InfluxDBClient(url=url, token=token, org=org)
    delete_api = client.delete_api()

    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    stop = datetime(2099, 12, 31, tzinfo=timezone.utc)
    predicate = f'house_id="{customer_id}"'

    delete_api.delete(start=start, stop=stop, predicate=predicate, bucket=bucket)
    client.close()
    return True


def trigger_dropbox_sync() -> bool:
    """Re-sync Dropbox meter CSV to remove deleted customer's meter."""
    try:
        from dropbox_sync import sync_meters
        return sync_meters()
    except ImportError:
        print("  dropbox_sync not available - skipping")
        return False
    except Exception as e:
        print(f"  Dropbox sync failed (non-fatal): {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Remove a customer from Homeside Fetcher')
    parser.add_argument('customer_id', type=str, help='Customer ID to remove (e.g., HEM_FJV_Villa_149_TEST)')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without deleting')
    args = parser.parse_args()

    customer_id = args.customer_id

    print("=" * 60)
    print(f"Remove Customer: {customer_id}")
    print("=" * 60)
    print()

    # Step 0: Validate — find what exists
    artifacts = find_artifacts(customer_id)
    profile_exists = artifacts['profile']['exists']
    signals_exists = artifacts['signals']['exists']
    env_exists = artifacts['env']['exists']

    if not profile_exists and not signals_exists and not env_exists:
        print(f"No artifacts found for {customer_id}.")
        print("  (Profile, signals, and .env entries are all absent)")
        print()
        print("InfluxDB data may still exist. Use --force to delete InfluxDB data only.")
        if not args.force:
            sys.exit(1)

    # Show what will be deleted
    print("Artifacts to delete:")
    print()

    if profile_exists:
        print(f"  [EXISTS] Profile:  {artifacts['profile']['path']}")
    else:
        print(f"  [ABSENT] Profile:  {artifacts['profile']['path']}")

    if signals_exists:
        print(f"  [EXISTS] Signals:  {artifacts['signals']['path']}")
    else:
        print(f"  [ABSENT] Signals:  {artifacts['signals']['path']}")

    if env_exists:
        print(f"  [EXISTS] .env entries:")
        for line_num, line_text in artifacts['env']['lines']:
            print(f"           L{line_num + 1}: {line_text}")
    else:
        print(f"  [ABSENT] .env entries: no HOUSE_{customer_id}_* lines")

    print(f"  [DELETE] InfluxDB: all data with house_id=\"{customer_id}\"")
    print(f"  [SYNC]   Dropbox:  re-sync meter CSV after deletion")
    print()

    if args.dry_run:
        print("DRY RUN — nothing was deleted.")
        sys.exit(0)

    # Confirm
    if not args.force:
        confirm = input(f"Delete all data for {customer_id}? Type 'yes' to confirm: ").strip()
        if confirm != 'yes':
            print("Aborted.")
            sys.exit(0)

    print()
    step = 0
    total_steps = 5

    # Step 1: Delete InfluxDB data
    step += 1
    print(f"[{step}/{total_steps}] Deleting InfluxDB data...")
    try:
        if delete_influxdb_data(customer_id):
            print(f"  Deleted all data with house_id=\"{customer_id}\"")
        else:
            print(f"  Failed to delete InfluxDB data (continuing)")
    except Exception as e:
        print(f"  InfluxDB deletion error (continuing): {e}")

    # Step 2: Delete profile JSON
    step += 1
    print(f"[{step}/{total_steps}] Deleting profile...")
    if profile_exists:
        os.remove(artifacts['profile']['path'])
        print(f"  Deleted: {artifacts['profile']['path']}")
    else:
        print(f"  Already absent")

    # Step 3: Delete signals JSON
    step += 1
    print(f"[{step}/{total_steps}] Deleting signals file...")
    if signals_exists:
        os.remove(artifacts['signals']['path'])
        print(f"  Deleted: {artifacts['signals']['path']}")
    else:
        print(f"  Already absent")

    # Step 4: Remove .env entries
    step += 1
    print(f"[{step}/{total_steps}] Removing .env entries...")
    if env_exists:
        removed = remove_env_lines(customer_id)
        print(f"  Removed {removed} lines from .env")
    else:
        print(f"  No entries to remove")

    # Step 5: Re-sync Dropbox
    step += 1
    print(f"[{step}/{total_steps}] Re-syncing Dropbox meter CSV...")
    try:
        if trigger_dropbox_sync():
            print(f"  Meter CSV synced to Dropbox")
        else:
            print(f"  Skipped (non-fatal)")
    except Exception as e:
        print(f"  Dropbox sync failed (non-fatal): {e}")

    # Summary
    print()
    print("=" * 60)
    print(f"Done! {customer_id} has been removed.")
    print("=" * 60)
    print()
    print("Notes:")
    print("  - Orchestrator will stop the subprocess within 60 seconds (if running)")
    print("  - InfluxDB data deletion is permanent and cannot be undone")
    print(f"  - Verify with: influx query 'from(bucket:\"heating\") |> range(start:-1y) |> filter(fn:(r) => r.house_id == \"{customer_id}\") |> limit(n:1)'")
    print()


if __name__ == "__main__":
    main()

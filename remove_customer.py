#!/usr/bin/env python3
"""
Remove Customer / Building Script

Removes a house or building from the Homeside Fetcher system.

Hard removal (default):
1. Deletes all InfluxDB data
2. Deletes config JSON (and signals JSON for houses)
3. Removes .env credential lines
4. Re-syncs Dropbox meter CSV (houses only)
5. Orchestrator auto-stops the subprocess within 60 seconds

Soft removal (--soft):
1. Deletes config JSON (and signals JSON for houses)
2. Removes .env credential lines
3. Adds entry to offboarded.json with 30-day grace period
4. InfluxDB data is purged automatically by orchestrator after grace period

Usage:
    # Hard remove house (immediate InfluxDB delete)
    python3 remove_customer.py HEM_FJV_Villa_149

    # Soft remove house (30-day grace period)
    python3 remove_customer.py HEM_FJV_Villa_149 --soft

    # Soft remove building
    python3 remove_customer.py TE236_HEM_Kontor --soft --type building

    # Custom grace period
    python3 remove_customer.py HEM_FJV_Villa_149 --soft --days 60

    # Hard remove building (immediate InfluxDB delete)
    python3 remove_customer.py TE236_HEM_Kontor --type building

    # Dry run / skip confirmation
    python3 remove_customer.py HEM_FJV_Villa_149 --dry-run
    python3 remove_customer.py HEM_FJV_Villa_149 --force
"""

import json
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone


# Project root (where this script lives)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PROFILES_DIR = os.path.join(PROJECT_ROOT, 'profiles')
BUILDINGS_DIR = os.path.join(PROJECT_ROOT, 'buildings')
ENV_FILE = os.path.join(PROJECT_ROOT, '.env')
OFFBOARDED_FILE = os.path.join(PROJECT_ROOT, 'offboarded.json')

DEFAULT_GRACE_DAYS = 30


def config_dir_for_type(entity_type: str) -> str:
    return BUILDINGS_DIR if entity_type == 'building' else PROFILES_DIR


def env_prefix_for_type(entity_type: str) -> str:
    return 'BUILDING' if entity_type == 'building' else 'HOUSE'


def influx_tag_for_type(entity_type: str) -> str:
    return 'building_id' if entity_type == 'building' else 'house_id'


def find_artifacts(entity_id: str, entity_type: str) -> dict:
    """Find all artifacts that exist for a house or building."""
    artifacts = {}
    cfg_dir = config_dir_for_type(entity_type)

    # Config JSON
    config_path = os.path.join(cfg_dir, f'{entity_id}.json')
    artifacts['config'] = {
        'path': config_path,
        'exists': os.path.exists(config_path),
    }

    # Signals JSON (houses only)
    if entity_type == 'house':
        signals_path = os.path.join(cfg_dir, f'{entity_id}_signals.json')
        artifacts['signals'] = {
            'path': signals_path,
            'exists': os.path.exists(signals_path),
        }

    # .env credential lines
    env_lines = find_env_lines(entity_id, entity_type)
    artifacts['env'] = {
        'path': ENV_FILE,
        'exists': len(env_lines) > 0,
        'lines': env_lines,
    }

    return artifacts


def get_friendly_name(entity_id: str, entity_type: str) -> str:
    """Try to read friendly_name from the config JSON."""
    cfg_dir = config_dir_for_type(entity_type)
    config_path = os.path.join(cfg_dir, f'{entity_id}.json')
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                data = json.load(f)
            return data.get('friendly_name', entity_id)
        except Exception:
            pass
    return entity_id


def find_env_lines(entity_id: str, entity_type: str) -> list:
    """Find all .env lines related to this entity and preceding comments."""
    if not os.path.exists(ENV_FILE):
        return []

    with open(ENV_FILE, 'r') as f:
        lines = f.readlines()

    prefix = f'{env_prefix_for_type(entity_type)}_{entity_id}_'
    matched = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(prefix):
            # Also include preceding comment line if it exists
            if i > 0 and lines[i - 1].strip().startswith('#'):
                matched.append((i - 1, lines[i - 1].rstrip()))
            matched.append((i, stripped))
    return matched


def remove_env_lines(entity_id: str, entity_type: str) -> int:
    """Remove credential lines and their preceding comments from .env."""
    if not os.path.exists(ENV_FILE):
        return 0

    with open(ENV_FILE, 'r') as f:
        lines = f.readlines()

    prefix = f'{env_prefix_for_type(entity_type)}_{entity_id}_'

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


def delete_influxdb_data(entity_id: str, entity_type: str) -> bool:
    """Delete all InfluxDB data for this entity."""
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

    tag = influx_tag_for_type(entity_type)
    client = InfluxDBClient(url=url, token=token, org=org)
    client.delete_api().delete(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        stop=datetime(2099, 12, 31, tzinfo=timezone.utc),
        predicate=f'{tag}="{entity_id}"',
        bucket=bucket,
    )
    client.close()
    return True


def add_to_offboarded(entity_id: str, entity_type: str, friendly_name: str,
                      grace_days: int) -> None:
    """Add an entry to offboarded.json for deferred InfluxDB purge."""
    data = {'pending_purge': [], 'purged': []}
    if os.path.exists(OFFBOARDED_FILE):
        try:
            with open(OFFBOARDED_FILE) as f:
                data = json.load(f)
        except Exception:
            pass

    # Don't add duplicates
    for entry in data.get('pending_purge', []):
        if entry['id'] == entity_id:
            print(f"  Already in offboarded.json (pending purge)")
            return

    now = datetime.now(timezone.utc)
    purge_after = now + timedelta(days=grace_days)

    entry = {
        'id': entity_id,
        'type': entity_type,
        'friendly_name': friendly_name,
        'offboarded_at': now.isoformat(),
        'purge_after': purge_after.isoformat(),
        'influx_tag': influx_tag_for_type(entity_type),
    }

    data.setdefault('pending_purge', []).append(entry)

    with open(OFFBOARDED_FILE, 'w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')

    print(f"  Added to offboarded.json — purge after {purge_after.strftime('%Y-%m-%d')}")


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
    parser = argparse.ArgumentParser(
        description='Remove a house or building from Homeside Fetcher')
    parser.add_argument('entity_id', type=str,
                        help='ID to remove (e.g., HEM_FJV_Villa_149 or TE236_HEM_Kontor)')
    parser.add_argument('--type', choices=['house', 'building'], default='house',
                        help='Entity type (default: house)')
    parser.add_argument('--soft', action='store_true',
                        help='Soft offboard: remove config/creds, defer InfluxDB purge')
    parser.add_argument('--days', type=int, default=DEFAULT_GRACE_DAYS,
                        help=f'Grace period in days for --soft (default: {DEFAULT_GRACE_DAYS})')
    parser.add_argument('--force', action='store_true',
                        help='Skip confirmation prompt')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would happen without doing it')
    args = parser.parse_args()

    entity_id = args.entity_id
    entity_type = args.type
    tag = influx_tag_for_type(entity_type)
    mode = "Soft offboard" if args.soft else "Hard remove"
    friendly_name = get_friendly_name(entity_id, entity_type)

    print("=" * 60)
    print(f"{mode} {entity_type}: {entity_id}")
    if friendly_name != entity_id:
        print(f"Friendly name: {friendly_name}")
    print("=" * 60)
    print()

    # Find what exists
    artifacts = find_artifacts(entity_id, entity_type)
    config_exists = artifacts['config']['exists']
    signals_exists = artifacts.get('signals', {}).get('exists', False)
    env_exists = artifacts['env']['exists']

    has_any = config_exists or signals_exists or env_exists

    if not has_any and not args.soft:
        print(f"No artifacts found for {entity_id}.")
        print(f"  (Config, signals, and .env entries are all absent)")
        print()
        print("InfluxDB data may still exist. Use --force to delete InfluxDB data only.")
        if not args.force:
            sys.exit(1)

    # Show what will happen
    print("Plan:")
    print()

    if config_exists:
        print(f"  [EXISTS] Config:   {artifacts['config']['path']}")
    else:
        print(f"  [ABSENT] Config:   {artifacts['config']['path']}")

    if entity_type == 'house':
        if signals_exists:
            print(f"  [EXISTS] Signals:  {artifacts['signals']['path']}")
        else:
            print(f"  [ABSENT] Signals:  {artifacts['signals']['path']}")

    if env_exists:
        print(f"  [EXISTS] .env entries:")
        for line_num, line_text in artifacts['env']['lines']:
            print(f"           L{line_num + 1}: {line_text}")
    else:
        prefix = env_prefix_for_type(entity_type)
        print(f"  [ABSENT] .env entries: no {prefix}_{entity_id}_* lines")

    if args.soft:
        print(f"  [DEFER]  InfluxDB: schedule purge of {tag}=\"{entity_id}\" "
              f"in {args.days} days")
    else:
        print(f"  [DELETE] InfluxDB: all data with {tag}=\"{entity_id}\"")

    if not args.soft:
        print(f"  [SYNC]   Dropbox:  re-sync meter CSV after deletion")

    print()

    if args.dry_run:
        print("DRY RUN — nothing was changed.")
        sys.exit(0)

    # Confirm
    if not args.force:
        action = "soft-offboard" if args.soft else "delete all data for"
        confirm = input(f"{mode} {entity_id}? Type 'yes' to confirm: ").strip()
        if confirm != 'yes':
            print("Aborted.")
            sys.exit(0)

    print()

    if args.soft:
        _run_soft_offboard(entity_id, entity_type, friendly_name, args.days,
                           artifacts, config_exists, signals_exists, env_exists)
    else:
        _run_hard_remove(entity_id, entity_type, artifacts,
                         config_exists, signals_exists, env_exists)


def _run_soft_offboard(entity_id, entity_type, friendly_name, grace_days,
                       artifacts, config_exists, signals_exists, env_exists):
    """Soft offboard: remove config/creds, schedule InfluxDB purge."""
    step = 0
    total_steps = 3 if entity_type == 'house' else 2
    total_steps += 1  # offboarded.json

    # Step 1: Delete config JSON
    step += 1
    print(f"[{step}/{total_steps}] Deleting config...")
    if config_exists:
        os.remove(artifacts['config']['path'])
        print(f"  Deleted: {artifacts['config']['path']}")
    else:
        print(f"  Already absent")

    # Step 2: Delete signals JSON (houses only)
    if entity_type == 'house':
        step += 1
        print(f"[{step}/{total_steps}] Deleting signals file...")
        if signals_exists:
            os.remove(artifacts['signals']['path'])
            print(f"  Deleted: {artifacts['signals']['path']}")
        else:
            print(f"  Already absent")

    # Step 3: Remove .env entries
    step += 1
    print(f"[{step}/{total_steps}] Removing .env entries...")
    if env_exists:
        removed = remove_env_lines(entity_id, entity_type)
        print(f"  Removed {removed} lines from .env")
    else:
        print(f"  No entries to remove")

    # Step 4: Add to offboarded.json
    step += 1
    print(f"[{step}/{total_steps}] Scheduling InfluxDB purge...")
    add_to_offboarded(entity_id, entity_type, friendly_name, grace_days)

    # Summary
    tag = influx_tag_for_type(entity_type)
    purge_date = (datetime.now(timezone.utc) + timedelta(days=grace_days)).strftime('%Y-%m-%d')
    print()
    print("=" * 60)
    print(f"Done! {entity_id} soft-offboarded.")
    print("=" * 60)
    print()
    print("Notes:")
    print("  - Orchestrator will stop the subprocess within 60 seconds")
    print(f"  - InfluxDB data ({tag}=\"{entity_id}\") will be purged on ~{purge_date}")
    print(f"  - To cancel: remove the entry from offboarded.json")
    print(f"  - To purge now: python3 remove_customer.py {entity_id} --type {entity_type} --force")
    print()


def _run_hard_remove(entity_id, entity_type, artifacts,
                     config_exists, signals_exists, env_exists):
    """Hard remove: delete everything immediately."""
    tag = influx_tag_for_type(entity_type)
    has_signals = entity_type == 'house'
    total_steps = 4 + (1 if has_signals else 0)
    step = 0

    # Step 1: Delete InfluxDB data
    step += 1
    print(f"[{step}/{total_steps}] Deleting InfluxDB data...")
    try:
        if delete_influxdb_data(entity_id, entity_type):
            print(f"  Deleted all data with {tag}=\"{entity_id}\"")
        else:
            print(f"  Failed to delete InfluxDB data (continuing)")
    except Exception as e:
        print(f"  InfluxDB deletion error (continuing): {e}")

    # Step 2: Delete config JSON
    step += 1
    print(f"[{step}/{total_steps}] Deleting config...")
    if config_exists:
        os.remove(artifacts['config']['path'])
        print(f"  Deleted: {artifacts['config']['path']}")
    else:
        print(f"  Already absent")

    # Step 3: Delete signals JSON (houses only)
    if has_signals:
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
        removed = remove_env_lines(entity_id, entity_type)
        print(f"  Removed {removed} lines from .env")
    else:
        print(f"  No entries to remove")

    # Step 5: Re-sync Dropbox (removes any meters tied to this entity)
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
    print(f"Done! {entity_id} has been removed.")
    print("=" * 60)
    print()
    print("Notes:")
    print("  - Orchestrator will stop the subprocess within 60 seconds (if running)")
    print("  - InfluxDB data deletion is permanent and cannot be undone")
    print(f"  - Verify with: influx query 'from(bucket:\"heating\") |> range(start:-1y) "
          f"|> filter(fn:(r) => r.{tag} == \"{entity_id}\") |> limit(n:1)'")
    print()


if __name__ == "__main__":
    main()

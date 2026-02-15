#!/usr/bin/env python3
"""
Add New Building Script

Automates onboarding a commercial building connected via Arrigo BMS:
1. Connects to Arrigo, discovers and categorizes signals
2. Creates buildings/<building_id>.json with discovered signals
3. Adds credentials to .env (BUILDING_<id>_USERNAME/PASSWORD)
4. Optionally adds energy meter ID and syncs Dropbox
5. Optionally runs historical data bootstrap (90 days)
6. Orchestrator auto-detects new config within 60 seconds

Usage:
    python3 add_building.py

    python3 add_building.py --non-interactive \
        --host exodrift05.systeminstallation.se \
        --username "Ulf Andersson" --password "xxx" \
        --name "HEM Kontor" --building-id TE236_HEM_Kontor

    python3 add_building.py --non-interactive \
        --host exodrift05.systeminstallation.se \
        --username "user" --password "pass" \
        --name "Building Name" --building-id SITE_Name \
        --meter-id 735999255020055028 --bootstrap
"""

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timezone

from arrigo_api import ArrigoAPI, save_building_config


def add_credentials_to_env(building_id: str, username: str, password: str,
                           friendly_name: str, meter_ids: list = None,
                           env_file: str = ".env") -> bool:
    """Append per-building credentials to .env file."""
    env_key = f"BUILDING_{building_id}_USERNAME"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            content = f.read()
        if env_key in content:
            print(f"  Credentials for {building_id} already exist in {env_file}")
            return False

    lines = [
        f"\n# {friendly_name} ({building_id})\n",
        f"BUILDING_{building_id}_USERNAME={username}\n",
        f"BUILDING_{building_id}_PASSWORD={password}\n",
    ]
    if meter_ids:
        lines.append(f"BUILDING_{building_id}_METER_IDS={','.join(meter_ids)}\n")

    with open(env_file, 'a') as f:
        f.writelines(lines)

    return True


def trigger_dropbox_sync() -> bool:
    """Trigger Dropbox meter sync so work server picks up new meter."""
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
    parser = argparse.ArgumentParser(description='Add a new commercial building')
    parser.add_argument('--non-interactive', action='store_true',
                        help='Run without prompts')
    parser.add_argument('--host', type=str,
                        help='Arrigo host (e.g., exodrift05.systeminstallation.se)')
    parser.add_argument('--username', type=str, help='Arrigo username')
    parser.add_argument('--password', type=str, help='Arrigo password')
    parser.add_argument('--name', type=str, help='Friendly name')
    parser.add_argument('--building-id', type=str,
                        help='Building ID (e.g., TE236_HEM_Kontor)')
    parser.add_argument('--account', type=str, default='',
                        help='Arrigo account name')
    parser.add_argument('--meter-id', type=str, action='append', dest='meter_ids',
                        help='Energy meter ID (can be repeated)')
    parser.add_argument('--bootstrap', action='store_true',
                        help='Import 90 days of historical data after setup')
    parser.add_argument('--bootstrap-days', type=int, default=90,
                        help='Days of historical data to bootstrap')
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    print("=" * 60)
    print("  ADD NEW BUILDING")
    print("=" * 60)

    # Gather info interactively if needed
    if args.non_interactive:
        host = args.host
        username = args.username
        password = args.password
        friendly_name = args.name
        building_id = args.building_id
        account = args.account
        meter_ids = args.meter_ids or []
        do_bootstrap = args.bootstrap
    else:
        host = input("Arrigo host (e.g., exodrift05.systeminstallation.se): ").strip()
        username = input("Arrigo username: ").strip()
        password = input("Arrigo password: ").strip()
        friendly_name = input("Friendly name (e.g., HEM Kontor TE236): ").strip()
        building_id = input("Building ID (e.g., TE236_HEM_Kontor): ").strip()
        account = input("Arrigo account name (press Enter to skip): ").strip()
        meter_input = input("Energy meter ID (press Enter to skip): ").strip()
        meter_ids = [meter_input] if meter_input else []
        do_bootstrap = input("Bootstrap 90 days of historical data? [y/N]: ").strip().lower() == 'y'

    if not all([host, username, password, friendly_name, building_id]):
        print("ERROR: All fields are required (host, username, password, name, building-id)")
        sys.exit(1)

    # Check if config already exists
    config_path = os.path.join('buildings', f'{building_id}.json')
    if os.path.exists(config_path):
        if args.non_interactive:
            print(f"WARNING: {config_path} already exists, will be overwritten")
        else:
            overwrite = input(f"{config_path} exists. Overwrite? [y/N]: ").strip().lower()
            if overwrite != 'y':
                print("Aborted.")
                sys.exit(0)

    # Step 1: Connect and discover signals
    print(f"\n1. Connecting to {host}...")
    import logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)
    logger = logging.getLogger('add_building')

    client = ArrigoAPI(
        host=host,
        username=username,
        password=password,
        logger=logger,
        verbose=args.verbose,
    )

    if not client.login():
        print("ERROR: Authentication failed")
        sys.exit(1)
    print("  Authenticated successfully")

    # Step 2: Full discovery via discover_building()
    print(f"\n2. Discovering building signals...")
    config = client.discover_building()

    # Override/set fields from user input
    config["building_id"] = building_id
    config["friendly_name"] = friendly_name
    config["meter_ids"] = meter_ids
    config["energy_separation"] = {
        "enabled": bool(meter_ids),
        "method": "k_calibration",
        "heat_loss_k": None,
        "k_percentile": 15,
        "calibration_date": None,
        "calibration_days": 60,
        "dhw_percentage": None,
        "assumed_indoor_temp": 21.0,
        "field_mapping": {
            "outdoor_temperature": "outdoor_temp_fvc",
            "hot_water_temp": "vv1_hot_water_temp"
        }
    }
    if account:
        config["connection"]["account"] = account
    config["poll_interval_minutes"] = 5

    analog_count = len(config.get("analog_signals", {}))
    digital_count = len(config.get("digital_signals", {}))

    saved_path = save_building_config(config)
    print(f"  Config saved: {saved_path}")
    print(f"  Signals: {analog_count} analog, {digital_count} digital")
    print(f"  NOTE: Edit the config to set field_name and fetch=true for signals you want")

    # Step 3: Add credentials to .env
    print(f"\n3. Adding credentials to .env...")
    if add_credentials_to_env(building_id, username, password, friendly_name, meter_ids):
        print(f"  Added BUILDING_{building_id}_USERNAME/PASSWORD to .env")
    else:
        print(f"  Credentials already exist in .env")

    # Step 4: Sync Dropbox meters (if meter IDs provided)
    if meter_ids:
        print(f"\n4. Syncing Dropbox meters...")
        if trigger_dropbox_sync():
            print("  Dropbox meter sync complete")
        else:
            print("  Dropbox sync skipped or failed (non-fatal)")
    else:
        print(f"\n4. No meter IDs - skipping Dropbox sync")

    # Step 5: Bootstrap historical data
    if do_bootstrap:
        print(f"\n5. Bootstrapping {args.bootstrap_days} days of historical data...")
        print("   (This may take a few minutes)")
        cmd = [
            sys.executable, 'building_import_historical_data.py',
            '--building', building_id,
            '--days', str(args.bootstrap_days),
        ]
        if args.verbose:
            cmd.append('--verbose')
        result = subprocess.run(cmd, env={**os.environ,
                                          'ARRIGO_USERNAME': username,
                                          'ARRIGO_PASSWORD': password})
        if result.returncode == 0:
            print("  Historical data import complete")
        else:
            print("  Historical data import failed (non-fatal)")
    else:
        print(f"\n5. Skipping historical data bootstrap")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  BUILDING ADDED SUCCESSFULLY")
    print(f"{'=' * 60}")
    print(f"  Building ID: {building_id}")
    print(f"  Config file: {saved_path}")
    print(f"  Signals discovered: {analog_count} analog, {digital_count} digital")
    if meter_ids:
        print(f"  Meter IDs: {', '.join(meter_ids)}")
        print(f"  Energy separation: enabled")
    print(f"\n  NEXT STEPS:")
    print(f"  1. Edit {saved_path} to set field_name and fetch=true for signals")
    print(f"  2. The orchestrator will auto-detect within 60 seconds")
    if not meter_ids:
        print(f"  3. Add meter_ids to the config when available")
        print(f"  4. Run: python3 dropbox_sync.py  (to sync meters)")


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Add New Customer Script

Automates the process of adding a new customer to the Homeside Fetcher system:
1. Creates customer profile JSON (with meter_ids + energy_separation config)
2. Appends per-house credentials to .env
3. Triggers Dropbox meter sync (so work server picks up new meter immediately)
4. Optionally triggers Arrigo 5-min data bootstrap
5. Orchestrator picks up the new profile within 60 seconds (no restart needed)

Usage:
    python3 add_customer.py
    python3 add_customer.py --non-interactive --username FC123 --password pass --name "House Name" --lat 56.67 --lon 12.86
    python3 add_customer.py --non-interactive --username FC123 --password pass --name "House Name" --lat 56.67 --lon 12.86 --meter-id 735999255020057923
    python3 add_customer.py --non-interactive --username FC123 --password pass --name "House Name" --lat 56.67 --lon 12.86 --meter-id 735999255020057923 --bootstrap
"""

import os
import sys
import json
import argparse
import re
from datetime import datetime, timedelta, timezone


def get_next_villa_number(profiles_dir: str = "profiles") -> int:
    """Find the next available villa number."""
    existing = []
    if os.path.exists(profiles_dir):
        for f in os.listdir(profiles_dir):
            match = re.search(r'HEM_FJV_Villa_(\d+)\.json', f)
            if match:
                existing.append(int(match.group(1)))
    return max(existing, default=0) + 1


def create_profile(customer_id: str, friendly_name: str,
                   meter_ids: list = None, enable_energy_sep: bool = False,
                   profiles_dir: str = "profiles") -> str:
    """Create a new customer profile JSON file."""
    # Calculate energy_data_start_date: 90 days back
    start_date = (datetime.now(timezone.utc) - timedelta(days=90)).strftime('%Y-%m-%d')

    profile = {
        "schema_version": 1,
        "customer_id": customer_id,
        "friendly_name": friendly_name,
        "building": {
            "description": "",
            "thermal_response": "medium"
        },
        "comfort": {
            "target_indoor_temp": 22.0,
            "acceptable_deviation": 1.0
        },
        "heating_system": {
            "response_time_minutes": 30,
            "max_supply_temp": 55
        },
        "learned": {
            "thermal_coefficient": None,
            "thermal_coefficient_confidence": 0.0,
            "hourly_bias": {},
            "samples_since_last_update": 0,
            "total_samples": 0,
            "next_update_at_samples": 24,
            "updated_at": None
        },
        "energy_separation": {
            "enabled": enable_energy_sep,
            "method": "k_calibration",
            "heat_loss_k": None,
            "k_percentile": 15,
            "calibration_days": 30,
            "dhw_percentage": None,
            "dhw_temp_threshold": 45.0,
            "dhw_temp_rise_threshold": 2.0,
            "dhw_baseline_temp": 25.0,
            "avg_dhw_power_kw": 25.0,
            "cold_water_temp": 8.0,
            "hot_water_target_temp": 55.0
        }
    }

    # Only set energy_data_start_date if meter_ids are provided
    if meter_ids:
        profile["energy_data_start_date"] = start_date

    os.makedirs(profiles_dir, exist_ok=True)
    filepath = os.path.join(profiles_dir, f"{customer_id}.json")

    with open(filepath, 'w') as f:
        json.dump(profile, f, indent=2)

    return filepath


def add_credentials_to_env(customer_id: str, username: str, password: str,
                           friendly_name: str, meter_ids: list = None,
                           env_file: str = ".env") -> bool:
    """Append per-house credentials and meter IDs to .env file."""
    # Check if credentials already exist
    env_key = f"HOUSE_{customer_id}_USERNAME"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            content = f.read()
        if env_key in content:
            print(f"  Credentials for {customer_id} already exist in {env_file}")
            return False

    # Append credentials + meter IDs
    lines = [
        f"\n# {friendly_name} ({customer_id})\n",
        f"HOUSE_{customer_id}_USERNAME={username}\n",
        f"HOUSE_{customer_id}_PASSWORD={password}\n",
    ]
    if meter_ids:
        lines.append(f"HOUSE_{customer_id}_METER_IDS={','.join(meter_ids)}\n")

    with open(env_file, 'a') as f:
        f.writelines(lines)

    return True


def trigger_dropbox_sync() -> bool:
    """Trigger Dropbox meter sync so work server picks up new meter immediately."""
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
    parser = argparse.ArgumentParser(description='Add a new customer to Homeside Fetcher')
    parser.add_argument('--non-interactive', action='store_true', help='Run without prompts')
    parser.add_argument('--username', type=str, help='HomeSide username (e.g., FC2000233091)')
    parser.add_argument('--password', type=str, help='HomeSide password')
    parser.add_argument('--name', type=str, help='Friendly name for the house')
    parser.add_argument('--lat', type=float, help='Latitude for weather')
    parser.add_argument('--lon', type=float, help='Longitude for weather')
    parser.add_argument('--villa-number', type=int, help='Villa number (auto-detected if not specified)')
    parser.add_argument('--meter-id', type=str, action='append', dest='meter_ids',
                        help='Energy meter ID (can specify multiple times)')
    parser.add_argument('--bootstrap', action='store_true',
                        help='Run Arrigo 5-min historical data bootstrap after profile creation')
    parser.add_argument('--source-house', type=str,
                        help='Source house to copy energy_meter data from in bootstrap (required with --bootstrap)')
    parser.add_argument('--enable-energy-sep', action='store_true',
                        help='Enable energy separation in profile immediately')
    args = parser.parse_args()

    print("=" * 60)
    print("Add New Customer to Homeside Fetcher")
    print("=" * 60)
    print()

    # Collect information
    if args.non_interactive:
        if not all([args.username, args.password, args.name]):
            print("ERROR: In non-interactive mode, --username, --password, and --name are required")
            sys.exit(1)
        username = args.username
        password = args.password
        friendly_name = args.name
        meter_ids = args.meter_ids or []
    else:
        username = args.username or input("HomeSide username (e.g., FC2000233091): ").strip()
        password = args.password or input("HomeSide password: ").strip()
        friendly_name = args.name or input("Friendly name for the house: ").strip()

        # Meter IDs - interactive prompt
        meter_ids = args.meter_ids or []
        if not meter_ids:
            meter_input = input("Energy meter ID (leave empty to skip): ").strip()
            if meter_input:
                meter_ids = [m.strip() for m in meter_input.split(',') if m.strip()]

    # Determine villa number
    villa_number = args.villa_number or get_next_villa_number()
    customer_id = f"HEM_FJV_Villa_{villa_number}"

    # Count steps
    total_steps = 2  # profile + credentials
    if meter_ids:
        total_steps += 1  # dropbox sync
    if args.bootstrap:
        total_steps += 1  # bootstrap

    print()
    print("Configuration:")
    print(f"  Customer ID:  {customer_id}")
    print(f"  Friendly name: {friendly_name}")
    print(f"  Username:     {username}")
    if meter_ids:
        print(f"  Meter IDs:    {', '.join(meter_ids)}")
    if args.enable_energy_sep:
        print(f"  Energy sep:   enabled")
    if args.bootstrap:
        src = args.source_house or 'none (heating only)'
        print(f"  Bootstrap:    yes (source: {src})")
    print()

    if not args.non_interactive:
        confirm = input("Proceed? [Y/n]: ").strip().lower()
        if confirm and confirm != 'y':
            print("Aborted.")
            sys.exit(0)

    step = 0

    # Step 1: Create profile
    step += 1
    print(f"\n[{step}/{total_steps}] Creating customer profile...")
    profile_path = create_profile(customer_id, friendly_name,
                                  meter_ids=meter_ids,
                                  enable_energy_sep=args.enable_energy_sep)
    print(f"  Created: {profile_path}")
    if meter_ids:
        print(f"  Meter IDs: {', '.join(meter_ids)}")
        print(f"  Energy data start: 90 days back")
    print(f"  Energy separation: {'enabled' if args.enable_energy_sep else 'disabled (enable after first import)'}")

    # Step 2: Add credentials to .env
    step += 1
    print(f"\n[{step}/{total_steps}] Adding credentials to .env...")
    if add_credentials_to_env(customer_id, username, password, friendly_name, meter_ids=meter_ids):
        print(f"  Added HOUSE_{customer_id}_USERNAME/PASSWORD to .env")
        if meter_ids:
            print(f"  Added HOUSE_{customer_id}_METER_IDS to .env")
    else:
        print("  Skipped (credentials may already exist)")

    # Step 3: Trigger Dropbox sync (if meter IDs provided)
    if meter_ids:
        step += 1
        print(f"\n[{step}/{total_steps}] Syncing meters to Dropbox...")
        if trigger_dropbox_sync():
            print("  Meter request synced to Dropbox")
        else:
            print("  Skipped (Dropbox not configured or sync failed - do it manually later)")

    # Step 4: Run bootstrap (if requested)
    if args.bootstrap:
        step += 1
        print(f"\n[{step}/{total_steps}] Running Arrigo bootstrap...")
        if not args.lat or not args.lon:
            print("  ERROR: --bootstrap requires --lat and --lon")
            print("  Skipping bootstrap - run manually (see next steps below)")
        else:
            try:
                import subprocess
                cmd = [
                    sys.executable, 'gap_filler.py',
                    '--bootstrap',
                    '--username', username,
                    '--password', password,
                    '--house-id', customer_id,
                    '--lat', str(args.lat),
                    '--lon', str(args.lon),
                    '--days', '90',
                    '--resolution', '5'
                ]
                if args.source_house:
                    cmd += ['--source-house-id', args.source_house]
                print(f"  Running: {' '.join(cmd[:4])} ...")
                result = subprocess.run(cmd, capture_output=False)
                if result.returncode != 0:
                    print(f"  Bootstrap exited with code {result.returncode}")
            except Exception as e:
                print(f"  Bootstrap failed (non-fatal): {e}")
                print("  You can run it manually (see next steps below)")

    # Print next steps
    print()
    print("=" * 60)
    print("Done! The orchestrator will detect the new profile within 60 seconds.")
    print("=" * 60)
    print()
    print("Next steps:")
    print(f"1. Check orchestrator logs: docker logs -f homeside-orchestrator")
    print(f"2. Look for: Spawned house '{friendly_name}'")

    if meter_ids:
        print(f"3. Meter synced to Dropbox - energy data will arrive within 24h")
        if not args.enable_energy_sep:
            print(f"4. After first energy import, enable separation:")
            print(f"   Edit {profile_path} -> \"energy_separation.enabled\": true")
        next_num = 5 if not args.enable_energy_sep else 4
    else:
        print(f"3. Add meter ID to profile when available:")
        print(f"   Edit {profile_path} -> \"meter_ids\": [\"YOUR_METER_ID\"]")
        print(f"   Then run: python3 dropbox_sync.py")
        next_num = 4

    if not args.bootstrap:
        lat_str = str(args.lat) if args.lat else "XX.XX"
        lon_str = str(args.lon) if args.lon else "XX.XX"
        print(f"{next_num}. (Optional) Bootstrap 3 months of 5-min data:")
        print(f"   python3 gap_filler.py --bootstrap --username {username} --password \"...\" \\")
        print(f"     --house-id {customer_id} \\")
        print(f"     --lat {lat_str} --lon {lon_str} --days 90 --resolution 5")
    print()


if __name__ == "__main__":
    main()

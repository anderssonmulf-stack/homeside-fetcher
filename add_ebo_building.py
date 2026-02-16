#!/usr/bin/env python3
"""
Add New EBO Building Script

Automates onboarding a commercial building connected via Schneider Electric EBO:
1. Connects to EBO, browses object tree, discovers signals
2. Creates buildings/<building_id>.json with discovered signals
3. Adds credential reference to .env (if not already present)
4. Optionally runs historical data bootstrap (90 days)
5. Orchestrator auto-detects new config within 60 seconds

Usage:
    python3 add_ebo_building.py

    python3 add_ebo_building.py --non-interactive \
        --base-url https://ebo.halmstad.se \
        --credential-ref EBO_HK_CRED1 \
        --site-path "/Kattegattgymnasiet 20942 AS3" \
        --name "Kattegattgymnasiet" --building-id HK_Kattegatt_20942 \
        --lat 56.67 --lon 12.86 --bootstrap
"""

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timezone

from ebo_api import EboApi
from arrigo_api import save_building_config


def add_credential_ref_to_env(credential_ref: str, username: str, password: str,
                               domain: str = "", friendly_name: str = "",
                               env_file: str = ".env") -> bool:
    """Append EBO credential reference to .env file if not already present."""
    env_key = f"{credential_ref}_USERNAME"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            content = f.read()
        if env_key in content:
            print(f"  Credentials {credential_ref} already exist in {env_file}")
            return False

    lines = [
        f"\n# {friendly_name} — EBO credentials ({credential_ref})\n",
        f"{credential_ref}_USERNAME={username}\n",
        f"{credential_ref}_PASSWORD={password}\n",
        f"{credential_ref}_DOMAIN={domain}\n",
    ]

    with open(env_file, 'a') as f:
        f.writelines(lines)

    return True


def browse_tree(api, path, depth=0, max_depth=2):
    """Recursively browse EBO object tree and print structure."""
    indent = "  " * (depth + 2)
    try:
        result = api.get_objects([path], levels=1, include_hidden=False)
        res = result.get('GetObjectsRes', result)
        objects = res.get('objects', []) if isinstance(res, dict) else []
        for obj in objects:
            name = obj.get('name', '?')
            obj_type = obj.get('type', '')
            obj_path = obj.get('path', '')
            print(f"{indent}{name}  ({obj_type})")
            if depth < max_depth and obj_path:
                browse_tree(api, obj_path, depth + 1, max_depth)
    except Exception as e:
        print(f"{indent}(error: {e})")


def discover_signals(api, site_path, verbose=False):
    """
    Walk the EBO object tree under site_path to find analog signals.

    Looks for objects with a /Value property that have numeric values.
    Returns a dict of signal_name -> {path, value, unit, ...}
    """
    signals = {}

    def walk(path, depth=0):
        if depth > 5:  # safety limit
            return
        try:
            result = api.get_objects([path], levels=1, include_hidden=False)
            res = result.get('GetObjectsRes', result)
            objects = res.get('objects', []) if isinstance(res, dict) else []
        except Exception as e:
            if verbose:
                print(f"    (walk error at {path}: {e})")
            return

        for obj in objects:
            name = obj.get('name', '')
            obj_path = obj.get('path', '')
            obj_type = obj.get('type', '')

            # Check if this object has a Value property (analog signal)
            value_path = f"{obj_path}/Value" if obj_path else None
            if value_path and obj_type in ('', 'AnalogValue', 'AnalogInput',
                                            'AnalogOutput', 'AV', 'AI', 'AO'):
                try:
                    props = api.get_multi_property([value_path])
                    prop_res = props.get('GetMultiPropertyRes', [])
                    if prop_res:
                        prop = prop_res[0] if isinstance(prop_res, list) else prop_res
                        raw_val = prop.get('value') if isinstance(prop, dict) else None
                        if raw_val is not None:
                            value = EboApi.decode_value(raw_val) if isinstance(raw_val, str) and raw_val.startswith('0x') else raw_val
                            unit = prop.get('unitDisplayName', '') if isinstance(prop, dict) else ''
                            signals[name] = {
                                'path': value_path,
                                'value': value,
                                'unit': unit,
                                'object_type': obj_type,
                            }
                            if verbose:
                                print(f"    Found: {name} = {value} {unit}")
                except Exception:
                    pass

            # Recurse into children
            if obj_path:
                walk(obj_path, depth + 1)

    print(f"  Walking object tree from {site_path}...")
    walk(site_path)
    return signals


def build_config(building_id, friendly_name, base_url, credential_ref, domain,
                 latitude, longitude, signals, meter_ids=None):
    """Build a building config dict for an EBO building."""
    analog_signals = {}
    for name, info in sorted(signals.items()):
        analog_signals[name] = {
            "signal_id": info['path'],
            "unit": info.get('unit', ''),
            "category": categorize_signal(name, info.get('unit', '')),
            "field_name": None,  # User needs to set this
            "fetch": False,      # User needs to enable
            "discovered_value": info.get('value'),
        }

    config = {
        "schema_version": 1,
        "building_id": building_id,
        "friendly_name": friendly_name,
        "building_type": "commercial",
        "meter_ids": meter_ids or [],
        "energy_separation": {
            "enabled": False,
            "method": "k_calibration",
            "heat_loss_k": None,
            "k_percentile": 15,
            "assumed_indoor_temp": 21.0,
            "field_mapping": {
                "outdoor_temperature": "outdoor_temp",
                "hot_water_temp": "hot_water_temp"
            }
        },
        "location": {
            "latitude": latitude,
            "longitude": longitude,
        },
        "connection": {
            "system": "ebo",
            "base_url": base_url,
            "credential_ref": credential_ref,
            "domain": domain or "",
        },
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "analog_signals": analog_signals,
        "digital_signals": {},
        "alarm_monitoring": {"enabled": False},
        "poll_interval_minutes": 5,
    }

    return config


def categorize_signal(name, unit):
    """Auto-categorize a signal based on its name and unit."""
    name_lower = name.lower()
    unit_lower = unit.lower() if unit else ''

    if any(k in name_lower for k in ('ute', 'outdoor', 'utetemp')):
        return 'climate'
    if any(k in name_lower for k in ('gt1', 'tillopp', 'supply', 'framled')):
        return 'heating'
    if any(k in name_lower for k in ('gt2', 'retur', 'return')):
        return 'heating'
    if any(k in name_lower for k in ('vv', 'tapv', 'hot_water', 'dhw')):
        return 'hot_water'
    if any(k in name_lower for k in ('effekt', 'power', 'energi', 'energy')):
        return 'district_heating'
    if any(k in name_lower for k in ('flow', 'flöde')):
        return 'district_heating'
    if any(k in name_lower for k in ('ta', 'fa', 'lb', 'fläkt', 'fan', 'luft', 'air')):
        return 'ventilation'
    if any(k in name_lower for k in ('kyl', 'cool')):
        return 'cooling'
    if any(k in name_lower for k in ('tryck', 'press')):
        return 'pressure'
    if 'kwh' in unit_lower:
        return 'energy_metering'
    if '°c' in unit_lower or '°f' in unit_lower:
        return 'temperature'
    return 'other'


def main():
    parser = argparse.ArgumentParser(description='Add a new EBO building')
    parser.add_argument('--non-interactive', action='store_true',
                        help='Run without prompts')
    parser.add_argument('--base-url', type=str,
                        help='EBO server URL (e.g., https://ebo.halmstad.se)')
    parser.add_argument('--username', type=str, help='EBO username')
    parser.add_argument('--password', type=str, help='EBO password')
    parser.add_argument('--domain', type=str, default='', help='EBO domain')
    parser.add_argument('--credential-ref', type=str,
                        help='Credential reference name (e.g., EBO_HK_CRED1)')
    parser.add_argument('--site-path', type=str,
                        help='EBO site path to discover (e.g., "/Kattegattgymnasiet 20942 AS3")')
    parser.add_argument('--name', type=str, help='Friendly name')
    parser.add_argument('--building-id', type=str,
                        help='Building ID (e.g., HK_Kattegatt_20942)')
    parser.add_argument('--lat', type=float,
                        help='Latitude for weather data')
    parser.add_argument('--lon', type=float,
                        help='Longitude for weather data')
    parser.add_argument('--meter-id', type=str, action='append', dest='meter_ids',
                        help='Energy meter ID (can be repeated)')
    parser.add_argument('--source-building', type=str,
                        help='Source entity ID to copy energy_meter data from')
    parser.add_argument('--bootstrap', action='store_true',
                        help='Run full bootstrap after onboarding')
    parser.add_argument('--bootstrap-days', type=int, default=90,
                        help='Days of historical data to bootstrap')
    parser.add_argument('--no-verify-ssl', action='store_true',
                        help='Skip SSL verification')
    parser.add_argument('--browse', action='store_true',
                        help='Browse object tree and exit (no config creation)')
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    print("=" * 60)
    print("  ADD NEW EBO BUILDING")
    print("=" * 60)

    # Gather info
    if args.non_interactive:
        base_url = args.base_url
        credential_ref = args.credential_ref
        domain = args.domain
        site_path = args.site_path
        friendly_name = args.name
        building_id = args.building_id
        latitude = args.lat
        longitude = args.lon
        meter_ids = args.meter_ids or []
        source_building = args.source_building
        do_bootstrap = args.bootstrap

        # Resolve credentials from credential_ref or CLI args
        if credential_ref:
            from dotenv import load_dotenv
            load_dotenv()
            username = args.username or os.getenv(f"{credential_ref}_USERNAME")
            password = args.password or os.getenv(f"{credential_ref}_PASSWORD")
            if not domain:
                domain = os.getenv(f"{credential_ref}_DOMAIN", "")
        else:
            username = args.username
            password = args.password
    else:
        base_url = input("EBO server URL (e.g., https://ebo.halmstad.se): ").strip()
        username = input("EBO username: ").strip()
        password = input("EBO password: ").strip()
        domain = input("EBO domain (press Enter for none): ").strip()
        credential_ref = input("Credential reference name (e.g., EBO_HK_CRED1): ").strip()
        site_path = input("Site path to discover (e.g., /Kattegattgymnasiet 20942 AS3): ").strip()
        friendly_name = input("Friendly name: ").strip()
        building_id = input("Building ID (e.g., HK_Kattegatt_20942): ").strip()
        lat_input = input("Latitude (e.g., 56.67): ").strip()
        lon_input = input("Longitude (e.g., 12.86): ").strip()
        latitude = float(lat_input) if lat_input else None
        longitude = float(lon_input) if lon_input else None
        meter_input = input("Energy meter ID (press Enter to skip): ").strip()
        meter_ids = [meter_input] if meter_input else []
        source_input = input("Source entity to copy energy data from (press Enter to skip): ").strip()
        source_building = source_input if source_input else None
        do_bootstrap = input("Bootstrap historical data? [y/N]: ").strip().lower() == 'y'

    if not all([base_url, username, password]):
        print("ERROR: base_url, username, and password are required")
        sys.exit(1)

    # Step 1: Connect to EBO
    print(f"\n1. Connecting to {base_url}...")
    api = EboApi(
        base_url=base_url,
        username=username,
        password=password,
        domain=domain,
        verify_ssl=not args.no_verify_ssl,
    )

    try:
        api.login()
        api.web_entry()
        print("  Authenticated successfully")
    except Exception as e:
        print(f"ERROR: EBO login failed: {e}")
        sys.exit(1)

    # Browse mode: just show the tree and exit
    if args.browse:
        print(f"\n  Object tree from '{site_path or '/'}':")
        browse_tree(api, site_path or "/", max_depth=3)
        sys.exit(0)

    if not all([site_path, friendly_name, building_id]):
        print("ERROR: site-path, name, and building-id are required")
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

    # Step 2: Discover signals
    print(f"\n2. Discovering signals at {site_path}...")
    signals = discover_signals(api, site_path, verbose=args.verbose)

    if not signals:
        print("  WARNING: No signals discovered. Try --browse to explore the tree first.")
        print("  You can still create a config and add signals manually.")
    else:
        print(f"  Discovered {len(signals)} signals")

    # Step 3: Create building config
    print(f"\n3. Creating building config...")
    config = build_config(
        building_id=building_id,
        friendly_name=friendly_name,
        base_url=base_url,
        credential_ref=credential_ref or "",
        domain=domain,
        latitude=latitude,
        longitude=longitude,
        signals=signals,
        meter_ids=meter_ids,
    )

    saved_path = save_building_config(config)
    analog_count = len(config.get("analog_signals", {}))
    print(f"  Config saved: {saved_path}")
    print(f"  Signals: {analog_count} analog")
    print(f"  NOTE: Edit the config to set field_name and fetch=true for signals you want")

    # Step 4: Add credentials to .env
    if credential_ref:
        print(f"\n4. Adding credential reference to .env...")
        if add_credential_ref_to_env(credential_ref, username, password, domain, friendly_name):
            print(f"  Added {credential_ref}_USERNAME/PASSWORD/DOMAIN to .env")
        else:
            print(f"  Credentials already exist in .env")
    else:
        print(f"\n4. No credential_ref specified — skipping .env update")
        print(f"  Tip: Add credentials manually or use --credential-ref")

    # Step 5: Bootstrap historical data
    if do_bootstrap:
        print(f"\n5. Bootstrapping {args.bootstrap_days} days of historical data...")
        cmd = [
            sys.executable, 'gap_filler.py',
            '--bootstrap',
            '--house-id', building_id,
            '--username', username,
            '--password', password,
            '--days', str(args.bootstrap_days),
            '--resolution', '5',
        ]
        if latitude and longitude:
            cmd += ['--lat', str(latitude), '--lon', str(longitude)]
        if source_building:
            cmd += ['--source-house-id', source_building]
        if args.verbose:
            cmd.append('--verbose')
        result = subprocess.run(cmd)
        if result.returncode == 0:
            print("  Bootstrap complete")
        else:
            print("  Bootstrap failed (non-fatal)")
    else:
        print(f"\n5. Skipping historical data bootstrap")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  EBO BUILDING ADDED SUCCESSFULLY")
    print(f"{'=' * 60}")
    print(f"  Building ID: {building_id}")
    print(f"  Config file: {saved_path}")
    print(f"  BMS system: ebo")
    print(f"  Server: {base_url}")
    print(f"  Signals discovered: {analog_count}")
    if credential_ref:
        print(f"  Credential ref: {credential_ref}")
    if meter_ids:
        print(f"  Meter IDs: {', '.join(meter_ids)}")
    if latitude and longitude:
        print(f"  Location: {latitude}, {longitude}")
    print(f"\n  NEXT STEPS:")
    print(f"  1. Edit {saved_path} to set field_name, fetch=true, and trend_log paths")
    print(f"  2. The orchestrator will auto-detect within 60 seconds")
    if not meter_ids:
        print(f"  3. Add meter_ids when available for energy separation")


if __name__ == '__main__':
    main()

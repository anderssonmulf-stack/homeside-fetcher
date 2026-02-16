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
import re
import argparse
import subprocess
from datetime import datetime, timezone

from ebo_api import EboApi
from arrigo_api import save_building_config


# ─── Signal auto-mapping table ──────────────────────────────────────────────
# Each rule: (base_name_regex, field_template, category, extra_flags)
# {subsys} replaced by detected subsystem (vs1, vv1, etc.)
# {n} replaced by regex capture group (heat curve breakpoint index)
SIGNAL_FIELD_MAP = [
    # Climate — fixed names
    (r'^GTU$',                   'outdoor_temp_fvc',              'climate',          {}),
    (r'^GTU_Däm$',              'outdoor_temp_damped',           'climate',          {}),
    (r'^GT5_Medel$',             'room_temperature',              'climate',          {}),
    (r'^MedelvärdeFastighet$',   'room_temperature',              'climate',          {}),
    # Heating — subsystem-templated
    (r'^GT1$',                   '{subsys}_supply_temp',          'heating',          {}),
    (r'^GT2$',                   '{subsys}_return_temp',          'heating',          {}),
    (r'^SV1$',                   '{subsys}_valve_output',         'heating',          {}),
    (r'^GT1_BB$',                '{subsys}_supply_setpoint',      'heating',          {}),
    (r'^GT1_FS$',                '{subsys}_curve_parallel_shift', 'heating',          {}),
    (r'^GT5_FS$',                '{subsys}_curve_parallel_shift', 'heating',          {}),
    (r'^GT5_B$',                 '{subsys}_room_setpoint',        'heating',          {}),
    (r'^GTU_UK$',                '{subsys}_outdoor_comp_supply',  'heating',          {}),
    (r'^EXP[-_]GP1$',           '{subsys}_system_pressure',      'pressure',         {}),
    # VP naming (GT1T/GT1R instead of GT1/GT2)
    (r'^GT1T$',                  '{subsys}_supply_temp',          'heating',          {}),
    (r'^GT1R$',                  '{subsys}_return_temp',          'heating',          {}),
    # Heat curve breakpoints
    (r'^GT1_X(\d+)$',           '{subsys}_hc_x{n}',             'heat_curve',       {'write_on_change': True}),
    (r'^GT1_Y(\d+)$',           '{subsys}_hc_y{n}',             'heat_curve',       {'write_on_change': True}),
    # District heating — fixed names
    (r'^VMM1_EF$',               'dh_power_total',               'district_heating', {}),
    (r'^MQ00_ETI$',              'dh_primary_supply',            'district_heating', {}),
    (r'^MQ00_EF$',               'dh_power_meter',               'district_heating', {}),
    (r'^MQ00_FFT$',              'dh_flow_total',                'district_heating', {}),
    (r'^Effektmedelvärde.1h$',  'dh_power_1h_avg',              'district_heating', {}),
    (r'^Effektstyrning_SS',      'power_control_pct',            'district_heating', {}),
]

# VV (domestic hot water) subsystem: GT1 = hot water temp, not supply temp
VV_FIELD_OVERRIDES = {
    'supply_temp': 'hot_water_temp',
}

_SUBSYS_PREFIX_RE = re.compile(r'^((?:VS|VV|VP|KS)\d+)[-_]', re.IGNORECASE)
_SUBSYS_FOLDER_RE = re.compile(r'^(?:VS|VV|VP|KS)\d+$', re.IGNORECASE)


def extract_base_name(signal_name):
    """Strip subsystem prefix from signal name to get the base signal identifier."""
    m = _SUBSYS_PREFIX_RE.match(signal_name)
    return signal_name[m.end():] if m else signal_name


def detect_subsystem(signal_name, signal_path):
    """Extract circuit prefix (e.g., vs1, vv1) from signal name or path."""
    # 1. From signal name prefix: VS1-GT1 → vs1, VS1_GT1 → vs1
    m = _SUBSYS_PREFIX_RE.match(signal_name)
    if m:
        return m.group(1).lower()
    # 2. From path segments: /VS1/Variabler/GT1_BB/Value → vs1
    for part in signal_path.split('/'):
        if _SUBSYS_FOLDER_RE.match(part):
            return part.lower()
    return None


def auto_map_signal(signal_name, signal_info, used_fields):
    """Match signal against SIGNAL_FIELD_MAP rules.

    Returns (field_name, fetch, category, extra, display_name) or None.
    used_fields: dict of field_name -> signal key, mutated to track collisions.
    """
    base_name = extract_base_name(signal_name)
    subsystem = detect_subsystem(signal_name, signal_info.get('path', ''))

    for pattern, field_template, category, extra in SIGNAL_FIELD_MAP:
        m = re.match(pattern, base_name)
        if not m:
            continue

        # Build field name from template
        field_name = field_template
        if '{subsys}' in field_name:
            if not subsystem:
                continue  # Need a subsystem for this template
            field_name = field_name.replace('{subsys}', subsystem)
            # VV overrides: supply_temp → hot_water_temp
            if subsystem.startswith('vv'):
                for old, new in VV_FIELD_OVERRIDES.items():
                    if field_name.endswith(f'_{old}'):
                        field_name = field_name[:-(len(old))] + new
                        category = 'hot_water'
                        break

        # Replace {n} with captured group (heat curve index)
        if '{n}' in field_name and m.groups():
            field_name = field_name.replace('{n}', m.group(1))

        # Check for collision
        if field_name in used_fields:
            print(f"    WARNING: '{signal_name}' → '{field_name}' "
                  f"already used by '{used_fields[field_name]}', skipping")
            return None

        # Build display/key name with subsystem prefix for clarity
        # Only prefix for subsystem-templated signals, not fixed-name ones
        display_name = signal_name
        uses_subsys = '{subsys}' in field_template
        if uses_subsys and subsystem and not _SUBSYS_PREFIX_RE.match(signal_name):
            # Signal from Variabler path without subsystem prefix in name
            if category == 'heat_curve' and base_name.startswith('GT1_'):
                # GT1_X1 in VS1 → VS1-HC-X1
                suffix = base_name[4:]  # Strip "GT1_"
                display_name = f"{subsystem.upper()}-HC-{suffix}"
            else:
                display_name = f"{subsystem.upper()}-{base_name}"

        used_fields[field_name] = display_name
        return field_name, True, category, extra, display_name

    return None


def discover_trend_logs(api, site_path, verbose=False):
    """Walk the EBO tree to discover trend log paths under Trendloggar folders."""
    trend_logs = {}
    checked = set()

    def scan_trendloggar(tl_path):
        """Read children of a Trendloggar folder."""
        if tl_path in checked:
            return
        checked.add(tl_path)
        try:
            result = api.get_objects([tl_path], levels=1, include_hidden=False)
            res = result.get('GetObjectsRes', result)
            items = res.get('results', []) if isinstance(res, dict) else []
        except Exception:
            return
        for obj in items:
            name = obj.get('name', '')
            obj_path = obj.get('path', '')
            child_path = f"{obj_path}/{name}" if obj_path else name
            if child_path != tl_path:
                trend_logs[name] = child_path
                if verbose:
                    print(f"    {name}")

    def browse_for_trendloggar(path, depth):
        """Recurse into folders looking for children named 'Trendloggar'."""
        if depth < 0 or path in checked:
            return
        checked.add(path)
        try:
            result = api.get_objects([path], levels=1, include_hidden=False)
            res = result.get('GetObjectsRes', result)
            items = res.get('results', []) if isinstance(res, dict) else []
        except Exception:
            return
        for obj in items:
            name = obj.get('name', '')
            obj_path = obj.get('path', '')
            obj_type = obj.get('objectType', '')
            child_path = f"{obj_path}/{name}" if obj_path else name
            if child_path == path:
                continue
            if name == 'Trendloggar':
                scan_trendloggar(child_path)
            elif depth > 0 and ('Folder' in obj_type or name.startswith('!')
                                or 'IOBus' in obj_type
                                or _SUBSYS_FOLDER_RE.match(name)
                                or name == 'Effektstyrning'):
                browse_for_trendloggar(child_path, depth - 1)

    print(f"  Scanning for trend logs...")
    browse_for_trendloggar(site_path, depth=3)
    return trend_logs


def match_trend_log(signal_name, display_name, subsystem, base_name, trend_logs):
    """Match a signal to its trend log by naming conventions."""
    if not trend_logs:
        return None
    subsys_upper = subsystem.upper() if subsystem else ''
    prefixed = f"{subsys_upper}-{base_name}" if subsystem else base_name

    # 1. Exact _Logg suffix: VS1-GT1 → VS1-GT1_Logg, GT5_Medel → GT5_Medel_Logg
    for candidate in [f"{display_name}_Logg", f"{prefixed}_Logg", f"{signal_name}_Logg"]:
        if candidate in trend_logs:
            return trend_logs[candidate]

    # 2. Starts-with: VV1-GT1 → VV1-GT1_1min.intervall_Logg
    for log_name, log_path in trend_logs.items():
        for prefix in (display_name, prefixed, signal_name):
            if log_name.startswith(prefix) and '_Logg' in log_name:
                return log_path

    # 3. Contains: VMM1_EF → any log containing VMM1_EF
    for log_name, log_path in trend_logs.items():
        if signal_name in log_name or base_name in log_name:
            return log_path

    return None


def validate_config(config):
    """Post-generation validation. Returns (errors, warnings, info) lists."""
    signals = config.get('analog_signals', {})
    errors, warnings, info = [], [], []

    field_names = {s.get('field_name') for s in signals.values() if s.get('field_name')}
    fetched = {k: v for k, v in signals.items() if v.get('fetch')}

    if 'outdoor_temp_fvc' not in field_names:
        errors.append("No 'outdoor_temp_fvc' signal mapped (required for weather correlation)")
    if not fetched:
        errors.append("No signals with fetch=true")
    if 'room_temperature' not in field_names:
        warnings.append("No 'room_temperature' signal mapped")
    if 'dh_power_total' not in field_names:
        warnings.append("No 'dh_power_total' signal mapped")

    no_trend = [k for k, v in fetched.items() if not v.get('trend_log')]
    if no_trend:
        names = ', '.join(no_trend[:5])
        extra = f" (+{len(no_trend) - 5} more)" if len(no_trend) > 5 else ""
        warnings.append(f"{len(no_trend)} fetch=true signals without trend_log "
                        f"(can't bootstrap): {names}{extra}")

    mapped = sum(1 for v in signals.values() if v.get('field_name'))
    with_trend = sum(1 for v in signals.values() if v.get('trend_log'))
    info.append(f"Mapped: {mapped}/{len(signals)} signals")
    info.append(f"Fetch enabled: {len(fetched)}/{len(signals)} signals")
    info.append(f"Trend logs matched: {with_trend}/{len(signals)} signals")

    return errors, warnings, info


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
        items = res.get('results', []) if isinstance(res, dict) else []
        for obj in items:
            name = obj.get('name', '?')
            obj_type = obj.get('objectType', obj.get('type', ''))
            obj_path = obj.get('path', '')
            # The child's full path is parent_path/name
            child_path = f"{obj_path}/{name}" if obj_path else name
            # Skip the parent itself (returned as first result)
            if child_path == path or (depth == 0 and name == path.split('/')[-1]):
                continue
            print(f"{indent}{name}  ({obj_type})")
            if depth < max_depth:
                browse_tree(api, child_path, depth + 1, max_depth)
    except Exception as e:
        print(f"{indent}(error: {e})")


def discover_signals(api, site_path, verbose=False):
    """
    Walk the EBO object tree under site_path to find analog signals.

    Looks for objects with Value properties that have numeric values.
    Returns a dict of signal_name -> {path, value, unit, ...}
    """
    import struct
    signals = {}

    POINT_TYPES = {
        'server.point.AV', 'server.point.AI', 'server.point.AO',
        'server.point.BV', 'server.point.BI', 'server.point.BO',
        'io.point.TemperatureInput', 'io.point.VoltageInput',
        'io.point.VoltageOutput', 'io.point.DigitalOutput',
        'io.point.DigitalInputNoLED',
    }

    def hex_to_double(hexval):
        if isinstance(hexval, str) and hexval.startswith('0x'):
            try:
                return struct.unpack('d', struct.pack('Q', int(hexval, 16)))[0]
            except (ValueError, struct.error):
                return None
        if isinstance(hexval, (int, float)):
            return float(hexval)
        return None

    def walk(path, depth=0):
        if depth > 5:  # safety limit
            return
        try:
            result = api.get_objects([path], levels=1, include_hidden=False)
            res = result.get('GetObjectsRes', result)
            items = res.get('results', []) if isinstance(res, dict) else []
        except Exception as e:
            if verbose:
                print(f"    (walk error at {path}: {e})")
            return

        for obj in items:
            name = obj.get('name', '')
            obj_path = obj.get('path', '')
            obj_type = obj.get('objectType', '')
            child_path = f"{obj_path}/{name}" if obj_path else name

            # Skip parent entry
            if child_path == path:
                continue

            # Check if this is a point object with a Value property
            if obj_type in POINT_TYPES:
                props = obj.get('properties', {})
                value_prop = props.get('Value', {})
                raw_val = value_prop.get('value')
                unit = value_prop.get('unitDisplayName', '')
                decoded = hex_to_double(raw_val) if raw_val is not None else raw_val
                if decoded is not None:
                    key = name
                    if key in signals:
                        # Collision: rename existing entry with its subsystem
                        existing = signals[key]
                        for part in existing['path'].split('/'):
                            if _SUBSYS_FOLDER_RE.match(part):
                                new_key = f"{part}-{name}"
                                if new_key not in signals:
                                    signals[new_key] = signals.pop(key)
                                break
                        # Prefix new entry with its subsystem
                        for part in child_path.split('/'):
                            if _SUBSYS_FOLDER_RE.match(part):
                                key = f"{part}-{name}"
                                break
                        # Fallback if still colliding
                        if key in signals:
                            n = 2
                            while f"{name}_{n}" in signals:
                                n += 1
                            key = f"{name}_{n}"
                    signals[key] = {
                        'path': f"{child_path}/Value",
                        'value': decoded,
                        'unit': unit,
                        'object_type': obj_type,
                    }
                    if verbose:
                        print(f"    Found: {key} = {decoded} {unit}")

            # Recurse into children (folders and containers)
            if obj_type.endswith('Folder') or 'base.Folder' in obj_type or obj_type.endswith('IOBus') or 'IO' in obj_type:
                walk(child_path, depth + 1)

    print(f"  Walking object tree from {site_path}...")
    walk(site_path)
    return signals


def build_config(building_id, friendly_name, base_url, credential_ref, domain,
                 latitude, longitude, signals, meter_ids=None,
                 trend_logs=None, auto_map=True):
    """Build a building config dict for an EBO building."""
    analog_signals = {}
    used_fields = {}

    for name, info in sorted(signals.items()):
        mapped = None
        if auto_map:
            mapped = auto_map_signal(name, info, used_fields)

        if mapped:
            field_name, fetch, category, extra, display_name = mapped
            entry = {
                "signal_id": info['path'],
                "unit": info.get('unit', ''),
                "category": category,
                "field_name": field_name,
                "fetch": fetch,
                "discovered_value": info.get('value'),
            }
            entry.update(extra)
            # Match trend log
            if trend_logs:
                subsystem = detect_subsystem(name, info.get('path', ''))
                base_name = extract_base_name(name)
                tl = match_trend_log(name, display_name, subsystem, base_name,
                                     trend_logs)
                if tl:
                    entry['trend_log'] = tl
            analog_signals[display_name] = entry
        else:
            analog_signals[name] = {
                "signal_id": info['path'],
                "unit": info.get('unit', ''),
                "category": categorize_signal(name, info.get('unit', '')),
                "field_name": None,
                "fetch": False,
                "discovered_value": info.get('value'),
            }

    # Auto-fix energy_separation field_mapping based on mapped signals
    outdoor_field = 'outdoor_temp'
    hw_field = 'hot_water_temp'
    for entry in analog_signals.values():
        fn = entry.get('field_name')
        if fn == 'outdoor_temp_fvc':
            outdoor_field = 'outdoor_temp_fvc'
        if fn and fn.endswith('_hot_water_temp'):
            hw_field = fn

    config = {
        "schema_version": 1,
        "building_id": building_id,
        "friendly_name": friendly_name,
        "building_type": "commercial",
        "meter_ids": meter_ids or [],
        "energy_separation": {
            "enabled": bool(meter_ids),
            "method": "k_calibration",
            "heat_loss_k": None,
            "k_percentile": 15,
            "assumed_indoor_temp": 21.0,
            "field_mapping": {
                "outdoor_temperature": outdoor_field,
                "hot_water_temp": hw_field,
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
    parser.add_argument('--skip-auto-map', action='store_true',
                        help='Skip auto-mapping of signals (manual config editing)')
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    print("=" * 60)
    print("  ADD NEW EBO BUILDING")
    print("=" * 60)

    # Gather info — --browse implies non-interactive if base_url is given
    if args.non_interactive or (args.browse and args.base_url):
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

    # Step 2b: Discover trend logs (for auto-mapping)
    auto_map = not args.skip_auto_map
    trend_logs = None
    if auto_map and signals:
        print(f"\n2b. Discovering trend logs at {site_path}...")
        trend_logs = discover_trend_logs(api, site_path, verbose=args.verbose)
        print(f"  Found {len(trend_logs)} trend logs")

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
        trend_logs=trend_logs,
        auto_map=auto_map,
    )

    # Step 3b: Validate and report
    if auto_map:
        errors, warnings, info_msgs = validate_config(config)
        print(f"\n  Mapping report:")
        for msg in info_msgs:
            print(f"    {msg}")
        for msg in warnings:
            print(f"    WARNING: {msg}")
        for msg in errors:
            print(f"    ERROR: {msg}")

    saved_path = save_building_config(config)
    analog_count = len(config.get("analog_signals", {}))
    fetched_count = sum(1 for v in config.get("analog_signals", {}).values()
                        if v.get('fetch'))
    print(f"\n  Config saved: {saved_path}")
    print(f"  Signals: {analog_count} total, {fetched_count} fetch-enabled")
    if not auto_map:
        print(f"  NOTE: Edit the config to set field_name and fetch=true for signals you want")
    else:
        print(f"  Review the config and adjust any incorrect mappings")

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

    # Step 5: Sync Dropbox meters (if meter IDs provided)
    if meter_ids:
        print(f"\n5. Syncing Dropbox meters...")
        if trigger_dropbox_sync():
            print("  Dropbox meter sync complete")
        else:
            print("  Dropbox sync skipped or failed (non-fatal)")
    else:
        print(f"\n5. No meter IDs - skipping Dropbox sync")

    # Step 6: Bootstrap historical data
    if do_bootstrap:
        print(f"\n6. Bootstrapping {args.bootstrap_days} days of historical data...")
        cmd = [
            sys.executable, 'gap_filler.py',
            '--bootstrap',
            '--house-id', building_id,
            '--username', username,
            '--password', password,
            '--days', str(args.bootstrap_days),
            '--resolution', '5',
            '--yes',
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
        print(f"\n6. Skipping historical data bootstrap")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  EBO BUILDING ADDED SUCCESSFULLY")
    print(f"{'=' * 60}")
    print(f"  Building ID: {building_id}")
    print(f"  Config file: {saved_path}")
    print(f"  BMS system: ebo")
    print(f"  Server: {base_url}")
    print(f"  Signals: {analog_count} total, {fetched_count} fetch-enabled")
    if credential_ref:
        print(f"  Credential ref: {credential_ref}")
    if meter_ids:
        print(f"  Meter IDs: {', '.join(meter_ids)}")
        print(f"  Energy separation: enabled")
    if latitude and longitude:
        print(f"  Location: {latitude}, {longitude}")
    print(f"\n  NEXT STEPS:")
    if auto_map:
        print(f"  1. Review {saved_path} — adjust any incorrect mappings")
    else:
        print(f"  1. Edit {saved_path} to set field_name, fetch=true, and trend_log paths")
    print(f"  2. The orchestrator will auto-detect within 60 seconds")
    if not meter_ids:
        print(f"  3. Add meter_ids to the config when available")
        print(f"  4. Run: python3 dropbox_sync.py  (to sync meters)")


if __name__ == '__main__':
    main()

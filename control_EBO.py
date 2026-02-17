#!/usr/bin/env python3
"""
EBO Write Controller — dedicated script for all EBO write operations.

Usage examples:
    # Read a signal
    python control_EBO.py --building-id HK_Kattegatt_20942 --read vs1_gt1_fs

    # Read full heat curve
    python control_EBO.py --building-id HK_Kattegatt_20942 --read-curve VS1

    # Dry-run write (default — logs but doesn't write)
    python control_EBO.py --building-id HK_Kattegatt_20942 --signal vs1_curve_parallel_shift --value 1.0

    # Live write with verification
    python control_EBO.py --building-id HK_Kattegatt_20942 --signal vs1_curve_parallel_shift --value 1.0 --live

    # Experimental: try all command variants against a signal
    python control_EBO.py --building-id HK_Kattegatt_20942 --experiment vs1_curve_parallel_shift --value 1.0 --live
"""

import argparse
import json
import os
import struct
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from ebo_api import EboApi


def _hex_to_double(hexval):
    """Convert EBO hex-encoded IEEE 754 double to float."""
    if isinstance(hexval, str) and hexval.startswith('0x'):
        try:
            return struct.unpack('d', struct.pack('Q', int(hexval, 16)))[0]
        except (ValueError, struct.error):
            return None
    if isinstance(hexval, (int, float)):
        return float(hexval)
    return None


# Default value limits for safety
DEFAULT_LIMITS = {
    'heat_curve': {'min': 10.0, 'max': 80.0},   # heat curve Y points (supply temp °C)
    'parallel_shift': {'min': -15.0, 'max': 15.0},  # GT1_FS parallel shift (°C)
    'supply_setpoint': {'min': 15.0, 'max': 80.0},  # GT1_BB supply setpoint (°C)
    'outdoor_x': {'min': -30.0, 'max': 25.0},       # heat curve X points (outdoor temp °C)
}

# Map category to limit key
CATEGORY_LIMITS = {
    'heat_curve': 'heat_curve',
    'heating': 'parallel_shift',  # default for heating signals
}

# Field-specific overrides
FIELD_LIMITS = {
    'curve_parallel_shift': 'parallel_shift',
    'supply_setpoint': 'supply_setpoint',
    'hc_y': 'heat_curve',
    'hc_x': 'outdoor_x',
}


class EBOController:
    """Controller for EBO write operations with safety features."""

    def __init__(self, building_id, dry_run=True, verify_delay=5, verbose=False):
        self.building_id = building_id
        self.dry_run = dry_run
        self.verify_delay = verify_delay
        self.verbose = verbose
        self.api = None
        self.config = None
        self.signals = {}      # field_name -> signal config dict
        self.writable = set()  # field_names that have write_on_change=true
        self.audit_log = []

        self._load_config()

    def _load_config(self):
        """Load building config and index signals by field_name."""
        config_path = Path(__file__).parent / 'buildings' / f'{self.building_id}.json'
        if not config_path.exists():
            raise FileNotFoundError(f"Building config not found: {config_path}")

        with open(config_path) as f:
            self.config = json.load(f)

        # Index signals by field_name for easy lookup
        for sig_key, sig in self.config.get('analog_signals', {}).items():
            field = sig.get('field_name')
            if field:
                sig['_key'] = sig_key
                self.signals[field] = sig
                if sig.get('write_on_change'):
                    self.writable.add(field)

        if self.verbose:
            print(f"Loaded {len(self.signals)} signals, {len(self.writable)} writable")

    def connect(self):
        """Login to EBO and initialize session."""
        conn = self.config.get('connection', {})
        base_url = conn.get('base_url')
        credential_ref = conn.get('credential_ref')

        # Resolve credentials from .env
        load_dotenv()
        username = os.getenv(f'{credential_ref}_USERNAME') if credential_ref else None
        password = os.getenv(f'{credential_ref}_PASSWORD') if credential_ref else None
        domain = os.getenv(f'{credential_ref}_DOMAIN', '') if credential_ref else ''

        if not username or not password:
            raise ValueError(f"Missing credentials for {credential_ref} in .env")

        self.api = EboApi(base_url, username, password, domain)
        print(f"Connecting to {base_url}...")
        self.api.login()
        self.api.web_entry()
        print("Connected.")

    def _resolve_signal(self, name):
        """Resolve a signal by field_name or signal key.

        Args:
            name: field_name (e.g. 'vs1_curve_parallel_shift') or
                  signal key (e.g. 'VS1-GT1_FS')

        Returns:
            (field_name, signal_config) tuple
        """
        # Try field_name first
        if name in self.signals:
            return name, self.signals[name]

        # Try signal key
        for field, sig in self.signals.items():
            if sig.get('_key') == name:
                return field, sig

        # Try partial match on field_name
        matches = [(f, s) for f, s in self.signals.items() if name.lower() in f.lower()]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            names = [m[0] for m in matches]
            raise ValueError(f"Ambiguous signal '{name}', matches: {names}")

        raise ValueError(f"Unknown signal '{name}'. Available: {sorted(self.signals.keys())}")

    def read_value(self, signal_name):
        """Read current value of a signal via EBO API.

        Returns:
            dict with 'field_name', 'signal_id', 'value', 'raw' keys
        """
        field, sig = self._resolve_signal(signal_name)
        signal_path = sig['signal_id']
        obj_path = signal_path.rsplit('/Value', 1)[0] if signal_path.endswith('/Value') else signal_path

        # Determine read strategy based on path
        if '/IO Bus/' in obj_path:
            # IO Bus: browse parent folder
            io_folder = obj_path.rsplit('/', 1)[0]
            point_name = obj_path.split('/')[-1]
            result = self.api.get_objects([io_folder], levels=2, include_hidden=True)
            res = result.get('GetObjectsRes', result)
            for r in res.get('results', []):
                if r.get('name') == point_name:
                    props = r.get('properties', {})
                    raw = props.get('Value', {}).get('value')
                    decoded = _hex_to_double(raw)
                    return {
                        'field_name': field,
                        'signal_id': signal_path,
                        'value': decoded,
                        'raw': raw,
                        'unit': sig.get('unit', ''),
                    }
            return {'field_name': field, 'signal_id': signal_path, 'value': None, 'raw': None}
        else:
            # Server variable: get_objects on the object path directly
            result = self.api.get_objects([obj_path], levels=0, include_hidden=True)
            res = result.get('GetObjectsRes', result)
            for r in res.get('results', []):
                props = r.get('properties', {})
                raw = props.get('Value', {}).get('value')
                decoded = _hex_to_double(raw)
                return {
                    'field_name': field,
                    'signal_id': signal_path,
                    'value': decoded,
                    'raw': raw,
                    'unit': sig.get('unit', ''),
                }
            return {'field_name': field, 'signal_id': signal_path, 'value': None, 'raw': None}

    def _get_value_limits(self, field_name, sig):
        """Get min/max limits for a signal based on its field and category."""
        # Check field-specific overrides
        for pattern, limit_key in FIELD_LIMITS.items():
            if pattern in field_name:
                return DEFAULT_LIMITS.get(limit_key)

        # Check category
        category = sig.get('category', '')
        limit_key = CATEGORY_LIMITS.get(category)
        if limit_key:
            return DEFAULT_LIMITS.get(limit_key)

        return None

    def write_value(self, signal_name, value):
        """Write a value to an EBO signal.

        Args:
            signal_name: field_name or signal key
            value: numeric value to write

        Returns:
            dict with result info
        """
        field, sig = self._resolve_signal(signal_name)
        signal_path = sig['signal_id']
        value = float(value)

        # Safety: check whitelist
        if field not in self.writable:
            return {
                'success': False,
                'error': f"Signal '{field}' is not writable (no write_on_change in config). "
                         f"Writable signals: {sorted(self.writable)}",
            }

        # Safety: check value limits
        limits = self._get_value_limits(field, sig)
        if limits:
            if value < limits['min'] or value > limits['max']:
                return {
                    'success': False,
                    'error': f"Value {value} out of range [{limits['min']}, {limits['max']}] for {field}",
                }

        # Audit entry
        entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'building_id': self.building_id,
            'field_name': field,
            'signal_path': signal_path,
            'value': value,
            'dry_run': self.dry_run,
        }

        if self.dry_run:
            entry['result'] = 'DRY_RUN — no write performed'
            self.audit_log.append(entry)
            print(f"[DRY RUN] Would write {field} = {value} to {signal_path}")
            return {'success': True, 'dry_run': True, 'field_name': field, 'value': value}

        # Actual write
        print(f"Writing {field} = {value} to {signal_path}...")
        result = self.api.set_property(signal_path, value)

        entry['result'] = result
        self.audit_log.append(entry)

        if result.get('success'):
            print(f"  Write succeeded via command: {result['command']}")
            if self.verbose:
                print(f"  Response: {json.dumps(result['response'], indent=2)}")
        else:
            print(f"  Write FAILED: {result.get('error')}")

        return result

    def write_and_verify(self, signal_name, value):
        """Write a value and verify by reading it back after a delay.

        Returns:
            dict with write result, read-back value, and match status
        """
        # Read old value first
        old = self.read_value(signal_name)
        old_value = old.get('value')
        print(f"Current value: {old_value}")

        # Write
        result = self.write_value(signal_name, value)
        if not result.get('success') or result.get('dry_run'):
            return result

        # Wait and read back
        print(f"Waiting {self.verify_delay}s for value to propagate...")
        time.sleep(self.verify_delay)

        new = self.read_value(signal_name)
        new_value = new.get('value')
        target = float(value)

        match = new_value is not None and abs(new_value - target) < 0.1
        print(f"Read-back value: {new_value} — {'MATCH' if match else 'MISMATCH'}")

        result['old_value'] = old_value
        result['readback_value'] = new_value
        result['verified'] = match
        return result

    def read_heat_curve(self, subsystem):
        """Read all X/Y heat curve points for a subsystem (e.g. VS1, VS2).

        Returns:
            dict with 'x_points' and 'y_points' lists, each as (field, value) tuples
        """
        sub = subsystem.lower()
        x_points = []
        y_points = []

        for field in sorted(self.signals.keys()):
            if not field.startswith(f'{sub}_hc_'):
                continue
            result = self.read_value(field)
            val = result.get('value')
            if '_x' in field:
                x_points.append((field, val))
            elif '_y' in field:
                y_points.append((field, val))

        return {'x_points': x_points, 'y_points': y_points, 'subsystem': subsystem}

    def experiment_write(self, signal_name, value):
        """Try all write command variants and log results for each.

        Used during experimental phase to discover which EBO command works.
        """
        field, sig = self._resolve_signal(signal_name)
        signal_path = sig['signal_id']
        value = float(value)
        hex_value = EboApi.encode_value(value)

        print(f"\n{'='*60}")
        print(f"EXPERIMENT: Writing {value} (hex: {hex_value}) to {field}")
        print(f"Path: {signal_path}")
        print(f"{'='*60}\n")

        # Read current value first
        current = self.read_value(signal_name)
        print(f"Current value: {current.get('value')} (raw: {current.get('raw')})\n")

        if self.dry_run:
            print("[DRY RUN] Skipping actual writes. Use --live to attempt writes.")
            return

        # Define all variants to try
        variants = [
            ("SetMultiProperty (plain)", "SetMultiProperty",
             {"command": "SetMultiProperty", "data": [{"path": signal_path, "value": value}]}),
            ("SetMultiProperty (hex)", "SetMultiProperty",
             {"command": "SetMultiProperty", "data": [{"path": signal_path, "value": hex_value}]}),
            ("SetProperty (plain)", "SetProperty",
             {"command": "SetProperty", "path": signal_path, "value": value}),
            ("SetProperty (hex)", "SetProperty",
             {"command": "SetProperty", "path": signal_path, "value": hex_value}),
            ("ForceProperty (plain)", "ForceProperty",
             {"command": "ForceProperty", "data": [{"path": signal_path, "value": value}]}),
            ("ForceProperty (hex)", "ForceProperty",
             {"command": "ForceProperty", "data": [{"path": signal_path, "value": hex_value}]}),
            ("WriteProperty (plain)", "WriteProperty",
             {"command": "WriteProperty", "data": [{"path": signal_path, "value": value}]}),
            ("SetMultiProperty (object)", "SetMultiProperty",
             {"command": "SetMultiProperty", "data": [{"path": signal_path, "property": "Value", "value": value}]}),
            ("SetMultiProperty (typed)", "SetMultiProperty",
             {"command": "SetMultiProperty", "data": [{"path": signal_path, "value": {"value": value}}]}),
        ]

        results = []
        for desc, cmd_name, cmd_data in variants:
            print(f"--- {desc} ---")
            print(f"  Request: {json.dumps(cmd_data, ensure_ascii=False)}")
            try:
                resp = self.api._post_command(cmd_data)
                print(f"  Response: {json.dumps(resp, indent=2, ensure_ascii=False)}")

                # Check for error
                is_error = False
                if isinstance(resp, dict):
                    err = resp.get('error') or resp.get('Error') or resp.get('ErrMsg')
                    if err:
                        is_error = True
                        print(f"  STATUS: ERROR — {err}")

                if not is_error:
                    print(f"  STATUS: OK (no error in response)")
                    # Wait and read back
                    print(f"  Waiting {self.verify_delay}s for propagation...")
                    time.sleep(self.verify_delay)
                    readback = self.read_value(signal_name)
                    rb_val = readback.get('value')
                    match = rb_val is not None and abs(rb_val - value) < 0.1
                    print(f"  Read-back: {rb_val} — {'MATCH!' if match else 'no change'}")
                    results.append((desc, 'OK', rb_val, match))
                else:
                    results.append((desc, 'ERROR', None, False))

            except Exception as e:
                print(f"  EXCEPTION: {e}")
                results.append((desc, 'EXCEPTION', str(e), False))
            print()

        # Summary
        print(f"\n{'='*60}")
        print("EXPERIMENT SUMMARY")
        print(f"{'='*60}")
        for desc, status, readback, match in results:
            marker = ' *** WORKS ***' if match else ''
            print(f"  {desc}: {status} (readback={readback}){marker}")

        # Restore original value
        orig = current.get('value')
        if orig is not None:
            print(f"\nRestoring original value: {orig}")
            working = [desc for desc, _, _, match in results if match]
            if working:
                print(f"(Using first working command)")
                self.api.set_property(signal_path, orig)
                time.sleep(self.verify_delay)
                final = self.read_value(signal_name)
                print(f"Final value: {final.get('value')}")


def format_heat_curve(curve_data):
    """Pretty-print a heat curve."""
    x_pts = curve_data['x_points']
    y_pts = curve_data['y_points']
    sub = curve_data['subsystem']

    print(f"\nHeat Curve — {sub}")
    print(f"{'Outdoor (°C)':>14}  {'Supply (°C)':>12}")
    print(f"{'─'*14}  {'─'*12}")
    for (xf, xv), (yf, yv) in zip(x_pts, y_pts):
        xstr = f"{xv:.1f}" if xv is not None else "N/A"
        ystr = f"{yv:.1f}" if yv is not None else "N/A"
        print(f"{xstr:>14}  {ystr:>12}")


def main():
    parser = argparse.ArgumentParser(
        description='EBO Write Controller — read and write EBO building signals',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Read a signal:
    python control_EBO.py --building-id HK_Kattegatt_20942 --read vs1_curve_parallel_shift

  Read heat curve:
    python control_EBO.py --building-id HK_Kattegatt_20942 --read-curve VS1

  Dry-run write (default):
    python control_EBO.py --building-id HK_Kattegatt_20942 --signal vs1_curve_parallel_shift --value 1.0

  Live write with verification:
    python control_EBO.py --building-id HK_Kattegatt_20942 --signal vs1_curve_parallel_shift --value 1.0 --live

  Experiment (try all command variants):
    python control_EBO.py --building-id HK_Kattegatt_20942 --experiment vs1_curve_parallel_shift --value 1.0 --live
        """,
    )
    parser.add_argument('--building-id', required=True, help='Building ID (e.g. HK_Kattegatt_20942)')
    parser.add_argument('--read', metavar='SIGNAL', help='Read a signal value')
    parser.add_argument('--read-curve', metavar='SUBSYSTEM', help='Read full heat curve (e.g. VS1, VS2)')
    parser.add_argument('--signal', metavar='SIGNAL', help='Signal to write')
    parser.add_argument('--value', type=float, help='Value to write')
    parser.add_argument('--experiment', metavar='SIGNAL', help='Experimental: try all write command variants')
    parser.add_argument('--live', action='store_true', help='Actually perform writes (default: dry-run)')
    parser.add_argument('--dry-run', action='store_true', default=True, help='Log but do not write (default)')
    parser.add_argument('--verify-delay', type=int, default=5, help='Seconds to wait for read-back (default: 5)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--list-signals', action='store_true', help='List all signals and exit')
    parser.add_argument('--list-writable', action='store_true', help='List writable signals and exit')

    args = parser.parse_args()

    # Determine dry_run: --live overrides --dry-run
    dry_run = not args.live

    try:
        ctrl = EBOController(
            building_id=args.building_id,
            dry_run=dry_run,
            verify_delay=args.verify_delay,
            verbose=args.verbose,
        )
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # List modes (no connection needed)
    if args.list_signals:
        print(f"\nSignals for {args.building_id}:")
        print(f"{'Field Name':<35} {'Key':<20} {'Unit':>5} {'Writable':>8}")
        print(f"{'─'*35} {'─'*20} {'─'*5} {'─'*8}")
        for field in sorted(ctrl.signals.keys()):
            sig = ctrl.signals[field]
            w = 'YES' if field in ctrl.writable else ''
            print(f"{field:<35} {sig['_key']:<20} {sig.get('unit',''):>5} {w:>8}")
        return

    if args.list_writable:
        print(f"\nWritable signals for {args.building_id}:")
        for field in sorted(ctrl.writable):
            sig = ctrl.signals[field]
            limits = ctrl._get_value_limits(field, sig)
            lim_str = f" [{limits['min']}, {limits['max']}]" if limits else ""
            print(f"  {field:<35} ({sig.get('unit', '')}){lim_str}")
        return

    # All other modes need a connection
    ctrl.connect()

    if args.read:
        result = ctrl.read_value(args.read)
        val = result.get('value')
        unit = result.get('unit', '')
        print(f"\n{result['field_name']} = {val} {unit}")
        if args.verbose:
            print(f"  Signal ID: {result['signal_id']}")
            print(f"  Raw value: {result.get('raw')}")

    elif args.read_curve:
        curve = ctrl.read_heat_curve(args.read_curve)
        format_heat_curve(curve)

    elif args.experiment:
        if args.value is None:
            print("Error: --value required with --experiment")
            sys.exit(1)
        ctrl.experiment_write(args.experiment, args.value)

    elif args.signal:
        if args.value is None:
            print("Error: --value required with --signal")
            sys.exit(1)
        result = ctrl.write_and_verify(args.signal, args.value)
        if not result.get('success') and not result.get('dry_run'):
            sys.exit(1)

    else:
        parser.print_help()

    # Print audit log
    if ctrl.audit_log:
        print(f"\n--- Audit Log ---")
        for entry in ctrl.audit_log:
            print(f"  {entry['timestamp']} | {entry['field_name']} = {entry['value']} | "
                  f"{'DRY_RUN' if entry['dry_run'] else 'LIVE'}")


if __name__ == '__main__':
    main()

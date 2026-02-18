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
from seq_logger import SeqLogger


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

    # Default force duration: 2 hours
    DEFAULT_FORCE_DURATION = 7200

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
        self._object_id_cache = {}  # path -> GUID cache
        self.seq = None

        self._load_config()
        self._init_seq()

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

    def _init_seq(self):
        """Initialize Seq structured logging."""
        load_dotenv()
        friendly_name = self.config.get('friendly_name', self.building_id)
        self.seq = SeqLogger(
            client_id=self.building_id,
            friendly_name=friendly_name,
            seq_url=os.getenv('SEQ_URL'),
            seq_api_key=os.getenv('SEQ_API_KEY'),
            component='EBOController',
            display_name_source='friendly_name',
        )

    def _seq_log(self, message, level='Information', **extra):
        """Log to Seq if available."""
        if self.seq:
            self.seq.log(message, level=level, properties=extra)

    def connect(self, credential_ref_override=None):
        """Login to EBO and initialize session."""
        conn = self.config.get('connection', {})
        base_url = conn.get('base_url')
        credential_ref = credential_ref_override or conn.get('credential_ref')

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

    def _get_server_path(self):
        """Derive the EBO server/AS path from the building config.

        Returns e.g. '/Kattegattgymnasiet 20942 AS3'
        """
        # Try site_path from config
        conn = self.config.get('connection', {})
        site_path = conn.get('site_path', '')
        if site_path:
            return site_path.rstrip('/')

        # Derive from any signal_id: take the first path component after root
        for sig in self.signals.values():
            sig_id = sig.get('signal_id', '')
            if sig_id:
                # "/Kattegattgymnasiet 20942 AS3/VS1/Variabler/GT1_FS/Value"
                # → "/Kattegattgymnasiet 20942 AS3"
                parts = sig_id.strip('/').split('/')
                if parts:
                    return '/' + parts[0]

        raise ValueError("Cannot determine server path from config")

    def _get_object_path(self, signal_path):
        """Strip /Value suffix from signal_id to get object path."""
        if signal_path.endswith('/Value'):
            return signal_path[:-6]
        return signal_path

    def _get_object_id(self, object_path):
        """Get the GUID for an EBO object via get_objects.

        Caches results to avoid repeated API calls.
        """
        if object_path in self._object_id_cache:
            return self._object_id_cache[object_path]

        result = self.api.get_objects([object_path], levels=0, include_hidden=True)
        res = result.get('GetObjectsRes', result)
        for r in res.get('results', []):
            oid = r.get('id')
            if oid:
                self._object_id_cache[object_path] = oid
                return oid

        raise ValueError(f"Could not get object ID for {object_path}")

    def _get_forced_state(self, object_path):
        """Read forced state of a signal.

        Returns:
            dict with 'value', 'forced', 'forced_until', 'true_value'
        """
        result = self.api.get_objects([object_path], levels=0, include_hidden=True)
        res = result.get('GetObjectsRes', result)
        for r in res.get('results', []):
            val_prop = r.get('properties', {}).get('Value', {})
            return {
                'value': _hex_to_double(val_prop.get('value')),
                'raw': val_prop.get('value'),
                'forced': val_prop.get('forced', False),
                'forced_until': val_prop.get('forcedUntil'),
                'true_value': _hex_to_double(val_prop.get('trueValue')),
            }
        return {'value': None, 'forced': False, 'forced_until': None, 'true_value': None}

    def force_signal(self, signal_name, value, duration_seconds=None):
        """Force a signal to a value with timed auto-release.

        If the signal is already forced, it will be unforced first.

        Args:
            signal_name: field_name or signal key
            value: numeric value to force
            duration_seconds: hold duration (default: DEFAULT_FORCE_DURATION)

        Returns:
            dict with result info
        """
        if duration_seconds is None:
            duration_seconds = self.DEFAULT_FORCE_DURATION

        field, sig = self._resolve_signal(signal_name)
        signal_path = sig['signal_id']
        object_path = self._get_object_path(signal_path)
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
            'duration_seconds': duration_seconds,
            'dry_run': self.dry_run,
        }

        if self.dry_run:
            h, m = divmod(duration_seconds, 3600)
            m, s = divmod(m, 60)
            entry['result'] = 'DRY_RUN — no force performed'
            self.audit_log.append(entry)
            print(f"[DRY RUN] Would force {field} = {value} for {int(h)}h{int(m)}m{int(s)}s")
            return {'success': True, 'dry_run': True, 'field_name': field, 'value': value}

        # Get object ID and server path
        server_path = self._get_server_path()
        object_id = self._get_object_id(object_path)

        if self.verbose:
            print(f"  Object path: {object_path}")
            print(f"  Object ID:   {object_id}")
            print(f"  Server path: {server_path}")

        # Check if already forced — unforce first
        state = self._get_forced_state(object_path)
        if state['forced']:
            unforce_val = state['true_value'] if state['true_value'] is not None else state['value']
            print(f"Signal is currently forced to {state['value']} "
                  f"({state['forced_until']/1000:.0f}s remaining). Unforcing first...")
            resp = self.api.unforce_value(object_path, object_id, unforce_val, server_path)
            commit_res = resp.get('CommitOperationsRes')
            if commit_res is None:
                err = resp.get('error') or resp.get('ErrMsg') or str(resp)
                print(f"  UnForce FAILED: {err}")
                entry['result'] = f'UnForce failed: {err}'
                self.audit_log.append(entry)
                return {'success': False, 'error': f'UnForce failed: {err}'}
            print(f"  Unforced OK.")
            time.sleep(1)  # Brief pause between unforce and force

        # Force with timed auto-release
        h, m = divmod(duration_seconds, 3600)
        m, s = divmod(m, 60)
        print(f"Forcing {field} = {value} for {int(h)}h{int(m)}m{int(s)}s...")
        resp = self.api.force_value(object_path, object_id, value, server_path,
                                    duration_seconds=duration_seconds)

        commit_res = resp.get('CommitOperationsRes')
        if commit_res is not None:
            print(f"  Force succeeded.")
            entry['result'] = 'SUCCESS'
            self.audit_log.append(entry)
            self._seq_log(
                f"[{{DisplayName}}] Forced {{FieldName}} = {{Value}} for {{Duration}}s",
                level='Information',
                FieldName=field, Value=value, Duration=duration_seconds,
                SignalPath=signal_path, Action='Force',
            )
            return {'success': True, 'field_name': field, 'value': value,
                    'duration_seconds': duration_seconds, 'response': resp}
        else:
            err = resp.get('error') or resp.get('ErrMsg') or str(resp)
            print(f"  Force FAILED: {err}")
            entry['result'] = f'Force failed: {err}'
            self.audit_log.append(entry)
            self._seq_log(
                f"[{{DisplayName}}] Force FAILED: {{FieldName}} = {{Value}} — {{Error}}",
                level='Error',
                FieldName=field, Value=value, Duration=duration_seconds,
                SignalPath=signal_path, Action='Force', Error=err,
            )
            return {'success': False, 'error': f'Force failed: {err}'}

    def unforce_signal(self, signal_name):
        """UnForce (release) a signal back to automatic control.

        Reads the trueValue (underlying automatic value) to send with the
        unforce command. Falls back to the current forced value if trueValue
        is not available.

        Args:
            signal_name: field_name or signal key

        Returns:
            dict with result info
        """
        field, sig = self._resolve_signal(signal_name)
        signal_path = sig['signal_id']
        object_path = self._get_object_path(signal_path)

        if self.dry_run:
            print(f"[DRY RUN] Would unforce {field}")
            return {'success': True, 'dry_run': True, 'field_name': field}

        server_path = self._get_server_path()
        object_id = self._get_object_id(object_path)

        # Read current state — use trueValue (the pre-force automatic value)
        state = self._get_forced_state(object_path)
        if not state['forced']:
            print(f"{field} is not currently forced.")
            return {'success': True, 'field_name': field, 'already_unforced': True}

        # trueValue = what the signal was before forcing; value = current forced value
        unforce_val = state['true_value'] if state['true_value'] is not None else state['value']
        print(f"Unforcing {field} (forced={state['value']}, restoring to={unforce_val})...")
        resp = self.api.unforce_value(object_path, object_id, unforce_val, server_path)

        commit_res = resp.get('CommitOperationsRes')
        if commit_res is not None:
            print(f"  Unforced OK.")
            self._seq_log(
                f"[{{DisplayName}}] Unforced {{FieldName}} (was={{ForcedValue}}, restored={{RestoredValue}})",
                level='Information',
                FieldName=field, ForcedValue=state['value'], RestoredValue=unforce_val,
                SignalPath=signal_path, Action='UnForce',
            )
            return {'success': True, 'field_name': field, 'response': resp}
        else:
            err = resp.get('error') or resp.get('ErrMsg') or str(resp)
            print(f"  UnForce FAILED: {err}")
            self._seq_log(
                f"[{{DisplayName}}] UnForce FAILED: {{FieldName}} — {{Error}}",
                level='Error',
                FieldName=field, SignalPath=signal_path, Action='UnForce', Error=err,
            )
            return {'success': False, 'error': f'UnForce failed: {err}'}

    def force_and_verify(self, signal_name, value, duration_seconds=None):
        """Force a signal and verify by reading it back.

        Returns:
            dict with force result, read-back value, and match status
        """
        # Read old value first
        field, sig = self._resolve_signal(signal_name)
        old = self.read_value(signal_name)
        old_value = old.get('value')
        print(f"Current value: {old_value}")

        # Force
        result = self.force_signal(signal_name, value, duration_seconds)
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
        description='EBO Write Controller — read, force, and unforce EBO building signals',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Read a signal:
    python control_EBO.py --building-id HK_Kattegatt_20942 --read vs1_curve_parallel_shift

  Read heat curve:
    python control_EBO.py --building-id HK_Kattegatt_20942 --read-curve VS1

  Force a signal (dry-run, default):
    python control_EBO.py --building-id HK_Kattegatt_20942 --force vs1_curve_parallel_shift --value 1.5

  Force a signal for 2 hours (live):
    python control_EBO.py --building-id HK_Kattegatt_20942 --force vs1_curve_parallel_shift --value 1.5 --duration 7200 --live

  Unforce a signal (live):
    python control_EBO.py --building-id HK_Kattegatt_20942 --unforce vs1_curve_parallel_shift --live
        """,
    )
    parser.add_argument('--building-id', required=True, help='Building ID (e.g. HK_Kattegatt_20942)')
    parser.add_argument('--read', metavar='SIGNAL', help='Read a signal value')
    parser.add_argument('--read-curve', metavar='SUBSYSTEM', help='Read full heat curve (e.g. VS1, VS2)')
    parser.add_argument('--force', metavar='SIGNAL', help='Force a signal to a value (requires --value)')
    parser.add_argument('--unforce', metavar='SIGNAL', help='Unforce (release) a signal')
    parser.add_argument('--value', type=float, help='Value to force')
    parser.add_argument('--duration', type=int, default=7200, help='Force hold duration in seconds (default: 7200 = 2h)')
    parser.add_argument('--live', action='store_true', help='Actually perform writes (default: dry-run)')
    parser.add_argument('--verify-delay', type=int, default=5, help='Seconds to wait for read-back (default: 5)')
    parser.add_argument('--credential-ref', metavar='REF', help='Override credential ref (e.g. EBO_HK_CRED2)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--list-signals', action='store_true', help='List all signals and exit')
    parser.add_argument('--list-writable', action='store_true', help='List writable signals and exit')
    # Legacy commands (kept for backwards compat)
    parser.add_argument('--signal', metavar='SIGNAL', help=argparse.SUPPRESS)
    parser.add_argument('--experiment', metavar='SIGNAL', help=argparse.SUPPRESS)

    args = parser.parse_args()

    # Determine dry_run: --live overrides default
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
    ctrl.connect(credential_ref_override=args.credential_ref)

    if args.read:
        result = ctrl.read_value(args.read)
        val = result.get('value')
        unit = result.get('unit', '')
        print(f"\n{result['field_name']} = {val} {unit}")
        if args.verbose:
            print(f"  Signal ID: {result['signal_id']}")
            print(f"  Raw value: {result.get('raw')}")
        # Also show forced state
        obj_path = ctrl._get_object_path(result['signal_id'])
        state = ctrl._get_forced_state(obj_path)
        if state['forced']:
            remaining = state['forced_until']
            if remaining:
                h, rem = divmod(remaining / 1000, 3600)
                m, s = divmod(rem, 60)
                print(f"  FORCED (auto-release in {int(h)}h{int(m)}m{int(s)}s)")
            else:
                print(f"  FORCED (no timer)")
        else:
            print(f"  Not forced (automatic)")

    elif args.read_curve:
        curve = ctrl.read_heat_curve(args.read_curve)
        format_heat_curve(curve)

    elif args.force:
        if args.value is None:
            print("Error: --value required with --force")
            sys.exit(1)
        result = ctrl.force_and_verify(args.force, args.value, duration_seconds=args.duration)
        if not result.get('success') and not result.get('dry_run'):
            sys.exit(1)

    elif args.unforce:
        result = ctrl.unforce_signal(args.unforce)
        if not result.get('success'):
            sys.exit(1)

    elif args.experiment:
        if args.value is None:
            print("Error: --value required with --experiment")
            sys.exit(1)
        ctrl.experiment_write(args.experiment, args.value)

    elif args.signal:
        # Legacy: --signal --value maps to force_and_verify
        if args.value is None:
            print("Error: --value required with --signal")
            sys.exit(1)
        result = ctrl.force_and_verify(args.signal, args.value, duration_seconds=args.duration)
        if not result.get('success') and not result.get('dry_run'):
            sys.exit(1)

    else:
        parser.print_help()

    # Print audit log
    if ctrl.audit_log:
        print(f"\n--- Audit Log ---")
        for entry in ctrl.audit_log:
            dur = entry.get('duration_seconds', '')
            dur_str = f" ({dur}s)" if dur else ''
            print(f"  {entry['timestamp']} | {entry['field_name']} = {entry.get('value', 'N/A')}{dur_str} | "
                  f"{'DRY_RUN' if entry.get('dry_run') else 'LIVE'}")


if __name__ == '__main__':
    main()

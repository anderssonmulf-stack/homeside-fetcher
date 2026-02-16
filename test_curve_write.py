#!/usr/bin/env python3
"""
Test script: verify we can read, write, and restore a heat curve point.

Tests writing via Cwl.Advise.A[70] and checks which variables actually change
(Yref7, CurveAdaptation_Y_7, Cwl.Advise.A[70] itself, PID setpoint).

Run inside Docker:
    docker exec SvenskEB-orchestrator python3 test_curve_write.py
"""

import os
import sys
import time
import logging

from homeside_api import HomeSideAPI

# Use Daggis8 (Villa_149) as test target
CUSTOMER_ID = 'HEM_FJV_Villa_149'
DELAY_SECONDS = 10

# Variables to watch for changes
WATCH_VARS = [
    'KU_VS1_GT_TILL_1_Yref7',
    'KU_VS1_GT_TILL_1_CurveAdaptation_Y_7',
    'KU_VS1_GT_TILL_1_SetPoint',
    'KU_VS1_GT_TILL_1_Output',
    'KU_VS1_GT_TILL_1_Adaption',
    'PID_VS1_GT_TILL_1_SetP',
]

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger('test_curve_write')


def read_all_watched(api):
    """Read all watched variables from the API."""
    data = api.get_heating_data()
    if not data or 'variables' not in data:
        return {}

    result = {}
    for var in data['variables']:
        short = var['variable'].split('.')[-1]
        path = var.get('path', '')
        value = var.get('value')

        # Check by short name
        if short in WATCH_VARS and value is not None:
            result[short] = value

        # Also check for Cwl.Advise.A[70] in the path
        if 'Cwl.Advise.A[70]' in path and value is not None:
            result['Cwl.Advise.A[70]'] = value

    return result


def print_snapshot(label, snapshot):
    """Pretty-print a variable snapshot."""
    print(f"  {label}:")
    for key in WATCH_VARS + ['Cwl.Advise.A[70]']:
        if key in snapshot:
            print(f"    {key:45s} = {snapshot[key]}")


def print_diff(before, after):
    """Print variables that changed between snapshots."""
    all_keys = sorted(set(list(before.keys()) + list(after.keys())))
    changed = False
    for key in all_keys:
        v1 = before.get(key)
        v2 = after.get(key)
        if v1 != v2:
            print(f"    CHANGED: {key:40s} {v1} -> {v2}")
            changed = True
    if not changed:
        print(f"    (no changes detected)")


def main():
    username = os.getenv(f'HOUSE_{CUSTOMER_ID}_USERNAME', '')
    password = os.getenv(f'HOUSE_{CUSTOMER_ID}_PASSWORD', '')

    if not username or not password:
        print(f"ERROR: Set HOUSE_{CUSTOMER_ID}_USERNAME and HOUSE_{CUSTOMER_ID}_PASSWORD")
        sys.exit(1)

    print(f"=== Heat Curve Write Test (Extended) ===")
    print(f"Target: {CUSTOMER_ID}")
    print(f"Delay:  {DELAY_SECONDS}s between steps")
    print()

    # Authenticate
    print("Step 0: Authenticating...")
    api = HomeSideAPI(
        session_token=None,
        clientid='',
        logger=logger,
        username=username,
        password=password,
        debug_mode=False,
    )

    if not api.refresh_session_token():
        print("FAIL: Could not get session token")
        sys.exit(1)

    if not api.get_bms_token():
        print("FAIL: Could not get BMS token")
        sys.exit(1)

    print("  OK: Authenticated\n")

    # Step 1: Read baseline snapshot
    print("Step 1: Reading baseline snapshot...")
    before = read_all_watched(api)
    print_snapshot("Before", before)
    original_yref7 = before.get('KU_VS1_GT_TILL_1_Yref7')
    print()

    # Step 2: Write via Cwl.Advise.A[70]
    test_value = (original_yref7 or 30.0) + 1.0
    print(f"Step 2: Writing Cwl.Advise.A[70] = {test_value}...")
    ok = api.write_value("Cwl.Advise.A[70]", test_value)
    print(f"  API returned: {'success' if ok else 'FAILURE'}")
    print()

    # Step 3: Wait
    print(f"Step 3: Waiting {DELAY_SECONDS}s...")
    time.sleep(DELAY_SECONDS)
    print()

    # Step 4: Read back
    print("Step 4: Reading after Cwl.Advise write...")
    after_cwl = read_all_watched(api)
    print_snapshot("After Cwl write", after_cwl)
    print("\n  Diff from baseline:")
    print_diff(before, after_cwl)
    print()

    # Step 5: Try writing via the Yref variable name directly
    test_value2 = (original_yref7 or 30.0) + 2.0
    print(f"Step 5: Writing KU_VS1_GT_TILL_1_Yref7 = {test_value2} (direct name)...")
    ok2 = api.write_value("KU_VS1_GT_TILL_1_Yref7", test_value2)
    print(f"  API returned: {'success' if ok2 else 'FAILURE'}")
    print()

    # Step 6: Wait
    print(f"Step 6: Waiting {DELAY_SECONDS}s...")
    time.sleep(DELAY_SECONDS)
    print()

    # Step 7: Read back
    print("Step 7: Reading after direct Yref write...")
    after_yref = read_all_watched(api)
    print_snapshot("After Yref write", after_yref)
    print("\n  Diff from baseline:")
    print_diff(before, after_yref)
    print()

    # Step 8: Try with full path prefix
    test_value3 = (original_yref7 or 30.0) + 3.0
    full_path = f"{CUSTOMER_ID}.KU_VS1_GT_TILL_1_Yref7"
    print(f"Step 8: Writing {full_path} = {test_value3} (full path)...")
    ok3 = api.write_value(full_path, test_value3)
    print(f"  API returned: {'success' if ok3 else 'FAILURE'}")
    print()

    # Step 9: Wait
    print(f"Step 9: Waiting {DELAY_SECONDS}s...")
    time.sleep(DELAY_SECONDS)
    print()

    # Step 10: Read back
    print("Step 10: Reading after full-path write...")
    after_full = read_all_watched(api)
    print_snapshot("After full-path write", after_full)
    print("\n  Diff from baseline:")
    print_diff(before, after_full)
    print()

    # Restore: write original back via all methods that showed changes
    print(f"Step 11: Restoring original value {original_yref7}°C via all methods...")
    if original_yref7 is not None:
        api.write_value("Cwl.Advise.A[70]", original_yref7)
        api.write_value("KU_VS1_GT_TILL_1_Yref7", original_yref7)
        api.write_value(f"{CUSTOMER_ID}.KU_VS1_GT_TILL_1_Yref7", original_yref7)
    print()

    print(f"Step 12: Waiting {DELAY_SECONDS}s...")
    time.sleep(DELAY_SECONDS)
    print()

    print("Step 13: Final verification...")
    final = read_all_watched(api)
    print_snapshot("Final", final)
    print("\n  Diff from original baseline:")
    print_diff(before, final)
    print()

    print("=== Done ===")


if __name__ == '__main__':
    main()

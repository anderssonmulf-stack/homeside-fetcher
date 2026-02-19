#!/usr/bin/env python3
"""Quick diagnostic: dump all HomeSide variables for a house."""
import sys
import os
import json
import logging

# Load .env manually
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, val = line.partition('=')
                val = val.strip().strip('"').strip("'")
                os.environ.setdefault(key.strip(), val)

from homeside_api import HomeSideAPI

def main():
    customer_id = sys.argv[1] if len(sys.argv) > 1 else "HEM_FJV_Villa_149"
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    # Get credentials
    username = os.getenv(f"HOUSE_{customer_id}_USERNAME")
    password = os.getenv(f"HOUSE_{customer_id}_PASSWORD")

    if not username or not password:
        print(f"Missing credentials: HOUSE_{customer_id}_USERNAME/PASSWORD")
        return

    logger = logging.getLogger("dump")
    logging.basicConfig(level=logging.WARNING)

    # clientid=None triggers auto-discovery
    api = HomeSideAPI("", None, logger, username=username, password=password)

    # Authenticate
    if not api.refresh_session_token():
        print("Failed to authenticate")
        return
    if not api.get_bms_token():
        print("Failed to get BMS token")
        return

    # Fetch all variables
    raw = api.get_heating_data()
    if not raw or 'variables' not in raw:
        print("Failed to fetch data")
        return

    variables = raw['variables']
    print(f"\n=== {customer_id} — {len(variables)} variables ===\n")

    # Sort by path for readability
    variables.sort(key=lambda v: v.get('path', ''))

    results = []
    for var in variables:
        path = var.get('path', '')
        name = var.get('variable', '')
        short = name.split('.')[-1] if '.' in name else name
        value = var.get('value')
        vtype = var.get('type', '')

        line = f"{path:30s}  {short:45s}  = {value}"
        print(line)
        results.append({
            'path': path,
            'variable': name,
            'short': short,
            'value': value,
            'type': vtype,
        })

    if output_file:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nSaved to {output_file}")

    api.cleanup()

if __name__ == '__main__':
    main()

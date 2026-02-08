#!/usr/bin/env python3
"""
Add New Customer Script

Automates the process of adding a new customer to the Homeside Fetcher system:
1. Creates customer profile JSON
2. Appends per-house credentials to .env
3. Orchestrator picks up the new profile within 60 seconds (no restart needed)

Usage:
    python3 add_customer.py
    python3 add_customer.py --non-interactive --username FC123 --password pass --name "House Name" --lat 56.67 --lon 12.86
"""

import os
import sys
import json
import argparse
import re


def get_next_villa_number(profiles_dir: str = "profiles") -> int:
    """Find the next available villa number."""
    existing = []
    if os.path.exists(profiles_dir):
        for f in os.listdir(profiles_dir):
            match = re.search(r'HEM_FJV_Villa_(\d+)\.json', f)
            if match:
                existing.append(int(match.group(1)))
    return max(existing, default=0) + 1


def create_profile(customer_id: str, friendly_name: str, profiles_dir: str = "profiles") -> str:
    """Create a new customer profile JSON file."""
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
        }
    }

    os.makedirs(profiles_dir, exist_ok=True)
    filepath = os.path.join(profiles_dir, f"{customer_id}.json")

    with open(filepath, 'w') as f:
        json.dump(profile, f, indent=2)

    return filepath


def add_credentials_to_env(customer_id: str, username: str, password: str,
                           friendly_name: str, env_file: str = ".env") -> bool:
    """Append per-house credentials to .env file."""
    # Check if credentials already exist
    env_key = f"HOUSE_{customer_id}_USERNAME"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            content = f.read()
        if env_key in content:
            print(f"  Credentials for {customer_id} already exist in {env_file}")
            return False

    # Append credentials
    lines = [
        f"\n# {friendly_name} ({customer_id})\n",
        f"HOUSE_{customer_id}_USERNAME={username}\n",
        f"HOUSE_{customer_id}_PASSWORD={password}\n",
    ]

    with open(env_file, 'a') as f:
        f.writelines(lines)

    return True


def main():
    parser = argparse.ArgumentParser(description='Add a new customer to Homeside Fetcher')
    parser.add_argument('--non-interactive', action='store_true', help='Run without prompts')
    parser.add_argument('--username', type=str, help='HomeSide username (e.g., FC2000233091)')
    parser.add_argument('--password', type=str, help='HomeSide password')
    parser.add_argument('--name', type=str, help='Friendly name for the house')
    parser.add_argument('--lat', type=float, help='Latitude for weather')
    parser.add_argument('--lon', type=float, help='Longitude for weather')
    parser.add_argument('--villa-number', type=int, help='Villa number (auto-detected if not specified)')
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
    else:
        username = args.username or input("HomeSide username (e.g., FC2000233091): ").strip()
        password = args.password or input("HomeSide password: ").strip()
        friendly_name = args.name or input("Friendly name for the house: ").strip()

    # Determine villa number
    villa_number = args.villa_number or get_next_villa_number()
    customer_id = f"HEM_FJV_Villa_{villa_number}"

    print()
    print("Configuration:")
    print(f"  Customer ID: {customer_id}")
    print(f"  Friendly name: {friendly_name}")
    print(f"  Username: {username}")
    print()

    if not args.non_interactive:
        confirm = input("Proceed? [Y/n]: ").strip().lower()
        if confirm and confirm != 'y':
            print("Aborted.")
            sys.exit(0)

    # Step 1: Create profile
    print("\n[1/2] Creating customer profile...")
    profile_path = create_profile(customer_id, friendly_name)
    print(f"  Created: {profile_path}")

    # Step 2: Add credentials to .env
    print("\n[2/2] Adding credentials to .env...")
    if add_credentials_to_env(customer_id, username, password, friendly_name):
        print(f"  Added HOUSE_{customer_id}_USERNAME/PASSWORD to .env")
    else:
        print("  Skipped (credentials may already exist)")

    print()
    print("=" * 60)
    print("Done! The orchestrator will detect the new profile within 60 seconds.")
    print("=" * 60)
    print()
    print("Next steps:")
    print(f"1. Check orchestrator logs: docker logs -f homeside-orchestrator")
    print(f"2. Look for: Spawned house '{friendly_name}'")
    print(f"3. If needed, update the profile's customer_id in {profile_path}")
    print(f"   to match the auto-discovered ID from the logs")
    print()


if __name__ == "__main__":
    main()

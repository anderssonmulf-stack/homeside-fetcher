#!/usr/bin/env python3
"""
Add New Customer Script

Automates the process of adding a new customer to the Homeside Fetcher system:
1. Creates customer profile JSON
2. Adds service to docker-compose.yml
3. Optionally rebuilds and starts the container

Usage:
    python3 add_customer.py
    python3 add_customer.py --non-interactive --username FC123 --password pass --name "House Name" --lat 56.67 --lon 12.86
"""

import os
import sys
import json
import argparse
import subprocess
import re
from datetime import datetime


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


def add_to_docker_compose(
    service_name: str,
    container_name: str,
    username: str,
    password: str,
    friendly_name: str,
    latitude: float,
    longitude: float,
    seq_api_key: str,
    compose_file: str = "docker-compose.yml"
) -> bool:
    """Add a new fetcher service to docker-compose.yml."""

    # Read current compose file
    with open(compose_file, 'r') as f:
        content = f.read()

    # Check if service already exists
    if f"homeside-fetcher-{service_name}:" in content:
        print(f"Service 'homeside-fetcher-{service_name}' already exists in {compose_file}")
        return False

    # Create new service definition
    new_service = f'''
  # Customer: {friendly_name}
  homeside-fetcher-{service_name}:
    build: .
    container_name: {container_name}
    restart: unless-stopped
    environment:
      - HOMESIDE_USERNAME={username}
      - HOMESIDE_PASSWORD={password}
      - HOMESIDE_CLIENTID=
      - FRIENDLY_NAME={friendly_name}
      - DISPLAY_NAME_SOURCE=friendly_name
      - POLL_INTERVAL_MINUTES=15
      - SEQ_URL=http://seq:5341
      - SEQ_API_KEY={seq_api_key}
      - LOG_LEVEL=INFO
      - DEBUG_MODE=false
      - INFLUXDB_URL=http://influxdb:8086
      - INFLUXDB_TOKEN=homeside_token_2026_secret
      - INFLUXDB_ORG=homeside
      - INFLUXDB_BUCKET=heating
      - INFLUXDB_ENABLED=true
      - LATITUDE={latitude}
      - LONGITUDE={longitude}
    volumes:
      - ./profiles:/app/profiles
    networks:
      - dryckesmail_beer-network
    depends_on:
      - influxdb
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
'''

    # Find position to insert (before 'influxdb:' service)
    insert_pos = content.find('\n  influxdb:')
    if insert_pos == -1:
        print("Could not find 'influxdb:' service in docker-compose.yml")
        return False

    # Insert new service
    new_content = content[:insert_pos] + new_service + content[insert_pos:]

    with open(compose_file, 'w') as f:
        f.write(new_content)

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
    parser.add_argument('--seq-api-key', type=str, default='hllDhc79UDrWDEJ0HPY2', help='Seq API key')
    parser.add_argument('--no-deploy', action='store_true', help='Skip docker build/deploy')
    args = parser.parse_args()

    print("=" * 60)
    print("Add New Customer to Homeside Fetcher")
    print("=" * 60)
    print()

    # Collect information
    if args.non_interactive:
        if not all([args.username, args.password, args.name, args.lat, args.lon]):
            print("ERROR: In non-interactive mode, all parameters are required:")
            print("  --username, --password, --name, --lat, --lon")
            sys.exit(1)
        username = args.username
        password = args.password
        friendly_name = args.name
        latitude = args.lat
        longitude = args.lon
    else:
        username = args.username or input("HomeSide username (e.g., FC2000233091): ").strip()
        password = args.password or input("HomeSide password: ").strip()
        friendly_name = args.name or input("Friendly name for the house: ").strip()
        lat_str = input(f"Latitude [{args.lat or 56.67}]: ").strip()
        latitude = float(lat_str) if lat_str else (args.lat or 56.67)
        lon_str = input(f"Longitude [{args.lon or 12.86}]: ").strip()
        longitude = float(lon_str) if lon_str else (args.lon or 12.86)

    # Determine villa number
    villa_number = args.villa_number or get_next_villa_number()
    customer_id = f"HEM_FJV_Villa_{villa_number}"
    container_name = f"homeside-fetcher-{customer_id}"
    service_name = friendly_name.lower().replace(' ', '-').replace('å', 'a').replace('ä', 'a').replace('ö', 'o')

    print()
    print("Configuration:")
    print(f"  Customer ID: {customer_id}")
    print(f"  Friendly name: {friendly_name}")
    print(f"  Username: {username}")
    print(f"  Location: {latitude}, {longitude}")
    print(f"  Container: {container_name}")
    print()

    if not args.non_interactive:
        confirm = input("Proceed? [Y/n]: ").strip().lower()
        if confirm and confirm != 'y':
            print("Aborted.")
            sys.exit(0)

    # Step 1: Create profile
    print("\n[1/3] Creating customer profile...")
    profile_path = create_profile(customer_id, friendly_name)
    print(f"  Created: {profile_path}")

    # Step 2: Add to docker-compose.yml
    print("\n[2/3] Adding service to docker-compose.yml...")
    if add_to_docker_compose(
        service_name=service_name,
        container_name=container_name,
        username=username,
        password=password,
        friendly_name=friendly_name,
        latitude=latitude,
        longitude=longitude,
        seq_api_key=args.seq_api_key
    ):
        print(f"  Added service: homeside-fetcher-{service_name}")
    else:
        print("  Skipped (service may already exist)")

    # Step 3: Build and deploy
    if not args.no_deploy:
        print("\n[3/3] Building and deploying...")
        if not args.non_interactive:
            deploy = input("Build and deploy now? [Y/n]: ").strip().lower()
            if deploy and deploy != 'y':
                print("  Skipped. Run manually:")
                print("    docker compose build")
                print("    docker compose up -d")
            else:
                subprocess.run(["docker", "compose", "build"], check=True)
                subprocess.run(["docker", "compose", "up", "-d"], check=True)
                print("  Deployed successfully!")
        else:
            subprocess.run(["docker", "compose", "build"], check=True)
            subprocess.run(["docker", "compose", "up", "-d"], check=True)
            print("  Deployed successfully!")
    else:
        print("\n[3/3] Skipping deploy (--no-deploy)")
        print("  Run manually:")
        print("    docker compose build")
        print("    docker compose up -d")

    print()
    print("=" * 60)
    print("Done! Next steps:")
    print("=" * 60)
    print(f"1. Check logs: docker logs -f {container_name}")
    print(f"2. Verify profile loaded: look for '✓ Customer profile loaded: {friendly_name}'")
    print(f"3. If needed, update the profile's customer_id in {profile_path}")
    print(f"   to match the auto-discovered ID from the logs")
    print()


if __name__ == "__main__":
    main()

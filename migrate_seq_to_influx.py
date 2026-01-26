#!/usr/bin/env python3
"""
One-time migration script: Seq logs -> InfluxDB thermal_history

Reads historical thermal data from Seq logs and writes to InfluxDB
so the thermal analyzer has data immediately after restart.

Usage:
    python migrate_seq_to_influx.py [--days 7] [--dry-run]
"""

import os
import sys
import argparse
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def fetch_seq_events(seq_url: str, seq_api_key: str, days: int = 7) -> list:
    """
    Fetch DataCollected events from Seq.

    Args:
        seq_url: Seq server URL
        seq_api_key: Seq API key (optional)
        days: Number of days to look back

    Returns:
        List of events with thermal data
    """
    # Build Seq query URL
    base_url = seq_url.rstrip('/')
    if '/api' in base_url:
        base_url = base_url.replace('/api', '')

    # Seq has two ports: 5341 for ingestion and 80 (mapped to 8081) for web/API
    # The events query API is on the web port, not ingestion port
    # Try to detect if we need to use a different port
    if ':5341' in base_url:
        # Try web port (80) instead of ingestion port (5341)
        web_url = base_url.replace(':5341', ':80')
        urls_to_try = [web_url, base_url]
    else:
        urls_to_try = [base_url]

    headers = {'Accept': 'application/json'}
    if seq_api_key:
        headers['X-Seq-ApiKey'] = seq_api_key

    params = {
        'filter': "EventType = 'DataCollected'",
        'count': 5000,  # Max events to fetch
        'shortCircuitAfter': 10000
    }

    for url in urls_to_try:
        query_url = f"{url}/api/events"
        print(f"Fetching events from Seq: {query_url}")
        print(f"Filter: {params['filter']}")

        try:
            response = requests.get(query_url, headers=headers, params=params, timeout=60)
            response.raise_for_status()

            events = response.json()
            print(f"Fetched {len(events)} events from Seq")
            return events

        except requests.exceptions.RequestException as e:
            print(f"Failed on {query_url}: {e}")
            if url != urls_to_try[-1]:
                print("Trying alternative URL...")
            continue

    print("Error: Could not fetch from Seq on any URL")
    return []


def parse_seq_event(event: dict) -> dict:
    """
    Parse a Seq event into thermal data format.

    Args:
        event: Seq event dictionary

    Returns:
        Dictionary with thermal data or None if invalid
    """
    try:
        # Get timestamp
        timestamp_str = event.get('Timestamp') or event.get('@t')
        if not timestamp_str:
            return None

        # Parse timestamp
        if timestamp_str.endswith('Z'):
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            timestamp = datetime.fromisoformat(timestamp_str)

        # Get properties - Seq stores them in different formats depending on API version
        raw_props = event.get('Properties', {})

        # Handle array format: [{"Name": "key", "Value": val}, ...]
        if isinstance(raw_props, list):
            props = {}
            for item in raw_props:
                name = item.get('Name')
                value = item.get('Value')
                if name:
                    props[name] = value
        elif isinstance(raw_props, dict):
            props = raw_props
        else:
            # Also check top-level for structured logging
            props = {k: v for k, v in event.items() if not k.startswith('@') and k != 'Timestamp'}

        # Extract required fields (PascalCase from Seq)
        room_temp = props.get('RoomTemperature')
        outdoor_temp = props.get('OutdoorTemperature')

        if room_temp is None or outdoor_temp is None:
            return None

        # Build thermal data point
        data = {
            'timestamp': timestamp,
            'room_temperature': float(room_temp),
            'outdoor_temperature': float(outdoor_temp),
        }

        # Optional fields
        if props.get('SupplyTemp') is not None:
            data['supply_temp'] = float(props['SupplyTemp'])
        if props.get('ElectricHeater') is not None:
            data['electric_heater'] = bool(props['ElectricHeater'])
        if props.get('ReturnTemp') is not None:
            data['return_temp'] = float(props['ReturnTemp'])

        return data

    except (ValueError, TypeError, KeyError) as e:
        return None


def write_to_influx(
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    house_id: str,
    data_points: list,
    dry_run: bool = False
) -> int:
    """
    Write thermal data points to InfluxDB.

    Args:
        influx_url: InfluxDB URL
        influx_token: InfluxDB token
        influx_org: InfluxDB organization
        influx_bucket: InfluxDB bucket
        house_id: House identifier tag
        data_points: List of thermal data dictionaries
        dry_run: If True, don't actually write

    Returns:
        Number of points written
    """
    if dry_run:
        print(f"\n[DRY RUN] Would write {len(data_points)} points to InfluxDB")
        for i, dp in enumerate(data_points[:5]):
            print(f"  {i+1}. {dp['timestamp']}: room={dp['room_temperature']:.1f}, outdoor={dp['outdoor_temperature']:.1f}")
        if len(data_points) > 5:
            print(f"  ... and {len(data_points) - 5} more")
        return 0

    try:
        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        written = 0
        for dp in data_points:
            point = Point("thermal_history") \
                .tag("house_id", house_id) \
                .field("room_temperature", dp['room_temperature']) \
                .field("outdoor_temperature", dp['outdoor_temperature']) \
                .time(dp['timestamp'], WritePrecision.S)

            if 'supply_temp' in dp:
                point.field("supply_temp", dp['supply_temp'])
            if 'electric_heater' in dp:
                point.field("electric_heater", 1 if dp['electric_heater'] else 0)
            if 'return_temp' in dp:
                point.field("return_temp", dp['return_temp'])

            write_api.write(bucket=influx_bucket, org=influx_org, record=point)
            written += 1

            if written % 100 == 0:
                print(f"  Written {written}/{len(data_points)} points...")

        client.close()
        return written

    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Migrate thermal data from Seq logs to InfluxDB'
    )
    parser.add_argument(
        '--days', type=int, default=7,
        help='Number of days to look back (default: 7)'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be migrated without writing'
    )
    parser.add_argument(
        '--seq-url', type=str, default=None,
        help='Override Seq URL (default: from .env)'
    )
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Get configuration
    seq_url = args.seq_url or os.getenv('SEQ_URL')
    seq_api_key = os.getenv('SEQ_API_KEY')
    influx_url = os.getenv('INFLUXDB_URL')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG')
    influx_bucket = os.getenv('INFLUXDB_BUCKET')
    house_id = os.getenv('HOMESIDE_CLIENTID', 'unknown')

    # Validate configuration
    if not seq_url:
        print("ERROR: SEQ_URL not set in .env")
        sys.exit(1)
    if not influx_url and not args.dry_run:
        print("ERROR: INFLUXDB_URL not set in .env")
        sys.exit(1)

    print("=" * 60)
    print("Seq to InfluxDB Thermal Data Migration")
    print("=" * 60)
    print(f"Seq URL: {seq_url}")
    print(f"InfluxDB URL: {influx_url or 'N/A (dry run)'}")
    print(f"Days to migrate: {args.days}")
    print(f"Dry run: {args.dry_run}")
    print()

    # Fetch events from Seq
    events = fetch_seq_events(seq_url, seq_api_key, args.days)

    if not events:
        print("No events found in Seq")
        sys.exit(0)

    # Parse events into thermal data
    data_points = []
    for event in events:
        dp = parse_seq_event(event)
        if dp:
            data_points.append(dp)

    print(f"Parsed {len(data_points)} valid thermal data points from {len(events)} events")

    if not data_points:
        print("No valid thermal data found")
        sys.exit(0)

    # Sort by timestamp
    data_points.sort(key=lambda x: x['timestamp'])

    # Show date range
    oldest = data_points[0]['timestamp']
    newest = data_points[-1]['timestamp']
    print(f"Date range: {oldest} to {newest}")

    # Write to InfluxDB
    print()
    written = write_to_influx(
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        house_id=house_id,
        data_points=data_points,
        dry_run=args.dry_run
    )

    if not args.dry_run:
        print(f"\nSuccessfully migrated {written} data points to InfluxDB")
        print("The thermal analyzer will now have historical data on next restart.")

    print("\nDone!")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Restore missing heating data from Docker logs to InfluxDB.

Parses heating data blocks from docker logs and writes them to InfluxDB
heating_system measurement.

Usage:
    python restore_from_docker_logs.py [--dry-run] [--after TIMESTAMP]
"""

import os
import re
import sys
import argparse
import subprocess
from datetime import datetime, timezone
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def get_docker_logs(container_name: str = "homeside-fetcher", from_stdin: bool = False) -> str:
    """Get all logs from the docker container or stdin."""
    if from_stdin:
        import sys
        return sys.stdin.read()

    try:
        result = subprocess.run(
            ["docker", "logs", container_name],
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.stdout + result.stderr
    except Exception as e:
        print(f"Error getting docker logs: {e}")
        return ""


def parse_heating_blocks(logs: str) -> list:
    """
    Parse heating data blocks from docker logs.

    Returns list of dictionaries with heating data.
    """
    # Pattern to match a complete heating data block
    block_pattern = re.compile(
        r'HEATING SYSTEM DATA - (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2})\s*\n'
        r'={60,}\n'
        r'((?:.*\n)*?)'
        r'(?=={60,}|HEATING SYSTEM DATA|$)',
        re.MULTILINE
    )

    data_points = []

    for match in block_pattern.finditer(logs):
        timestamp_str = match.group(1)
        block_content = match.group(2)

        try:
            # Parse timestamp
            timestamp = datetime.fromisoformat(timestamp_str)

            data = {'timestamp': timestamp}

            # Parse each line
            for line in block_content.split('\n'):
                line = line.strip()
                if ':' not in line:
                    continue

                # Split on first colon
                parts = line.split(':', 1)
                if len(parts) != 2:
                    continue

                label = parts[0].strip()
                value_str = parts[1].strip()

                # Map label to field name and parse value
                if label == 'Room Temperature':
                    data['room_temperature'] = float(value_str.replace('°C', ''))
                elif label == 'Target Setpoint':
                    data['target_temp_setpoint'] = float(value_str.replace('°C', ''))
                elif label == 'Away Mode Setpoint':
                    data['away_temp_setpoint'] = float(value_str.replace('°C', ''))
                elif label == 'Outdoor Temperature':
                    data['outdoor_temperature'] = float(value_str.replace('°C', ''))
                elif label == 'Outdoor 24h Average':
                    data['outdoor_temp_24h_avg'] = float(value_str.replace('°C', ''))
                elif label == 'Supply Temperature':
                    data['supply_temp'] = float(value_str.replace('°C', ''))
                elif label == 'Return Temperature':
                    data['return_temp'] = float(value_str.replace('°C', ''))
                elif label == 'Hot Water Temp':
                    data['hot_water_temp'] = float(value_str.replace('°C', ''))
                elif label == 'System Pressure':
                    data['system_pressure'] = float(value_str.replace(' bar', ''))
                elif label == 'Electric Heater':
                    data['electric_heater'] = value_str.lower() == 'true'
                elif label == 'Heat Recovery':
                    data['heat_recovery'] = value_str.lower() == 'true'
                elif label == 'Away Mode':
                    data['away_mode'] = value_str.lower() == 'true'

            # Only add if we have minimum required fields
            if 'room_temperature' in data and 'outdoor_temperature' in data:
                data_points.append(data)

        except (ValueError, TypeError) as e:
            print(f"Warning: Failed to parse block at {timestamp_str}: {e}")
            continue

    return data_points


def write_to_influx(
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    house_id: str,
    data_points: list,
    dry_run: bool = False
) -> int:
    """Write heating data points to InfluxDB."""

    if dry_run:
        print(f"\n[DRY RUN] Would write {len(data_points)} points to InfluxDB")
        for i, dp in enumerate(data_points[:5]):
            ts = dp['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            print(f"  {i+1}. {ts}: room={dp.get('room_temperature', 'N/A'):.1f}°C, "
                  f"outdoor={dp.get('outdoor_temperature', 'N/A'):.1f}°C, "
                  f"supply={dp.get('supply_temp', 'N/A'):.1f}°C")
        if len(data_points) > 5:
            print(f"  ... and {len(data_points) - 5} more")
        return 0

    try:
        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        written = 0
        for dp in data_points:
            point = Point("heating_system") \
                .tag("house_id", house_id) \
                .time(dp['timestamp'], WritePrecision.S)

            # Temperature fields
            if 'room_temperature' in dp:
                point.field("room_temperature", round(float(dp['room_temperature']), 2))
            if 'outdoor_temperature' in dp:
                point.field("outdoor_temperature", round(float(dp['outdoor_temperature']), 2))
            if 'outdoor_temp_24h_avg' in dp:
                point.field("outdoor_temp_24h_avg", round(float(dp['outdoor_temp_24h_avg']), 2))
            if 'supply_temp' in dp:
                point.field("supply_temp", round(float(dp['supply_temp']), 2))
            if 'return_temp' in dp:
                point.field("return_temp", round(float(dp['return_temp']), 2))
            if 'hot_water_temp' in dp:
                point.field("hot_water_temp", round(float(dp['hot_water_temp']), 2))
            if 'system_pressure' in dp:
                point.field("system_pressure", round(float(dp['system_pressure']), 2))
            if 'target_temp_setpoint' in dp:
                point.field("target_temp_setpoint", round(float(dp['target_temp_setpoint']), 2))
            if 'away_temp_setpoint' in dp:
                point.field("away_temp_setpoint", round(float(dp['away_temp_setpoint']), 2))

            # Boolean fields (as int for easier graphing)
            if 'electric_heater' in dp:
                point.field("electric_heater", 1 if dp['electric_heater'] else 0)
            if 'heat_recovery' in dp:
                point.field("heat_recovery", 1 if dp['heat_recovery'] else 0)
            if 'away_mode' in dp:
                point.field("away_mode", 1 if dp['away_mode'] else 0)

            write_api.write(bucket=influx_bucket, org=influx_org, record=point)
            written += 1

            if written % 20 == 0:
                print(f"  Written {written}/{len(data_points)} points...")

        # Also write to thermal_history for the thermal analyzer
        print("\nWriting to thermal_history measurement...")
        thermal_written = 0
        for dp in data_points:
            point = Point("thermal_history") \
                .tag("house_id", house_id) \
                .field("room_temperature", round(float(dp['room_temperature']), 2)) \
                .field("outdoor_temperature", round(float(dp['outdoor_temperature']), 2)) \
                .time(dp['timestamp'], WritePrecision.S)

            if 'supply_temp' in dp:
                point.field("supply_temp", round(float(dp['supply_temp']), 2))
            if 'electric_heater' in dp:
                point.field("electric_heater", 1 if dp['electric_heater'] else 0)
            if 'return_temp' in dp:
                point.field("return_temp", round(float(dp['return_temp']), 2))

            write_api.write(bucket=influx_bucket, org=influx_org, record=point)
            thermal_written += 1

        print(f"  Written {thermal_written} thermal_history points")

        client.close()
        return written

    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Restore heating data from Docker logs to InfluxDB'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be written without actually writing'
    )
    parser.add_argument(
        '--after', type=str, default=None,
        help='Only restore data after this timestamp (ISO format, e.g., 2026-01-26T16:31:00)'
    )
    parser.add_argument(
        '--container', type=str, default='homeside-fetcher',
        help='Docker container name (default: homeside-fetcher)'
    )
    parser.add_argument(
        '--stdin', action='store_true',
        help='Read logs from stdin instead of docker'
    )
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Get configuration
    influx_url = os.getenv('INFLUXDB_URL')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG')
    influx_bucket = os.getenv('INFLUXDB_BUCKET')
    house_id = os.getenv('HOMESIDE_CLIENTID', '')

    # Extract short form if full path provided
    if house_id and '/' in house_id:
        house_id = house_id.split('/')[-1]

    # If house_id not in env, use default
    if not house_id:
        house_id = "HEM_FJV_Villa_149"

    if not influx_url and not args.dry_run:
        print("ERROR: INFLUXDB_URL not set in .env")
        sys.exit(1)

    print("=" * 60)
    print("Restore Heating Data from Docker Logs")
    print("=" * 60)
    print(f"Container: {args.container}")
    print(f"InfluxDB URL: {influx_url or 'N/A (dry run)'}")
    print(f"House ID: {house_id}")
    if args.after:
        print(f"Only data after: {args.after}")
    print(f"Dry run: {args.dry_run}")
    print()

    # Get docker logs
    if args.stdin:
        print("Reading logs from stdin...")
    else:
        print("Fetching Docker logs...")
    logs = get_docker_logs(args.container, from_stdin=args.stdin)

    if not logs:
        print("No logs found")
        sys.exit(1)

    print(f"Got {len(logs)} bytes of logs")

    # Parse heating data blocks
    print("Parsing heating data blocks...")
    data_points = parse_heating_blocks(logs)

    print(f"Found {len(data_points)} heating data points")

    if not data_points:
        print("No heating data found in logs")
        sys.exit(0)

    # Filter by timestamp if specified
    if args.after:
        after_ts = datetime.fromisoformat(args.after)
        if after_ts.tzinfo is None:
            after_ts = after_ts.replace(tzinfo=timezone.utc)

        data_points = [dp for dp in data_points if dp['timestamp'] > after_ts]
        print(f"After filtering: {len(data_points)} data points")

    if not data_points:
        print("No data points to restore after filtering")
        sys.exit(0)

    # Sort by timestamp
    data_points.sort(key=lambda x: x['timestamp'])

    # Show date range
    oldest = data_points[0]['timestamp']
    newest = data_points[-1]['timestamp']
    print(f"Date range: {oldest} to {newest}")

    # Count fields
    all_fields = set()
    for dp in data_points:
        all_fields.update(dp.keys())
    all_fields.discard('timestamp')
    print(f"Fields found: {', '.join(sorted(all_fields))}")

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
        print(f"\nSuccessfully restored {written} data points to InfluxDB")

    print("\nDone!")


if __name__ == "__main__":
    main()

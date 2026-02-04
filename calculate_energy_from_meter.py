#!/usr/bin/env python3
"""
Calculate Energy from Meter Readings

Reads dh_energy_total (cumulative meter readings) from heating_system
and calculates hourly energy consumption by taking differences.

Usage:
    python calculate_energy_from_meter.py --house Villa_34 --start 2026-01-01 --dry-run
    python calculate_energy_from_meter.py --house Villa_34 --start 2026-01-01 --write
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def main():
    parser = argparse.ArgumentParser(description='Calculate energy from meter readings')
    parser.add_argument('--house', type=str, required=True, help='House ID (e.g., Villa_34)')
    parser.add_argument('--start', type=str, default='2026-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=None, help='End date (YYYY-MM-DD), defaults to now')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be written')
    parser.add_argument('--write', action='store_true', help='Write to InfluxDB')
    parser.add_argument('--source-field', type=str, default='dh_energy_total',
                        help='Source field name (default: dh_energy_total)')
    args = parser.parse_args()

    # Get configuration
    influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.getenv('INFLUXDB_BUCKET', 'heating')

    if not influx_token:
        # Try reading from webgui/.env
        env_path = os.path.join(os.path.dirname(__file__), 'webgui', '.env')
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    if line.startswith('INFLUXDB_TOKEN='):
                        influx_token = line.strip().split('=', 1)[1]
                        break

    if not influx_token:
        print("ERROR: INFLUXDB_TOKEN not set")
        sys.exit(1)

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Build time range
    end_clause = f', stop: {args.end}' if args.end else ''

    # Query meter readings
    print(f"Fetching {args.source_field} readings for {args.house}...")
    query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: {args.start}{end_clause})
        |> filter(fn: (r) => r["_measurement"] == "heating_system")
        |> filter(fn: (r) => r["house_id"] == "{args.house}")
        |> filter(fn: (r) => r["_field"] == "{args.source_field}")
        |> sort(columns: ["_time"])
    '''

    tables = query_api.query(query, org=influx_org)

    readings = []
    house_id_full = None
    for table in tables:
        for record in table.records:
            if house_id_full is None:
                house_id_full = record.values.get('house_id')
            readings.append({
                'time': record.get_time(),
                'value': record.get_value()
            })

    print(f"Found {len(readings)} meter readings")

    if len(readings) < 2:
        print("Not enough readings to calculate differences")
        client.close()
        return

    print(f"House ID: {house_id_full}")
    print(f"First: {readings[0]['time']} = {readings[0]['value']:.3f} MWh")
    print(f"Last:  {readings[-1]['time']} = {readings[-1]['value']:.3f} MWh")

    # Detect unit (MWh or kWh) based on values
    max_val = max(r['value'] for r in readings)
    if max_val < 1000:
        # Likely MWh, convert to kWh
        unit_multiplier = 1000
        unit_name = "MWh"
    else:
        # Already kWh
        unit_multiplier = 1
        unit_name = "kWh"

    print(f"Unit detected: {unit_name} (multiplier: {unit_multiplier})")

    # Calculate differences
    energy_points = []
    daily_totals = defaultdict(float)
    negative_count = 0
    zero_count = 0

    for i in range(len(readings) - 1):
        t1 = readings[i]['time']
        t2 = readings[i + 1]['time']
        v1 = readings[i]['value']
        v2 = readings[i + 1]['value']

        diff_kwh = (v2 - v1) * unit_multiplier

        # Handle meter rollover or reset (negative values)
        if diff_kwh < 0:
            negative_count += 1
            continue  # Skip negative differences

        if diff_kwh == 0:
            zero_count += 1

        # Skip unreasonably large values (> 100 kWh in one interval)
        if diff_kwh > 100:
            print(f"  Warning: Skipping large value {diff_kwh:.1f} kWh at {t2}")
            continue

        date_str = t2.strftime('%Y-%m-%d')
        daily_totals[date_str] += diff_kwh

        energy_points.append({
            'time': t2,
            'value': diff_kwh,
            'house_id': house_id_full
        })

    print(f"\nCalculated {len(energy_points)} energy points")
    print(f"  Zero values: {zero_count}")
    print(f"  Skipped negative: {negative_count}")

    # Show daily totals
    print(f"\nDaily energy consumption:")
    print("-" * 40)
    total_kwh = 0
    for date in sorted(daily_totals.keys()):
        kwh = daily_totals[date]
        total_kwh += kwh
        print(f"  {date}: {kwh:>6.1f} kWh")

    print("-" * 40)
    print(f"  Total: {total_kwh:>10.1f} kWh")
    print(f"  Daily avg: {total_kwh/len(daily_totals):>6.1f} kWh")

    # Write to InfluxDB
    if args.write:
        print(f"\nWriting {len(energy_points)} points to energy_consumption...")

        points = []
        for ep in energy_points:
            point = Point("energy_consumption") \
                .tag("house_id", ep['house_id']) \
                .tag("energy_type", "fjv_total") \
                .tag("source", "meter_diff") \
                .time(ep['time'], WritePrecision.S) \
                .field("value", float(ep['value']))
            points.append(point)

        # Write in batches
        batch_size = 1000
        for i in range(0, len(points), batch_size):
            batch = points[i:i+batch_size]
            write_api.write(bucket=influx_bucket, org=influx_org, record=batch)
            print(f"  Wrote batch {i//batch_size + 1} ({len(batch)} points)")

        print("Done!")

    elif args.dry_run:
        print(f"\nDry run - would write {len(energy_points)} points")
        print("Use --write to actually write to InfluxDB")

    else:
        print("\nNo action taken. Use --dry-run or --write")

    client.close()


if __name__ == '__main__':
    main()

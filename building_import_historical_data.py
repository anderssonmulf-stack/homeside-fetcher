#!/usr/bin/env python3
"""
Building Historical Data Import

Bootstraps building_system data from Arrigo history so that energy separation
can start immediately instead of waiting days for enough real-time data.

Usage:
    # Dry run - see what would be imported
    python3 building_import_historical_data.py --building TE236_HEM_Kontor --days 90 --dry-run

    # Import 90 days of history
    python3 building_import_historical_data.py --building TE236_HEM_Kontor --days 90

    # Override credentials
    ARRIGO_USERNAME="user" ARRIGO_PASSWORD="pass" \
        python3 building_import_historical_data.py --building TE236_HEM_Kontor --days 90
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from arrigo_api import ArrigoAPI, load_building_config, get_fetch_signals

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('building_import')


def main():
    parser = argparse.ArgumentParser(
        description='Import historical building data from Arrigo into InfluxDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--building', required=True,
                        help='Building ID (matches buildings/<id>.json)')
    parser.add_argument('--days', type=int, default=90,
                        help='Days of history to import (default: 90)')
    parser.add_argument('--resolution', type=int, default=3600,
                        help='Data resolution in seconds (default: 3600 = hourly)')
    parser.add_argument('--username', help='Override Arrigo username')
    parser.add_argument('--password', help='Override Arrigo password')
    parser.add_argument('--dry-run', action='store_true',
                        help='Fetch data but do not write to InfluxDB')
    parser.add_argument('--verbose', '-v', action='store_true')

    # InfluxDB overrides
    parser.add_argument('--influx-url',
                        default=os.getenv('INFLUXDB_URL', 'http://localhost:8086'))
    parser.add_argument('--influx-token', default=os.getenv('INFLUXDB_TOKEN', ''))
    parser.add_argument('--influx-org', default=os.getenv('INFLUXDB_ORG', 'homeside'))
    parser.add_argument('--influx-bucket', default=os.getenv('INFLUXDB_BUCKET', 'heating'))

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load building config
    config = load_building_config(args.building)
    if not config:
        logger.error(f"Building config not found: buildings/{args.building}.json")
        sys.exit(1)

    building_id = config['building_id']
    connection = config.get('connection', {})
    host = connection.get('host')

    if not host:
        logger.error("No host in building config")
        sys.exit(1)

    # Resolve credentials
    username = args.username or os.getenv('ARRIGO_USERNAME')
    password = args.password or os.getenv('ARRIGO_PASSWORD')

    if not username or not password:
        logger.error("Credentials required. Use --username/--password or "
                     "ARRIGO_USERNAME/ARRIGO_PASSWORD env vars")
        sys.exit(1)

    # Get signals to fetch (only those with fetch=true)
    analog_fetch, _ = get_fetch_signals(config)
    if not analog_fetch:
        logger.error("No signals configured for fetching in building config")
        sys.exit(1)

    signal_ids = [info['signal_id'] for info in analog_fetch.values()]
    signal_id_to_field = {info['signal_id']: info['field_name']
                          for info in analog_fetch.values()}

    logger.info(f"Building: {building_id}")
    logger.info(f"Host: {host}")
    logger.info(f"Signals to fetch: {len(signal_ids)}")
    logger.info(f"Period: last {args.days} days at {args.resolution}s resolution")

    # Connect to Arrigo
    client = ArrigoAPI(
        host=host,
        username=username,
        password=password,
        logger=logger,
        verbose=args.verbose,
    )

    if not client.login():
        logger.error("Authentication failed")
        sys.exit(1)

    # Fetch historical data
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=args.days)

    logger.info(f"Fetching history from {start_time.date()} to {end_time.date()}...")

    items = client.fetch_analog_history(
        signal_ids=signal_ids,
        start_time=start_time,
        end_time=end_time,
        resolution_seconds=args.resolution,
    )

    if not items:
        logger.warning("No historical data returned from Arrigo")
        sys.exit(0)

    logger.info(f"Received {len(items)} data points from Arrigo")

    # Group by timestamp for batch writes
    # Each timestamp gets one InfluxDB point with all fields
    points_by_time = defaultdict(dict)
    unknown_signals = set()

    for item in items:
        signal_id = item.get('signalId')
        value = item.get('value')
        time_str = item.get('time')

        if signal_id not in signal_id_to_field:
            unknown_signals.add(signal_id)
            continue

        if value is None or time_str is None:
            continue

        field_name = signal_id_to_field[signal_id]
        points_by_time[time_str][field_name] = value

    if unknown_signals:
        logger.warning(f"Skipped {len(unknown_signals)} unknown signal IDs")

    logger.info(f"Grouped into {len(points_by_time)} unique timestamps")

    if args.dry_run:
        # Show sample of what would be imported
        sample_times = sorted(points_by_time.keys())[:5]
        logger.info("DRY RUN - Sample data:")
        for ts in sample_times:
            fields = points_by_time[ts]
            field_summary = ", ".join(f"{k}={v:.2f}" for k, v in list(fields.items())[:4])
            logger.info(f"  {ts}: {field_summary} ({len(fields)} fields)")

        if len(points_by_time) > 5:
            logger.info(f"  ... and {len(points_by_time) - 5} more timestamps")

        logger.info(f"Would write {len(points_by_time)} points to "
                     f"building_system (building_id={building_id})")
        return

    # Write to InfluxDB
    if not INFLUX_AVAILABLE:
        logger.error("influxdb_client not installed")
        sys.exit(1)

    if not args.influx_token:
        logger.error("INFLUXDB_TOKEN not set")
        sys.exit(1)

    influx_client = InfluxDBClient(
        url=args.influx_url,
        token=args.influx_token,
        org=args.influx_org,
        timeout=30_000,
    )
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    # Batch write in chunks of 500
    BATCH_SIZE = 500
    points = []
    written = 0

    for time_str, fields in sorted(points_by_time.items()):
        try:
            ts = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except ValueError:
            continue

        point = Point("building_system") \
            .tag("building_id", building_id) \
            .time(ts, WritePrecision.S)

        for field_name, value in fields.items():
            if isinstance(value, (int, float)):
                point.field(field_name, round(float(value), 4))

        points.append(point)

        if len(points) >= BATCH_SIZE:
            write_api.write(bucket=args.influx_bucket, org=args.influx_org,
                            record=points)
            written += len(points)
            logger.info(f"  Written {written}/{len(points_by_time)} points...")
            points = []

    # Write remaining
    if points:
        write_api.write(bucket=args.influx_bucket, org=args.influx_org,
                        record=points)
        written += len(points)

    influx_client.close()

    logger.info(f"Import complete: {written} points written to "
                f"building_system (building_id={building_id})")


if __name__ == '__main__':
    main()

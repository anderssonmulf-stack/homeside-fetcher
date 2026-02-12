#!/usr/bin/env python3
"""
Commercial Building Fetcher

Polls Arrigo BMS API on a fixed schedule and writes selected signals to InfluxDB.
Reads building config from buildings/<building_id>.json to know which signals to fetch.

Usage:
    # Run with building config
    python3 building_fetcher.py --building TE236_HEM_Kontor

    # Override credentials via env vars
    ARRIGO_USERNAME="Ulf Andersson" ARRIGO_PASSWORD="xxx" \
        python3 building_fetcher.py --building TE236_HEM_Kontor

    # Single fetch (no loop)
    python3 building_fetcher.py --building TE236_HEM_Kontor --once

    # Dry run (fetch but don't write to InfluxDB)
    python3 building_fetcher.py --building TE236_HEM_Kontor --once --dry-run
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta

from arrigo_api import ArrigoAPI, load_building_config, get_fetch_signals

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False

try:
    from seq_logger import SeqLogger
    SEQ_AVAILABLE = True
except ImportError:
    SEQ_AVAILABLE = False


class BuildingInfluxWriter:
    """Writes commercial building data to InfluxDB."""

    def __init__(self, url: str, token: str, org: str, bucket: str,
                 building_id: str, logger=None):
        self.building_id = building_id
        self.logger = logger or logging.getLogger(__name__)
        self.bucket = bucket
        self.org = org

        if not INFLUX_AVAILABLE:
            self.logger.warning("influxdb_client not available, writes disabled")
            self.client = None
            self.write_api = None
            return

        try:
            self.client = InfluxDBClient(url=url, token=token, org=org)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.logger.info(f"InfluxDB connected: {url}")
        except Exception as e:
            self.logger.error(f"InfluxDB connection failed: {e}")
            self.client = None
            self.write_api = None

    def write_analog_signals(self, values: dict, timestamp: datetime = None) -> bool:
        """
        Write analog signal values to InfluxDB.

        Args:
            values: Dict of field_name -> value
            timestamp: Optional timestamp (default: now)

        Returns:
            True if write successful
        """
        if not self.write_api:
            return False

        if not timestamp:
            timestamp = datetime.now(timezone.utc)

        try:
            point = Point("building_system") \
                .tag("building_id", self.building_id) \
                .time(timestamp, WritePrecision.S)

            for field_name, value in values.items():
                if value is not None and isinstance(value, (int, float)):
                    point.field(field_name, round(float(value), 4))

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"InfluxDB write failed: {e}")
            return False

    def write_alarms(self, alarms: list, timestamp: datetime = None) -> bool:
        """Write alarm snapshot to InfluxDB."""
        if not self.write_api or not alarms:
            return False

        if not timestamp:
            timestamp = datetime.now(timezone.utc)

        try:
            # Summary point
            by_status = {}
            for a in alarms:
                status = a.get('status', 'UNKNOWN')
                by_status[status] = by_status.get(status, 0) + 1

            point = Point("building_alarms") \
                .tag("building_id", self.building_id) \
                .field("total_count", len(alarms)) \
                .time(timestamp, WritePrecision.S)

            for status, count in by_status.items():
                point.field(f"count_{status.lower()}", count)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"InfluxDB alarm write failed: {e}")
            return False

    def close(self):
        if self.client:
            self.client.close()


def fetch_and_write(client: ArrigoAPI, analog_fetch: dict,
                    influx: BuildingInfluxWriter, config: dict,
                    logger, dry_run: bool = False) -> dict:
    """
    Single fetch iteration: get current values from Arrigo, write to InfluxDB.

    Args:
        client: Authenticated ArrigoAPI instance
        analog_fetch: Dict of signal_name -> {signal_id, field_name, ...}
        influx: InfluxDB writer (or None for dry run)
        config: Building config dict
        logger: Logger instance
        dry_run: If True, skip InfluxDB writes

    Returns:
        Dict of fetched values if successful, empty dict on failure
    """
    timestamp = datetime.now(timezone.utc)

    # Fetch current analog values
    if not client.discover_signals():
        logger.warning("Failed to discover signals")
        return {}

    # Map fetched values to field names
    values = {}
    missing = []
    for signal_name, fetch_info in analog_fetch.items():
        signal_id = fetch_info['signal_id']
        field_name = fetch_info['field_name']

        if signal_id in client.signal_map:
            raw_value = client.signal_map[signal_id].get('current_value')
            if raw_value is not None:
                values[field_name] = raw_value
            else:
                missing.append(field_name)
        else:
            missing.append(field_name)

    if not values:
        logger.error("No values fetched from API")
        return {}

    if missing and len(missing) <= 10:
        logger.debug(f"Missing signals: {missing}")

    # Dry run - just return values without writing
    if dry_run:
        return values

    if influx:
        success = influx.write_analog_signals(values, timestamp)
        if not success:
            logger.error("Failed to write to InfluxDB")
            return {}

    # Fetch and write alarms if enabled
    alarm_config = config.get('alarm_monitoring', {})
    if alarm_config.get('enabled') and influx:
        try:
            alarms = client.get_alarms(first=100)
            if alarms:
                influx.write_alarms(alarms, timestamp)
                active = sum(1 for a in alarms if a.get('status') == 'ALARMED')
                if active > 0:
                    logger.warning(f"{active} active alarms")
        except Exception as e:
            logger.warning(f"Alarm fetch failed: {e}")

    return values


def calculate_sleep(interval_minutes: int) -> float:
    """
    Calculate seconds to sleep to align with clock boundaries.
    E.g., with 15-min interval, aligns to :00, :15, :30, :45.
    """
    now = datetime.now()
    minutes_past = now.minute % interval_minutes
    seconds_past = minutes_past * 60 + now.second + now.microsecond / 1_000_000
    sleep_seconds = (interval_minutes * 60) - seconds_past

    # Avoid running twice in quick succession
    if sleep_seconds < 10:
        sleep_seconds += interval_minutes * 60

    return sleep_seconds


def main():
    parser = argparse.ArgumentParser(
        description='Commercial building data fetcher',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--building', required=True,
                        help='Building ID (matches buildings/<id>.json)')
    parser.add_argument('--username', help='Override Arrigo username (or ARRIGO_USERNAME env)')
    parser.add_argument('--password', help='Override Arrigo password (or ARRIGO_PASSWORD env)')
    parser.add_argument('--once', action='store_true', help='Fetch once and exit (no loop)')
    parser.add_argument('--dry-run', action='store_true', help='Fetch but do not write to InfluxDB')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')

    # InfluxDB overrides
    parser.add_argument('--influx-url', default=os.getenv('INFLUXDB_URL', 'http://localhost:8086'))
    parser.add_argument('--influx-token', default=os.getenv('INFLUXDB_TOKEN', ''))
    parser.add_argument('--influx-org', default=os.getenv('INFLUXDB_ORG', 'homeside'))
    parser.add_argument('--influx-bucket', default=os.getenv('INFLUXDB_BUCKET', 'heating'))

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logger = logging.getLogger('building_fetcher')

    # Load building config
    config = load_building_config(args.building)
    if not config:
        logger.error(f"Building config not found: {args.building}")
        sys.exit(1)

    building_id = config['building_id']
    friendly_name = config.get('friendly_name') or building_id
    connection = config.get('connection', {})
    host = connection.get('host')
    interval_minutes = config.get('poll_interval_minutes', 15)

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

    # Get fetch signal map
    analog_fetch, digital_fetch = get_fetch_signals(config)
    if not analog_fetch:
        logger.error("No signals configured for fetching")
        sys.exit(1)

    # Initialize Seq structured logging
    seq = None
    if SEQ_AVAILABLE:
        seq = SeqLogger(
            client_id=building_id,
            friendly_name=friendly_name,
            username=username,
            seq_url=os.getenv('SEQ_URL'),
            seq_api_key=os.getenv('SEQ_API_KEY'),
            component='BuildingFetcher',
            display_name_source='friendly_name',
        )

    logger.info(f"Building Fetcher: {friendly_name} | host={host} | "
                f"signals={len(analog_fetch)} analog, {len(digital_fetch)} digital | "
                f"interval={interval_minutes}min")

    # Initialize Arrigo API client
    client = ArrigoAPI(
        host=host,
        username=username,
        password=password,
        logger=logger,
        verbose=args.verbose,
    )

    # Authenticate
    if not client.login():
        logger.error("Authentication failed")
        if seq:
            seq.log_error("Authentication failed", properties={
                'EventType': 'AuthFailed', 'Host': host})
        sys.exit(1)

    # Initialize InfluxDB writer
    influx = None
    if not args.dry_run:
        influx = BuildingInfluxWriter(
            url=args.influx_url,
            token=args.influx_token,
            org=args.influx_org,
            bucket=args.influx_bucket,
            building_id=building_id,
            logger=logger,
        )

    # ── Single fetch mode ────────────────────────────────────────
    if args.once:
        values = fetch_and_write(client, analog_fetch, influx, config,
                                 logger, dry_run=args.dry_run)
        if influx:
            influx.close()
        sys.exit(0 if values else 1)

    # ── Continuous loop ──────────────────────────────────────────
    if seq and seq.enabled:
        seq.log(
            f"[{friendly_name}] Building fetcher started | "
            f"{len(analog_fetch)} signals | {interval_minutes}min interval",
            level='Information',
            properties={
                'EventType': 'FetcherStarted',
                'Host': host,
                'AnalogSignals': len(analog_fetch),
                'DigitalSignals': len(digital_fetch),
                'IntervalMinutes': interval_minutes,
            }
        )

    iteration = 0
    consecutive_failures = 0
    first_failure_time = None

    try:
        while True:
            iteration += 1
            now = datetime.now(timezone.utc)

            try:
                values = fetch_and_write(client, analog_fetch, influx,
                                         config, logger)

                if values:
                    consecutive_failures = 0
                    first_failure_time = None

                    # Build Seq properties from fetched values
                    props = {
                        'EventType': 'BuildingDataCollected',
                        'Iteration': iteration,
                        'SignalCount': len(values),
                        'TotalConfigured': len(analog_fetch),
                    }
                    # Include key values as structured properties
                    for field_name, value in values.items():
                        if isinstance(value, (int, float)):
                            pascal_key = ''.join(
                                word.capitalize() for word in field_name.split('_'))
                            props[pascal_key] = round(float(value), 2)

                    # Build concise message with key readings
                    msg_parts = [f"#{iteration}"]
                    key_readings = {
                        'outdoor_temp_fvc': ('Out', '°C'),
                        'dh_power_total': ('DH', 'kW'),
                        'dh_primary_supply': ('Sup', '°C'),
                        'dh_primary_return': ('Ret', '°C'),
                    }
                    for field, (label, unit) in key_readings.items():
                        if field in values:
                            msg_parts.append(f"{label}={values[field]:.1f}{unit}")

                    msg_parts.append(f"{len(values)}/{len(analog_fetch)} signals")

                    if seq:
                        seq.log(
                            f"[{friendly_name}] {' | '.join(msg_parts)}",
                            level='Information',
                            properties=props,
                        )

                else:
                    consecutive_failures += 1
                    if first_failure_time is None:
                        first_failure_time = now

                    # Try re-authenticating on failure
                    if consecutive_failures >= 2:
                        logger.warning("Multiple failures, re-authenticating...")
                        client.login()

                    # Escalate to error after 2 hours of failures
                    failure_duration = (now - first_failure_time).total_seconds() / 60
                    if failure_duration > 120:
                        if seq:
                            seq.log_error(
                                f"Persistent failure for {failure_duration:.0f} min",
                                properties={
                                    'EventType': 'PersistentFailure',
                                    'FailureMinutes': round(failure_duration),
                                    'ConsecutiveFailures': consecutive_failures,
                                })
                    else:
                        if seq:
                            seq.log_warning(
                                f"Fetch failed (attempt {consecutive_failures})",
                                properties={
                                    'EventType': 'FetchFailed',
                                    'ConsecutiveFailures': consecutive_failures,
                                })

            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Unexpected error in fetch loop: {e}",
                             exc_info=args.verbose)
                if seq:
                    seq.log_error("Unexpected error in fetch loop",
                                  error=e,
                                  properties={'EventType': 'FetchLoopError'})

            # Sleep until next aligned interval
            sleep_seconds = calculate_sleep(interval_minutes)
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        logger.info(f"Stopping {friendly_name} fetcher...")
    finally:
        if influx:
            influx.close()


if __name__ == "__main__":
    main()

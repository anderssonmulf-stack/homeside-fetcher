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

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from arrigo_api import load_building_config, get_fetch_signals

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
    """Writes commercial building data to InfluxDB (with circuit breaker)."""

    def __init__(self, url: str, token: str, org: str, bucket: str,
                 building_id: str, logger=None, settings: dict = None):
        self.building_id = building_id
        self.logger = logger or logging.getLogger(__name__)
        self.bucket = bucket
        self.org = org

        # Circuit breaker settings (from settings.json "influxdb" section)
        influx_settings = settings or {}
        self._write_timeout_ms = influx_settings.get('write_timeout_ms', 5000)
        self._circuit_breaker_threshold = influx_settings.get('circuit_breaker_threshold', 3)
        self._circuit_cooldown = influx_settings.get('circuit_breaker_cooldown_seconds', 60)
        self._consecutive_failures = 0
        self._circuit_open_time = None

        # Store connection params for reconnect
        self._url = url
        self._token = token
        self._org = org

        if not INFLUX_AVAILABLE:
            self.logger.warning("influxdb_client not available, writes disabled")
            self.client = None
            self.write_api = None
            return

        try:
            self.client = InfluxDBClient(url=url, token=token, org=org,
                                         timeout=self._write_timeout_ms)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.logger.info(f"InfluxDB connected: {url}")
        except Exception as e:
            self.logger.error(f"InfluxDB connection failed: {e}")
            self.client = None
            self.write_api = None

    def _should_write(self) -> bool:
        """Circuit breaker guard — returns False to skip writes when InfluxDB is down."""
        if not self.write_api:
            return False

        if self._consecutive_failures < self._circuit_breaker_threshold:
            return True

        if self._circuit_open_time is None:
            self._circuit_open_time = time.monotonic()

        elapsed = time.monotonic() - self._circuit_open_time
        if elapsed < self._circuit_cooldown:
            return False

        # Cooldown elapsed — attempt reconnect
        if self._reconnect():
            return True
        self._circuit_open_time = time.monotonic()
        return False

    def _reconnect(self) -> bool:
        """Close old client, create fresh one, health check."""
        self.logger.info("Attempting InfluxDB reconnect...")
        try:
            if self.client:
                try:
                    self.client.close()
                except Exception:
                    pass
            self.client = InfluxDBClient(
                url=self._url, token=self._token, org=self._org,
                timeout=self._write_timeout_ms
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            health = self.client.health()
            if health.status == "pass":
                self._consecutive_failures = 0
                self._circuit_open_time = None
                self.logger.info("InfluxDB reconnected successfully")
                return True
            self.logger.warning(f"InfluxDB reconnect health check failed: {health.status}")
            return False
        except Exception as e:
            self.logger.warning(f"InfluxDB reconnect failed: {e}")
            return False

    def write_analog_signals(self, values: dict, timestamp: datetime = None) -> bool:
        """
        Write analog signal values to InfluxDB.

        Args:
            values: Dict of field_name -> value
            timestamp: Optional timestamp (default: now)

        Returns:
            True if write successful
        """
        if not self._should_write():
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
            self._consecutive_failures = 0
            self._circuit_open_time = None
            return True

        except Exception as e:
            self._consecutive_failures += 1
            if self._consecutive_failures == self._circuit_breaker_threshold:
                self._circuit_open_time = time.monotonic()
                self.logger.warning(
                    f"Circuit breaker OPEN after {self._consecutive_failures} failures — "
                    f"skipping writes for {self._circuit_cooldown}s"
                )
            self.logger.error(f"InfluxDB write failed: {e}")
            return False

    def write_alarms(self, alarms: list, timestamp: datetime = None) -> bool:
        """Write alarm snapshot to InfluxDB."""
        if not self._should_write() or not alarms:
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
            self._consecutive_failures = 0
            self._circuit_open_time = None
            return True

        except Exception as e:
            self._consecutive_failures += 1
            if self._consecutive_failures == self._circuit_breaker_threshold:
                self._circuit_open_time = time.monotonic()
                self.logger.warning(
                    f"Circuit breaker OPEN after {self._consecutive_failures} failures — "
                    f"skipping writes for {self._circuit_cooldown}s"
                )
            self.logger.error(f"InfluxDB alarm write failed: {e}")
            return False

    def close(self):
        if self.client:
            self.client.close()


def fetch_and_write(client, analog_fetch: dict,
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


def calculate_sleep(interval_minutes: int, poll_offset: int = 0) -> float:
    """
    Calculate seconds to sleep to align with clock boundaries + per-process offset.
    E.g., with 15-min interval, aligns to :00, :15, :30, :45 plus offset seconds.
    """
    now = datetime.now()
    minutes_past = now.minute % interval_minutes
    seconds_past = minutes_past * 60 + now.second + now.microsecond / 1_000_000
    sleep_seconds = (interval_minutes * 60) - seconds_past + poll_offset

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
    system = connection.get('system', 'arrigo')
    interval_minutes = config.get('poll_interval_minutes', 5)

    # Resolve credentials: CLI args > BMS_* env > credential_ref > BUILDING_<id>_* > ARRIGO_*
    username = args.username or os.getenv('BMS_USERNAME')
    password = args.password or os.getenv('BMS_PASSWORD')

    if not username or not password:
        # Try credential_ref from config (named shared credentials)
        credential_ref = connection.get('credential_ref')
        if credential_ref:
            username = username or os.getenv(f'{credential_ref}_USERNAME')
            password = password or os.getenv(f'{credential_ref}_PASSWORD')

    if not username or not password:
        # Try legacy per-building credentials
        username = username or os.getenv(f'BUILDING_{building_id}_USERNAME')
        password = password or os.getenv(f'BUILDING_{building_id}_PASSWORD')

    if not username or not password:
        # Try legacy ARRIGO_* fallback
        username = username or os.getenv('ARRIGO_USERNAME')
        password = password or os.getenv('ARRIGO_PASSWORD')

    if not username or not password:
        logger.error("Credentials required. Use --username/--password, "
                     "BMS_USERNAME/BMS_PASSWORD env vars, or credential_ref in config")
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

    # Initialize BMS client based on system type
    if system == 'ebo':
        from ebo_adapter import EboBmsAdapter
        base_url = connection.get('base_url')
        domain = connection.get('domain', '')
        if not domain and connection.get('credential_ref'):
            domain = os.getenv(f'{connection["credential_ref"]}_DOMAIN', '')
        if not base_url:
            logger.error("No base_url in building config connection block")
            sys.exit(1)
        client = EboBmsAdapter(
            base_url=base_url,
            username=username,
            password=password,
            domain=domain,
            logger=logger,
            verbose=args.verbose,
        )
        # Configure subscription paths from signal IDs (which are EBO paths)
        signal_paths = [info['signal_id'] for info in analog_fetch.values()]
        client.configure_signals(signal_paths)
        bms_label = base_url
    else:
        from arrigo_api import ArrigoAPI
        host = connection.get('host')
        if not host:
            logger.error("No host in building config")
            sys.exit(1)
        client = ArrigoAPI(
            host=host,
            username=username,
            password=password,
            logger=logger,
            verbose=args.verbose,
        )
        bms_label = host

    logger.info(f"Building Fetcher: {friendly_name} | {system}={bms_label} | "
                f"signals={len(analog_fetch)} analog, {len(digital_fetch)} digital | "
                f"interval={interval_minutes}min")

    # Authenticate
    if not client.login():
        logger.error("Authentication failed")
        if seq:
            seq.log_error("Authentication failed", properties={
                'EventType': 'AuthFailed', 'System': system, 'Host': bms_label})
        sys.exit(1)

    # Load settings for InfluxDB circuit breaker config
    influx_settings = {}
    for settings_path in ['settings.json', '/app/settings.json']:
        if os.path.exists(settings_path):
            try:
                with open(settings_path) as f:
                    influx_settings = json.load(f).get('influxdb', {})
                break
            except Exception:
                pass

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
            settings=influx_settings,
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
                'Host': bms_label,
                'AnalogSignals': len(analog_fetch),
                'DigitalSignals': len(digital_fetch),
                'IntervalMinutes': interval_minutes,
            }
        )

    # Stagger startup so processes don't all hit InfluxDB at the same time
    poll_offset = int(os.getenv('POLL_OFFSET_SECONDS', '0'))
    if poll_offset > 0:
        logger.info(f"Startup delay: {poll_offset}s (staggered poll offset)")
        time.sleep(poll_offset)

    iteration = 0
    consecutive_failures = 0
    first_failure_time = None

    # Energy separation pipeline tracking
    from zoneinfo import ZoneInfo
    SWEDISH_TZ = ZoneInfo('Europe/Stockholm')
    last_pipeline_date = None  # Swedish date string of last energy separation run
    last_recalibration_time = None  # datetime of last k recalibration

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

            # ── Energy separation pipeline triggers ──────────────────
            now_swedish = datetime.now(SWEDISH_TZ)
            today_str = now_swedish.strftime('%Y-%m-%d')
            current_hour = now_swedish.hour

            # Daily at 08:00 Swedish time: run energy separation
            if (current_hour >= 8 and last_pipeline_date != today_str
                    and consecutive_failures == 0):
                last_pipeline_date = today_str
                logger.info("Running daily energy separation pipeline...")
                from heating_energy_calibrator import run_energy_separation
                run_energy_separation(
                    entity_id=building_id, entity_type="building",
                    influx_url=args.influx_url, influx_token=args.influx_token,
                    influx_org=args.influx_org, influx_bucket=args.influx_bucket,
                    config=config, logger=logger, seq=seq,
                )

            # Every 72 hours: run k-value recalibration
            if last_recalibration_time is None:
                last_recalibration_time = now  # Don't run on first iteration
            elif (now - last_recalibration_time).total_seconds() >= 72 * 3600:
                last_recalibration_time = now
                logger.info("Running 72h k-value recalibration...")
                from k_recalibrator import recalibrate_entity
                result = recalibrate_entity(
                    entity_id=building_id, entity_type="building",
                    influx_url=args.influx_url, influx_token=args.influx_token,
                    influx_org=args.influx_org, influx_bucket=args.influx_bucket,
                    days=30,
                )
                if result:
                    logger.info(f"K recalibration: k={result.k_value:.4f} ({result.days_used} days, {result.confidence:.0%})")
                    if seq:
                        seq.log(
                            f"[{building_id}] K recalibrated: k={result.k_value:.4f}",
                            level='Information',
                            properties={'EventType': 'KRecalibration', 'KValue': round(result.k_value, 4),
                                        'DaysUsed': result.days_used, 'Confidence': round(result.confidence, 2)},
                        )
                else:
                    logger.info("K recalibration: insufficient data")

            # Re-read interval from building config (live reload — no restart needed)
            refreshed_config = load_building_config(args.building)
            if refreshed_config:
                new_interval = refreshed_config.get('poll_interval_minutes', 5)
                if new_interval != interval_minutes:
                    logger.info(f"Poll interval changed: {interval_minutes} → {new_interval} min")
                    interval_minutes = new_interval
                config = refreshed_config  # Also refresh config for energy separation

            # Sleep until next aligned interval + per-process offset
            sleep_seconds = calculate_sleep(interval_minutes, poll_offset)
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        logger.info(f"Stopping {friendly_name} fetcher...")
    finally:
        if influx:
            influx.close()


if __name__ == "__main__":
    main()

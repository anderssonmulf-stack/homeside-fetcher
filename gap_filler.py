#!/usr/bin/env python3
"""
Gap Filler Module

Detects and fills gaps in InfluxDB heating data by fetching from Arrigo API.
Can be used:
1. As a standalone script to fill specific time ranges
2. From fetcher startup to automatically fill gaps

Usage:
    # Fill gap from midnight today until now
    python3 gap_filler.py --username FC... --password "..." --from-midnight

    # Fill specific time range
    python3 gap_filler.py --username FC... --password "..." --start "2026-02-02 00:00" --end "2026-02-02 08:00"

    # Dry run - see what would be filled
    python3 gap_filler.py --username FC... --password "..." --from-midnight --dry-run
"""

import json
import os
import sys
import argparse
import statistics
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False

from import_historical_data import ArrigoHistoricalClient
from smhi_weather import SMHIWeather

# Swedish timezone
SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


class DummyLogger:
    """Minimal logger for standalone script use."""
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass
    def debug(self, msg): pass


class GapDetector:
    """Detects gaps in InfluxDB data."""

    def __init__(self, url: str, token: str, org: str, bucket: str, house_id: str):
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=5_000)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        self.org = org
        self.house_id = house_id
        # Escape forward slashes for Flux regex
        # house_id is now short format (no slashes), no escaping needed
        self.escaped_house_id = house_id

    def find_gaps(
        self,
        start_time: datetime,
        end_time: datetime,
        expected_interval_minutes: int = 15,
        measurement: str = "heating_system"
    ) -> List[Tuple[datetime, datetime]]:
        """
        Find gaps in data where no points exist for longer than expected interval.

        Args:
            start_time: Start of time range to check
            end_time: End of time range to check
            expected_interval_minutes: Expected data interval (gaps > 2x this are flagged)
            measurement: InfluxDB measurement to check

        Returns:
            List of (gap_start, gap_end) tuples
        """
        # Query all timestamps in the range - just get one field to reduce data
        query = f'''
            from(bucket: "{self.bucket}")
            |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["house_id"] == "{self.house_id}")
            |> filter(fn: (r) => r["_field"] == "room_temperature" or r["_field"] == "outdoor_temperature")
            |> keep(columns: ["_time"])
        '''

        try:
            tables = self.query_api.query(query, org=self.org)

            timestamps = []
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    if ts:
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        timestamps.append(ts)

            # Sort and deduplicate
            timestamps = sorted(set(timestamps))

            if not timestamps:
                # No data at all - entire range is a gap
                return [(start_time, end_time)]

            gaps = []
            gap_threshold = timedelta(minutes=expected_interval_minutes * 2)

            # Check gap at start
            if timestamps[0] - start_time > gap_threshold:
                gaps.append((start_time, timestamps[0]))

            # Check gaps between points
            for i in range(1, len(timestamps)):
                delta = timestamps[i] - timestamps[i-1]
                if delta > gap_threshold:
                    gaps.append((timestamps[i-1], timestamps[i]))

            # Check gap at end
            if end_time - timestamps[-1] > gap_threshold:
                gaps.append((timestamps[-1], end_time))

            return gaps

        except Exception as e:
            print(f"Error detecting gaps: {e}")
            return []

    def get_existing_data(
        self,
        start_time: datetime,
        end_time: datetime,
        measurement: str = "heating_system"
    ) -> Dict[datetime, Dict[str, float]]:
        """
        Get existing data points in a time range.

        Returns:
            Dict mapping timestamp -> {field: value}
        """
        query = f'''
            from(bucket: "{self.bucket}")
            |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["house_id"] == "{self.house_id}")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        try:
            tables = self.query_api.query(query, org=self.org)

            data = {}
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    if ts:
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)

                        if ts not in data:
                            data[ts] = {}

                        # Get all field values
                        for key, value in record.values.items():
                            if key.startswith('_') or key in ['result', 'table', 'house_id']:
                                continue
                            if value is not None:
                                data[ts][key] = value

            return data

        except Exception as e:
            print(f"Error getting existing data: {e}")
            return {}

    def close(self):
        self.client.close()


class GapFiller:
    """Fills gaps in InfluxDB data using Arrigo API."""

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        house_id: str,
        arrigo_username: str,
        arrigo_password: str,
        arrigo_host: str = None,
        verbose: bool = False
    ):
        self.influx_url = influx_url
        self.influx_token = influx_token
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.house_id = house_id

        self.arrigo_username = arrigo_username
        self.arrigo_password = arrigo_password
        self.arrigo_host = arrigo_host
        self.verbose = verbose

        self.detector = None
        self.arrigo_client = None
        self.write_client = None

    def log(self, message: str):
        if self.verbose:
            print(f"  [DEBUG] {message}")

    def _init_detector(self):
        if not self.detector:
            self.detector = GapDetector(
                url=self.influx_url,
                token=self.influx_token,
                org=self.influx_org,
                bucket=self.influx_bucket,
                house_id=self.house_id
            )

    def _init_arrigo(self) -> bool:
        if self.arrigo_client:
            return True

        self.arrigo_client = ArrigoHistoricalClient(
            username=self.arrigo_username,
            password=self.arrigo_password,
            arrigo_host=self.arrigo_host,
            verbose=self.verbose
        )

        if not self.arrigo_client.authenticate():
            print("Failed to authenticate with Arrigo")
            return False

        if not self.arrigo_client.select_house(0):
            print("Failed to select house in Arrigo")
            return False

        if not self.arrigo_client.login_to_arrigo():
            print("Failed to login to Arrigo server")
            return False

        if not self.arrigo_client.discover_signals():
            print("Warning: Failed to discover signals")

        return True

    def _init_write_client(self):
        if not self.write_client:
            self.write_client = InfluxDBClient(
                url=self.influx_url,
                token=self.influx_token,
                org=self.influx_org,
                timeout=5_000
            )

    def detect_gaps(
        self,
        start_time: datetime,
        end_time: datetime,
        expected_interval_minutes: int = 15
    ) -> List[Tuple[datetime, datetime]]:
        """Detect gaps in the specified time range."""
        self._init_detector()
        return self.detector.find_gaps(start_time, end_time, expected_interval_minutes)

    def fill_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        dry_run: bool = False,
        skip_existing: bool = True
    ) -> Tuple[int, int, int]:
        """
        Fill data for a specific time range.

        Args:
            start_time: Start of range to fill
            end_time: End of range to fill
            dry_run: If True, don't write to InfluxDB
            skip_existing: If True, skip timestamps that have non-zero data

        Returns:
            Tuple of (written_count, skipped_count, error_count)
        """
        print(f"\nFilling data from {start_time} to {end_time}")

        # Initialize clients
        self._init_detector()
        if not self._init_arrigo():
            return 0, 0, 1
        self._init_write_client()

        # Get existing data to check for duplicates
        existing_data = {}
        if skip_existing:
            print("  Checking for existing data...")
            existing_data = self.detector.get_existing_data(start_time, end_time)
            print(f"  Found {len(existing_data)} existing data points")

        # Calculate days for Arrigo query
        days_back = (datetime.now(timezone.utc) - start_time).days + 1

        # Fetch from Arrigo
        print(f"  Fetching from Arrigo (last {days_back} days)...")

        # We need to fetch with appropriate time range
        arrigo_end = end_time
        arrigo_start = start_time

        # Use the _fetch_via_graphql directly with our time range
        # Arrigo stores data at 60-minute resolution, not 15
        raw_data = self.arrigo_client._fetch_via_graphql(
            arrigo_start,
            arrigo_end,
            resolution_minutes=60  # Arrigo stores hourly data
        )

        if not raw_data:
            print("  Warning: No data retrieved from Arrigo")
            return 0, 0, 0

        print(f"  Retrieved {len(raw_data)} data points from Arrigo")

        # Group by timestamp
        data_by_time = {}
        for point in raw_data:
            ts = point['timestamp']
            if ts not in data_by_time:
                data_by_time[ts] = {}
            data_by_time[ts][point['field']] = point['value']

        # Filter to only include points in our target range
        filtered_data = {}
        for ts, fields in data_by_time.items():
            if start_time <= ts <= end_time:
                filtered_data[ts] = fields

        print(f"  {len(filtered_data)} points within target range")

        # Write to InfluxDB, avoiding duplicates
        written = 0
        skipped = 0
        errors = 0

        write_api = self.write_client.write_api(write_options=SYNCHRONOUS)

        for ts, fields in sorted(filtered_data.items()):
            # Check if we should skip this timestamp
            if skip_existing and ts in existing_data:
                existing = existing_data[ts]
                # Skip if existing data has non-zero values for core fields
                has_real_data = any(
                    existing.get(f, 0) != 0
                    for f in ['room_temperature', 'outdoor_temperature', 'supply_temp']
                    if f in existing
                )
                if has_real_data:
                    self.log(f"Skipping {ts} - has existing non-zero data")
                    skipped += 1
                    continue

            # Check we have required fields
            if 'room_temperature' not in fields or 'outdoor_temperature' not in fields:
                self.log(f"Skipping {ts} - missing core fields")
                skipped += 1
                continue

            try:
                if not dry_run:
                    # Write to thermal_history
                    thermal_point = Point("thermal_history") \
                        .tag("house_id", self.house_id) \
                        .field("room_temperature", round(float(fields['room_temperature']), 2)) \
                        .field("outdoor_temperature", round(float(fields['outdoor_temperature']), 2)) \
                        .time(ts, WritePrecision.S)

                    if 'supply_temp' in fields:
                        thermal_point.field("supply_temp", round(float(fields['supply_temp']), 2))
                    if 'return_temp' in fields:
                        thermal_point.field("return_temp", round(float(fields['return_temp']), 2))

                    write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=thermal_point)

                    # Write to heating_system
                    heating_point = Point("heating_system") \
                        .tag("house_id", self.house_id) \
                        .time(ts, WritePrecision.S)

                    for field, value in fields.items():
                        heating_point.field(field, round(float(value), 2))

                    write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=heating_point)

                written += 1

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error writing {ts}: {e}")

        action = "Would write" if dry_run else "Wrote"
        print(f"\n  {action}: {written}, Skipped: {skipped}, Errors: {errors}")

        return written, skipped, errors

    def fill_gaps_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        expected_interval_minutes: int = 15,
        dry_run: bool = False
    ) -> Tuple[int, int, int]:
        """
        Detect and fill all gaps in a time range.

        Returns:
            Tuple of (total_written, total_skipped, total_errors)
        """
        print(f"\nDetecting gaps from {start_time} to {end_time}...")

        gaps = self.detect_gaps(start_time, end_time, expected_interval_minutes)

        if not gaps:
            print("  No gaps detected")
            return 0, 0, 0

        print(f"  Found {len(gaps)} gap(s):")
        for gap_start, gap_end in gaps:
            duration = (gap_end - gap_start).total_seconds() / 60
            print(f"    {gap_start} to {gap_end} ({duration:.0f} minutes)")

        total_written = 0
        total_skipped = 0
        total_errors = 0

        for gap_start, gap_end in gaps:
            w, s, e = self.fill_time_range(gap_start, gap_end, dry_run=dry_run)
            total_written += w
            total_skipped += s
            total_errors += e

        return total_written, total_skipped, total_errors

    def close(self):
        if self.detector:
            self.detector.close()
        if self.write_client:
            self.write_client.close()


class WeatherGapFiller:
    """Fills weather observation gaps using SMHI historical data."""

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        house_id: str,
        latitude: float,
        longitude: float,
        logger=None
    ):
        self.influx_url = influx_url
        self.influx_token = influx_token
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.house_id = house_id
        self.latitude = latitude
        self.longitude = longitude
        self.logger = logger or DummyLogger()

        self.client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org, timeout=5_000)
        self.query_api = self.client.query_api()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

        # Escape forward slashes for Flux regex
        # house_id is now short format (no slashes), no escaping needed
        self.escaped_house_id = house_id

    def detect_weather_gaps(
        self,
        start_time: datetime,
        end_time: datetime,
        expected_interval_minutes: int = 15
    ) -> List[Tuple[datetime, datetime]]:
        """Detect gaps in weather observation data."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "weather_observation")
            |> filter(fn: (r) => r["house_id"] == "{self.house_id}")
            |> filter(fn: (r) => r["_field"] == "temperature")
            |> keep(columns: ["_time"])
        '''

        try:
            tables = self.query_api.query(query, org=self.influx_org)

            timestamps = []
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    if ts:
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        timestamps.append(ts)

            timestamps = sorted(set(timestamps))

            if not timestamps:
                return [(start_time, end_time)]

            gaps = []
            gap_threshold = timedelta(minutes=expected_interval_minutes * 2)

            # Check gap at start
            if timestamps[0] - start_time > gap_threshold:
                gaps.append((start_time, timestamps[0]))

            # Check gaps between points
            for i in range(len(timestamps) - 1):
                delta = timestamps[i + 1] - timestamps[i]
                if delta > gap_threshold:
                    gaps.append((timestamps[i], timestamps[i + 1]))

            # Check gap at end
            if end_time - timestamps[-1] > gap_threshold:
                gaps.append((timestamps[-1], end_time))

            return gaps

        except Exception as e:
            print(f"Error detecting weather gaps: {e}")
            return []

    def fill_weather_gaps(
        self,
        start_time: datetime,
        end_time: datetime,
        expected_interval_minutes: int = 15,
        dry_run: bool = False
    ) -> Tuple[int, int, int]:
        """
        Detect and fill weather observation gaps using SMHI historical data.

        Returns:
            Tuple of (written, skipped, errors)
        """
        gaps = self.detect_weather_gaps(start_time, end_time, expected_interval_minutes)

        if not gaps:
            return 0, 0, 0

        total_gap_minutes = sum(
            (gap_end - gap_start).total_seconds() / 60
            for gap_start, gap_end in gaps
        )
        print(f"  Weather: Found {len(gaps)} gap(s) ({total_gap_minutes:.0f} minutes)")

        # Fetch historical observations from SMHI
        smhi = SMHIWeather(
            latitude=self.latitude,
            longitude=self.longitude,
            logger=self.logger
        )

        # Fetch for the entire range (SMHI returns all available data)
        observations = smhi.get_historical_observations(start_time, end_time)

        if not observations:
            print("  Weather: No SMHI historical data available")
            return 0, 0, 0

        print(f"  Weather: Retrieved {len(observations)} observations from SMHI")

        # Filter observations to those within gaps
        gap_observations = []
        for obs in observations:
            ts = obs['timestamp']
            for gap_start, gap_end in gaps:
                if gap_start <= ts <= gap_end:
                    gap_observations.append(obs)
                    break

        if not gap_observations:
            print("  Weather: No observations fall within gap periods")
            return 0, 0, 0

        print(f"  Weather: {len(gap_observations)} observations within gaps")

        written = 0
        skipped = 0
        errors = 0

        for obs in gap_observations:
            try:
                if dry_run:
                    written += 1
                    continue

                point = Point("weather_observation") \
                    .tag("house_id", self.house_id) \
                    .tag("station_name", obs.get('station_name', 'unknown')) \
                    .tag("station_id", str(obs.get('station_id', 0))) \
                    .tag("source", "gap_fill") \
                    .field("temperature", round(float(obs['temperature']), 2)) \
                    .time(obs['timestamp'])

                if obs.get('wind_speed') is not None:
                    point.field("wind_speed", round(float(obs['wind_speed']), 2))
                if obs.get('humidity') is not None:
                    point.field("humidity", round(float(obs['humidity']), 2))
                if obs.get('distance_km') is not None:
                    point.field("distance_km", round(float(obs['distance_km']), 2))

                self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
                written += 1

            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"  Weather write error: {e}")

        action = "Would write" if dry_run else "Wrote"
        print(f"  Weather: {action} {written}, Skipped: {skipped}, Errors: {errors}")

        return written, skipped, errors

    def close(self):
        self.client.close()


class ArrigoBootstrapper:
    """
    Bootstraps a new building with 5-min historical Arrigo data.

    Full pipeline:
      Phase 1: Fetch 5-min Arrigo data (per-signal to stay under 50k limit)
      Phase 2: Fetch SMHI weather history (~4 months via latest-months)
      Phase 3: Copy energy_meter data from source house
      Phase 4: Sanity checks (stale signals, coverage, range)
      Phase 5: Write to InfluxDB (heating_system, thermal_history, weather, energy)
      Phase 6: Run calibration pipeline on learning period (days - backtest_days)
      Phase 7: Backtest predictions on the last backtest_days

    Supports both houses (house_id tag) and buildings (building_id tag),
    auto-detected from the buildings/ directory.
    """

    RANGE_CHECKS = {
        'outdoor_temperature': (-40, 45),
        'room_temperature': (10, 35),
        'supply_temp': (15, 80),
        'return_temp': (10, 70),
        'hot_water_temp': (5, 80),
        'system_pressure': (0, 10),
    }

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        house_id: str,
        source_house_id: str,
        username: str,
        password: str,
        latitude: float,
        longitude: float,
        days: int = 90,
        resolution: int = 5,
        arrigo_host: str = None,
        verbose: bool = False,
        backtest_days: int = 10
    ):
        self.influx_url = influx_url
        self.influx_token = influx_token
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.house_id = house_id
        self.source_house_id = source_house_id
        self.username = username
        self.password = password
        self.latitude = latitude
        self.longitude = longitude
        self.days = days
        self.resolution = resolution
        self.arrigo_host = arrigo_host
        self.verbose = verbose
        self.backtest_days = backtest_days

        # Auto-detect entity type: if config exists in buildings/, it's a building
        self.entity_type = "building" if os.path.exists(os.path.join("buildings", f"{house_id}.json")) else "house"
        self.influx_tag = "building_id" if self.entity_type == "building" else "house_id"

        self.arrigo_client = None
        self.influx_client = None

        # Unified fields populated by _init_arrigo()
        self.signal_map = {}       # signal_id -> {field_name, arrigo_name, unit, ...}
        self.graphql_session = None  # requests.Session with auth headers
        self.graphql_url = None      # GraphQL endpoint URL

    def log(self, msg):
        if self.verbose:
            print(f"  [DEBUG] {msg}")

    def _init_arrigo(self) -> bool:
        """Authenticate with Arrigo and discover signals.

        Houses: HomeSide → BMS token → Arrigo GraphQL (ArrigoHistoricalClient)
        Buildings: Direct Arrigo login → JWT → GraphQL (ArrigoAPI)

        Both paths populate unified fields:
          self.signal_map      - signal_id -> {field_name, arrigo_name, unit, ...}
          self.graphql_session  - requests.Session with auth headers
          self.graphql_url      - GraphQL endpoint URL
        """
        if self.entity_type == "building":
            return self._init_arrigo_building()
        else:
            return self._init_arrigo_house()

    def _init_arrigo_house(self) -> bool:
        """Authenticate via HomeSide → Arrigo (residential houses)."""
        self.arrigo_client = ArrigoHistoricalClient(
            username=self.username,
            password=self.password,
            arrigo_host=self.arrigo_host,
            verbose=self.verbose
        )

        if not self.arrigo_client.authenticate():
            print("ERROR: Failed to authenticate with Arrigo")
            return False

        if not self.arrigo_client.select_house(0):
            print("ERROR: Failed to select house")
            return False

        if not self.arrigo_client.login_to_arrigo():
            print("ERROR: Failed to login to Arrigo server")
            return False

        if not self.arrigo_client.discover_signals():
            print("ERROR: Failed to discover signals")
            return False

        # Copy to unified fields
        self.signal_map = self.signal_map
        self.graphql_session = self.graphql_session
        self.graphql_url = self.graphql_url

        return True

    def _init_arrigo_building(self) -> bool:
        """Authenticate directly with Arrigo (commercial buildings)."""
        from arrigo_api import ArrigoAPI, load_building_config, get_fetch_signals

        # Load building config
        config = load_building_config(self.house_id)
        if not config:
            print(f"ERROR: Building config not found: buildings/{self.house_id}.json")
            return False

        host = self.arrigo_host or config.get('connection', {}).get('host')
        if not host:
            print("ERROR: No Arrigo host (use --arrigo-host or set in building config)")
            return False

        # Resolve credentials: CLI args > .env
        username = self.username
        password = self.password
        if not username or not password:
            from dotenv import load_dotenv
            load_dotenv()
            username = username or os.getenv(f'BUILDING_{self.house_id}_USERNAME')
            password = password or os.getenv(f'BUILDING_{self.house_id}_PASSWORD')

        if not username or not password:
            print(f"ERROR: No credentials. Use --username/--password or set "
                  f"BUILDING_{self.house_id}_USERNAME/PASSWORD in .env")
            return False

        # Resolve lat/lon from config if not provided via CLI
        location = config.get('location', {})
        if not self.latitude and location.get('latitude'):
            self.latitude = location['latitude']
        if not self.longitude and location.get('longitude'):
            self.longitude = location['longitude']

        # Create ArrigoAPI client and login
        import logging
        logger = logging.getLogger('bootstrap')
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        arrigo_api = ArrigoAPI(
            host=host,
            username=username,
            password=password,
            logger=logger,
            verbose=self.verbose,
        )

        if not arrigo_api.login():
            print("ERROR: Failed to authenticate with Arrigo")
            return False

        if not arrigo_api.discover_signals():
            print("ERROR: Failed to discover signals")
            return False

        # Build signal_map from building config (only fetch=true signals)
        analog_fetch, _ = get_fetch_signals(config)
        self.signal_map = {}
        for name, info in analog_fetch.items():
            self.signal_map[info['signal_id']] = {
                'field_name': info['field_name'],
                'arrigo_name': name,
                'unit': info.get('unit', ''),
                'current_value': arrigo_api.signal_map.get(info['signal_id'], {}).get('current_value'),
            }

        # Set unified session fields
        self.graphql_session = arrigo_api.session
        self.graphql_url = arrigo_api.graphql_url

        print(f"  Arrigo: {host} ({len(self.signal_map)} signals)")
        return True

    def _init_influx(self):
        """Initialize InfluxDB client."""
        if not self.influx_client:
            self.influx_client = InfluxDBClient(
                url=self.influx_url,
                token=self.influx_token,
                org=self.influx_org,
                timeout=5_000
            )

    def _fetch_signal_history(self, signal_id, field_name, start_time, end_time, resolution_seconds):
        """
        Fetch history for a single signal via GraphQL.
        Uses cursor pagination if >50k points.

        Returns list of (timestamp, value) tuples.
        """
        query = '''
        query GetHistory($first: Int!, $after: String, $filter: AnalogEventFilter) {
            analogsHistory(first: $first, after: $after, filter: $filter) {
                totalCount
                pageInfo {
                    hasNextPage
                    endCursor
                }
                items {
                    time
                    value
                    reliability
                }
            }
        }
        '''

        all_points = []
        cursor = None
        page = 0

        while True:
            page += 1
            variables = {
                'first': 50000,
                'filter': {
                    'signalId': [signal_id],
                    'ranges': [{
                        'from': start_time.isoformat(),
                        'to': end_time.isoformat()
                    }],
                    'timeLength': resolution_seconds,
                    'timeLengthComparer': 'equal'
                }
            }
            if cursor:
                variables['after'] = cursor

            try:
                response = self.graphql_session.post(
                    self.graphql_url,
                    json={'query': query, 'variables': variables},
                    timeout=300
                )

                if response.status_code != 200:
                    print(f"    ERROR: GraphQL returned {response.status_code} for {field_name}")
                    break

                result = response.json()
                if 'errors' in result:
                    # Fallback: retry without timeLengthComparer (may not be supported)
                    if page == 1 and 'timeLengthComparer' in str(result['errors']):
                        self.log(f"timeLengthComparer not supported, retrying without it")
                        del variables['filter']['timeLengthComparer']
                        response = self.graphql_session.post(
                            self.graphql_url,
                            json={'query': query, 'variables': variables},
                            timeout=300
                        )
                        result = response.json()
                        if 'errors' in result:
                            print(f"    ERROR: {result['errors']}")
                            break
                    else:
                        print(f"    ERROR: {result['errors']}")
                        break

                history = result.get('data', {}).get('analogsHistory', {})
                items = history.get('items', [])
                total = history.get('totalCount', 0)
                page_info = history.get('pageInfo', {})

                if page == 1:
                    self.log(f"  {field_name}: {total} total points available")

                for item in items:
                    value = item.get('value')
                    if value is not None:
                        time_str = item['time']
                        try:
                            ts = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                        except ValueError:
                            ts = datetime.fromisoformat(time_str)
                        all_points.append((ts, float(value)))

                # Check for more pages
                if page_info.get('hasNextPage'):
                    cursor = page_info.get('endCursor')
                    self.log(f"  {field_name}: fetched page {page}, continuing...")
                else:
                    break

            except Exception as e:
                print(f"    ERROR fetching {field_name}: {e}")
                break

        return all_points

    def _phase1_fetch_arrigo(self, start_time, end_time):
        """Phase 1: Fetch 5-min Arrigo data, one signal at a time."""
        print(f"\n{'='*60}")
        print("Phase 1: Fetch Arrigo historical data")
        print(f"{'='*60}")
        print(f"  Resolution: {self.resolution} min")
        print(f"  Time range: {start_time.date()} to {end_time.date()}")
        print(f"  Signals: {len(self.signal_map)}")

        resolution_seconds = self.resolution * 60
        data_by_time = {}  # timestamp -> {field: value}
        signal_stats = {}

        for signal_id, info in self.signal_map.items():
            field_name = info['field_name']
            print(f"  Fetching {field_name}...", end=' ', flush=True)

            points = self._fetch_signal_history(
                signal_id, field_name, start_time, end_time, resolution_seconds
            )

            print(f"{len(points)} points")
            signal_stats[field_name] = len(points)

            for ts, value in points:
                if ts not in data_by_time:
                    data_by_time[ts] = {}
                data_by_time[ts][field_name] = value

        # Summary
        print(f"\n  Total unique timestamps: {len(data_by_time)}")
        mem_estimate = len(data_by_time) * len(signal_stats) * 16 / 1024 / 1024
        print(f"  Estimated memory: ~{mem_estimate:.1f} MB")

        return data_by_time, signal_stats

    def _phase2_fetch_weather(self, start_time, end_time):
        """Phase 2: Fetch SMHI weather history for the bootstrap period."""
        print(f"\n{'='*60}")
        print("Phase 2: Fetch SMHI weather history")
        print(f"{'='*60}")

        smhi = SMHIWeather(
            latitude=self.latitude,
            longitude=self.longitude,
            logger=DummyLogger()
        )

        observations = smhi.get_historical_observations(start_time, end_time)

        if observations:
            print(f"  Retrieved {len(observations)} hourly observations from SMHI")
            first_ts = observations[0]['timestamp']
            last_ts = observations[-1]['timestamp']
            days_covered = (last_ts - first_ts).days
            print(f"  Coverage: {first_ts.date()} to {last_ts.date()} ({days_covered} days)")
            station = observations[0].get('station_name', 'unknown')
            print(f"  Station: {station}")
        else:
            print("  WARNING: No SMHI historical data available")
            print("  (SMHI latest-months covers ~4 months back)")

        return observations

    def _phase3_copy_energy(self, start_time, end_time):
        """Phase 3: Copy energy_meter data from source house/building."""
        print(f"\n{'='*60}")
        print(f"Phase 3: Copy energy_meter data from {self.source_house_id}")
        print(f"{'='*60}")

        if not self.source_house_id:
            print("  Skipped (no --source-house-id)")
            return []

        self._init_influx()
        query_api = self.influx_client.query_api()

        # Determine the tag to filter on for the source entity
        source_is_building = os.path.exists(os.path.join("buildings", f"{self.source_house_id}.json"))
        source_tag = "building_id" if source_is_building else "house_id"

        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "energy_meter")
            |> filter(fn: (r) => r["{source_tag}"] == "{self.source_house_id}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        '''

        try:
            tables = query_api.query(query, org=self.influx_org)
            records = []
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    fields = {}
                    meter_id = record.values.get('meter_id', '')
                    for key, value in record.values.items():
                        if key.startswith('_') or key in ['result', 'table', 'house_id', 'meter_id']:
                            continue
                        if value is not None:
                            try:
                                fields[key] = float(value)
                            except (ValueError, TypeError):
                                pass
                    if fields:
                        records.append({
                            'timestamp': ts,
                            'fields': fields,
                            'meter_id': meter_id
                        })

            print(f"  Found {len(records)} energy_meter records from source")
            if records:
                first_ts = records[0]['timestamp']
                last_ts = records[-1]['timestamp']
                print(f"  Date range: {first_ts.date()} to {last_ts.date()}")
            return records

        except Exception as e:
            print(f"  ERROR: Failed to query source energy data: {e}")
            return []

    def _phase4_sanity_check(self, data_by_time, signal_stats, weather_data, energy_data):
        """Phase 4: Validate data quality before writing."""
        print(f"\n{'='*60}")
        print("Phase 4: Sanity checks")
        print(f"{'='*60}")

        issues = []
        warnings = []

        # Check 1: Stale value detection (stddev near 0)
        # Setpoints and constants are expected to be stable - only warn, don't flag
        non_varying_signals = {'target_temp_setpoint', 'supply_setpoint', 'outdoor_temp_24h_avg'}
        print("\n  Stale signal detection:")
        for field_name in sorted(signal_stats):
            values = [data_by_time[ts][field_name]
                      for ts in data_by_time if field_name in data_by_time[ts]]
            if len(values) < 10:
                continue
            stddev = statistics.stdev(values) if len(values) > 1 else 0
            mean_val = statistics.mean(values)
            status = "OK"
            if stddev < 0.01 and mean_val != 0:
                if field_name in non_varying_signals or 'setpoint' in field_name.lower():
                    status = "CONST"
                    warnings.append(f"Signal '{field_name}' is constant ({mean_val:.2f}) - expected for setpoint")
                else:
                    status = "STUCK"
                    issues.append(f"Signal '{field_name}' appears stuck (stddev={stddev:.4f}, mean={mean_val:.2f})")
            elif stddev < 0.1:
                status = "LOW_VAR"
                warnings.append(f"Signal '{field_name}' has low variance (stddev={stddev:.4f})")
            print(f"    {field_name:30s} stddev={stddev:8.3f}  mean={mean_val:8.2f}  [{status}]")

        # Check 2: Data coverage (only core signals are critical)
        print("\n  Data coverage:")
        if self.entity_type == "building":
            core_signals = {'outdoor_temperature'}  # buildings don't have room_temperature
        else:
            from import_historical_data import CORE_SIGNALS
            core_signals = set(CORE_SIGNALS)
        expected_slots = self.days * 24 * (60 // self.resolution)
        for field_name in sorted(signal_stats):
            count = signal_stats[field_name]
            coverage = count / expected_slots * 100 if expected_slots > 0 else 0
            status = "OK" if coverage > 50 else "LOW"
            is_core = field_name in core_signals
            if coverage < 20 and is_core:
                issues.append(f"Core signal '{field_name}' has very low coverage ({coverage:.0f}%)")
            elif coverage < 20:
                warnings.append(f"Signal '{field_name}' has low coverage ({coverage:.0f}%)")
            print(f"    {field_name:30s} {count:>6} / {expected_slots} ({coverage:.0f}%)  [{status}]{'  (core)' if is_core else ''}")

        # Check 3: Range checks
        print("\n  Range checks:")
        for field_name, (lo, hi) in self.RANGE_CHECKS.items():
            values = [data_by_time[ts][field_name]
                      for ts in data_by_time if field_name in data_by_time[ts]]
            if not values:
                continue
            min_val = min(values)
            max_val = max(values)
            out_of_range = sum(1 for v in values if v < lo or v > hi)
            pct_oor = out_of_range / len(values) * 100
            status = "OK" if pct_oor < 5 else "WARN"
            if pct_oor > 20:
                issues.append(f"Signal '{field_name}' has {pct_oor:.0f}% out of range [{lo}, {hi}]")
            print(f"    {field_name:30s} [{min_val:.1f}, {max_val:.1f}] expected [{lo}, {hi}]  {pct_oor:.1f}% OOR  [{status}]")

        # Check 4: Core signals present
        print("\n  Core signals:")
        for sig in sorted(core_signals):
            present = sig in signal_stats and signal_stats[sig] > 0
            status = "OK" if present else "MISSING"
            if not present:
                issues.append(f"Core signal '{sig}' is missing")
            print(f"    {sig:30s} [{status}]")

        # Check 5: Weather and energy data
        print(f"\n  Weather data: {len(weather_data)} observations")
        if not weather_data:
            warnings.append("No weather history (effective temp will use defaults)")
        print(f"  Energy meter data: {len(energy_data)} records")
        if not energy_data:
            warnings.append("No energy meter data (calibration needs manual energy import)")

        # Summary
        print(f"\n  {'='*40}")
        if issues:
            print(f"  ISSUES ({len(issues)}):")
            for issue in issues:
                print(f"    - {issue}")
        if warnings:
            print(f"  WARNINGS ({len(warnings)}):")
            for w in warnings:
                print(f"    - {w}")
        if not issues and not warnings:
            print("  All checks passed!")

        return len(issues) == 0

    def _get_all_house_ids(self):
        """Get all house_ids from profiles/ and buildings/ directories.

        Excludes auxiliary files like *_signals.json which are metadata,
        not actual house/building profiles.
        """
        house_ids = set()
        for directory in ['profiles', 'buildings']:
            if os.path.isdir(directory):
                for filename in os.listdir(directory):
                    if filename.endswith('.json') and '_signals.json' not in filename:
                        house_ids.add(filename.replace('.json', ''))
        return sorted(house_ids)

    def _get_existing_weather_timestamps(self, house_id, start_time, end_time):
        """Query existing weather_observation timestamps for a house."""
        self._init_influx()
        query_api = self.influx_client.query_api()

        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "weather_observation")
            |> filter(fn: (r) => r["house_id"] == "{house_id}")
            |> filter(fn: (r) => r["_field"] == "temperature")
            |> keep(columns: ["_time"])
        '''

        try:
            tables = query_api.query(query, org=self.influx_org)
            timestamps = set()
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    if ts:
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        # Round to nearest hour for comparison (SMHI data is hourly)
                        ts = ts.replace(minute=0, second=0, microsecond=0)
                        timestamps.add(ts)
            return timestamps
        except Exception as e:
            self.log(f"Failed to query existing weather for {house_id}: {e}")
            return set()

    def _save_signal_metadata(self, data_by_time, signal_stats):
        """Save signal discovery and quality metadata to a JSON file.

        Houses: saves to profiles/<house_id>_signals.json
        Buildings: saves to buildings/<building_id>_signals.json
        """
        if self.entity_type == "building":
            CORE_SIGNALS = {'outdoor_temperature'}
        else:
            from import_historical_data import CORE_SIGNALS

        expected_slots = self.days * 24 * (60 // self.resolution)
        non_varying_signals = {'target_temp_setpoint', 'supply_setpoint', 'outdoor_temp_24h_avg'}

        signals_meta = {}
        for signal_id, info in self.signal_map.items():
            field_name = info['field_name']
            arrigo_name = info.get('arrigo_name', '')
            unit = info.get('unit', '')
            current_value = info.get('current_value')

            # Compute quality stats from fetched data
            values = [data_by_time[ts][field_name]
                      for ts in data_by_time if field_name in data_by_time[ts]]
            count = signal_stats.get(field_name, 0)
            coverage_pct = round(count / expected_slots * 100, 1) if expected_slots > 0 else 0

            if len(values) > 1:
                stddev = round(statistics.stdev(values), 4)
                mean_val = round(statistics.mean(values), 2)
                min_val = round(min(values), 2)
                max_val = round(max(values), 2)
            elif len(values) == 1:
                stddev = 0.0
                mean_val = round(values[0], 2)
                min_val = mean_val
                max_val = mean_val
            else:
                stddev = None
                mean_val = None
                min_val = None
                max_val = None

            # Determine status
            if count == 0:
                status = "no_data"
                fetch = False
            elif stddev is not None and stddev < 0.01 and mean_val == 0:
                status = "always_zero"
                fetch = False
            elif stddev is not None and stddev < 0.01:
                if field_name in non_varying_signals or 'setpoint' in field_name.lower():
                    status = "constant_setpoint"
                    fetch = True
                else:
                    status = "stale"
                    fetch = False
            elif coverage_pct < 20:
                status = "low_coverage"
                fetch = True
            else:
                status = "ok"
                fetch = True

            signals_meta[arrigo_name] = {
                "signal_id": signal_id,
                "field_name": field_name,
                "unit": unit,
                "is_core": field_name in CORE_SIGNALS,
                "fetch": fetch,
                "status": status,
                "discovered_value": current_value,
                "stats": {
                    "points": count,
                    "coverage_pct": coverage_pct,
                    "mean": mean_val,
                    "stddev": stddev,
                    "min": min_val,
                    "max": max_val,
                }
            }

        # Build the metadata document
        metadata = {
            "schema_version": 1,
            "house_id": self.house_id,
            "bootstrap_date": datetime.now(timezone.utc).isoformat(),
            "source": "arrigo_bootstrap",
            "resolution_minutes": self.resolution,
            "days_fetched": self.days,
            "expected_slots": expected_slots,
            "total_signals_discovered": len(self.signal_map),
            "signals": signals_meta
        }

        # Write to profiles/ or buildings/ based on entity type
        if self.entity_type == "building":
            out_path = os.path.join("buildings", f"{self.house_id}_signals.json")
        else:
            out_path = os.path.join("profiles", f"{self.house_id}_signals.json")
        with open(out_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"\n  Signal metadata saved to {out_path}")

        # Print summary
        status_counts = {}
        for sig in signals_meta.values():
            s = sig['status']
            status_counts[s] = status_counts.get(s, 0) + 1
        summary_parts = [f"{count} {status}" for status, count in sorted(status_counts.items())]
        print(f"  Signal summary: {', '.join(summary_parts)}")

    def _phase5_write_influx(self, data_by_time, weather_data, energy_data, dry_run=False):
        """Phase 5: Write all data to InfluxDB in batches.

        Weather data is written for ALL buildings (shared station) with
        deduplication: only timestamps not already present are written.
        Uses self.influx_tag (house_id or building_id) based on entity type.

        Houses: writes to heating_system + thermal_history
        Buildings: writes to building_system only (no thermal_history)
        """
        print(f"\n{'='*60}")
        print(f"Phase 5: Write to InfluxDB{' (DRY RUN)' if dry_run else ''}")
        print(f"{'='*60}")
        print(f"  Entity: {self.house_id} (tag: {self.influx_tag})")

        # Measurement name depends on entity type
        data_measurement = "building_system" if self.entity_type == "building" else "heating_system"

        # For houses, only count timestamps with both room+outdoor temp
        # For buildings, count all timestamps with any data
        if self.entity_type == "building":
            total_data_points = len(data_by_time)
        else:
            total_data_points = sum(1 for ts in data_by_time
                                    if 'room_temperature' in data_by_time[ts]
                                    and 'outdoor_temperature' in data_by_time[ts])

        # Determine weather write plan (all houses, deduped)
        all_house_ids = self._get_all_house_ids()
        weather_ts_set = {obs['timestamp'].replace(minute=0, second=0, microsecond=0)
                          for obs in weather_data} if weather_data else set()

        if dry_run:
            self._init_influx()
            if self.entity_type == "building":
                print(f"  Would write {total_data_points} {data_measurement} points for {self.house_id}")
            else:
                print(f"  Would write {total_data_points} {data_measurement} + thermal_history points for {self.house_id}")
            print(f"  Would write {len(energy_data)} energy_meter points for {self.house_id}")
            print(f"\n  Weather dedup check ({len(weather_data)} SMHI observations, {len(all_house_ids)} buildings):")
            for hid in all_house_ids:
                if weather_data:
                    start_ts = min(weather_ts_set)
                    end_ts = max(weather_ts_set) + timedelta(hours=1)
                    existing = self._get_existing_weather_timestamps(hid, start_ts, end_ts)
                    new_count = len(weather_ts_set - existing)
                    print(f"    {hid:40s} {len(existing):>5} existing, {new_count:>5} new")
                else:
                    print(f"    {hid:40s} (no weather data to write)")
            return

        self._init_influx()

        # Delete existing data for bootstrap entity only
        # (NOT weather - we handle that with dedup below)
        delete_api = self.influx_client.delete_api()
        now = datetime.now(timezone.utc)
        start_delete = now - timedelta(days=self.days + 1)

        delete_measurements = [data_measurement, 'energy_meter']
        if self.entity_type == "house":
            delete_measurements.append('thermal_history')

        for measurement in delete_measurements:
            predicate = f'_measurement="{measurement}" AND {self.influx_tag}="{self.house_id}"'
            try:
                delete_api.delete(start_delete, now, predicate,
                                  bucket=self.influx_bucket, org=self.influx_org)
                self.log(f"Deleted existing {measurement} for {self.house_id}")
            except Exception as e:
                self.log(f"Delete {measurement} (may not exist): {e}")

        write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        batch_size = 500

        # === Write data measurement (+ thermal_history for houses) ===
        if self.entity_type == "building":
            print(f"  Writing {data_measurement}...", end=' ', flush=True)
        else:
            print(f"  Writing {data_measurement} + thermal_history...", end=' ', flush=True)
        points = []
        written_count = 0
        for ts in sorted(data_by_time.keys()):
            fields = data_by_time[ts]

            # For houses, require core fields; for buildings, write any data
            if self.entity_type == "house":
                if 'room_temperature' not in fields or 'outdoor_temperature' not in fields:
                    continue

            hp = Point(data_measurement).tag(self.influx_tag, self.house_id).time(ts, WritePrecision.S)
            for field, value in fields.items():
                hp.field(field, round(float(value), 2))
            points.append(hp)

            # thermal_history only for houses
            if self.entity_type == "house":
                tp = Point("thermal_history").tag(self.influx_tag, self.house_id).time(ts, WritePrecision.S)
                tp.field("room_temperature", round(float(fields['room_temperature']), 2))
                tp.field("outdoor_temperature", round(float(fields['outdoor_temperature']), 2))
                if 'supply_temp' in fields:
                    tp.field("supply_temp", round(float(fields['supply_temp']), 2))
                if 'return_temp' in fields:
                    tp.field("return_temp", round(float(fields['return_temp']), 2))
                points.append(tp)

            written_count += 1

            if len(points) >= batch_size:
                write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
                points = []
        if points:
            write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
        print(f"{written_count} points")

        # === Write weather for ALL buildings (deduped per house) ===
        if weather_data:
            print(f"  Writing weather_observation (all buildings, deduped):")
            start_ts = min(weather_ts_set)
            end_ts = max(weather_ts_set) + timedelta(hours=1)

            for hid in all_house_ids:
                existing_ts = self._get_existing_weather_timestamps(hid, start_ts, end_ts)
                new_obs = [obs for obs in weather_data
                           if obs['timestamp'].replace(minute=0, second=0, microsecond=0)
                           not in existing_ts]

                if not new_obs:
                    print(f"    {hid}: 0 new (all {len(existing_ts)} already exist)")
                    continue

                points = []
                for obs in new_obs:
                    p = Point("weather_observation") \
                        .tag("house_id", hid) \
                        .tag("station_name", obs.get('station_name', 'unknown')) \
                        .tag("station_id", str(obs.get('station_id', 0))) \
                        .tag("source", "bootstrap") \
                        .field("temperature", round(float(obs['temperature']), 2)) \
                        .time(obs['timestamp'])

                    if obs.get('wind_speed') is not None:
                        p.field("wind_speed", round(float(obs['wind_speed']), 2))
                    if obs.get('humidity') is not None:
                        p.field("humidity", round(float(obs['humidity']), 2))
                    if obs.get('distance_km') is not None:
                        p.field("distance_km", round(float(obs['distance_km']), 2))
                    points.append(p)

                    if len(points) >= batch_size:
                        write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
                        points = []
                if points:
                    write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
                print(f"    {hid}: {len(new_obs)} new (skipped {len(existing_ts)} existing)")

        # === Write energy_meter (bootstrap entity only) ===
        if energy_data:
            print(f"  Writing energy_meter...", end=' ', flush=True)
            points = []
            for record in energy_data:
                p = Point("energy_meter") \
                    .tag(self.influx_tag, self.house_id) \
                    .tag("meter_id", record.get('meter_id', '')) \
                    .time(record['timestamp'])
                for field, value in record['fields'].items():
                    p.field(field, float(value))
                points.append(p)

                if len(points) >= batch_size:
                    write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
                    points = []
            if points:
                write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            print(f"{len(energy_data)} points")

        print(f"  Done!")

    def _phase6_calibrate(self):
        """Phase 6: Run energy separation + k recalibration.

        Uses learning_days (days - backtest_days) so the last backtest_days
        are reserved for prediction validation in Phase 7.
        """
        learning_days = self.days - self.backtest_days
        print(f"\n{'='*60}")
        print("Phase 6: Run calibration pipeline")
        print(f"{'='*60}")
        print(f"  Learning on {learning_days} days (reserving {self.backtest_days} days for backtest)")

        from heating_energy_calibrator import HeatingEnergyCalibrator
        from k_recalibrator import recalibrate_entity

        # Step 1: Energy separation
        print(f"\n  Running energy separation for {self.house_id}...")
        calibrator = HeatingEnergyCalibrator(
            influx_url=self.influx_url,
            influx_token=self.influx_token,
            influx_org=self.influx_org,
            influx_bucket=self.influx_bucket,
            latitude=self.latitude,
            longitude=self.longitude,
            entity_tag=self.influx_tag
        )

        start_date = (datetime.now(timezone.utc) - timedelta(days=self.days)).strftime('%Y-%m-%d')
        end_date = (datetime.now(timezone.utc) - timedelta(days=self.backtest_days)).strftime('%Y-%m-%d')
        analyses, used_k = calibrator.analyze(
            house_id=self.house_id,
            start_date=start_date,
            k_percentile=15,
            quiet=False
        )

        if analyses:
            # Only use analyses from the learning period (exclude backtest days)
            learning_analyses = [a for a in analyses if a.date < end_date]
            written = calibrator.write_to_influx(self.house_id, learning_analyses, used_k)
            print(f"  Energy separation: {written} days written (of {len(analyses)} total), k={used_k:.4f}")
        else:
            print(f"  WARNING: No energy separation results (need energy_meter data)")
            used_k = 0.0

        calibrator.close()

        # Step 2: K recalibration
        print(f"\n  Running k-recalibration for {self.house_id}...")
        result = recalibrate_entity(
            entity_id=self.house_id,
            entity_type=self.entity_type,
            influx_url=self.influx_url,
            influx_token=self.influx_token,
            influx_org=self.influx_org,
            influx_bucket=self.influx_bucket,
            days=learning_days,
            update_config=True,
        )

        if result:
            print(f"  K-recalibration: k={result.k_value:.4f}, confidence={result.confidence:.0%}")
        else:
            print(f"  WARNING: K-recalibration produced no result")

        return used_k, result

    def _phase7_backtest_predictions(self):
        """Phase 7: Run prediction backtest on the last N days.

        Houses: Uses thermal coefficient + TemperatureForecaster (Model C)
                to backtest indoor temperature predictions.
        Buildings: Uses heat_loss_k to backtest daily energy predictions
                   against actual energy_meter data.

        Writes results to 'prediction_backtest' measurement.
        """
        print(f"\n{'='*60}")
        print(f"Phase 7: Prediction backtest ({self.backtest_days} days)")
        print(f"{'='*60}")

        if self.entity_type == "building":
            return self._phase7_backtest_energy()

        self._init_influx()
        query_api = self.influx_client.query_api()

        end_time = datetime.now(timezone.utc)
        backtest_start = end_time - timedelta(days=self.backtest_days)

        # Load the profile (with thermal coefficient from Phase 6)
        from customer_profile import CustomerProfile
        try:
            profile = CustomerProfile.load(self.house_id, profiles_dir='profiles')
        except FileNotFoundError:
            print(f"  WARNING: No profile found for {self.house_id}, skipping backtest")
            return

        thermal_coeff = profile.learned.thermal_coefficient
        if thermal_coeff is None:
            print(f"  WARNING: No thermal coefficient learned, skipping backtest")
            return

        print(f"  Thermal coefficient: {thermal_coeff:.6f}")
        print(f"  Target indoor temp: {profile.comfort.target_indoor_temp}°C")

        # Fetch actual heating data for the backtest period
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {backtest_start.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "heating_system")
            |> filter(fn: (r) => r["{self.influx_tag}"] == "{self.house_id}")
            |> filter(fn: (r) =>
                r["_field"] == "room_temperature" or
                r["_field"] == "outdoor_temperature"
            )
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        '''

        try:
            tables = query_api.query(query, org=self.influx_org)
        except Exception as e:
            print(f"  ERROR: Failed to query backtest data: {e}")
            return

        # Build time series from query results
        actual_data = []
        for table in tables:
            for record in table.records:
                ts = record.get_time()
                indoor = record.values.get('room_temperature')
                outdoor = record.values.get('outdoor_temperature')
                if ts and indoor is not None and outdoor is not None:
                    actual_data.append({
                        'timestamp': ts,
                        'indoor': float(indoor),
                        'outdoor': float(outdoor)
                    })

        if len(actual_data) < 10:
            print(f"  WARNING: Only {len(actual_data)} data points in backtest period, skipping")
            return

        print(f"  Actual data points: {len(actual_data)}")

        # Initialize the forecaster with the learned profile
        from temperature_forecaster import TemperatureForecaster
        forecaster = TemperatureForecaster(profile)

        # Walk through the backtest period, generating predictions
        # For each point, use the previous indoor temp + current outdoor temp
        # to predict what the indoor temp should be
        results = []
        errors = []

        for i in range(1, len(actual_data)):
            prev = actual_data[i - 1]
            curr = actual_data[i]

            # Generate a 1-step prediction using the physics model
            weather_forecast = [{
                'time': curr['timestamp'].isoformat(),
                'temp': curr['outdoor']
            }]

            try:
                forecast_points = forecaster.generate_forecast(
                    current_indoor=prev['indoor'],
                    current_outdoor=prev['outdoor'],
                    weather_forecast=weather_forecast
                )

                # Find the indoor_temp prediction
                predicted_indoor = None
                for fp in forecast_points:
                    if fp.forecast_type == 'indoor_temp':
                        predicted_indoor = fp.value
                        break

                if predicted_indoor is not None:
                    error = predicted_indoor - curr['indoor']
                    results.append({
                        'timestamp': curr['timestamp'],
                        'predicted': predicted_indoor,
                        'actual': curr['indoor'],
                        'outdoor': curr['outdoor'],
                        'error': error,
                        'abs_error': abs(error)
                    })
                    errors.append(abs(error))

            except Exception as e:
                self.log(f"Prediction error at {curr['timestamp']}: {e}")

        if not results:
            print(f"  WARNING: No predictions generated")
            return

        # Calculate statistics
        mae = sum(errors) / len(errors)
        max_error = max(errors)
        within_05 = sum(1 for e in errors if e <= 0.5) / len(errors) * 100
        within_10 = sum(1 for e in errors if e <= 1.0) / len(errors) * 100

        # Group by day for per-day summary
        daily_errors = {}
        for r in results:
            day = r['timestamp'].strftime('%Y-%m-%d')
            if day not in daily_errors:
                daily_errors[day] = []
            daily_errors[day].append(r['abs_error'])

        print(f"\n  Backtest Results:")
        print(f"  {'='*50}")
        print(f"  Predictions: {len(results)}")
        print(f"  MAE (Mean Absolute Error): {mae:.2f}°C")
        print(f"  Max error: {max_error:.2f}°C")
        print(f"  Within 0.5°C: {within_05:.0f}%")
        print(f"  Within 1.0°C: {within_10:.0f}%")

        print(f"\n  Per-day MAE:")
        for day in sorted(daily_errors):
            day_mae = sum(daily_errors[day]) / len(daily_errors[day])
            count = len(daily_errors[day])
            print(f"    {day}: {day_mae:.2f}°C ({count} points)")

        # Write results to InfluxDB
        write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        batch_size = 500
        points = []

        for r in results:
            p = Point("prediction_backtest") \
                .tag(self.influx_tag, self.house_id) \
                .tag("model", "model_c") \
                .time(r['timestamp'], WritePrecision.S) \
                .field("predicted_indoor", round(r['predicted'], 2)) \
                .field("actual_indoor", round(r['actual'], 2)) \
                .field("outdoor_temp", round(r['outdoor'], 2)) \
                .field("error", round(r['error'], 2)) \
                .field("abs_error", round(r['abs_error'], 2))
            points.append(p)

            if len(points) >= batch_size:
                write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
                points = []

        if points:
            write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)

        print(f"\n  Wrote {len(results)} backtest results to InfluxDB (prediction_backtest)")

    def _phase7_backtest_energy(self):
        """Phase 7 (buildings): Backtest energy predictions using heat_loss_k.

        For each day in the backtest period:
          predicted_heating = k * degree_hours
          predicted_total = predicted_heating + dhw_baseline
        Compare against actual energy_meter consumption.
        """
        from arrigo_api import load_building_config

        config = load_building_config(self.house_id)
        if not config:
            print(f"  WARNING: No building config for {self.house_id}, skipping backtest")
            return

        es = config.get('energy_separation', {})
        k = es.get('heat_loss_k')
        if not k:
            print(f"  WARNING: No heat_loss_k calibrated yet, skipping energy backtest")
            return

        assumed_indoor = es.get('assumed_indoor_temp', 21.0)
        dhw_pct = es.get('dhw_percentage')
        field_mapping = es.get('field_mapping', {})
        outdoor_field = field_mapping.get('outdoor_temperature', 'outdoor_temp_fvc')

        print(f"  heat_loss_k: {k:.4f}")
        print(f"  assumed_indoor: {assumed_indoor}°C")
        print(f"  outdoor signal: {outdoor_field}")

        self._init_influx()
        query_api = self.influx_client.query_api()

        end_time = datetime.now(timezone.utc)
        backtest_start = end_time - timedelta(days=self.backtest_days)

        # Query outdoor temperature from building_system
        outdoor_query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {backtest_start.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "building_system")
            |> filter(fn: (r) => r["building_id"] == "{self.house_id}")
            |> filter(fn: (r) => r["_field"] == "{outdoor_field}")
            |> sort(columns: ["_time"])
        '''

        # Query actual energy consumption
        energy_query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {backtest_start.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "energy_meter")
            |> filter(fn: (r) => r["building_id"] == "{self.house_id}")
            |> filter(fn: (r) => r["_field"] == "consumption")
            |> sort(columns: ["_time"])
        '''

        try:
            outdoor_tables = query_api.query(outdoor_query, org=self.influx_org)
            energy_tables = query_api.query(energy_query, org=self.influx_org)
        except Exception as e:
            print(f"  ERROR: Failed to query backtest data: {e}")
            return

        # Parse outdoor temps by Swedish date
        outdoor_by_date = {}  # date_str -> [temps]
        for table in outdoor_tables:
            for record in table.records:
                ts = record.get_time()
                val = record.get_value()
                if ts and val is not None:
                    swedish_date = ts.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d')
                    outdoor_by_date.setdefault(swedish_date, []).append(float(val))

        # Parse energy consumption by Swedish date
        energy_by_date = {}  # date_str -> total_kwh
        for table in energy_tables:
            for record in table.records:
                ts = record.get_time()
                val = record.get_value()
                if ts and val is not None:
                    swedish_date = ts.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d')
                    energy_by_date[swedish_date] = energy_by_date.get(swedish_date, 0) + float(val)

        if not outdoor_by_date:
            print(f"  WARNING: No outdoor temperature data for backtest period")
            return
        if not energy_by_date:
            print(f"  WARNING: No energy_meter data for backtest period")
            return

        # Calculate per-day predictions
        results = []
        for date_str in sorted(outdoor_by_date):
            if date_str not in energy_by_date:
                continue

            temps = outdoor_by_date[date_str]
            avg_outdoor = sum(temps) / len(temps)

            # Degree hours: sum of (indoor - outdoor) for each hour, clamped >= 0
            delta_t = max(0, assumed_indoor - avg_outdoor)
            degree_hours = delta_t * 24  # approximate: avg delta * 24h

            predicted_heating = k * degree_hours

            # Add DHW baseline if known
            actual_total = energy_by_date[date_str]
            if dhw_pct and dhw_pct > 0:
                # dhw_pct is percentage of total, so dhw = total * pct / 100
                # For prediction: use historical average DHW
                dhw_daily = actual_total * (dhw_pct / 100)
                predicted_total = predicted_heating + dhw_daily
            else:
                predicted_total = predicted_heating

            error = predicted_total - actual_total

            results.append({
                'date': date_str,
                'timestamp': datetime.strptime(date_str, '%Y-%m-%d').replace(
                    hour=12, tzinfo=SWEDISH_TZ).astimezone(timezone.utc),
                'predicted_energy': predicted_total,
                'actual_energy': actual_total,
                'error': error,
                'abs_error': abs(error),
                'outdoor_avg': avg_outdoor,
                'degree_hours': degree_hours,
            })

        if not results:
            print(f"  WARNING: No overlapping days with both outdoor and energy data")
            return

        # Statistics
        errors = [r['abs_error'] for r in results]
        mae = sum(errors) / len(errors)
        max_error = max(errors)
        actual_totals = [r['actual_energy'] for r in results]
        avg_actual = sum(actual_totals) / len(actual_totals) if actual_totals else 1
        mape = (mae / avg_actual * 100) if avg_actual > 0 else 0

        print(f"\n  Energy Backtest Results:")
        print(f"  {'='*50}")
        print(f"  Days with data: {len(results)}")
        print(f"  MAE (Mean Abs Error): {mae:.1f} kWh/day")
        print(f"  MAPE: {mape:.1f}%")
        print(f"  Max error: {max_error:.1f} kWh/day")

        print(f"\n  Per-day breakdown:")
        print(f"    {'Date':12s} {'Predicted':>10s} {'Actual':>10s} {'Error':>10s} {'OutdoorAvg':>11s} {'DegreeH':>8s}")
        for r in results:
            print(f"    {r['date']:12s} {r['predicted_energy']:>10.1f} {r['actual_energy']:>10.1f} "
                  f"{r['error']:>+10.1f} {r['outdoor_avg']:>10.1f}°C {r['degree_hours']:>8.0f}")

        # Write to InfluxDB
        write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        points = []
        for r in results:
            p = Point("prediction_backtest") \
                .tag("building_id", self.house_id) \
                .tag("model", "energy_k") \
                .time(r['timestamp'], WritePrecision.S) \
                .field("predicted_energy", round(r['predicted_energy'], 2)) \
                .field("actual_energy", round(r['actual_energy'], 2)) \
                .field("error", round(r['error'], 2)) \
                .field("abs_error", round(r['abs_error'], 2)) \
                .field("outdoor_avg", round(r['outdoor_avg'], 2)) \
                .field("degree_hours", round(r['degree_hours'], 1))
            points.append(p)

        if points:
            write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
        print(f"\n  Wrote {len(results)} backtest results to InfluxDB (prediction_backtest)")

    def _print_comparison(self, used_k, recal_result):
        """Print side-by-side comparison with source house."""
        if not self.source_house_id:
            return

        print(f"\n{'='*60}")
        print("Bootstrap Calibration Results")
        print(f"{'='*60}")

        try:
            from customer_profile import CustomerProfile
            source_profile = CustomerProfile.load(self.source_house_id, profiles_dir='profiles')
            test_profile = CustomerProfile.load(self.house_id, profiles_dir='profiles')

            src_es = source_profile.energy_separation
            tst_es = test_profile.energy_separation

            print(f"\n  {'':30s} {'Source (15-min)':>18s}  {'Bootstrap (5-min)':>18s}")
            print(f"  {'-'*70}")
            print(f"  {'House':30s} {self.source_house_id:>18s}  {self.house_id:>18s}")
            print(f"  {'k_value (heat_loss_k)':30s} {src_es.heat_loss_k:>18.5f}  {tst_es.heat_loss_k:>18.5f}")
            print(f"  {'DHW percentage':30s} {src_es.dhw_percentage:>17.1f}%  {tst_es.dhw_percentage:>17.1f}%")
            print(f"  {'Calibration days':30s} {src_es.calibration_days:>18d}  {tst_es.calibration_days:>18d}")
            print(f"  {'Calibration date':30s} {str(src_es.calibration_date):>18s}  {str(tst_es.calibration_date):>18s}")

            if src_es.heat_loss_k > 0 and tst_es.heat_loss_k > 0:
                k_diff_pct = abs(tst_es.heat_loss_k - src_es.heat_loss_k) / src_es.heat_loss_k * 100
                print(f"\n  K-value difference: {k_diff_pct:.1f}%")
                if k_diff_pct < 20:
                    print(f"  Result: GOOD (within 20% tolerance)")
                else:
                    print(f"  Result: INVESTIGATE (>20% difference)")

        except Exception as e:
            print(f"  Could not load profiles for comparison: {e}")

    def run(self, dry_run=False, no_calibrate=False):
        """Execute the full bootstrap pipeline."""
        print("=" * 60)
        print(f"Bootstrap: {self.house_id}")
        print(f"Entity type: {self.entity_type} (tag: {self.influx_tag})")
        print(f"Source: {self.source_house_id or 'none'}")
        print(f"Days: {self.days}, Resolution: {self.resolution} min")
        print(f"Learn: {self.days - self.backtest_days} days, Backtest: {self.backtest_days} days")
        print("=" * 60)

        # Initialize Arrigo
        if not self._init_arrigo():
            return False

        # Calculate time range
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.days)

        # Phase 1: Fetch Arrigo data
        data_by_time, signal_stats = self._phase1_fetch_arrigo(start_time, end_time)
        if not data_by_time:
            print("ERROR: No data from Arrigo. Aborting.")
            return False

        # Phase 2: Fetch weather history
        weather_data = self._phase2_fetch_weather(start_time, end_time)

        # Phase 3: Copy energy_meter data
        energy_data = self._phase3_copy_energy(start_time, end_time)

        # Phase 4: Sanity checks
        passed = self._phase4_sanity_check(data_by_time, signal_stats, weather_data, energy_data)
        if not passed and not dry_run:
            print("\nSanity checks found critical issues.")
            response = input("Continue anyway? [y/N]: ").strip().lower()
            if response != 'y':
                print("Aborted.")
                return False
        elif not passed:
            print("\n(Sanity issues found - continuing dry run to show write plan)")

        # Save signal discovery metadata (always, even on dry-run)
        self._save_signal_metadata(data_by_time, signal_stats)

        # Phase 5: Write to InfluxDB
        self._phase5_write_influx(data_by_time, weather_data, energy_data, dry_run=dry_run)

        if dry_run:
            print("\nDry run complete. No data written.")
            return True

        # Phase 6: Run calibration (on learning period only)
        if no_calibrate:
            print("\nSkipping calibration (--no-calibrate)")
            return True

        used_k, recal_result = self._phase6_calibrate()

        # Phase 7: Backtest predictions on reserved days
        if self.backtest_days > 0:
            self._phase7_backtest_predictions()

        # Print comparison
        self._print_comparison(used_k, recal_result)

        print("\nBootstrap complete!")
        return True

    def close(self):
        if self.influx_client:
            self.influx_client.close()


def fill_gaps_on_startup(
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    house_id: str,
    username: str,
    password: str,
    settings: dict,
    logger,
    latitude: float = None,
    longitude: float = None
) -> bool:
    """
    Called from fetcher startup to detect and fill gaps.

    Args:
        Various InfluxDB credentials
        settings: Application settings dict
        logger: Logger instance
        latitude: Location latitude for weather data (optional)
        longitude: Location longitude for weather data (optional)

    Returns:
        True if gaps were filled successfully (or no gaps found)

    Note:
        Arrigo-based heating gap filling is DISABLED as of 2026-02-05.
        Arrigo data (dh_power, energy) was unreliable with gaps.
        We now rely on:
        - HomeSide API for real-time heating data (15-min)
        - Dropbox import for energy meter data (hourly, reliable)
        - SMHI for weather data (still active below)

        The Arrigo scripts (import_historical_data.py, GapFiller class)
        are kept for potential future use with new house bootstrapping.
    """
    try:
        # Check for gaps in last 24 hours
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)

        # === HEATING DATA GAPS (DISABLED) ===
        # Arrigo-based gap filling disabled - data was unreliable
        # HomeSide API provides real-time data, Dropbox import provides energy
        heating_written = 0
        heating_errors = 0
        logger.info("Heating gap filling skipped (Arrigo disabled)")
        print("ℹ️  Heating gap filling: disabled (using HomeSide + Dropbox only)")

        # === WEATHER DATA GAPS ===
        weather_written = 0
        weather_errors = 0

        if latitude and longitude:
            try:
                weather_filler = WeatherGapFiller(
                    influx_url=influx_url,
                    influx_token=influx_token,
                    influx_org=influx_org,
                    influx_bucket=influx_bucket,
                    house_id=house_id,
                    latitude=latitude,
                    longitude=longitude,
                    logger=logger
                )

                weather_written, _, weather_errors = weather_filler.fill_weather_gaps(
                    start_time, end_time, dry_run=False
                )

                weather_filler.close()

                if weather_written == 0 and weather_errors == 0:
                    print("✓ No weather data gaps in last 24 hours")

            except Exception as e:
                logger.warning(f"Weather gap filling failed: {e}")
                print(f"⚠ Weather gap filling failed: {e}")
        else:
            logger.debug("Weather gap filling skipped - no lat/lon provided")

        # === SUMMARY ===
        total_written = heating_written + weather_written
        total_errors = heating_errors + weather_errors

        if total_errors > 0:
            logger.warning(f"Gap filling completed with {total_errors} errors")
            print(f"⚠ Gap filling: {total_written} written, {total_errors} errors")
            return False
        elif total_written > 0:
            logger.info(f"Gap filling completed: {total_written} points written")
            print(f"✓ Gap filling: {total_written} points written")
            return True
        else:
            return True

    except Exception as e:
        logger.warning(f"Gap filling failed: {e}")
        print(f"⚠ Gap filling failed: {e}")
        return False


def run_bootstrap(args):
    """Run the bootstrap pipeline from CLI args."""
    print("=" * 60)
    print("Arrigo Bootstrap")
    print("=" * 60)

    bootstrapper = ArrigoBootstrapper(
        influx_url=args.influx_url,
        influx_token=args.influx_token,
        influx_org=args.influx_org,
        influx_bucket=args.influx_bucket,
        house_id=args.house_id,
        source_house_id=args.source_house_id,
        username=getattr(args, 'username', None) or '',
        password=getattr(args, 'password', None) or '',
        latitude=args.lat or 0,
        longitude=args.lon or 0,
        days=args.days,
        resolution=args.resolution,
        arrigo_host=getattr(args, 'arrigo_host', None),
        verbose=args.verbose,
        backtest_days=args.backtest_days
    )

    try:
        success = bootstrapper.run(
            dry_run=args.dry_run,
            no_calibrate=args.no_calibrate
        )
        if not success:
            sys.exit(1)
    finally:
        bootstrapper.close()


def main():
    parser = argparse.ArgumentParser(
        description='Fill gaps in InfluxDB heating and weather data, or bootstrap new buildings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fill heating gaps from midnight
    python3 gap_filler.py --username FC... --password "..." --from-midnight

    # Fill both heating AND weather gaps (requires lat/lon)
    python3 gap_filler.py --username FC... --password "..." --from-midnight --lat 56.67 --lon 12.86

    # Fill weather gaps only
    python3 gap_filler.py --username FC... --password "..." --from-midnight --lat 56.67 --lon 12.86 --weather-only

    # Dry run
    python3 gap_filler.py --username FC... --password "..." --from-midnight --dry-run

    # Detect gaps only (no filling)
    python3 gap_filler.py --username FC... --password "..." --detect-only --last-hours 24 --lat 56.67 --lon 12.86

    # Bootstrap new building with 5-min Arrigo data
    python3 gap_filler.py --bootstrap --username FC... --password "..." \\
        --house-id HEM_FJV_Villa_149_TEST --source-house-id HEM_FJV_Villa_149 \\
        --lat 56.67 --lon 12.86 --days 90 --resolution 5

    # Bootstrap dry run (fetch + sanity check, no writes)
    python3 gap_filler.py --bootstrap --username FC... --password "..." \\
        --house-id HEM_FJV_Villa_149_TEST --source-house-id HEM_FJV_Villa_149 \\
        --lat 56.67 --lon 12.86 --days 90 --dry-run

    # Bootstrap without calibration (data only)
    python3 gap_filler.py --bootstrap --username FC... --password "..." \\
        --house-id HEM_FJV_Villa_149_TEST --source-house-id HEM_FJV_Villa_149 \\
        --lat 56.67 --lon 12.86 --days 90 --no-calibrate

    # Bootstrap a building (credentials from .env, lat/lon from config)
    python3 gap_filler.py --bootstrap --house-id TE236_HEM_Kontor \\
        --days 90 --resolution 5

    # Bootstrap a building with explicit credentials
    python3 gap_filler.py --bootstrap --house-id TE236_HEM_Kontor \\
        --username "Ulf Andersson" --password "xxx" \\
        --lat 56.67 --lon 12.86 --days 90
        """
    )

    # Authentication (required for gap filling, optional for building bootstrap)
    parser.add_argument('--username', help='HomeSide username (or Arrigo username for buildings)')
    parser.add_argument('--password', help='HomeSide password (or Arrigo password for buildings)')

    # Bootstrap mode
    parser.add_argument('--bootstrap', action='store_true',
                        help='Bootstrap a new house/building with 5-min Arrigo historical data')
    parser.add_argument('--source-house-id', type=str,
                        help='Source entity ID to copy energy_meter data from (bootstrap mode)')
    parser.add_argument('--arrigo-host', type=str,
                        help='Arrigo host for direct connection (buildings, overrides config)')
    parser.add_argument('--resolution', type=int, default=5,
                        help='Data resolution in minutes for bootstrap (default: 5)')
    parser.add_argument('--days', type=int, default=90,
                        help='Days of history for bootstrap (default: 90)')
    parser.add_argument('--no-calibrate', action='store_true',
                        help='Skip calibration after bootstrap (data import only)')
    parser.add_argument('--backtest-days', type=int, default=10,
                        help='Days to reserve for prediction backtest (default: 10)')

    # Time range options
    parser.add_argument('--from-midnight', action='store_true',
                        help='Fill from midnight (Swedish time) until now')
    parser.add_argument('--last-hours', type=int,
                        help='Fill gaps in the last N hours')
    parser.add_argument('--start', help='Start time (YYYY-MM-DD HH:MM, Swedish time)')
    parser.add_argument('--end', help='End time (YYYY-MM-DD HH:MM, Swedish time)')

    # InfluxDB options
    parser.add_argument('--influx-url', default='http://localhost:8086')
    parser.add_argument('--influx-token', default=os.getenv('INFLUXDB_TOKEN', ''))
    parser.add_argument('--influx-org', default=os.getenv('INFLUXDB_ORG', 'bvpro'))
    parser.add_argument('--influx-bucket', default='heating')
    parser.add_argument('--house-id', help='House ID (auto-detected if not specified)')

    # Location options (for weather gap filling)
    parser.add_argument('--lat', type=float, help='Latitude for weather data')
    parser.add_argument('--lon', type=float, help='Longitude for weather data')
    parser.add_argument('--weather-only', action='store_true', help='Only fill weather gaps (skip heating)')
    parser.add_argument('--heating-only', action='store_true', help='Only fill heating gaps (skip weather)')

    # Options
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--detect-only', action='store_true', help='Only detect gaps, do not fill')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--interval', type=int, default=15,
                        help='Expected data interval in minutes (default: 15)')

    args = parser.parse_args()

    # Bootstrap mode - separate code path
    if args.bootstrap:
        if not args.house_id:
            parser.error("--bootstrap requires --house-id")
        # Buildings can get lat/lon and credentials from config/.env
        is_building = os.path.exists(os.path.join("buildings", f"{args.house_id}.json"))
        if not is_building:
            # Houses require credentials and location
            if not args.username or not args.password:
                parser.error("--bootstrap for houses requires --username and --password")
            if not args.lat or not args.lon:
                parser.error("--bootstrap for houses requires --lat and --lon")
        run_bootstrap(args)
        return

    # Gap filling mode requires credentials
    if not args.username or not args.password:
        parser.error("Gap filling requires --username and --password")

    # Determine time range
    now = datetime.now(SWEDISH_TZ)

    if args.from_midnight:
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = now
    elif args.last_hours:
        end_time = now
        start_time = now - timedelta(hours=args.last_hours)
    elif args.start and args.end:
        start_time = datetime.strptime(args.start, "%Y-%m-%d %H:%M").replace(tzinfo=SWEDISH_TZ)
        end_time = datetime.strptime(args.end, "%Y-%m-%d %H:%M").replace(tzinfo=SWEDISH_TZ)
    else:
        parser.error("Specify --from-midnight, --last-hours, or both --start and --end")

    # Convert to UTC for queries
    start_utc = start_time.astimezone(timezone.utc)
    end_utc = end_time.astimezone(timezone.utc)

    print("=" * 60)
    print("Gap Filler")
    print("=" * 60)
    print(f"\nTime range (Swedish): {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"Time range (UTC):     {start_utc.strftime('%Y-%m-%d %H:%M')} to {end_utc.strftime('%Y-%m-%d %H:%M')}")

    # Get house_id from Arrigo if not specified
    house_id = args.house_id
    if not house_id:
        # Authenticate to get house info
        client = ArrigoHistoricalClient(
            username=args.username,
            password=args.password,
            verbose=args.verbose
        )
        if client.authenticate() and client.select_house(0):
            if client.client_id:
                parts = client.client_id.split('/')
                house_id = parts[-1] if parts else client.client_id
        else:
            print("ERROR: Could not determine house_id")
            sys.exit(1)

    print(f"House ID: {house_id}")

    total_written = 0
    total_skipped = 0
    total_errors = 0

    # === HEATING DATA ===
    if not args.weather_only:
        print("\n--- Heating Data ---")
        filler = GapFiller(
            influx_url=args.influx_url,
            influx_token=args.influx_token,
            influx_org=args.influx_org,
            influx_bucket=args.influx_bucket,
            house_id=house_id,
            arrigo_username=args.username,
            arrigo_password=args.password,
            verbose=args.verbose
        )

        if args.detect_only:
            # Just detect gaps
            gaps = filler.detect_gaps(start_utc, end_utc, args.interval)
            if gaps:
                print(f"Found {len(gaps)} heating gap(s):")
                for gap_start, gap_end in gaps:
                    duration = (gap_end - gap_start).total_seconds() / 60
                    gs = gap_start.astimezone(SWEDISH_TZ)
                    ge = gap_end.astimezone(SWEDISH_TZ)
                    print(f"  {gs.strftime('%Y-%m-%d %H:%M')} to {ge.strftime('%Y-%m-%d %H:%M')} ({duration:.0f} min)")
            else:
                print("No heating gaps found")
        else:
            # Fill gaps
            written, skipped, errors = filler.fill_gaps_in_range(
                start_utc, end_utc, args.interval, dry_run=args.dry_run
            )
            total_written += written
            total_skipped += skipped
            total_errors += errors

        filler.close()

    # === WEATHER DATA ===
    if not args.heating_only and args.lat and args.lon:
        print("\n--- Weather Data ---")
        weather_filler = WeatherGapFiller(
            influx_url=args.influx_url,
            influx_token=args.influx_token,
            influx_org=args.influx_org,
            influx_bucket=args.influx_bucket,
            house_id=house_id,
            latitude=args.lat,
            longitude=args.lon,
            logger=DummyLogger()
        )

        if args.detect_only:
            gaps = weather_filler.detect_weather_gaps(start_utc, end_utc, args.interval)
            if gaps:
                print(f"Found {len(gaps)} weather gap(s):")
                for gap_start, gap_end in gaps:
                    duration = (gap_end - gap_start).total_seconds() / 60
                    gs = gap_start.astimezone(SWEDISH_TZ)
                    ge = gap_end.astimezone(SWEDISH_TZ)
                    print(f"  {gs.strftime('%Y-%m-%d %H:%M')} to {ge.strftime('%Y-%m-%d %H:%M')} ({duration:.0f} min)")
            else:
                print("No weather gaps found")
        else:
            written, skipped, errors = weather_filler.fill_weather_gaps(
                start_utc, end_utc, args.interval, dry_run=args.dry_run
            )
            total_written += written
            total_skipped += skipped
            total_errors += errors

        weather_filler.close()
    elif not args.heating_only and not (args.lat and args.lon):
        print("\n--- Weather Data ---")
        print("Skipped (no --lat/--lon provided)")

    # Summary
    if not args.detect_only:
        print(f"\n{'='*60}")
        print(f"Summary:")
        print(f"  Written: {total_written}")
        print(f"  Skipped: {total_skipped}")
        print(f"  Errors:  {total_errors}")
    print()


if __name__ == "__main__":
    main()

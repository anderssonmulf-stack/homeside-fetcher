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

import os
import sys
import argparse
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
        self.client = InfluxDBClient(url=url, token=token, org=org)
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
                org=self.influx_org
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

        self.client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
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
        Various InfluxDB and Arrigo credentials
        settings: Application settings dict
        logger: Logger instance
        latitude: Location latitude for weather data (optional)
        longitude: Location longitude for weather data (optional)

    Returns:
        True if gaps were filled successfully (or no gaps found)
    """
    if not username or not password:
        logger.info("Gap filling skipped - no Arrigo credentials")
        return True

    try:
        # Check for gaps in last 24 hours
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)

        expected_interval = settings.get('data_collection', {}).get('heating_data_interval_minutes', 15)

        # === HEATING DATA GAPS ===
        filler = GapFiller(
            influx_url=influx_url,
            influx_token=influx_token,
            influx_org=influx_org,
            influx_bucket=influx_bucket,
            house_id=house_id,
            arrigo_username=username,
            arrigo_password=password,
            verbose=False
        )

        # Detect heating data gaps
        gaps = filler.detect_gaps(start_time, end_time, expected_interval)

        heating_written = 0
        heating_errors = 0

        if not gaps:
            logger.info("No heating data gaps detected in last 24 hours")
            print("✓ No heating data gaps in last 24 hours")
        else:
            # Calculate total gap duration
            total_gap_minutes = sum(
                (gap_end - gap_start).total_seconds() / 60
                for gap_start, gap_end in gaps
            )

            logger.info(f"Detected {len(gaps)} heating gap(s) totaling {total_gap_minutes:.0f} minutes")
            print(f"ℹ️  Heating: {len(gaps)} gap(s) ({total_gap_minutes:.0f} minutes)")

            # Fill the heating gaps
            heating_written, _, heating_errors = filler.fill_gaps_in_range(
                start_time, end_time, expected_interval, dry_run=False
            )

        filler.close()

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
                    start_time, end_time, expected_interval, dry_run=False
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


def main():
    parser = argparse.ArgumentParser(
        description='Fill gaps in InfluxDB heating and weather data',
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
        """
    )

    # Authentication
    parser.add_argument('--username', required=True, help='HomeSide username')
    parser.add_argument('--password', required=True, help='HomeSide password')

    # Time range options
    parser.add_argument('--from-midnight', action='store_true',
                        help='Fill from midnight (Swedish time) until now')
    parser.add_argument('--last-hours', type=int,
                        help='Fill gaps in the last N hours')
    parser.add_argument('--start', help='Start time (YYYY-MM-DD HH:MM, Swedish time)')
    parser.add_argument('--end', help='End time (YYYY-MM-DD HH:MM, Swedish time)')

    # InfluxDB options
    parser.add_argument('--influx-url', default='http://localhost:8086')
    parser.add_argument('--influx-token', default=os.getenv('INFLUXDB_TOKEN', 'homeside_token_2026_secret'))
    parser.add_argument('--influx-org', default='homeside')
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

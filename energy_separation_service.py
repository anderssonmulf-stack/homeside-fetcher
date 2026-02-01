#!/usr/bin/env python3
"""
Energy Separation Service

Runs energy separation for houses with energy_separation enabled in their profile.
Separates district heating energy into space heating vs domestic hot water (DHW).

Usage:
    python energy_separation_service.py                    # Process last 24 hours
    python energy_separation_service.py --hours 48         # Process last 48 hours
    python energy_separation_service.py --dry-run          # Show what would be written
    python energy_separation_service.py --house HEM_FJV_Villa_149  # Process specific house

Environment variables:
    INFLUXDB_URL    - InfluxDB URL (default: http://localhost:8086)
    INFLUXDB_TOKEN  - InfluxDB token (required)
    INFLUXDB_ORG    - InfluxDB org (default: homeside)
    INFLUXDB_BUCKET - InfluxDB bucket (default: heating)
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from dataclasses import asdict

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from customer_profile import CustomerProfile
from energy_models import get_energy_separator, EnergySeparationResult


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnergySeparationService:
    """Service to run energy separation for configured houses."""

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        profiles_dir: str = "profiles",
        dry_run: bool = False
    ):
        self.influx_url = influx_url
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.profiles_dir = profiles_dir
        self.dry_run = dry_run

        # Initialize InfluxDB client
        self.client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org
        )
        self.query_api = self.client.query_api()

        if not dry_run:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        else:
            self.write_api = None
            logger.info("Dry run mode - no data will be written")

    def get_enabled_houses(self) -> List[CustomerProfile]:
        """Get all houses with energy separation enabled."""
        enabled = []

        if not os.path.exists(self.profiles_dir):
            logger.warning(f"Profiles directory not found: {self.profiles_dir}")
            return enabled

        for filename in os.listdir(self.profiles_dir):
            if not filename.endswith('.json'):
                continue

            try:
                profile = CustomerProfile.load_by_path(
                    os.path.join(self.profiles_dir, filename)
                )

                if profile.energy_separation.enabled:
                    enabled.append(profile)
                    logger.info(f"Energy separation enabled for {profile.friendly_name} ({profile.customer_id})")

            except Exception as e:
                logger.warning(f"Failed to load profile {filename}: {e}")

        return enabled

    def fetch_hot_water_temps(
        self,
        house_id: str,
        hours: int = 24
    ) -> List[Dict]:
        """Fetch hot water temperature data from InfluxDB."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{hours}h)
            |> filter(fn: (r) => r["_measurement"] == "heating_system")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> filter(fn: (r) => r["_field"] == "hot_water_temp")
            |> sort(columns: ["_time"])
        '''

        try:
            tables = self.query_api.query(query, org=self.influx_org)

            results = []
            for table in tables:
                for record in table.records:
                    results.append({
                        'timestamp': record.get_time(),
                        'value': record.get_value()
                    })

            logger.info(f"Fetched {len(results)} hot water temp readings for {house_id}")
            return results

        except Exception as e:
            logger.error(f"Failed to fetch hot water temps: {e}")
            return []

    def fetch_energy_data(
        self,
        house_id: str,
        hours: int = 24
    ) -> List[Dict]:
        """Fetch energy consumption data from InfluxDB."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{hours}h)
            |> filter(fn: (r) => r["_measurement"] == "energy_meter")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> filter(fn: (r) => r["_field"] == "consumption")
            |> sort(columns: ["_time"])
        '''

        try:
            tables = self.query_api.query(query, org=self.influx_org)

            results = []
            for table in tables:
                for record in table.records:
                    results.append({
                        'timestamp': record.get_time(),
                        'consumption': record.get_value()
                    })

            logger.info(f"Fetched {len(results)} energy readings for {house_id}")
            return results

        except Exception as e:
            logger.error(f"Failed to fetch energy data: {e}")
            return []

    def write_separation_results(
        self,
        house_id: str,
        results: List[EnergySeparationResult]
    ) -> int:
        """Write separation results to InfluxDB."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would write {len(results)} separation results for {house_id}")
            for r in results:
                logger.info(f"  {r.timestamp}: total={r.total_energy_kwh:.2f} heating={r.heating_energy_kwh:.2f} dhw={r.dhw_energy_kwh:.2f} (conf={r.confidence:.2f})")
            return len(results)

        points = []
        for result in results:
            # Write to energy_separated measurement
            point = Point("energy_separated") \
                .tag("house_id", house_id) \
                .tag("method", result.method) \
                .time(result.timestamp, WritePrecision.S) \
                .field("total_energy_kwh", result.total_energy_kwh) \
                .field("heating_energy_kwh", result.heating_energy_kwh) \
                .field("dhw_energy_kwh", result.dhw_energy_kwh) \
                .field("dhw_event_count", len(result.dhw_events)) \
                .field("confidence", result.confidence)

            points.append(point)

            # Also write individual DHW events if any
            for event in result.dhw_events:
                event_point = Point("dhw_event") \
                    .tag("house_id", house_id) \
                    .time(event.start_time, WritePrecision.S) \
                    .field("duration_minutes", event.duration_minutes) \
                    .field("peak_temp", event.peak_temp) \
                    .field("temp_rise", event.temp_rise) \
                    .field("estimated_energy_kwh", event.estimated_energy_kwh)

                points.append(event_point)

        if points:
            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            logger.info(f"Wrote {len(points)} points for {house_id}")

        return len(results)

    def process_house(
        self,
        profile: CustomerProfile,
        hours: int = 24
    ) -> int:
        """Process energy separation for a single house."""
        house_id = profile.customer_id
        config = profile.energy_separation

        logger.info(f"Processing {profile.friendly_name} ({house_id}) with method: {config.method}")

        # Fetch data
        hot_water_temps = self.fetch_hot_water_temps(house_id, hours)
        energy_data = self.fetch_energy_data(house_id, hours)

        if not hot_water_temps:
            logger.warning(f"No hot water temperature data for {house_id}")
            return 0

        if not energy_data:
            logger.warning(f"No energy data for {house_id}")
            return 0

        # Get separator with house-specific config
        separator_config = {
            'dhw_temp_threshold': config.dhw_temp_threshold,
            'dhw_temp_rise_threshold': config.dhw_temp_rise_threshold,
            'dhw_baseline_temp': config.dhw_baseline_temp,
            'avg_dhw_power_kw': config.avg_dhw_power_kw,
            'cold_water_temp': config.cold_water_temp,
            'hot_water_target_temp': config.hot_water_target_temp,
        }

        try:
            separator = get_energy_separator(config.method, separator_config)
        except ValueError as e:
            logger.error(f"Invalid separation method for {house_id}: {e}")
            return 0

        # Run separation
        results = separator.separate_energy(
            energy_data=energy_data,
            hot_water_temps=hot_water_temps,
            period_hours=24  # Daily aggregation
        )

        if not results:
            logger.warning(f"No separation results for {house_id}")
            return 0

        # Write results
        return self.write_separation_results(house_id, results)

    def run(
        self,
        hours: int = 24,
        house_filter: Optional[str] = None
    ) -> Dict:
        """
        Run energy separation for all enabled houses.

        Args:
            hours: Number of hours of data to process
            house_filter: Optional specific house_id to process

        Returns:
            Summary of processing
        """
        if house_filter:
            # Process specific house
            try:
                profile = CustomerProfile.load(house_filter, self.profiles_dir)
                if not profile.energy_separation.enabled:
                    logger.warning(f"Energy separation not enabled for {house_filter}")
                    return {'houses': 0, 'records': 0}

                count = self.process_house(profile, hours)
                return {'houses': 1, 'records': count}

            except FileNotFoundError:
                logger.error(f"House profile not found: {house_filter}")
                return {'houses': 0, 'records': 0, 'error': 'House not found'}

        # Process all enabled houses
        houses = self.get_enabled_houses()

        if not houses:
            logger.info("No houses with energy separation enabled")
            return {'houses': 0, 'records': 0}

        total_records = 0
        for profile in houses:
            try:
                count = self.process_house(profile, hours)
                total_records += count
            except Exception as e:
                logger.error(f"Failed to process {profile.customer_id}: {e}")

        return {'houses': len(houses), 'records': total_records}

    def close(self):
        """Close connections."""
        if self.client:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(
        description='Run energy separation for configured houses'
    )
    parser.add_argument(
        '--hours', type=int, default=24,
        help='Hours of data to process (default: 24)'
    )
    parser.add_argument(
        '--house', type=str,
        help='Process specific house (customer_id)'
    )
    parser.add_argument(
        '--profiles-dir', default='profiles',
        help='Directory containing customer profiles'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be written without writing'
    )
    args = parser.parse_args()

    # Get configuration from environment
    influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.getenv('INFLUXDB_BUCKET', 'heating')

    if not influx_token and not args.dry_run:
        print("ERROR: INFLUXDB_TOKEN environment variable not set")
        sys.exit(1)

    service = EnergySeparationService(
        influx_url=influx_url,
        influx_token=influx_token or '',
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        profiles_dir=args.profiles_dir,
        dry_run=args.dry_run
    )

    try:
        result = service.run(hours=args.hours, house_filter=args.house)
        print(f"\nProcessed {result['houses']} house(s), {result['records']} record(s)")
    finally:
        service.close()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Dropbox Sync - Meter Request File Manager

Manages the meters.json request file in Dropbox that tells the work server
which meters to export data for and from what date.

The request file structure:
{
    "meters": [
        {
            "meter_id": "12345678",
            "from_date": "2026-02-01",
            "house_name": "Daggis8"
        }
    ],
    "updated_at": "2026-02-03T07:55:00Z"
}

Usage:
    from dropbox_sync import MeterRequestManager

    manager = MeterRequestManager()
    manager.sync_meters_to_dropbox()  # After profile changes or imports
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from customer_profile import get_meter_ids_from_env
from dropbox_client import DropboxClient, create_client_from_env

logger = logging.getLogger(__name__)

# Default: request data starting 90 days ago for new meters (3 months)
DEFAULT_LOOKBACK_DAYS = 90

# Request file path in Dropbox (CSV format for easy parsing by work server)
REQUEST_FILE_PATH = '/data/SvenskEB_DH.csv'


class MeterRequestManager:
    """
    Manages the meter request file that tells the work server which
    meters to export data for.
    """

    def __init__(
        self,
        dropbox_client: Optional[DropboxClient] = None,
        profiles_dir: str = 'profiles',
        influx_url: Optional[str] = None,
        influx_token: Optional[str] = None,
        influx_org: str = 'homeside',
        influx_bucket: str = 'heating'
    ):
        """
        Initialize the meter request manager.

        Args:
            dropbox_client: DropboxClient instance (auto-created from env if None)
            profiles_dir: Directory containing customer profiles
            influx_url: InfluxDB URL for querying last import dates
            influx_token: InfluxDB token
            influx_org: InfluxDB organization
            influx_bucket: InfluxDB bucket
        """
        self.dropbox = dropbox_client or create_client_from_env()
        self.profiles_dir = profiles_dir

        # InfluxDB settings for querying last import dates
        self.influx_url = influx_url or os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        self.influx_token = influx_token or os.getenv('INFLUXDB_TOKEN')
        self.influx_org = influx_org or os.getenv('INFLUXDB_ORG', 'homeside')
        self.influx_bucket = influx_bucket or os.getenv('INFLUXDB_BUCKET', 'heating')

        self._influx_client = None

    def _get_influx_client(self):
        """Lazy-initialize InfluxDB client."""
        if self._influx_client is None and self.influx_token:
            try:
                from influxdb_client import InfluxDBClient
                self._influx_client = InfluxDBClient(
                    url=self.influx_url,
                    token=self.influx_token,
                    org=self.influx_org
                )
            except ImportError:
                logger.warning("influxdb_client not installed - cannot query last import dates")
            except Exception as e:
                logger.warning(f"Failed to connect to InfluxDB: {e}")

        return self._influx_client

    def load_all_profiles(self) -> List[Dict]:
        """
        Load all customer profiles that have meter_ids configured.

        Returns:
            List of profile dicts with meter_ids
        """
        profiles = []

        if not os.path.exists(self.profiles_dir):
            logger.warning(f"Profiles directory not found: {self.profiles_dir}")
            return profiles

        for filename in os.listdir(self.profiles_dir):
            if not filename.endswith('.json') or '_signals.json' in filename:
                continue

            filepath = os.path.join(self.profiles_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)

                customer_id = data.get('customer_id', '')
                # Meter IDs only from env vars (HOUSE_<id>_METER_IDS) — never from profile JSON
                meter_ids = get_meter_ids_from_env(customer_id)
                if meter_ids:
                    profiles.append({
                        'customer_id': data.get('customer_id', ''),
                        'friendly_name': data.get('friendly_name', ''),
                        'meter_ids': meter_ids,
                        'energy_data_start_date': data.get('energy_data_start_date')
                    })
            except Exception as e:
                logger.error(f"Error loading profile {filename}: {e}")

        return profiles

    def get_last_import_date(self, meter_id: str) -> Optional[datetime]:
        """
        Query InfluxDB for the latest data point for this meter.

        Args:
            meter_id: The energy meter ID

        Returns:
            datetime of last data point, or None if no data
        """
        client = self._get_influx_client()
        if not client:
            return None

        try:
            query_api = client.query_api()

            # Query for the latest timestamp for this meter
            query = f'''
                from(bucket: "{self.influx_bucket}")
                    |> range(start: -365d)
                    |> filter(fn: (r) => r._measurement == "energy_meter")
                    |> filter(fn: (r) => r.meter_id == "{meter_id}")
                    |> group()
                    |> last()
            '''

            tables = query_api.query(query, org=self.influx_org)

            for table in tables:
                for record in table.records:
                    # Return the timestamp of the last record
                    return record.get_time()

        except Exception as e:
            logger.warning(f"Failed to query last import date for meter {meter_id}: {e}")

        return None

    def build_request_list(self) -> List[Dict]:
        """
        Build the list of meter requests from all profiles.

        Returns:
            List of meter request dicts
        """
        profiles = self.load_all_profiles()
        requests = []

        for profile in profiles:
            for meter_id in profile['meter_ids']:
                meter_id = str(meter_id).strip()
                if not meter_id:
                    continue

                # Get last import timestamp
                last_timestamp = self.get_last_import_date(meter_id)

                if last_timestamp:
                    # Request data from the hour after last import
                    next_hour = last_timestamp + timedelta(hours=1)
                    from_datetime = next_hour.strftime('%Y-%m-%d %H:00')
                elif profile.get('energy_data_start_date'):
                    # Use profile-specified start date for new meters
                    from_datetime = f"{profile['energy_data_start_date']} 00:00"
                else:
                    # New meter - request DEFAULT_LOOKBACK_DAYS of historical data
                    start = datetime.now(timezone.utc) - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                    from_datetime = start.strftime('%Y-%m-%d %H:00')

                requests.append({
                    'meter_id': meter_id,
                    'from_datetime': from_datetime,
                    'house_name': profile['friendly_name'],
                    'customer_id': profile['customer_id']
                })

        return requests

    def sync_meters_to_dropbox(self) -> bool:
        """
        Sync the meter request file to Dropbox.

        Reads all profiles with meter_ids and writes the request file
        as CSV with the list of meters and their from-dates.

        Returns:
            True if successful, False otherwise
        """
        if not self.dropbox:
            logger.error("Dropbox client not configured - cannot sync meters")
            return False

        try:
            requests = self.build_request_list()

            if not requests:
                logger.info("No meters configured in any profile - nothing to sync")
                return True

            # Ensure data folder exists
            self.dropbox.ensure_folders('/data')

            # Build CSV content (semicolon-separated to match Swedish conventions)
            lines = ['meter_id;from_datetime;house_name']
            for req in requests:
                lines.append(f"{req['meter_id']};{req['from_datetime']};{req['house_name']}")

            content = '\n'.join(lines)
            self.dropbox.write_file(REQUEST_FILE_PATH, content)

            logger.info(f"Synced {len(requests)} meter(s) to Dropbox {REQUEST_FILE_PATH}")

            # Log details
            for req in requests:
                logger.debug(f"  Meter {req['meter_id']} ({req['house_name']}): from {req['from_datetime']}")

            return True

        except Exception as e:
            logger.error(f"Failed to sync meters to Dropbox: {e}")
            return False

    def read_current_requests(self) -> Optional[List[Dict]]:
        """
        Read the current request file from Dropbox.

        Returns:
            List of meter request dicts, or None if not found
        """
        if not self.dropbox:
            return None

        try:
            content = self.dropbox.read_file(REQUEST_FILE_PATH)
            lines = content.strip().split('\n')
            if len(lines) < 2:
                return []

            # Parse CSV (skip header)
            requests = []
            for line in lines[1:]:
                parts = line.split(';')
                if len(parts) >= 3:
                    requests.append({
                        'meter_id': parts[0],
                        'from_datetime': parts[1],
                        'house_name': parts[2]
                    })
            return requests
        except Exception as e:
            logger.warning(f"Could not read request file: {e}")
            return None

    def close(self):
        """Close InfluxDB connection."""
        if self._influx_client:
            self._influx_client.close()
            self._influx_client = None


def sync_meters():
    """
    Convenience function to sync meters to Dropbox.

    Can be called from other modules after profile changes.
    """
    manager = MeterRequestManager()
    try:
        return manager.sync_meters_to_dropbox()
    finally:
        manager.close()


def main():
    """CLI entry point for manual sync."""
    import argparse

    parser = argparse.ArgumentParser(description='Sync meter requests to Dropbox')
    parser.add_argument('--profiles-dir', default='profiles', help='Profiles directory')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be synced')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    manager = MeterRequestManager(profiles_dir=args.profiles_dir)

    try:
        if args.dry_run:
            print("Dry run - showing what would be synced:")
            print()
            requests = manager.build_request_list()

            if not requests:
                print("No meters configured in any profile")
                return

            print(f"Found {len(requests)} meter(s):")
            print()
            for req in requests:
                print(f"  {req['meter_id']}")
                print(f"    House: {req['house_name']} ({req['customer_id']})")
                print(f"    From:  {req['from_datetime']}")
                print()

            print(f"CSV file that would be written to {REQUEST_FILE_PATH}:")
            print()
            print("meter_id;from_datetime;house_name")
            for req in requests:
                print(f"{req['meter_id']};{req['from_datetime']};{req['house_name']}")

        else:
            success = manager.sync_meters_to_dropbox()
            if success:
                print("Successfully synced meters to Dropbox")
            else:
                print("Failed to sync meters")
                exit(1)

    finally:
        manager.close()


if __name__ == '__main__':
    main()

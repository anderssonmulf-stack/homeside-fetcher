#!/usr/bin/env python3
"""
Energy File Importer

Imports energy data files from Dropbox into InfluxDB.
Files are expected to be semicolon-separated with a header row.

Usage:
    python energy_importer.py              # Process all files in incoming/
    python energy_importer.py --dry-run    # Show what would be imported without writing

Environment variables:
    DROPBOX_ACCESS_TOKEN  - Dropbox API token (required)
    INFLUXDB_URL          - InfluxDB URL (default: http://localhost:8086)
    INFLUXDB_TOKEN        - InfluxDB token (required)
    INFLUXDB_ORG          - InfluxDB org (default: homeside)
    INFLUXDB_BUCKET       - InfluxDB bucket (default: heating)
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from io import StringIO

import dropbox
from dropbox.files import FileMetadata
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from customer_profile import build_meter_mapping


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Column name mappings (header name -> InfluxDB field name)
# Add more mappings as needed
COLUMN_MAPPINGS = {
    # ID columns (used as tags)
    'id': 'meter_id',
    'meter_id': 'meter_id',
    'meterid': 'meter_id',

    # Timestamp columns
    'timestamp': 'timestamp',
    'time': 'timestamp',
    'datetime': 'timestamp',
    'datum': 'timestamp',
    'tidpunkt': 'timestamp',

    # Meter reading (cumulative)
    'meterstand': 'meter_reading',
    'meter_reading': 'meter_reading',
    'meterreading': 'meter_reading',
    'cumulative': 'meter_reading',
    'total': 'meter_reading',

    # Consumption (hourly energy)
    'consumption': 'consumption',
    'energy': 'consumption',
    'kwh': 'consumption',
    'mwh': 'consumption',
    'forbrukning': 'consumption',

    # Flow (volume flow rate)
    'flow': 'flow',
    'flode': 'flow',
    'volume_flow': 'flow',
    'm3h': 'flow',

    # Temperature in (supply/forward)
    'tempin': 'temp_in',
    'temp_in': 'temp_in',
    'supply_temp': 'temp_in',
    'forward_temp': 'temp_in',
    'framledning': 'temp_in',
    't_in': 'temp_in',

    # Temperature out (return)
    'tempout': 'temp_out',
    'temp_out': 'temp_out',
    'return_temp': 'temp_out',
    'returledning': 'temp_out',
    't_out': 'temp_out',

    # Power
    'power': 'power',
    'effect': 'power',
    'kw': 'power',
    'mw': 'power',
}


class EnergyImporter:
    """Imports energy files from Dropbox to InfluxDB."""

    def __init__(
        self,
        dropbox_token: str,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        profiles_dir: str = 'profiles',
        separator: str = ';',
        dry_run: bool = False
    ):
        self.separator = separator
        self.dry_run = dry_run
        self.profiles_dir = profiles_dir

        # Load meter_id -> customer_id mapping from profiles
        self.meter_mapping = build_meter_mapping(profiles_dir)
        if self.meter_mapping:
            logger.info(f"Loaded {len(self.meter_mapping)} meter mapping(s)")
        else:
            logger.warning("No meter mappings found - unknown meters will be rejected")

        # Initialize Dropbox client
        self.dbx = dropbox.Dropbox(dropbox_token)
        logger.info("Connected to Dropbox")

        # Initialize InfluxDB client
        if not dry_run:
            self.influx_client = InfluxDBClient(
                url=influx_url,
                token=influx_token,
                org=influx_org
            )
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            self.influx_bucket = influx_bucket
            self.influx_org = influx_org
            logger.info(f"Connected to InfluxDB: {influx_url}")
        else:
            self.influx_client = None
            self.write_api = None
            logger.info("Dry run mode - no data will be written")

    def list_incoming_files(self) -> List[FileMetadata]:
        """List files in the incoming folder."""
        try:
            result = self.dbx.files_list_folder('/incoming')
            files = [
                entry for entry in result.entries
                if isinstance(entry, FileMetadata) and entry.name.endswith('.txt')
            ]
            logger.info(f"Found {len(files)} file(s) in /incoming")
            return files
        except dropbox.exceptions.ApiError as e:
            if 'not_found' in str(e):
                logger.warning("Folder /incoming not found, creating it...")
                self.dbx.files_create_folder_v2('/incoming')
                self.dbx.files_create_folder_v2('/processed')
                self.dbx.files_create_folder_v2('/failed')
                return []
            raise

    def download_file(self, path: str) -> str:
        """Download file content from Dropbox."""
        _, response = self.dbx.files_download(path)
        content = response.content.decode('utf-8-sig')  # Handle BOM
        return content

    def parse_file(self, content: str, filename: str) -> Tuple[List[Dict], List[str]]:
        """
        Parse file content into data records.

        Returns:
            Tuple of (records, errors)
        """
        records = []
        errors = []

        lines = content.strip().split('\n')
        if len(lines) < 2:
            errors.append(f"File {filename} has no data rows")
            return records, errors

        # Parse header
        header = lines[0].strip().split(self.separator)
        header_lower = [h.strip().lower() for h in header]

        # Map columns
        column_map = {}
        for i, col in enumerate(header_lower):
            if col in COLUMN_MAPPINGS:
                column_map[i] = COLUMN_MAPPINGS[col]
            else:
                logger.debug(f"Unknown column '{col}' at position {i}")

        if 'timestamp' not in column_map.values():
            errors.append(f"No timestamp column found in {filename}")
            return records, errors

        # Parse data rows
        for line_num, line in enumerate(lines[1:], start=2):
            if not line.strip():
                continue

            try:
                values = line.strip().split(self.separator)
                record = {}

                for i, value in enumerate(values):
                    if i in column_map:
                        field_name = column_map[i]
                        value = value.strip()

                        if field_name == 'timestamp':
                            record['timestamp'] = self._parse_timestamp(value)
                        elif field_name == 'meter_id':
                            record['meter_id'] = value
                        else:
                            # Numeric field
                            try:
                                # Handle both . and , as decimal separator
                                value = value.replace(',', '.')
                                record[field_name] = float(value)
                            except ValueError:
                                logger.warning(f"Line {line_num}: Cannot parse '{value}' as number for {field_name}")

                if 'timestamp' in record:
                    records.append(record)
                else:
                    errors.append(f"Line {line_num}: Missing timestamp")

            except Exception as e:
                errors.append(f"Line {line_num}: {str(e)}")

        return records, errors

    def _parse_timestamp(self, value: str) -> datetime:
        """Parse timestamp string to datetime."""
        # Try various formats
        formats = [
            '%Y-%m-%d %H:%M:%S',  # 2026-01-28 14:00:00
            '%Y-%m-%d %H:%M',     # 2026-01-28 14:00
            '%Y-%m-%dT%H:%M:%S',  # ISO format
            '%Y-%m-%dT%H:%M',     # ISO without seconds
            '%d/%m/%Y %H:%M:%S',  # European
            '%d/%m/%Y %H:%M',     # European without seconds
            '%Y%m%d%H%M%S',       # Compact
            '%Y%m%d%H%M',         # Compact without seconds
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(value, fmt)
                # Assume Swedish time (CET/CEST) if no timezone
                # For simplicity, store as-is (local time interpretation)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        raise ValueError(f"Cannot parse timestamp: {value}")

    def write_to_influx(self, records: List[Dict], meter_id: str) -> Tuple[int, List[str]]:
        """
        Write records to InfluxDB.

        Uses meter_mapping to translate meter_id -> customer_id (house_id).
        Records with unknown meter_ids are rejected.

        Returns:
            Tuple of (records_written, errors)
        """
        errors = []

        # Look up customer_id for this meter
        customer_id = self.meter_mapping.get(str(meter_id).strip())

        if not customer_id:
            error_msg = f"Unknown meter_id '{meter_id}' - not mapped to any customer. Configure meter_ids in the customer profile."
            logger.warning(error_msg)
            return 0, [error_msg]

        if self.dry_run:
            logger.info(f"[DRY RUN] Would write {len(records)} records for meter {meter_id} -> house {customer_id}")
            return len(records), []

        points = []
        for record in records:
            point = Point("energy_meter") \
                .tag("house_id", customer_id) \
                .tag("meter_id", meter_id) \
                .time(record['timestamp'], WritePrecision.S)

            # Add all numeric fields
            for field, value in record.items():
                if field not in ('timestamp', 'meter_id') and isinstance(value, (int, float)):
                    point = point.field(field, value)

            points.append(point)

        if points:
            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            logger.info(f"Wrote {len(points)} records for meter {meter_id} -> house {customer_id}")

        return len(points), []

    def move_file(self, from_path: str, to_folder: str):
        """Move file to another folder."""
        filename = from_path.split('/')[-1]
        to_path = f"/{to_folder}/{filename}"

        # Add timestamp to avoid conflicts
        if to_folder in ('processed', 'failed'):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            name, ext = filename.rsplit('.', 1)
            to_path = f"/{to_folder}/{name}_{timestamp}.{ext}"

        try:
            self.dbx.files_move_v2(from_path, to_path)
            logger.info(f"Moved {from_path} -> {to_path}")
        except dropbox.exceptions.ApiError as e:
            logger.error(f"Failed to move file: {e}")

    def process_file(self, file_meta: FileMetadata) -> Tuple[int, List[str]]:
        """Process a single file."""
        path = file_meta.path_display
        logger.info(f"Processing: {path}")

        # Download
        content = self.download_file(path)

        # Parse
        records, errors = self.parse_file(content, file_meta.name)

        if errors:
            for error in errors:
                logger.warning(f"  {error}")

        if not records:
            logger.warning(f"No valid records in {file_meta.name}")
            if not self.dry_run:
                self.move_file(path, 'failed')
            return 0, errors

        # Get meter ID from first record, or use filename
        meter_id = records[0].get('meter_id', file_meta.name.replace('.txt', ''))

        # Write to InfluxDB (will reject unknown meters)
        count, write_errors = self.write_to_influx(records, meter_id)
        errors.extend(write_errors)

        # Move to appropriate folder
        if not self.dry_run:
            if count > 0:
                self.move_file(path, 'processed')
            else:
                # No records written (unknown meter) - move to failed
                self.move_file(path, 'failed')

        return count, errors

    def run(self) -> Dict:
        """Process all incoming files."""
        files = self.list_incoming_files()

        if not files:
            logger.info("No files to process")
            return {'files': 0, 'records': 0, 'errors': []}

        total_records = 0
        all_errors = []

        for file_meta in files:
            count, errors = self.process_file(file_meta)
            total_records += count
            all_errors.extend(errors)

        return {
            'files': len(files),
            'records': total_records,
            'errors': all_errors
        }

    def close(self):
        """Close connections."""
        if self.influx_client:
            self.influx_client.close()


def main():
    parser = argparse.ArgumentParser(description='Import energy files from Dropbox to InfluxDB')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be imported without writing')
    parser.add_argument('--separator', default=';', help='Column separator (default: ;)')
    parser.add_argument('--profiles-dir', default='profiles', help='Directory containing customer profiles (default: profiles)')
    args = parser.parse_args()

    # Get configuration from environment
    dropbox_token = os.getenv('DROPBOX_ACCESS_TOKEN')
    if not dropbox_token:
        print("ERROR: DROPBOX_ACCESS_TOKEN environment variable not set")
        sys.exit(1)

    influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.getenv('INFLUXDB_BUCKET', 'heating')

    if not args.dry_run and not influx_token:
        print("ERROR: INFLUXDB_TOKEN environment variable not set")
        sys.exit(1)

    # Run importer
    importer = EnergyImporter(
        dropbox_token=dropbox_token,
        influx_url=influx_url,
        influx_token=influx_token or '',
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        profiles_dir=args.profiles_dir,
        separator=args.separator,
        dry_run=args.dry_run
    )

    try:
        result = importer.run()
        print(f"\nProcessed {result['files']} file(s), {result['records']} record(s)")
        if result['errors']:
            print(f"Warnings: {len(result['errors'])}")
    finally:
        importer.close()


if __name__ == '__main__':
    main()

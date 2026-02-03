#!/usr/bin/env python3
"""
Energy File Importer

Imports energy data files from Dropbox into InfluxDB.
Files are expected to be semicolon-separated with a header row.

Usage:
    python energy_importer.py              # Process all files in /data/
    python energy_importer.py --dry-run    # Show what would be imported without writing

Environment variables:
    DROPBOX_APP_KEY       - Dropbox app key (for refresh token auth)
    DROPBOX_APP_SECRET    - Dropbox app secret (for refresh token auth)
    DROPBOX_REFRESH_TOKEN - Dropbox refresh token (recommended)
    DROPBOX_ACCESS_TOKEN  - Dropbox API token (legacy, deprecated)
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
from dropbox_client import DropboxClient, create_client_from_env
from seq_logger import SeqLogger


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
    'serviceid': 'meter_id',  # SvenskEB export format

    # Timestamp columns
    'timestamp': 'timestamp',
    'time': 'timestamp',
    'datetime': 'timestamp',
    'datum': 'timestamp',
    'tidpunkt': 'timestamp',

    # Meter reading (cumulative kWh)
    'meterstand': 'meter_reading',
    'meter_reading': 'meter_reading',
    'meterreading': 'meter_reading',
    'cumulative': 'meter_reading',
    'total': 'meter_reading',

    # Consumption (hourly energy kWh)
    'consumption': 'consumption',
    'energy': 'consumption',
    'kwh': 'consumption',
    'mwh': 'consumption',
    'forbrukning': 'consumption',

    # Volume (m³ per hour)
    'volume': 'volume',
    'flow': 'volume',
    'flode': 'volume',
    'volume_flow': 'volume',
    'm3h': 'volume',
    'm3': 'volume',

    # PRIMARY side temperatures (from district heating utility, NOT house side)
    # These are the temps at the heat exchanger utility side
    'temperaturein': 'primary_temp_in',   # Hot water from utility
    'temperatureout': 'primary_temp_out', # Cooled water back to utility
    'tempin': 'primary_temp_in',
    'tempout': 'primary_temp_out',
    'temp_in': 'primary_temp_in',
    'temp_out': 'primary_temp_out',
    't_in': 'primary_temp_in',
    't_out': 'primary_temp_out',
    'framledning_primar': 'primary_temp_in',
    'returledning_primar': 'primary_temp_out',

    # Power
    'power': 'power',
    'effect': 'power',
    'kw': 'power',
    'mw': 'power',
}

# Values that represent missing/null data
MISSING_VALUES = {'-', '', 'null', 'NULL', 'None', 'N/A', 'n/a', '#N/A'}


class EnergyImporter:
    """Imports energy files from Dropbox to InfluxDB."""

    def __init__(
        self,
        dropbox_client: Optional[DropboxClient] = None,
        dropbox_token: str = None,  # Legacy: deprecated, use dropbox_client
        influx_url: str = None,
        influx_token: str = None,
        influx_org: str = None,
        influx_bucket: str = None,
        profiles_dir: str = 'profiles',
        separator: str = ';',
        dry_run: bool = False,
        seq_logger: Optional[SeqLogger] = None
    ):
        self.separator = separator
        self.dry_run = dry_run
        self.profiles_dir = profiles_dir
        self.seq = seq_logger

        # Load meter_id -> customer_id mapping from profiles
        self.meter_mapping = build_meter_mapping(profiles_dir)
        if self.meter_mapping:
            logger.info(f"Loaded {len(self.meter_mapping)} meter mapping(s)")
        else:
            logger.warning("No meter mappings found - unknown meters will be rejected")

        # Initialize Dropbox client
        # Prefer new DropboxClient with refresh token
        if dropbox_client:
            self._dropbox_client = dropbox_client
            self.dbx = dropbox_client.dbx
            logger.info("Using DropboxClient with refresh token")
        elif dropbox_token:
            # Legacy: use access token directly (deprecated)
            self._dropbox_client = None
            self.dbx = dropbox.Dropbox(dropbox_token)
            logger.warning("Using legacy DROPBOX_ACCESS_TOKEN - consider using refresh token auth")
        else:
            raise ValueError("Either dropbox_client or dropbox_token must be provided")
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
        """List energy data files (.txt) in /data/ folder."""
        try:
            result = self.dbx.files_list_folder('/data')
            files = [
                entry for entry in result.entries
                if isinstance(entry, FileMetadata) and entry.name.endswith('.txt')
            ]
            logger.info(f"Found {len(files)} energy file(s) in /data")
            return files
        except dropbox.exceptions.ApiError as e:
            if 'not_found' in str(e):
                logger.warning("Folder /data not found, creating it...")
                self.dbx.files_create_folder_v2('/data')
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
                            # Numeric field - skip missing values
                            if value in MISSING_VALUES:
                                continue  # Skip this field, don't include in record
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
        # Handle ISO format with Z suffix (UTC)
        if value.endswith('Z'):
            value = value[:-1]  # Strip Z suffix

        # Try various formats
        formats = [
            '%Y-%m-%dT%H:%M:%S',  # ISO format (most common from exports)
            '%Y-%m-%dT%H:%M',     # ISO without seconds
            '%Y-%m-%d %H:%M:%S',  # 2026-01-28 14:00:00
            '%Y-%m-%d %H:%M',     # 2026-01-28 14:00
            '%d/%m/%Y %H:%M:%S',  # European
            '%d/%m/%Y %H:%M',     # European without seconds
            '%Y%m%d%H%M%S',       # Compact
            '%Y%m%d%H%M',         # Compact without seconds
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(value, fmt)
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

    def delete_file(self, path: str):
        """Delete file from Dropbox after successful import."""
        try:
            self.dbx.files_delete_v2(path)
            logger.info(f"Deleted {path}")
        except dropbox.exceptions.ApiError as e:
            logger.error(f"Failed to delete file {path}: {e}")

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
            # Don't delete - leave file for investigation
            return 0, errors

        # Get meter ID from first record, or use filename
        meter_id = records[0].get('meter_id', file_meta.name.replace('.txt', ''))

        # Write to InfluxDB (will reject unknown meters)
        count, write_errors = self.write_to_influx(records, meter_id)
        errors.extend(write_errors)

        # Delete file after successful import
        if not self.dry_run and count > 0:
            self.delete_file(path)

        return count, errors

    def run(self, sync_meters: bool = True) -> Dict:
        """
        Process all incoming files.

        Args:
            sync_meters: If True, sync meter request file to Dropbox after import

        Returns:
            Dict with files, records, and errors counts
        """
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

        # Sync meters to Dropbox after successful import
        # This updates the from_date for imported meters
        if sync_meters and total_records > 0 and not self.dry_run:
            try:
                from dropbox_sync import MeterRequestManager
                manager = MeterRequestManager(
                    dropbox_client=self._dropbox_client,
                    profiles_dir=self.profiles_dir
                )
                manager.sync_meters_to_dropbox()
                manager.close()
                logger.info("Synced meter requests to Dropbox")
            except Exception as e:
                logger.warning(f"Failed to sync meters to Dropbox: {e}")

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
    parser.add_argument('--no-sync', action='store_true', help='Skip syncing meter requests to Dropbox after import')
    args = parser.parse_args()

    # Initialize Seq logger
    seq = SeqLogger(
        component='EnergyImporter',
        friendly_name='EnergyImport'
    )

    # Get configuration from environment
    # Prefer new refresh token auth, fall back to legacy access token
    dropbox_client = create_client_from_env()
    dropbox_token = os.getenv('DROPBOX_ACCESS_TOKEN')

    if not dropbox_client and not dropbox_token:
        seq.log("Dropbox not configured - missing credentials", level='Error')
        sys.exit(1)

    influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.getenv('INFLUXDB_BUCKET', 'heating')

    if not args.dry_run and not influx_token:
        seq.log("InfluxDB token not configured", level='Error')
        sys.exit(1)

    # Run importer
    importer = EnergyImporter(
        dropbox_client=dropbox_client,
        dropbox_token=dropbox_token if not dropbox_client else None,
        influx_url=influx_url,
        influx_token=influx_token or '',
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        profiles_dir=args.profiles_dir,
        separator=args.separator,
        dry_run=args.dry_run,
        seq_logger=seq
    )

    try:
        result = importer.run(sync_meters=not args.no_sync)

        if result['records'] > 0:
            seq.log(
                "Energy import completed: {Files} file(s), {Records} record(s)",
                level='Information',
                properties={
                    'Files': result['files'],
                    'Records': result['records'],
                    'Errors': len(result['errors']),
                    'DryRun': args.dry_run
                }
            )
        elif result['files'] == 0:
            # No files to process - this is normal, don't log as error
            logger.info("No energy files to import")
        else:
            seq.log(
                "Energy import failed: {Files} file(s), 0 records imported",
                level='Warning',
                properties={
                    'Files': result['files'],
                    'Errors': result['errors']
                }
            )

    except Exception as e:
        seq.log(
            "Energy import error: {Error}",
            level='Error',
            properties={'Error': str(e)}
        )
        raise
    finally:
        importer.close()


if __name__ == '__main__':
    main()

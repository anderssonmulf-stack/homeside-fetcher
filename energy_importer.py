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
# Logging handled by SeqLogger
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

        # Initialize Dropbox client
        # Prefer new DropboxClient with refresh token
        if dropbox_client:
            self._dropbox_client = dropbox_client
            self.dbx = dropbox_client.dbx
        elif dropbox_token:
            # Legacy: use access token directly (deprecated)
            self._dropbox_client = None
            self.dbx = dropbox.Dropbox(dropbox_token)
        else:
            raise ValueError("Either dropbox_client or dropbox_token must be provided")

        # Store InfluxDB settings for passing to other components
        self.influx_url = influx_url
        self.influx_token = influx_token
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket

        # Initialize InfluxDB client
        if not dry_run:
            self.influx_client = InfluxDBClient(
                url=influx_url,
                token=influx_token,
                org=influx_org,
                timeout=5_000
            )
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            self._log(f"Connected to InfluxDB: {influx_url}")
        else:
            self.influx_client = None
            self.write_api = None
            self._log("Dry run mode - no data will be written")

    def _log(self, message: str, level: str = None, properties: dict = None):
        """Log to Seq. Uses Debug level for dry-run, otherwise uses specified level."""
        if not self.seq:
            return
        # In dry-run mode, use Debug level unless explicitly specified
        if level is None:
            level = 'Debug' if self.dry_run else 'Information'
        self.seq.log(message, level=level, properties=properties)

    def _log_warning(self, message: str, properties: dict = None):
        """Log warning to Seq."""
        if self.seq:
            self.seq.log(message, level='Warning', properties=properties)

    def _log_error(self, message: str, properties: dict = None):
        """Log error to Seq."""
        if self.seq:
            self.seq.log(message, level='Error', properties=properties)

    def list_incoming_files(self) -> List[FileMetadata]:
        """List energy data files (.txt) in /data/ folder."""
        try:
            result = self.dbx.files_list_folder('/data')
            files = [
                entry for entry in result.entries
                if isinstance(entry, FileMetadata) and entry.name.endswith('.txt')
            ]
            self._log(f"Found {len(files)} energy file(s) in /data")
            return files
        except dropbox.exceptions.ApiError as e:
            if 'not_found' in str(e):
                self._log_warning("Folder /data not found, creating it...")
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
                self._log(f"Unknown column '{col}' at position {i}")

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
                                self._log_warning(f"Line {line_num}: Cannot parse '{value}' as number for {field_name}")

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
        Duplicates (records with timestamps that already exist) are skipped.

        Returns:
            Tuple of (records_written, errors)
        """
        errors = []

        # Look up customer_id for this meter
        customer_id = self.meter_mapping.get(str(meter_id).strip())

        if not customer_id:
            error_msg = f"Unknown meter_id '{meter_id}' - not mapped to any customer. Configure meter_ids in the customer profile."
            self._log_warning(error_msg, properties={'MeterId': meter_id})
            return 0, [error_msg]

        # Get existing data for this house to detect duplicates
        existing_data = self._get_existing_data(customer_id, records)

        # Filter: skip only if timestamp exists AND values are identical
        records_to_write = []
        skipped = 0
        updated = 0

        for record in records:
            ts = record['timestamp']
            if ts in existing_data:
                if self._records_match(record, existing_data[ts]):
                    # Identical data - skip
                    skipped += 1
                else:
                    # Different values - will overwrite
                    records_to_write.append(record)
                    updated += 1
            else:
                # New timestamp
                records_to_write.append(record)

        new_count = len(records_to_write) - updated

        if skipped > 0:
            self._log(f"Skipping {skipped} identical records for meter {meter_id}", properties={'MeterId': meter_id, 'Skipped': skipped})
        if updated > 0:
            self._log(f"Updating {updated} records for meter {meter_id}", properties={'MeterId': meter_id, 'Updated': updated})

        if not records_to_write:
            self._log(f"No records to write for meter {meter_id} -> house {customer_id} (all {len(records)} identical)")
            return 0, []

        if self.dry_run:
            self._log(f"[DRY RUN] Would write {len(records_to_write)} records for meter {meter_id} -> house {customer_id}",
                     properties={'MeterId': meter_id, 'HouseId': customer_id, 'New': new_count, 'Updated': updated, 'Skipped': skipped})
            return len(records_to_write), []

        points = []
        for record in records_to_write:
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
            self._log(
                f"Wrote {len(points)} records for meter {meter_id} -> house {customer_id}",
                level='Information',  # Always log actual writes as Information
                properties={
                    'HouseId': customer_id,
                    'MeterId': meter_id,
                    'RecordsWritten': len(points),
                    'NewRecords': new_count,
                    'UpdatedRecords': updated,
                    'SkippedRecords': skipped
                }
            )

        return len(points), []

    def _get_existing_data(self, house_id: str, records: List[Dict]) -> dict:
        """
        Query InfluxDB for existing data to detect duplicates.

        Returns dict mapping timestamp -> {field: value, ...}
        Only queries the time range covered by the records to be imported.
        """
        if not records:
            return {}

        # Get time range from records
        timestamps = [r['timestamp'] for r in records]
        min_time = min(timestamps)
        max_time = max(timestamps)

        # Add buffer to ensure we catch edge cases
        from datetime import timedelta
        start_time = (min_time - timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
        stop_time = (max_time + timedelta(hours=1)).isoformat().replace('+00:00', 'Z')

        query = f'''
from(bucket: "{self.influx_bucket}")
  |> range(start: {start_time}, stop: {stop_time})
  |> filter(fn: (r) => r._measurement == "energy_meter")
  |> filter(fn: (r) => r.house_id == "{house_id}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
'''

        existing = {}
        try:
            query_api = self.influx_client.query_api()
            result = query_api.query(query)
            for table in result:
                for record in table.records:
                    ts = record.get_time()
                    existing[ts] = {
                        'consumption': record.values.get('consumption'),
                        'meter_reading': record.values.get('meter_reading'),
                        'volume': record.values.get('volume'),
                        'primary_temp_in': record.values.get('primary_temp_in'),
                        'primary_temp_out': record.values.get('primary_temp_out'),
                    }
        except Exception as e:
            self._log_warning(f"Failed to query existing data: {e}")
            # Continue without dedup if query fails

        return existing

    def _records_match(self, new_record: Dict, existing: Dict) -> bool:
        """Check if new record values match existing data (within tolerance for floats)."""
        fields_to_check = ['consumption', 'meter_reading', 'volume', 'primary_temp_in', 'primary_temp_out']

        for field in fields_to_check:
            new_val = new_record.get(field)
            old_val = existing.get(field)

            # Both None or missing - match
            if new_val is None and old_val is None:
                continue

            # One is None, other isn't - no match
            if new_val is None or old_val is None:
                return False

            # Compare with small tolerance for floats
            if abs(float(new_val) - float(old_val)) > 0.001:
                return False

        return True

    def delete_file(self, path: str):
        """Delete file from Dropbox after successful import."""
        try:
            self.dbx.files_delete_v2(path)
            self._log(f"Deleted {path}")
        except dropbox.exceptions.ApiError as e:
            self._log_error(f"Failed to delete file {path}: {e}", properties={'Path': path, 'Error': str(e)})

    def process_file(self, file_meta: FileMetadata) -> Tuple[int, List[str]]:
        """Process a single file."""
        path = file_meta.path_display
        self._log(f"Processing: {path}", properties={'Path': path})

        # Download
        content = self.download_file(path)

        # Parse
        records, errors = self.parse_file(content, file_meta.name)

        if errors:
            for error in errors:
                self._log_warning(f"Parse error: {error}", properties={'File': file_meta.name})

        if not records:
            self._log_warning(f"No valid records in {file_meta.name}", properties={'File': file_meta.name})
            # Don't delete - leave file for investigation
            return 0, errors

        # Group records by meter_id
        from collections import defaultdict
        records_by_meter = defaultdict(list)
        for record in records:
            meter_id = record.get('meter_id', file_meta.name.replace('.txt', ''))
            records_by_meter[meter_id].append(record)

        # Write each meter's records to InfluxDB
        total_written = 0
        total_processed = 0
        for meter_id, meter_records in records_by_meter.items():
            count, write_errors = self.write_to_influx(meter_records, meter_id)
            total_written += count
            total_processed += len(meter_records)
            errors.extend(write_errors)

        # Delete file after successful processing (even if all records were duplicates)
        # Only keep file if there were errors or no records could be mapped to a house
        if not self.dry_run and total_processed > 0 and len(errors) == 0:
            self.delete_file(path)

        return total_written, errors

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
            self._log("No files to process")
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
                    profiles_dir=self.profiles_dir,
                    influx_url=self.influx_url,
                    influx_token=self.influx_token,
                    influx_org=self.influx_org,
                    influx_bucket=self.influx_bucket
                )
                manager.sync_meters_to_dropbox()
                manager.close()
                self._log("Synced meter requests to Dropbox")
            except Exception as e:
                self._log_warning(f"Failed to sync meters to Dropbox: {e}")

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
            # No files to process - this is normal, log at debug level
            seq.log("No energy files to import", level='Debug' if args.dry_run else 'Information')
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

"""
Energy Consumption CSV Importer for Svenskeb Settings GUI
Handles CSV parsing, validation, and InfluxDB import operations.

Expected CSV format (from Swedish district heating providers):
- Semicolon delimiter
- Swedish decimal format (comma as decimal separator)
- Timestamps in Swedish local time (start of hour period)

Example:
"Datum";"Fjärrvärme kWh"
"2025-12-01 00:00";"2,000"
"""

import csv
import io
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


class EnergyImporter:
    """Imports energy consumption data from CSV files to InfluxDB"""

    def __init__(self):
        self.url = os.environ.get('INFLUXDB_URL', 'http://influxdb:8086')
        self.token = os.environ.get('INFLUXDB_TOKEN', '')
        self.org = os.environ.get('INFLUXDB_ORG', 'homeside')
        self.bucket = os.environ.get('INFLUXDB_BUCKET', 'heating')
        self.client = None
        self._connect()

    def _connect(self):
        """Connect to InfluxDB"""
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            health = self.client.health()
            if health.status != "pass":
                print(f"InfluxDB health check failed: {health.status}")
                self.client = None
        except Exception as e:
            print(f"Failed to connect to InfluxDB: {e}")
            self.client = None

    def _parse_swedish_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse Swedish local time string and convert to UTC.

        Handles DST transitions properly using zoneinfo.
        Input format: "YYYY-MM-DD HH:MM"
        """
        # Remove quotes if present
        clean = timestamp_str.strip().strip('"')

        # Parse as naive datetime
        naive_dt = datetime.strptime(clean, "%Y-%m-%d %H:%M")

        # Localize to Swedish timezone (handles DST automatically)
        swedish_dt = naive_dt.replace(tzinfo=SWEDISH_TZ)

        # Convert to UTC for storage
        utc_dt = swedish_dt.astimezone(timezone.utc)

        return utc_dt

    def _parse_swedish_decimal(self, value_str: str) -> float:
        """
        Parse Swedish decimal format (comma as decimal separator).

        Examples:
            "2,000" -> 2.0
            "123,456" -> 123.456
            "1 234,56" -> 1234.56 (with optional thousands separator)
        """
        # Remove quotes and whitespace
        clean = value_str.strip().strip('"')
        # Remove potential thousands separator (space or period in Swedish format)
        clean = clean.replace(' ', '').replace('.', '')
        # Replace comma with period for decimal
        clean = clean.replace(',', '.')
        return float(clean)

    def parse_csv(self, file_content: bytes, energy_type: str = 'fjv_total') -> Tuple[List[Dict], List[str]]:
        """
        Parse CSV file with Swedish format.

        Args:
            file_content: Raw CSV file bytes
            energy_type: Type tag for the data (e.g., 'fjv_total')

        Returns:
            Tuple of (parsed_rows, error_messages)
            parsed_rows: List of {'timestamp': datetime (UTC), 'value': float, 'energy_type': str}
            error_messages: List of parsing error descriptions
        """
        parsed_rows = []
        errors = []

        try:
            # Decode and handle BOM
            content = file_content.decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                content = file_content.decode('latin-1')
            except Exception as e:
                errors.append(f"Could not decode file: {e}")
                return [], errors

        # Parse CSV
        reader = csv.reader(io.StringIO(content), delimiter=';')

        # Skip header row
        header = next(reader, None)
        if not header:
            errors.append("Empty file or no header row")
            return [], errors

        for line_num, row in enumerate(reader, start=2):
            if len(row) < 2:
                errors.append(f"Line {line_num}: Not enough columns")
                continue

            timestamp_str = row[0]
            value_str = row[1]

            # Skip empty rows
            if not timestamp_str.strip() or not value_str.strip():
                continue

            try:
                timestamp = self._parse_swedish_timestamp(timestamp_str)
            except ValueError as e:
                errors.append(f"Line {line_num}: Invalid timestamp '{timestamp_str}' - {e}")
                continue

            try:
                value = self._parse_swedish_decimal(value_str)
            except ValueError as e:
                errors.append(f"Line {line_num}: Invalid value '{value_str}' - {e}")
                continue

            # Validate value
            if value < 0:
                errors.append(f"Line {line_num}: Negative value not allowed ({value})")
                continue

            parsed_rows.append({
                'timestamp': timestamp,
                'value': value,
                'energy_type': energy_type
            })

        return parsed_rows, errors

    def check_duplicates(self, house_id: str, timestamps: List[datetime],
                         energy_type: str) -> List[datetime]:
        """
        Check which timestamps already exist in InfluxDB.

        Returns list of timestamps that would be duplicates.
        """
        if not self.client or not timestamps:
            return []

        try:
            query_api = self.client.query_api()

            min_time = min(timestamps)
            max_time = max(timestamps)

            # Format timestamps for Flux query (ISO format with Z suffix)
            start_time = min_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            # Add 1 hour buffer to end time to include the last timestamp
            end_time = (max_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: {start_time}, stop: {end_time})
                |> filter(fn: (r) => r["_measurement"] == "energy_consumption")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["energy_type"] == "{energy_type}")
                |> keep(columns: ["_time"])
                |> distinct(column: "_time")
            '''

            tables = query_api.query(query, org=self.org)

            existing = set()
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    existing.add(ts)

            # Find which of our timestamps are duplicates
            duplicates = []
            for ts in timestamps:
                # Normalize to compare
                ts_utc = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
                if ts_utc in existing:
                    duplicates.append(ts)

            return duplicates

        except Exception as e:
            print(f"Failed to check duplicates: {e}")
            return []

    def dry_run(self, house_id: str, file_content: bytes,
                energy_type: str = 'fjv_total') -> Dict:
        """
        Parse CSV and check for duplicates without writing.

        Returns:
            {
                'success': bool,
                'total_rows': int,
                'valid_rows': int,
                'duplicate_count': int,
                'new_rows': int,
                'total_kwh': float,
                'new_kwh': float,
                'date_range': {'start': str, 'end': str},
                'errors': List[str],
                'preview_data': List[Dict],  # First 10 rows for display
                'parsed_data': List[Dict]    # All parsed data for import
            }
        """
        result = {
            'success': False,
            'total_rows': 0,
            'valid_rows': 0,
            'duplicate_count': 0,
            'new_rows': 0,
            'total_kwh': 0.0,
            'new_kwh': 0.0,
            'date_range': {'start': None, 'end': None},
            'errors': [],
            'preview_data': [],
            'parsed_data': []
        }

        # Parse CSV
        parsed_rows, errors = self.parse_csv(file_content, energy_type)
        result['errors'] = errors

        if not parsed_rows:
            if not errors:
                result['errors'].append("No valid data rows found")
            return result

        result['total_rows'] = len(parsed_rows) + len(errors)
        result['valid_rows'] = len(parsed_rows)
        result['total_kwh'] = sum(row['value'] for row in parsed_rows)

        # Sort by timestamp
        parsed_rows.sort(key=lambda x: x['timestamp'])

        # Trim trailing days with only zero values (likely no data yet)
        parsed_rows, trimmed_days = self._trim_trailing_zero_days(parsed_rows)
        result['trimmed_days'] = trimmed_days

        if not parsed_rows:
            result['errors'].append("All data was trimmed (only zero values)")
            return result

        # Get date range (after trimming)
        result['date_range'] = {
            'start': parsed_rows[0]['timestamp'].astimezone(SWEDISH_TZ).strftime('%Y-%m-%d %H:%M'),
            'end': parsed_rows[-1]['timestamp'].astimezone(SWEDISH_TZ).strftime('%Y-%m-%d %H:%M')
        }

        # Check for duplicates
        timestamps = [row['timestamp'] for row in parsed_rows]
        duplicates = self.check_duplicates(house_id, timestamps, energy_type)
        duplicate_set = set(duplicates)

        result['duplicate_count'] = len(duplicates)

        # Mark duplicates and calculate new rows
        new_rows = []
        for row in parsed_rows:
            is_dup = row['timestamp'] in duplicate_set
            row['is_duplicate'] = is_dup
            row['timestamp_display'] = row['timestamp'].astimezone(SWEDISH_TZ).strftime('%Y-%m-%d %H:%M')
            if not is_dup:
                new_rows.append(row)

        result['new_rows'] = len(new_rows)
        result['new_kwh'] = sum(row['value'] for row in new_rows)

        # Preview data - first 10 and last 10 rows
        result['preview_first'] = parsed_rows[:10]
        result['preview_last'] = parsed_rows[-10:] if len(parsed_rows) > 10 else []

        # Store all non-duplicate data for import
        result['parsed_data'] = new_rows
        result['success'] = True

        return result

    def _trim_trailing_zero_days(self, rows: List[Dict]) -> Tuple[List[Dict], int]:
        """
        Remove complete trailing days where all hourly values are zero.

        Returns:
            Tuple of (trimmed_rows, number_of_days_trimmed)
        """
        if not rows:
            return rows, 0

        # Group rows by date (in Swedish timezone)
        from collections import defaultdict
        days = defaultdict(list)
        for row in rows:
            date_key = row['timestamp'].astimezone(SWEDISH_TZ).strftime('%Y-%m-%d')
            days[date_key].append(row)

        # Get sorted list of dates
        sorted_dates = sorted(days.keys())

        # Find trailing days with only zeros
        days_to_trim = []
        for date in reversed(sorted_dates):
            day_rows = days[date]
            # Check if all values in this day are zero
            if all(row['value'] == 0 for row in day_rows):
                days_to_trim.append(date)
            else:
                # Found a day with non-zero values, stop trimming
                break

        # Remove trimmed days
        trimmed_count = len(days_to_trim)
        if trimmed_count > 0:
            dates_to_keep = set(sorted_dates) - set(days_to_trim)
            rows = [row for row in rows
                    if row['timestamp'].astimezone(SWEDISH_TZ).strftime('%Y-%m-%d') in dates_to_keep]

        return rows, trimmed_count

    def import_data(self, house_id: str, parsed_data: List[Dict]) -> Dict:
        """
        Write parsed data to InfluxDB.

        Args:
            house_id: House identifier for tagging
            parsed_data: List of {'timestamp': datetime, 'value': float, 'energy_type': str}

        Returns:
            {
                'success': bool,
                'rows_written': int,
                'error': str (if failed)
            }
        """
        if not self.client:
            return {'success': False, 'rows_written': 0, 'error': 'Not connected to InfluxDB'}

        if not parsed_data:
            return {'success': True, 'rows_written': 0, 'error': None}

        try:
            write_api = self.client.write_api(write_options=SYNCHRONOUS)

            points = []
            for row in parsed_data:
                point = Point("energy_consumption") \
                    .tag("house_id", house_id) \
                    .tag("energy_type", row['energy_type']) \
                    .field("value", float(row['value'])) \
                    .time(row['timestamp'], WritePrecision.S)
                points.append(point)

            # Write in batches of 1000
            batch_size = 1000
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                write_api.write(bucket=self.bucket, org=self.org, record=batch)

            return {'success': True, 'rows_written': len(points), 'error': None}

        except Exception as e:
            return {'success': False, 'rows_written': 0, 'error': str(e)}

    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()


# Singleton instance
_importer = None


def get_energy_importer() -> EnergyImporter:
    """Get or create the energy importer instance"""
    global _importer
    if _importer is None:
        _importer = EnergyImporter()
    return _importer

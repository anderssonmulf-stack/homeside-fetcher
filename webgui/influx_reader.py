"""
InfluxDB Reader for Svenskeb Settings GUI
Fetches real-time and historical data for display in the GUI.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from zoneinfo import ZoneInfo
from influxdb_client import InfluxDBClient

# Swedish timezone
SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


class InfluxReader:
    """Reads heating system data from InfluxDB"""

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
            # Test connection
            health = self.client.health()
            if health.status != "pass":
                print(f"InfluxDB health check failed: {health.status}")
                self.client = None
        except Exception as e:
            print(f"Failed to connect to InfluxDB: {e}")
            self.client = None

    def get_latest_heating_data(self, house_id: str) -> Optional[Dict]:
        """
        Get the most recent heating system data for a house.

        Returns dict with:
            - room_temperature
            - outdoor_temperature
            - supply_temp
            - return_temp
            - hot_water_temp
            - system_pressure
            - target_temp_setpoint
            - away_mode
            - electric_heater
            - timestamp
        """
        if not self.client:
            return None

        try:
            query_api = self.client.query_api()

            # house_id in InfluxDB is full path like "38/xxx/HEM_FJV_149/HEM_FJV_Villa_149"
            # Match by checking if it contains the customer_id
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -1h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
                |> last()
            '''

            tables = query_api.query(query, org=self.org)

            if not tables:
                return None

            # Collect all fields from the result
            data = {'house_id': house_id}
            timestamp = None

            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    data[field] = value
                    if timestamp is None:
                        timestamp = record.get_time()

            if timestamp:
                # Convert to Swedish timezone
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                swedish_time = timestamp.astimezone(SWEDISH_TZ)
                data['timestamp'] = timestamp.isoformat()
                data['timestamp_swedish'] = swedish_time.isoformat()
                data['timestamp_friendly'] = swedish_time.strftime('%Y-%m-%d %H:%M')
                # Calculate age in minutes for freshness check
                now = datetime.now(timezone.utc)
                age_minutes = (now - timestamp).total_seconds() / 60
                data['age_minutes'] = round(age_minutes, 1)

            return data if len(data) > 2 else None

        except Exception as e:
            print(f"Failed to query InfluxDB: {e}")
            return None

    def get_latest_weather(self, house_id: str) -> Optional[Dict]:
        """Get latest weather observation"""
        if not self.client:
            return None

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -2h)
                |> filter(fn: (r) => r["_measurement"] == "weather_observation")
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
                |> last()
            '''

            tables = query_api.query(query, org=self.org)

            if not tables:
                return None

            data = {}
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    data[field] = value

            return data if data else None

        except Exception as e:
            print(f"Failed to query weather: {e}")
            return None

    def get_forecast_summary(self, house_id: str) -> Optional[Dict]:
        """Get latest weather forecast summary"""
        if not self.client:
            return None

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -1h)
                |> filter(fn: (r) => r["_measurement"] == "weather_forecast")
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
                |> last()
            '''

            tables = query_api.query(query, org=self.org)

            if not tables:
                return None

            data = {}
            for table in tables:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    data[field] = value
                    # Get tags too
                    if 'trend' not in data:
                        data['trend'] = record.values.get('trend', '')

            return data if data else None

        except Exception as e:
            print(f"Failed to query forecast: {e}")
            return None

    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()


# Singleton instance
_reader = None

def get_influx_reader() -> InfluxReader:
    """Get or create the InfluxDB reader instance"""
    global _reader
    if _reader is None:
        _reader = InfluxReader()
    return _reader

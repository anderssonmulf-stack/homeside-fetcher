"""
InfluxDB Reader for Svenskeb Settings GUI
Fetches real-time and historical data for display in the GUI.
"""

import os
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
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

    def _ensure_connection(self):
        """Ensure we have a valid connection, reconnect if needed"""
        if self.client is None:
            self._connect()
        elif self.client:
            try:
                health = self.client.health()
                if health.status != "pass":
                    self._connect()
            except Exception:
                self._connect()

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

            # house_id in InfluxDB is short form like "HEM_FJV_Villa_149"
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -1h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
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
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
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
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
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

    def get_data_availability(self, house_id: str, days: int = 30) -> Dict:
        """
        Get data availability per day for each measurement type.
        Returns dict with measurement info and daily counts for Plotly chart.

        Structure:
        {
            'categories': [
                {
                    'name': 'Heating Data',
                    'measurement': 'heating_system',
                    'type': 'measured',  # or 'predicted'
                    'data': [{'date': '2026-01-01', 'count': 96}, ...]
                },
                ...
            ],
            'date_range': {'start': '2026-01-01', 'end': '2026-01-31'}
        }
        """
        if not self.client:
            return {'categories': [], 'date_range': {}}

        # Define measurements to query
        measurements = [
            ('heating_system', 'Heating Data', 'measured'),
            ('weather_observation', 'Weather Obs', 'measured'),
            ('weather_forecast', 'Weather Forecast', 'predicted'),
            ('temperature_forecast', 'Temp Predictions', 'predicted'),
            ('heating_control', 'ML Control', 'measured'),
            ('heat_curve_adjustment', 'Curve Adjustments', 'measured'),
            ('energy_consumption', 'Energy Data', 'measured'),
        ]

        categories = []
        query_api = self.client.query_api()

        for measurement, display_name, data_type in measurements:
            try:
                # Query daily counts for this measurement
                query = f'''
                    from(bucket: "{self.bucket}")
                    |> range(start: -{days}d)
                    |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                    |> filter(fn: (r) => r["house_id"] == "{house_id}")
                    |> aggregateWindow(every: 1d, fn: count, createEmpty: true)
                    |> yield(name: "count")
                '''

                tables = query_api.query(query, org=self.org)

                # Collect daily counts
                daily_data = {}
                for table in tables:
                    for record in table.records:
                        # Get date in YYYY-MM-DD format
                        timestamp = record.get_time()
                        if timestamp:
                            date_str = timestamp.strftime('%Y-%m-%d')
                            count = record.get_value() or 0
                            # Sum counts across fields for the same day
                            if date_str in daily_data:
                                daily_data[date_str] += count
                            else:
                                daily_data[date_str] = count

                # Convert to sorted list
                data_list = [
                    {'date': date, 'count': count}
                    for date, count in sorted(daily_data.items())
                ]

                # Only include if there's any data
                if any(d['count'] > 0 for d in data_list):
                    categories.append({
                        'name': display_name,
                        'measurement': measurement,
                        'type': data_type,
                        'data': data_list
                    })

            except Exception as e:
                print(f"Failed to query {measurement}: {e}")
                continue

        # Calculate date range
        all_dates = []
        for cat in categories:
            for d in cat['data']:
                all_dates.append(d['date'])

        date_range = {}
        if all_dates:
            date_range = {
                'start': min(all_dates),
                'end': max(all_dates)
            }

        return {
            'categories': categories,
            'date_range': date_range
        }

    def get_cloud_cover_history(self, house_id: str, hours: int = 168) -> dict:
        """
        Get cloud cover data from weather_forecast measurement.
        Returns dict mapping timestamp (rounded to hour) to cloud cover value (0-8 octas).
        """
        self._ensure_connection()
        if not self.client:
            return {}

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "weather_forecast")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "avg_cloud_cover")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            cloud_data = {}
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    value = record.get_value()
                    if timestamp and value is not None:
                        # Round to nearest hour for matching with weather obs
                        hour_key = timestamp.replace(minute=0, second=0, microsecond=0)
                        cloud_data[hour_key.isoformat()] = value

            return cloud_data

        except Exception as e:
            print(f"Failed to query cloud cover: {e}")
            return {}

    def get_weather_history(self, house_id: str, hours: int = 168) -> list:
        """
        Get historical weather data for effective temperature calculation.

        Args:
            house_id: House identifier
            hours: Hours of history to fetch (default 168 = 7 days)

        Returns:
            List of dicts with timestamp, temperature, wind_speed, humidity, cloud_cover
        """
        self._ensure_connection()
        if not self.client:
            return []

        try:
            query_api = self.client.query_api()

            # Get cloud cover data first (from weather_forecast)
            cloud_data = self.get_cloud_cover_history(house_id, hours)

            # Query weather observations
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "weather_observation")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            results = []
            last_cloud_cover = 4.0  # Default: partly cloudy

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        # Convert to Swedish timezone for display
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        # Find cloud cover for this hour
                        hour_key = timestamp.replace(minute=0, second=0, microsecond=0).isoformat()
                        cloud_cover = cloud_data.get(hour_key, last_cloud_cover)
                        if hour_key in cloud_data:
                            last_cloud_cover = cloud_cover

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_swedish': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'temperature': record.values.get('temperature'),
                            'wind_speed': record.values.get('wind_speed', 0),
                            'humidity': record.values.get('humidity', 50),
                            'cloud_cover': cloud_cover,
                        })

            return results

        except Exception as e:
            print(f"Failed to query weather history: {e}")
            return []

    def get_heating_and_weather_history(self, house_id: str, hours: int = 168) -> dict:
        """
        Get combined heating system and weather data for analysis.

        Returns dict with 'heating' and 'weather' lists, plus 'location' for solar calc.
        """
        if not self.client:
            return {'heating': [], 'weather': [], 'location': None}

        try:
            query_api = self.client.query_api()

            # Get heating data (includes outdoor temp from HomeSide sensor)
            heating_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "outdoor_temperature" or
                                     r["_field"] == "room_temperature" or
                                     r["_field"] == "supply_temp")
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            heating_tables = query_api.query(heating_query, org=self.org)

            heating_data = []
            for table in heating_tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        heating_data.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_swedish': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'outdoor_temperature': record.values.get('outdoor_temperature'),
                            'room_temperature': record.values.get('room_temperature'),
                            'supply_temp': record.values.get('supply_temp'),
                        })

            # Get weather observations (SMHI data with wind, humidity)
            weather_data = self.get_weather_history(house_id, hours)

            # Try to get location from weather forecast tags
            location = None
            try:
                loc_query = f'''
                    from(bucket: "{self.bucket}")
                    |> range(start: -1h)
                    |> filter(fn: (r) => r["_measurement"] == "weather_forecast")
                    |> filter(fn: (r) => r["house_id"] == "{house_id}")
                    |> last()
                    |> keep(columns: ["latitude", "longitude"])
                '''
                # Location might be stored elsewhere - fallback to profile
            except Exception:
                pass

            return {
                'heating': heating_data,
                'weather': weather_data,
                'location': location
            }

        except Exception as e:
            print(f"Failed to query heating/weather history: {e}")
            return {'heating': [], 'weather': [], 'location': None}

    def get_weather_forecast(self, house_id: str, hours_ahead: int = 12) -> list:
        """
        Get weather forecast data for effective temperature calculation.

        Args:
            house_id: House identifier
            hours_ahead: Hours of forecast to fetch (default 12)

        Returns:
            List of dicts with forecast timestamp, temperature, cloud_cover
        """
        self._ensure_connection()
        if not self.client:
            return []

        try:
            query_api = self.client.query_api()

            # Query outdoor temperature forecasts - get most recent forecast per hour
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: now(), stop: {hours_ahead}h)
                |> filter(fn: (r) => r["_measurement"] == "temperature_forecast")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["forecast_type"] == "outdoor_temp")
                |> filter(fn: (r) => r["_field"] == "value")
                |> group(columns: ["_time"])
                |> last()
                |> group()
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            # Get cloud cover from weather_forecast
            cloud_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -2h)
                |> filter(fn: (r) => r["_measurement"] == "weather_forecast")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "avg_cloud_cover")
                |> last()
            '''

            cloud_tables = query_api.query(cloud_query, org=self.org)
            default_cloud_cover = 4.0
            for table in cloud_tables:
                for record in table.records:
                    default_cloud_cover = record.get_value() or 4.0

            results = []
            seen_times = set()  # Deduplicate by time

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    temp = record.get_value()

                    if timestamp and temp is not None:
                        # Deduplicate - only one entry per hour
                        time_key = timestamp.strftime('%Y-%m-%d %H:00')
                        if time_key in seen_times:
                            continue
                        seen_times.add(time_key)

                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_swedish': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'temperature': temp,
                            'cloud_cover': default_cloud_cover,
                            'is_forecast': True
                        })

            # Sort by timestamp
            results.sort(key=lambda x: x['timestamp'])
            return results

        except Exception as e:
            print(f"Failed to query weather forecast: {e}")
            return []

    def get_smhi_forecast(self, latitude: float, longitude: float, hours_ahead: int = 12) -> list:
        """
        Fetch weather forecast directly from SMHI API with full parameters.

        Args:
            latitude: Location latitude
            longitude: Location longitude
            hours_ahead: Hours of forecast to fetch (default 12)

        Returns:
            List of dicts with timestamp, temperature, wind_speed, humidity, cloud_cover
        """
        try:
            # SMHI PMP3G API endpoint
            url = f"https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/{longitude}/lat/{latitude}/data.json"

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()

            forecasts = []
            now = datetime.now(timezone.utc)
            cutoff_time = now + timedelta(hours=hours_ahead)

            for time_series in data.get('timeSeries', []):
                valid_time_str = time_series.get('validTime')
                valid_time = datetime.fromisoformat(valid_time_str.replace('Z', '+00:00'))

                # Only include forecasts within our time window
                if valid_time > cutoff_time:
                    break

                if valid_time < now:
                    continue

                # Extract forecast parameters
                temp = None
                cloud_cover = None
                wind_speed = None
                humidity = None

                for param in time_series.get('parameters', []):
                    param_name = param.get('name')
                    if param_name == 't':  # Temperature at 2m
                        temp = param.get('values', [None])[0]
                    elif param_name == 'tcc_mean':  # Total cloud cover (0-8 octas)
                        cloud_cover = param.get('values', [None])[0]
                    elif param_name == 'ws':  # Wind speed (m/s)
                        wind_speed = param.get('values', [None])[0]
                    elif param_name == 'r':  # Relative humidity (%)
                        humidity = param.get('values', [None])[0]

                if temp is not None:
                    if valid_time.tzinfo is None:
                        valid_time = valid_time.replace(tzinfo=timezone.utc)
                    swedish_time = valid_time.astimezone(SWEDISH_TZ)

                    forecasts.append({
                        'timestamp': valid_time.isoformat(),
                        'timestamp_swedish': swedish_time.strftime('%Y-%m-%d %H:%M'),
                        'temperature': temp,
                        'wind_speed': wind_speed if wind_speed is not None else 3.0,
                        'humidity': humidity if humidity is not None else 60.0,
                        'cloud_cover': cloud_cover if cloud_cover is not None else 4.0,
                        'is_forecast': True
                    })

            return forecasts

        except Exception as e:
            print(f"Failed to fetch SMHI forecast: {e}")
            return []

    def get_temperature_history(self, house_id: str, hours: int = 168) -> list:
        """
        Get temperature history for primary (DH) and secondary (house) side.

        Returns list of dicts with:
            - timestamp, timestamp_display
            - dh_supply_temp, dh_return_temp (district heating primary side from energy_meter)
            - supply_temp, return_temp (house secondary side from heating_system)
            - outdoor_temperature
            - supply_temp_heat_curve_ml (predicted supply temp using effective temp)
        """
        self._ensure_connection()
        if not self.client:
            return []

        try:
            query_api = self.client.query_api()

            # Query house-side data from heating_system (15-min resolution)
            house_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) =>
                    r["_field"] == "supply_temp" or
                    r["_field"] == "return_temp" or
                    r["_field"] == "outdoor_temperature" or
                    r["_field"] == "supply_temp_heat_curve_ml"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            # Query DH primary side from energy_meter (hourly resolution)
            dh_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "energy_meter")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) =>
                    r["_field"] == "primary_temp_in" or
                    r["_field"] == "primary_temp_out"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            house_tables = query_api.query(house_query, org=self.org)
            dh_tables = query_api.query(dh_query, org=self.org)

            # Build DH data lookup by hour
            dh_by_hour = {}
            for table in dh_tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        # Round to hour for matching
                        hour_key = timestamp.replace(minute=0, second=0, microsecond=0)
                        dh_by_hour[hour_key] = {
                            'dh_supply_temp': record.values.get('primary_temp_in'),
                            'dh_return_temp': record.values.get('primary_temp_out'),
                        }

            results = []
            for table in house_tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        # Look up DH data for this hour
                        hour_key = timestamp.replace(minute=0, second=0, microsecond=0)
                        dh_data = dh_by_hour.get(hour_key, {})

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'dh_supply_temp': dh_data.get('dh_supply_temp'),
                            'dh_return_temp': dh_data.get('dh_return_temp'),
                            'supply_temp': record.values.get('supply_temp'),
                            'return_temp': record.values.get('return_temp'),
                            'outdoor_temperature': record.values.get('outdoor_temperature'),
                            'supply_temp_heat_curve_ml': record.values.get('supply_temp_heat_curve_ml'),
                        })

            return results

        except Exception as e:
            print(f"Failed to query temperature history: {e}")
            return []

    def get_supply_return_with_forecast(self, house_id: str, hours: int = 168) -> dict:
        """
        Get supply/return temperatures with heat curve and forecast data.

        Returns dict with:
            - history: list of historical data points
            - forecast: list of forecast data points
        """
        self._ensure_connection()
        if not self.client:
            return {'history': [], 'forecast': []}

        try:
            query_api = self.client.query_api()

            # Query historical data
            history_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) =>
                    r["_field"] == "supply_temp" or
                    r["_field"] == "return_temp" or
                    r["_field"] == "supply_temp_heat_curve" or
                    r["_field"] == "supply_temp_heat_curve_ml"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(history_query, org=self.org)

            history = []
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        history.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'supply_temp': record.values.get('supply_temp'),
                            'return_temp': record.values.get('return_temp'),
                            'heat_curve': record.values.get('supply_temp_heat_curve'),
                            'heat_curve_ml': record.values.get('supply_temp_heat_curve_ml'),
                        })

            # Query forecast data (future predictions)
            # Note: forecast_type is a tag, so different values end up in
            # separate Flux tables. We skip pivot and merge in Python instead.
            forecast_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: now(), stop: 24h)
                |> filter(fn: (r) => r["_measurement"] == "temperature_forecast")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) =>
                    r["forecast_type"] == "supply_temp_baseline" or
                    r["forecast_type"] == "supply_temp_ml"
                )
                |> sort(columns: ["_time"])
            '''

            forecast_tables = query_api.query(forecast_query, org=self.org)

            # Merge forecast types by timestamp in Python
            forecast_by_time = {}
            for table in forecast_tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if not timestamp:
                        continue
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)
                    forecast_type = record.values.get('forecast_type')
                    value = record.get_value()
                    ts_key = timestamp.isoformat()
                    if ts_key not in forecast_by_time:
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)
                        forecast_by_time[ts_key] = {
                            'timestamp': ts_key,
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'supply_baseline': None,
                            'supply_ml': None,
                        }
                    if forecast_type == 'supply_temp_baseline':
                        forecast_by_time[ts_key]['supply_baseline'] = value
                    elif forecast_type == 'supply_temp_ml':
                        forecast_by_time[ts_key]['supply_ml'] = value

            forecast = sorted(forecast_by_time.values(), key=lambda x: x['timestamp'])

            return {'history': history, 'forecast': forecast}

        except Exception as e:
            print(f"Failed to query supply/return with forecast: {e}")
            return {'history': [], 'forecast': []}

    def get_energy_consumption_history(self, house_id: str, days: int = 30,
                                        aggregation: str = 'daily') -> dict:
        """
        Get energy consumption history with optional aggregation.

        Falls back to calculating energy from live power data if no imported data.

        Args:
            house_id: House identifier
            days: Number of days to fetch
            aggregation: 'hourly', 'daily', or 'monthly'

        Returns dict with:
            - data: list of consumption records by energy_type
            - totals: summary totals by energy_type
            - data_source: 'imported' or 'live'
        """
        self._ensure_connection()
        if not self.client:
            return {'data': {}, 'totals': {}, 'data_source': None}

        try:
            query_api = self.client.query_api()

            # Map aggregation to Flux window
            agg_window = {'hourly': '1h', 'daily': '1d', 'monthly': '1mo'}.get(aggregation, '1d')

            # First, try imported energy data
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "energy_consumption")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "value")
                |> aggregateWindow(every: {agg_window}, fn: sum, createEmpty: false)
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            # Group by energy_type
            data_by_type = {}
            totals = {}

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    value = record.get_value() or 0
                    energy_type = record.values.get('energy_type', 'fjv_total')

                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        if energy_type not in data_by_type:
                            data_by_type[energy_type] = []
                            totals[energy_type] = 0

                        data_by_type[energy_type].append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d' if aggregation != 'hourly' else '%Y-%m-%d %H:%M'),
                            'value': round(value, 2),
                            'energy_type': energy_type
                        })
                        totals[energy_type] += value

            # If imported data found, return it
            if data_by_type:
                totals = {k: round(v, 1) for k, v in totals.items()}
                return {
                    'data': data_by_type,
                    'totals': totals,
                    'aggregation': aggregation,
                    'days': days,
                    'data_source': 'imported'
                }

            # Fall back to calculating energy from live power data
            result = self._get_energy_from_live_power(house_id, days, aggregation, query_api)
            return result

        except Exception as e:
            print(f"Failed to query energy consumption: {e}")
            return {'data': {}, 'totals': {}, 'data_source': None}

    def _get_energy_from_live_power(self, house_id: str, days: int,
                                     aggregation: str, query_api) -> dict:
        """
        Calculate energy consumption from live power (dh_power) readings.

        Integrates power over time to get energy in kWh.
        For 15-minute readings: energy = mean_power * hours_in_window
        """
        try:
            agg_window = {'hourly': '1h', 'daily': '1d', 'monthly': '1mo'}.get(aggregation, '1d')
            hours_multiplier = {'hourly': 1, 'daily': 24, 'monthly': 730}.get(aggregation, 24)

            # Query live power data and calculate mean for each window
            # Mean power (kW) * window hours = energy (kWh)
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "dh_power")
                |> aggregateWindow(every: {agg_window}, fn: mean, createEmpty: false)
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            data_list = []
            total_kwh = 0

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    mean_power = record.get_value()

                    if timestamp and mean_power is not None:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        # For partial windows (like the current hour/day), estimate based on mean
                        # Mean power (kW) * hours = energy (kWh)
                        kwh = mean_power * hours_multiplier

                        data_list.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d' if aggregation != 'hourly' else '%Y-%m-%d %H:%M'),
                            'value': round(kwh, 2),
                            'energy_type': 'fjv_total'
                        })
                        total_kwh += kwh

            if not data_list:
                return {
                    'data': {},
                    'totals': {},
                    'aggregation': aggregation,
                    'days': days,
                    'data_source': None
                }

            return {
                'data': {'fjv_total': data_list},
                'totals': {'fjv_total': round(total_kwh, 1)},
                'aggregation': aggregation,
                'days': days,
                'data_source': 'live'
            }

        except Exception as e:
            print(f"Failed to calculate energy from live power: {e}")
            return {'data': {}, 'totals': {}, 'data_source': None}

    def get_efficiency_metrics(self, house_id: str, hours: int = 168) -> list:
        """
        Get efficiency metrics: delta T, power, flow rate.

        Returns list of dicts with:
            - timestamp, timestamp_display
            - dh_delta_t: DH supply - return (higher = more heat extracted, from energy_meter)
            - house_delta_t: House supply - return (from heating_system)
            - dh_flow: Flow rate in l/h (from energy_meter volume)
            - efficiency_ratio: dh_delta_t / house_delta_t (heat transfer efficiency)
        """
        self._ensure_connection()
        if not self.client:
            return []

        try:
            query_api = self.client.query_api()

            # Query house-side data from heating_system
            house_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) =>
                    r["_field"] == "supply_temp" or
                    r["_field"] == "return_temp"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            # Query DH primary side from energy_meter (temps and flow)
            dh_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "energy_meter")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) =>
                    r["_field"] == "primary_temp_in" or
                    r["_field"] == "primary_temp_out" or
                    r["_field"] == "volume"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            house_tables = query_api.query(house_query, org=self.org)
            dh_tables = query_api.query(dh_query, org=self.org)

            # Build DH data lookup by hour
            # DH timestamps are at the beginning of each hour (e.g., 20:00 = data for 20:00-21:00)
            dh_by_hour = {}
            for table in dh_tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        hour_key = timestamp.replace(minute=0, second=0, microsecond=0)
                        dh_supply = record.values.get('primary_temp_in')
                        dh_return = record.values.get('primary_temp_out')
                        volume = record.values.get('volume')  # m³/h

                        dh_delta_t = None
                        if dh_supply is not None and dh_return is not None:
                            dh_delta_t = round(dh_supply - dh_return, 1)

                        dh_by_hour[hour_key] = {
                            'dh_supply': dh_supply,
                            'dh_return': dh_return,
                            'dh_delta_t': dh_delta_t,
                            'dh_flow': round(volume * 1000, 0) if volume else None,  # Convert m³ to liters
                        }

            results = []
            for table in house_tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        house_supply = record.values.get('supply_temp')
                        house_return = record.values.get('return_temp')

                        # Calculate house delta T
                        house_delta_t = None
                        if house_supply is not None and house_return is not None:
                            house_delta_t = round(house_supply - house_return, 1)

                        # Look up DH data for this hour
                        hour_key = timestamp.replace(minute=0, second=0, microsecond=0)
                        dh_data = dh_by_hour.get(hour_key, {})
                        dh_delta_t = dh_data.get('dh_delta_t')
                        dh_flow = dh_data.get('dh_flow')

                        # Efficiency: how well heat is transferred from DH to house
                        efficiency_ratio = None
                        if dh_delta_t and house_delta_t and house_delta_t > 0:
                            efficiency_ratio = round(dh_delta_t / house_delta_t, 2)

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'dh_delta_t': dh_delta_t,
                            'house_delta_t': house_delta_t,
                            'dh_power': None,  # Not available from energy_meter
                            'dh_flow': dh_flow,
                            'efficiency_ratio': efficiency_ratio,
                        })

            return results

        except Exception as e:
            print(f"Failed to query efficiency metrics: {e}")
            return []

    def get_power_history(self, house_id: str, hours: int = 168) -> dict:
        """
        Get power consumption history from imported energy data (Dropbox).

        Uses ONLY imported energy data from energy_meter measurement (hourly kWh).
        For hourly data, kWh ≈ average kW for that hour.

        Does NOT use Arrigo API dh_power as it's unreliable with data gaps.

        Returns dict with:
            - data: list of dicts with timestamp and dh_power
            - data_source: 'imported' or None
        """
        self._ensure_connection()
        if not self.client:
            return {'data': [], 'data_source': None}

        try:
            query_api = self.client.query_api()
            days = max(1, hours // 24)

            # Query imported energy data from energy_meter (from Dropbox import)
            # consumption field = hourly kWh (which equals average kW for that hour)
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "energy_meter")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "consumption")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            results = []
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    value = record.get_value()

                    if timestamp and value is not None:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        # kWh for 1 hour = average kW for that hour
                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'dh_power': round(value, 1)
                        })

            if results:
                return {'data': results, 'data_source': 'imported'}

            return {'data': [], 'data_source': None}

        except Exception as e:
            print(f"Failed to query power history: {e}")
            return {'data': [], 'data_source': None}

    def get_energy_signature_hourly(self, house_id: str, days: int = 90) -> dict:
        """
        Get hourly energy consumption paired with outdoor temperature for energy signature plot.
        Returns list of {consumption_kwh, outdoor_temp} pairs from energy_meter + heating_system.
        """
        self._ensure_connection()
        if not self.client:
            return {'data': []}

        try:
            query_api = self.client.query_api()

            # Query hourly consumption from energy_meter
            consumption_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "energy_meter")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "consumption")
                |> sort(columns: ["_time"])
            '''

            # Query outdoor temperature averaged per hour from heating_system
            outdoor_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "outdoor_temperature")
                |> aggregateWindow(every: 1h, fn: mean)
                |> filter(fn: (r) => exists r["_value"])
            '''

            consumption_tables = query_api.query(consumption_query, org=self.org)
            outdoor_tables = query_api.query(outdoor_query, org=self.org)

            # Build outdoor temp lookup by hour key (YYYY-MM-DD HH)
            outdoor_by_hour = {}
            for table in outdoor_tables:
                for record in table.records:
                    ts = record.get_time()
                    val = record.get_value()
                    if ts and val is not None:
                        hour_key = ts.strftime('%Y-%m-%d %H')
                        outdoor_by_hour[hour_key] = val

            # Match consumption with outdoor temp
            results = []
            for table in consumption_tables:
                for record in table.records:
                    ts = record.get_time()
                    consumption = record.get_value()
                    if ts and consumption is not None and consumption > 0:
                        hour_key = ts.strftime('%Y-%m-%d %H')
                        outdoor = outdoor_by_hour.get(hour_key)
                        if outdoor is not None:
                            results.append({
                                'consumption_kwh': round(consumption, 2),
                                'outdoor_temp': round(outdoor, 1),
                            })

            return {'data': results}

        except Exception as e:
            print(f"Failed to query hourly energy signature: {e}")
            return {'data': []}

    def get_energy_separation(self, house_id: str, days: int = 30) -> dict:
        """
        Get energy separation data (heating vs DHW) from calibration.

        Args:
            house_id: House identifier
            days: Number of days to fetch

        Returns dict with:
            - data: list of daily records with actual, heating, dhw, predicted energy
            - totals: summary totals
            - k_value: calibrated heat loss coefficient
        """
        self._ensure_connection()
        if not self.client:
            return {'data': [], 'totals': {}, 'k_value': None}

        try:
            query_api = self.client.query_api()

            # Query energy_separated data (any method - k_calibration or homeside_ondemand_dhw)
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "energy_separated")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            results = []
            totals = {'actual': 0, 'heating': 0, 'dhw': 0, 'predicted': 0}
            k_value = None
            date_to_idx = {}  # Map dates to result indices

            # First, get k_value from k_calibration_history
            k_query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "k_calibration_history")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> filter(fn: (r) => r["_field"] == "k_value")
                |> last()
            '''
            k_tables = query_api.query(k_query, org=self.org)
            for table in k_tables:
                for record in table.records:
                    k_value = record.get_value()

            # Get today's date (Swedish timezone) to filter incomplete days
            today_swedish = datetime.now(SWEDISH_TZ).strftime('%Y-%m-%d')

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)
                        date_str = swedish_time.strftime('%Y-%m-%d')

                        # Skip today (incomplete data)
                        if date_str >= today_swedish:
                            continue

                        # Handle both field names: total_energy_kwh (new) and actual_energy_kwh (legacy)
                        actual = record.values.get('total_energy_kwh') or record.values.get('actual_energy_kwh') or 0
                        heating = record.values.get('heating_energy_kwh') or 0
                        dhw = record.values.get('dhw_energy_kwh') or 0
                        k = record.values.get('k_value')
                        no_breakdown = bool(record.values.get('no_breakdown', 0))

                        # Legacy: skip low-coverage days that lack the no_breakdown field
                        data_coverage = record.values.get('data_coverage', 1.0)
                        if data_coverage < 0.8 and not no_breakdown:
                            continue

                        if k is not None:
                            k_value = k

                        date_to_idx[date_str] = len(results)

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': date_str,
                            'date_key': date_str,
                            'actual_kwh': round(actual, 1),
                            'heating_kwh': round(heating, 1),
                            'dhw_kwh': round(dhw, 1),
                            'no_breakdown': no_breakdown,
                            'predicted_kwh': None,  # Will be filled in
                            'avg_outdoor': None,
                            'avg_temp_diff': record.values.get('avg_temp_difference'),
                            'dhw_events': record.values.get('dhw_event_count'),
                            'avg_effective_outdoor': round(record.values.get('avg_effective_outdoor_temp', 0), 1) if record.values.get('avg_effective_outdoor_temp') is not None else None,
                        })

                        totals['actual'] += actual
                        # Only count heating/dhw for days with full breakdown
                        if not no_breakdown:
                            totals['heating'] += heating
                            totals['dhw'] += dhw

            # Query stored hourly energy forecasts and aggregate by day (ML2 model)
            if results:
                # Query energy_forecast measurement for hourly predictions
                forecast_query = f'''
                    from(bucket: "{self.bucket}")
                    |> range(start: -{days}d)
                    |> filter(fn: (r) => r["_measurement"] == "energy_forecast")
                    |> filter(fn: (r) => r["house_id"] == "{house_id}")
                    |> filter(fn: (r) => r["_field"] == "heating_energy_kwh")
                '''

                forecast_tables = query_api.query(forecast_query, org=self.org)

                # Aggregate hourly forecasts by Swedish day (track count to filter incomplete days)
                predicted_by_day = {}  # {date: {'sum': float, 'count': int}}
                for table in forecast_tables:
                    for record in table.records:
                        timestamp = record.get_time()
                        energy = record.get_value()
                        if timestamp and energy is not None:
                            swedish_time = timestamp.astimezone(SWEDISH_TZ)
                            date_str = swedish_time.strftime('%Y-%m-%d')
                            if date_str not in predicted_by_day:
                                predicted_by_day[date_str] = {'sum': 0, 'count': 0}
                            predicted_by_day[date_str]['sum'] += energy
                            predicted_by_day[date_str]['count'] += 1

                # Also query outdoor temps for display (group by Swedish day)
                outdoor_query = f'''
                    import "timezone"
                    option location = timezone.location(name: "Europe/Stockholm")
                    from(bucket: "{self.bucket}")
                    |> range(start: -{days}d)
                    |> filter(fn: (r) => r["_measurement"] == "heating_system")
                    |> filter(fn: (r) => r["house_id"] == "{house_id}")
                    |> filter(fn: (r) => r["_field"] == "outdoor_temperature")
                    |> aggregateWindow(every: 1d, fn: mean)
                '''

                outdoor_tables = query_api.query(outdoor_query, org=self.org)
                outdoor_by_day = {}
                for table in outdoor_tables:
                    for record in table.records:
                        timestamp = record.get_time()
                        outdoor = record.get_value()
                        if timestamp and outdoor is not None:
                            # With timezone option, window end is at Swedish midnight
                            # Convert to Swedish time for correct date label
                            swedish_time = timestamp.astimezone(SWEDISH_TZ)
                            date_str = swedish_time.strftime('%Y-%m-%d')
                            outdoor_by_day[date_str] = outdoor

                # Match predictions with actual data
                # Only include predictions for days with sufficient hourly data (>=20 hours)
                # Also track heating energy for days with predictions (for accuracy calculation)
                MIN_HOURLY_FORECASTS = 20
                heating_with_predictions = 0  # Sum of heating kWh for days that have predictions
                days_with_predictions = 0

                for date_str, idx in date_to_idx.items():
                    forecast_data = predicted_by_day.get(date_str)
                    outdoor = outdoor_by_day.get(date_str)

                    # Only show prediction if we have enough hourly forecasts
                    # and the day has a full breakdown (no_breakdown days excluded)
                    if forecast_data and forecast_data['count'] >= MIN_HOURLY_FORECASTS:
                        predicted = forecast_data['sum']
                        results[idx]['predicted_kwh'] = round(predicted, 1)
                        totals['predicted'] += predicted
                        # Track heating energy for days with predictions (exclude no-breakdown)
                        if not results[idx].get('no_breakdown'):
                            heating_with_predictions += results[idx]['heating_kwh']
                            days_with_predictions += 1

                    if outdoor is not None:
                        results[idx]['avg_outdoor'] = round(outdoor, 1)

            totals = {k: round(v, 1) for k, v in totals.items()}

            # Calculate percentages
            if totals['actual'] > 0:
                totals['heating_pct'] = round(100 * totals['heating'] / totals['actual'], 1)
                totals['dhw_pct'] = round(100 * totals['dhw'] / totals['actual'], 1)
            else:
                totals['heating_pct'] = 0
                totals['dhw_pct'] = 0

            # Calculate prediction accuracy (predicted / heating for days with predictions)
            if heating_with_predictions > 0 and totals['predicted'] > 0:
                totals['prediction_accuracy'] = round(100 * totals['predicted'] / heating_with_predictions, 1)
                totals['days_with_predictions'] = days_with_predictions
            else:
                totals['prediction_accuracy'] = None
                totals['days_with_predictions'] = 0

            # Clean up date_key from results
            for r in results:
                r.pop('date_key', None)

            return {
                'data': results,
                'totals': totals,
                'k_value': round(k_value, 4) if k_value else None,
                'days': days
            }

        except Exception as e:
            print(f"Failed to query energy separation data: {e}")
            return {'data': [], 'totals': {}, 'k_value': None}

    def get_energy_separation_all(self, days: int = 30) -> dict:
        """
        Get aggregated energy separation across ALL houses.
        Sums actual, heating, and DHW energy by date.
        """
        self._ensure_connection()
        if not self.client:
            return {'data': [], 'totals': {}, 'k_value': None}

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "energy_separated")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            # Aggregate by date across all houses
            today_swedish = datetime.now(SWEDISH_TZ).strftime('%Y-%m-%d')
            day_data = {}  # date_str -> {actual, heating, dhw, no_breakdown_count, house_count}

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if not timestamp:
                        continue
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)
                    swedish_time = timestamp.astimezone(SWEDISH_TZ)
                    date_str = swedish_time.strftime('%Y-%m-%d')

                    if date_str >= today_swedish:
                        continue

                    actual = record.values.get('total_energy_kwh') or record.values.get('actual_energy_kwh') or 0
                    heating = record.values.get('heating_energy_kwh') or 0
                    dhw = record.values.get('dhw_energy_kwh') or 0
                    no_breakdown = bool(record.values.get('no_breakdown', 0))
                    data_coverage = record.values.get('data_coverage', 1.0)

                    # Legacy skip
                    if data_coverage < 0.8 and not no_breakdown:
                        continue

                    if date_str not in day_data:
                        day_data[date_str] = {
                            'actual': 0, 'heating': 0, 'dhw': 0,
                            'no_breakdown_count': 0, 'house_count': 0,
                            'timestamp': timestamp
                        }

                    day_data[date_str]['actual'] += actual
                    day_data[date_str]['house_count'] += 1
                    if no_breakdown:
                        day_data[date_str]['no_breakdown_count'] += 1
                    else:
                        day_data[date_str]['heating'] += heating
                        day_data[date_str]['dhw'] += dhw

            # Build results
            results = []
            totals = {'actual': 0, 'heating': 0, 'dhw': 0, 'predicted': 0}

            for date_str in sorted(day_data.keys()):
                d = day_data[date_str]
                # Mark as no_breakdown only if ALL houses lack breakdown
                all_no_breakdown = d['no_breakdown_count'] == d['house_count']

                results.append({
                    'timestamp': d['timestamp'].isoformat(),
                    'timestamp_display': date_str,
                    'actual_kwh': round(d['actual'], 1),
                    'heating_kwh': round(d['heating'], 1),
                    'dhw_kwh': round(d['dhw'], 1),
                    'no_breakdown': all_no_breakdown,
                    'predicted_kwh': None,
                    'avg_outdoor': None,
                    'house_count': d['house_count'],
                })

                totals['actual'] += d['actual']
                totals['heating'] += d['heating']
                totals['dhw'] += d['dhw']

            totals = {k: round(v, 1) for k, v in totals.items()}
            if totals['actual'] > 0:
                totals['heating_pct'] = round(100 * totals['heating'] / totals['actual'], 1)
                totals['dhw_pct'] = round(100 * totals['dhw'] / totals['actual'], 1)
            else:
                totals['heating_pct'] = 0
                totals['dhw_pct'] = 0

            return {
                'data': results,
                'totals': totals,
                'k_value': None,
                'days': days
            }

        except Exception as e:
            print(f"Failed to query aggregated energy separation: {e}")
            return {'data': [], 'totals': {}, 'k_value': None}

    def get_energy_forecast_all(self, hours: int = 24) -> dict:
        """
        Get aggregated energy forecast across ALL houses.
        Sums hourly predictions by timestamp.
        """
        self._ensure_connection()
        if not self.client:
            return {'forecast': [], 'summary': {}}

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: now(), stop: {hours}h)
                |> filter(fn: (r) => r["_measurement"] == "energy_forecast")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            # Aggregate by hour across all houses
            hour_data = {}  # iso_timestamp -> {energy, power, outdoor_temps, house_ids}

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if not timestamp:
                        continue
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)

                    # Round to hour for grouping
                    hour_key = timestamp.replace(minute=0, second=0, microsecond=0)
                    key = hour_key.isoformat()

                    energy_kwh = record.values.get('heating_energy_kwh', 0) or 0
                    power_kw = record.values.get('heating_power_kw', 0) or 0
                    outdoor = record.values.get('outdoor_temp')
                    house_id = record.values.get('house_id', '')

                    if key not in hour_data:
                        hour_data[key] = {
                            'timestamp': hour_key,
                            'energy': 0, 'power': 0,
                            'outdoor_temps': [], 'house_ids': set()
                        }

                    hour_data[key]['energy'] += energy_kwh
                    hour_data[key]['power'] += power_kw
                    if house_id:
                        hour_data[key]['house_ids'].add(house_id)
                    if outdoor is not None:
                        hour_data[key]['outdoor_temps'].append(outdoor)

            # Build results
            forecast = []
            total_energy = 0
            peak_power = 0
            peak_hour = None

            for key in sorted(hour_data.keys()):
                d = hour_data[key]
                ts = d['timestamp']
                swedish_time = ts.astimezone(SWEDISH_TZ)

                avg_outdoor = None
                if d['outdoor_temps']:
                    avg_outdoor = sum(d['outdoor_temps']) / len(d['outdoor_temps'])

                forecast.append({
                    'timestamp': ts.isoformat(),
                    'timestamp_display': swedish_time.strftime('%H:%M'),
                    'timestamp_date': swedish_time.strftime('%Y-%m-%d'),
                    'hour': swedish_time.hour,
                    'heating_energy_kwh': round(d['energy'], 2),
                    'heating_power_kw': round(d['power'], 3),
                    'outdoor_temp': round(avg_outdoor, 1) if avg_outdoor is not None else None,
                    'effective_temp': None,
                    'solar_effect': None,
                    'wind_effect': None,
                    'lead_time_hours': None,
                    'house_count': len(d['house_ids']),
                })

                total_energy += d['energy']
                if d['power'] > peak_power:
                    peak_power = d['power']
                    peak_hour = swedish_time.strftime('%H:%M')

            summary = {
                'total_energy_kwh': round(total_energy, 1),
                'avg_power_kw': round(total_energy / len(forecast), 2) if forecast else 0,
                'peak_power_kw': round(peak_power, 2),
                'peak_hour': peak_hour,
                'hours_forecasted': len(forecast),
            }

            return {
                'forecast': forecast,
                'summary': summary,
                'generated_at': None,
                'hours': hours
            }

        except Exception as e:
            print(f"Failed to query aggregated energy forecast: {e}")
            return {'forecast': [], 'summary': {}}

    def get_k_value_history(self, house_id: str, days: int = 30) -> dict:
        """
        Get k-value calibration history for convergence visualization.

        Returns:
            Dict with 'data' list of k-value records over time
        """
        self._ensure_connection()
        if not self.client:
            return {'data': [], 'current_k': None}

        try:
            query_api = self.client.query_api()

            # Extract short house_id (last part after /) for k-value lookup
            # k_calibration_history stores short IDs like "HEM_FJV_Villa_149"
            short_id = house_id.split('/')[-1] if '/' in house_id else house_id

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "k_calibration_history")
                |> filter(fn: (r) => r["house_id"] == "{short_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            results = []
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    # Convert to Swedish time for display
                    ts_swedish = ts.astimezone(ZoneInfo('Europe/Stockholm'))

                    results.append({
                        'timestamp': ts.isoformat(),
                        'timestamp_display': ts_swedish.strftime('%Y-%m-%d %H:%M'),
                        'k_value': record.values.get('k_value'),
                        'k_median': record.values.get('k_median'),
                        'k_stddev': record.values.get('k_stddev'),
                        'confidence': record.values.get('confidence'),
                        'days_used': record.values.get('days_used'),
                        'avg_outdoor_temp': record.values.get('avg_outdoor_temp'),
                    })

            # Get current k from profile if available
            current_k = None
            if results:
                current_k = results[-1].get('k_value')

            return {
                'data': results,
                'current_k': current_k,
                'days': days
            }

        except Exception as e:
            print(f"Failed to query k-value history: {e}")
            return {'data': [], 'current_k': None}

    def get_solar_events_ml2(self, house_id: str, days: int = 30) -> dict:
        """
        Get detected solar heating events (ML2 model) for visualization.

        Solar events indicate when heating demand dropped due to solar gain.
        Used to visualize when the building is benefiting from free solar heating.

        Args:
            house_id: House identifier
            days: Number of days to fetch

        Returns:
            Dict with 'events' list and summary statistics
        """
        self._ensure_connection()
        if not self.client:
            return {'events': [], 'summary': {}}

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "solar_event_ml2")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            events = []
            total_duration = 0
            coefficients = []

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        duration = record.values.get('duration_minutes', 0)
                        coeff = record.values.get('implied_solar_coefficient_ml2', 0)

                        events.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'duration_minutes': duration,
                            'avg_supply_return_delta': record.values.get('avg_supply_return_delta'),
                            'avg_outdoor_temp': record.values.get('avg_outdoor_temp'),
                            'avg_indoor_temp': record.values.get('avg_indoor_temp'),
                            'avg_cloud_cover': record.values.get('avg_cloud_cover'),
                            'avg_sun_elevation': record.values.get('avg_sun_elevation'),
                            'avg_wind_speed': record.values.get('avg_wind_speed'),
                            'implied_solar_coefficient_ml2': coeff,
                            'observations_count': record.values.get('observations_count'),
                            'peak_sun_elevation': record.values.get('peak_sun_elevation'),
                        })

                        total_duration += duration or 0
                        if coeff:
                            coefficients.append(coeff)

            # Calculate summary
            summary = {
                'total_events': len(events),
                'total_duration_hours': round(total_duration / 60, 1),
                'avg_duration_minutes': round(total_duration / len(events), 0) if events else 0,
            }

            if coefficients:
                coefficients.sort()
                summary['median_coefficient'] = coefficients[len(coefficients) // 2]
                summary['min_coefficient'] = min(coefficients)
                summary['max_coefficient'] = max(coefficients)

            return {
                'events': events,
                'summary': summary,
                'days': days
            }

        except Exception as e:
            print(f"Failed to query solar events: {e}")
            return {'events': [], 'summary': {}}

    def get_weather_coefficients_ml2_history(self, house_id: str, days: int = 30) -> dict:
        """
        Get ML2 weather coefficient history for convergence visualization.

        Shows how solar coefficient has evolved over time as more events are detected.

        Args:
            house_id: House identifier
            days: Number of days to fetch

        Returns:
            Dict with 'data' list of coefficient updates over time
        """
        self._ensure_connection()
        if not self.client:
            return {'data': [], 'current': {}}

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "weather_coefficients_ml2")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            results = []
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'solar_coefficient_ml2': record.values.get('solar_coefficient_ml2'),
                            'wind_coefficient_ml2': record.values.get('wind_coefficient_ml2'),
                            'solar_confidence_ml2': record.values.get('solar_confidence_ml2'),
                            'total_solar_events': record.values.get('total_solar_events'),
                        })

            # Get current values
            current = {}
            if results:
                latest = results[-1]
                current = {
                    'solar_coefficient_ml2': latest['solar_coefficient_ml2'],
                    'wind_coefficient_ml2': latest['wind_coefficient_ml2'],
                    'solar_confidence_ml2': latest['solar_confidence_ml2'],
                    'total_solar_events': latest['total_solar_events'],
                }

            return {
                'data': results,
                'current': current,
                'days': days
            }

        except Exception as e:
            print(f"Failed to query weather coefficients history: {e}")
            return {'data': [], 'current': {}}

    def get_energy_forecast(self, house_id: str, hours: int = 24) -> dict:
        """
        Get hourly energy consumption forecast for demand response.

        Returns predicted heating energy (kWh) per hour for the next N hours.
        This data is essential for:
        - Homeowner energy planning
        - Energy company demand response (aggregate load shifting)
        - Forecast accuracy tracking at different lead times

        Args:
            house_id: House identifier
            hours: Number of hours to forecast (default 24)

        Returns:
            Dict with:
            - 'forecast': List of hourly predictions with timestamp, energy, power
            - 'summary': Total energy, peak hour, avg power
            - 'generated_at': When the forecast was generated
        """
        self._ensure_connection()
        if not self.client:
            return {'forecast': [], 'summary': {}}

        try:
            query_api = self.client.query_api()

            # Get future forecast points (timestamp > now)
            now = datetime.now(timezone.utc)
            end = now + timedelta(hours=hours)

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: now(), stop: {hours}h)
                |> filter(fn: (r) => r["_measurement"] == "energy_forecast")
                |> filter(fn: (r) => r["house_id"] == "{house_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            forecast = []
            total_energy = 0
            peak_power = 0
            peak_hour = None

            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        swedish_time = timestamp.astimezone(SWEDISH_TZ)

                        energy_kwh = record.values.get('heating_energy_kwh', 0) or 0
                        power_kw = record.values.get('heating_power_kw', 0) or 0
                        lead_time = record.values.get('lead_time_hours', 0) or 0

                        forecast.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%H:%M'),
                            'timestamp_date': swedish_time.strftime('%Y-%m-%d'),
                            'hour': swedish_time.hour,
                            'heating_energy_kwh': round(energy_kwh, 2),
                            'heating_power_kw': round(power_kw, 3),
                            'outdoor_temp': record.values.get('outdoor_temp'),
                            'effective_temp': record.values.get('effective_temp'),
                            'solar_effect': record.values.get('solar_effect'),
                            'wind_effect': record.values.get('wind_effect'),
                            'lead_time_hours': round(lead_time, 1),
                        })

                        total_energy += energy_kwh
                        if power_kw > peak_power:
                            peak_power = power_kw
                            peak_hour = swedish_time.strftime('%H:%M')

            # Calculate summary
            summary = {
                'total_energy_kwh': round(total_energy, 1),
                'avg_power_kw': round(total_energy / len(forecast), 2) if forecast else 0,
                'peak_power_kw': round(peak_power, 2),
                'peak_hour': peak_hour,
                'hours_forecasted': len(forecast),
            }

            # Get forecast generation time from the lead_time of the first point
            generated_at = None
            if forecast and forecast[0].get('lead_time_hours'):
                first_ts = datetime.fromisoformat(forecast[0]['timestamp'].replace('Z', '+00:00'))
                lead_hours = forecast[0]['lead_time_hours']
                gen_time = first_ts - timedelta(hours=lead_hours)
                generated_at = gen_time.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d %H:%M')

            return {
                'forecast': forecast,
                'summary': summary,
                'generated_at': generated_at,
                'hours': hours
            }

        except Exception as e:
            print(f"Failed to query energy forecast: {e}")
            return {'forecast': [], 'summary': {}}

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
    else:
        # Ensure connection is still valid
        _reader._ensure_connection()
    return _reader

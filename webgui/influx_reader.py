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
                    |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
                    |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
            - dh_supply_temp, dh_return_temp (district heating primary side)
            - supply_temp, return_temp (house secondary side)
            - outdoor_temperature
        """
        self._ensure_connection()
        if not self.client:
            return []

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
                |> filter(fn: (r) =>
                    r["_field"] == "dh_supply_temp" or
                    r["_field"] == "dh_return_temp" or
                    r["_field"] == "supply_temp" or
                    r["_field"] == "return_temp" or
                    r["_field"] == "outdoor_temperature"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
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
                            'dh_supply_temp': record.values.get('dh_supply_temp'),
                            'dh_return_temp': record.values.get('dh_return_temp'),
                            'supply_temp': record.values.get('supply_temp'),
                            'return_temp': record.values.get('return_temp'),
                            'outdoor_temperature': record.values.get('outdoor_temperature'),
                        })

            return results

        except Exception as e:
            print(f"Failed to query temperature history: {e}")
            return []

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
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
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
            - dh_delta_t: DH supply - return (higher = more heat extracted)
            - house_delta_t: House supply - return
            - dh_power: Power in kW
            - dh_flow: Flow rate in l/h
            - efficiency_ratio: dh_delta_t / house_delta_t (heat transfer efficiency)
        """
        self._ensure_connection()
        if not self.client:
            return []

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
                |> filter(fn: (r) =>
                    r["_field"] == "dh_supply_temp" or
                    r["_field"] == "dh_return_temp" or
                    r["_field"] == "supply_temp" or
                    r["_field"] == "return_temp" or
                    r["_field"] == "dh_power" or
                    r["_field"] == "dh_flow"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
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

                        dh_supply = record.values.get('dh_supply_temp')
                        dh_return = record.values.get('dh_return_temp')
                        house_supply = record.values.get('supply_temp')
                        house_return = record.values.get('return_temp')
                        dh_power = record.values.get('dh_power')
                        dh_flow = record.values.get('dh_flow')

                        # Calculate delta T values
                        dh_delta_t = None
                        house_delta_t = None
                        efficiency_ratio = None

                        if dh_supply is not None and dh_return is not None:
                            dh_delta_t = round(dh_supply - dh_return, 1)

                        if house_supply is not None and house_return is not None:
                            house_delta_t = round(house_supply - house_return, 1)

                        # Efficiency: how well heat is transferred from DH to house
                        if dh_delta_t and house_delta_t and house_delta_t > 0:
                            efficiency_ratio = round(dh_delta_t / house_delta_t, 2)

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'dh_delta_t': dh_delta_t,
                            'house_delta_t': house_delta_t,
                            'dh_power': round(dh_power, 1) if dh_power is not None else None,
                            'dh_flow': round(dh_flow, 0) if dh_flow is not None else None,
                            'efficiency_ratio': efficiency_ratio,
                        })

            return results

        except Exception as e:
            print(f"Failed to query efficiency metrics: {e}")
            return []

    def get_power_history(self, house_id: str, hours: int = 168) -> dict:
        """
        Get power consumption history (real-time kW readings).

        Falls back to imported energy data (kWh) if no live power data available.
        For hourly imported data, kWh ≈ average kW for that hour.

        Returns dict with:
            - data: list of dicts with timestamp and dh_power
            - data_source: 'live' or 'imported'
        """
        self._ensure_connection()
        if not self.client:
            return {'data': [], 'data_source': None}

        try:
            query_api = self.client.query_api()

            # First, try to get live power data from heating_system
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{hours}h)
                |> filter(fn: (r) => r["_measurement"] == "heating_system")
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
                |> filter(fn: (r) => r["_field"] == "dh_power")
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

                        results.append({
                            'timestamp': timestamp.isoformat(),
                            'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                            'dh_power': round(value, 1)
                        })

            # If live data found, return it
            if results:
                return {'data': results, 'data_source': 'live'}

            # Fall back to imported energy consumption data
            imported_results = self._get_power_from_imported_energy(house_id, hours, query_api)
            if imported_results:
                return {'data': imported_results, 'data_source': 'imported'}

            return {'data': [], 'data_source': None}

        except Exception as e:
            print(f"Failed to query power history: {e}")
            return {'data': [], 'data_source': None}

    def _get_power_from_imported_energy(self, house_id: str, hours: int, query_api) -> list:
        """
        Get power consumption from imported energy data as fallback.

        Queries energy_consumption measurement and converts kWh to average kW.
        For hourly data: kWh value ≈ average kW for that hour.
        """
        try:
            days = max(1, hours // 24)

            # Query imported energy data (fjv_total preferred, or fjv_heating)
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "energy_consumption")
                |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
                |> filter(fn: (r) => r["_field"] == "value")
                |> filter(fn: (r) => r["energy_type"] == "fjv_total" or r["energy_type"] == "fjv_heating")
                |> aggregateWindow(every: 1h, fn: sum, createEmpty: false)
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            # Collect data, preferring fjv_total over fjv_heating
            data_by_time = {}
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    value = record.get_value()
                    energy_type = record.values.get('energy_type', 'fjv_total')

                    if timestamp and value is not None:
                        ts_key = timestamp.isoformat()
                        # Prefer fjv_total; only use fjv_heating if no fjv_total
                        if ts_key not in data_by_time or energy_type == 'fjv_total':
                            data_by_time[ts_key] = {
                                'timestamp': timestamp,
                                'value': value,
                                'energy_type': energy_type
                            }

            # Convert to results format (kWh per hour ≈ average kW)
            results = []
            for ts_key in sorted(data_by_time.keys()):
                entry = data_by_time[ts_key]
                timestamp = entry['timestamp']

                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                swedish_time = timestamp.astimezone(SWEDISH_TZ)

                # kWh for 1 hour = average kW for that hour
                avg_kw = entry['value']

                results.append({
                    'timestamp': timestamp.isoformat(),
                    'timestamp_display': swedish_time.strftime('%Y-%m-%d %H:%M'),
                    'dh_power': round(avg_kw, 1)
                })

            return results

        except Exception as e:
            print(f"Failed to query imported energy data: {e}")
            return []

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

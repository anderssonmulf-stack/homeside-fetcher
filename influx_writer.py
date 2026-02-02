#!/usr/bin/env python3
"""
InfluxDB Client Module
Stores heating system metrics in InfluxDB for time-series analysis
and visualization in Grafana
"""

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.delete_api import DeleteApi
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone


class InfluxDBWriter:
    """
    Writes heating system data to InfluxDB for historical analysis

    Data structure:
    - Measurement: "heating_system"
    - Tags: house_id, location
    - Fields: all temperature and status values
    - Timestamp: data collection time
    """

    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        house_id: str,
        logger,
        enabled: bool = True,
        seq_logger=None
    ):
        """
        Initialize InfluxDB client

        Args:
            url: InfluxDB URL (e.g., "http://influxdb:8086")
            token: InfluxDB authentication token
            org: InfluxDB organization name
            bucket: Bucket to write data to
            house_id: Unique identifier for this house
            logger: Logger instance
            enabled: Whether InfluxDB writing is enabled
            seq_logger: Optional SeqLogger for error reporting
        """
        self.enabled = enabled
        self.house_id = house_id
        self.logger = logger
        self.seq_logger = seq_logger
        self._consecutive_failures = 0

        if not self.enabled:
            self.logger.info("InfluxDB writing disabled")
            self.client = None
            self.write_api = None
            return

        try:
            self.client = InfluxDBClient(url=url, token=token, org=org)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.bucket = bucket
            self.org = org

            # Test connection
            health = self.client.health()
            if health.status == "pass":
                self.logger.info(f"InfluxDB connected successfully: {url}")
            else:
                self.logger.warning(f"InfluxDB health check failed: {health.status}")
                self.enabled = False

        except Exception as e:
            self.logger.error(f"Failed to initialize InfluxDB client: {str(e)}")
            self._log_influx_error("InfluxDB initialization failed", e, "init")
            self.enabled = False
            self.client = None
            self.write_api = None

    def _log_influx_error(self, message: str, error: Exception, operation: str):
        """Log InfluxDB error to Seq if configured."""
        self._consecutive_failures += 1

        if self.seq_logger:
            self.seq_logger.log_error(
                f"InfluxDB {operation} failed: {message}",
                error=error,
                properties={
                    'EventType': 'InfluxDBError',
                    'Operation': operation,
                    'ConsecutiveFailures': self._consecutive_failures,
                    'HouseId': self.house_id
                }
            )

    def _log_influx_success(self):
        """Reset failure counter on successful write."""
        if self._consecutive_failures > 0:
            if self.seq_logger:
                self.seq_logger.log(
                    f"InfluxDB connection restored after {self._consecutive_failures} failures",
                    level='Information',
                    properties={
                        'EventType': 'InfluxDBRestored',
                        'PreviousFailures': self._consecutive_failures,
                        'HouseId': self.house_id
                    }
                )
            self._consecutive_failures = 0

    def write_heating_data(self, data: Dict) -> bool:
        """
        Write heating system data to InfluxDB

        Args:
            data: Dictionary with heating system metrics
                {
                    'timestamp': ISO timestamp,
                    'room_temperature': float,
                    'outdoor_temperature': float,
                    'outdoor_temp_24h_avg': float,
                    'supply_temp': float,
                    'return_temp': float,
                    'hot_water_temp': float,
                    'system_pressure': float,
                    'target_temp_setpoint': float,
                    'away_temp_setpoint': float,
                    'electric_heater': bool,
                    'heat_recovery': bool,
                    'away_mode': bool,
                    'dh_supply_temp': float,  # District heating supply
                    'dh_return_temp': float,  # District heating return
                    'dh_power': float,        # District heating power (kW)
                    'dh_flow': float,         # District heating flow (l/h)
                    'supply_setpoint': float  # PID supply temp target
                }

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not data:
            return False

        try:
            # Create data point
            point = Point("heating_system") \
                .tag("house_id", self.house_id) \
                .time(data.get('timestamp', datetime.utcnow()), WritePrecision.S)

            # Add temperature fields (rounded to 2 decimals)
            if 'room_temperature' in data:
                point.field("room_temperature", round(float(data['room_temperature']), 2))
            if 'outdoor_temperature' in data:
                point.field("outdoor_temperature", round(float(data['outdoor_temperature']), 2))
            if 'outdoor_temp_24h_avg' in data:
                point.field("outdoor_temp_24h_avg", round(float(data['outdoor_temp_24h_avg']), 2))
            if 'supply_temp' in data:
                point.field("supply_temp", round(float(data['supply_temp']), 2))
            if 'supply_temp_heat_curve' in data:
                point.field("supply_temp_heat_curve", round(float(data['supply_temp_heat_curve']), 2))
            if 'supply_temp_heat_curve_ml' in data:
                point.field("supply_temp_heat_curve_ml", round(float(data['supply_temp_heat_curve_ml']), 2))
            if 'return_temp' in data:
                point.field("return_temp", round(float(data['return_temp']), 2))
            if 'hot_water_temp' in data:
                point.field("hot_water_temp", round(float(data['hot_water_temp']), 2))
            if 'system_pressure' in data:
                point.field("system_pressure", round(float(data['system_pressure']), 2))
            if 'target_temp_setpoint' in data:
                point.field("target_temp_setpoint", round(float(data['target_temp_setpoint']), 2))
            if 'away_temp_setpoint' in data:
                point.field("away_temp_setpoint", round(float(data['away_temp_setpoint']), 2))

            # District heating fields
            if 'dh_supply_temp' in data:
                point.field("dh_supply_temp", round(float(data['dh_supply_temp']), 2))
            if 'dh_return_temp' in data:
                point.field("dh_return_temp", round(float(data['dh_return_temp']), 2))
            if 'dh_power' in data:
                point.field("dh_power", round(float(data['dh_power']), 2))
            if 'dh_flow' in data:
                point.field("dh_flow", round(float(data['dh_flow']), 2))
            if 'supply_setpoint' in data:
                point.field("supply_setpoint", round(float(data['supply_setpoint']), 2))

            # Effective temperature fields (for ML analysis)
            if 'effective_temp' in data:
                point.field("effective_temp", round(float(data['effective_temp']), 2))
            if 'effective_temp_wind_effect' in data:
                point.field("effective_temp_wind_effect", round(float(data['effective_temp_wind_effect']), 2))
            if 'effective_temp_solar_effect' in data:
                point.field("effective_temp_solar_effect", round(float(data['effective_temp_solar_effect']), 2))

            # Add boolean status fields (convert to int for easier graphing)
            if 'electric_heater' in data:
                point.field("electric_heater", 1 if data['electric_heater'] else 0)
            if 'heat_recovery' in data:
                point.field("heat_recovery", 1 if data['heat_recovery'] else 0)
            if 'away_mode' in data:
                point.field("away_mode", 1 if data['away_mode'] else 0)

            # Write to InfluxDB
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self._log_influx_success()
            return True

        except Exception as e:
            self.logger.error(f"Failed to write to InfluxDB: {str(e)}")
            self._log_influx_error("Heating data write failed", e, "write_heating_data")
            return False

    def write_forecast_data(self, forecast: Dict) -> bool:
        """
        Write weather forecast data to InfluxDB

        Args:
            forecast: Dictionary with forecast data
                {
                    'current_temp': float,
                    'avg_temp': float,
                    'max_temp': float,
                    'min_temp': float,
                    'trend': str,
                    'trend_symbol': str,
                    'change': float,
                    'forecast_hours': int,
                    'avg_cloud_cover': float,
                    'cloud_condition': str
                }

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not forecast:
            return False

        try:
            point = Point("weather_forecast") \
                .tag("house_id", self.house_id) \
                .tag("trend", forecast.get('trend', 'unknown')) \
                .tag("cloud_condition", forecast.get('cloud_condition', 'unknown')) \
                .field("current_temp", round(float(forecast['current_temp']), 2)) \
                .field("avg_temp", round(float(forecast['avg_temp']), 2)) \
                .field("max_temp", round(float(forecast['max_temp']), 2)) \
                .field("min_temp", round(float(forecast['min_temp']), 2)) \
                .field("temp_change", round(float(forecast['change']), 2)) \
                .field("forecast_hours", int(forecast['forecast_hours'])) \
                .time(datetime.utcnow(), WritePrecision.S)

            # Add cloud cover if available
            if 'avg_cloud_cover' in forecast and forecast['avg_cloud_cover'] is not None:
                point.field("avg_cloud_cover", round(float(forecast['avg_cloud_cover']), 2))

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"Failed to write forecast to InfluxDB: {str(e)}")
            self._log_influx_error("Forecast write failed", e, "write_forecast_data")
            return False

    def write_weather_forecast_points(self, forecast_data: List[Dict]) -> bool:
        """
        Write detailed hourly weather forecast points to InfluxDB.

        Stores the raw SMHI forecast data for historical analysis, allowing
        us to see what weather data predictions were based on.

        Args:
            forecast_data: List of forecast dictionaries from SMHI:
                [
                    {
                        'time': str (ISO timestamp),
                        'temp': float,
                        'hour': float (hours from now),
                        'cloud_cover': float (optional, 0-8 octas),
                        'wind_speed': float (optional, m/s),
                        'wind_gust': float (optional, m/s),
                        'wind_direction': float (optional, degrees),
                        'humidity': float (optional, %),
                        'precipitation': float (optional, mm/h),
                        'visibility': float (optional, km)
                    },
                    ...
                ]

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not forecast_data:
            return False

        try:
            points = []
            for data in forecast_data:
                time_str = data.get('time')
                if not time_str:
                    continue

                # Parse forecast target time
                target_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))

                # No forecast_generated_at tag - points with same timestamp will overwrite
                # This gives us: history (past forecasts kept) + latest future forecast
                point = Point("weather_forecast_hourly") \
                    .tag("house_id", self.house_id) \
                    .field("temperature", round(float(data['temp']), 2)) \
                    .field("lead_time_hours", round(float(data.get('hour', 0)), 1)) \
                    .time(target_time, WritePrecision.S)

                # Add optional fields
                if data.get('cloud_cover') is not None:
                    point.field("cloud_cover", round(float(data['cloud_cover']), 1))
                if data.get('wind_speed') is not None:
                    point.field("wind_speed", round(float(data['wind_speed']), 1))
                if data.get('wind_gust') is not None:
                    point.field("wind_gust", round(float(data['wind_gust']), 1))
                if data.get('wind_direction') is not None:
                    point.field("wind_direction", round(float(data['wind_direction']), 0))
                if data.get('humidity') is not None:
                    point.field("humidity", round(float(data['humidity']), 0))
                if data.get('precipitation') is not None:
                    point.field("precipitation", round(float(data['precipitation']), 2))
                if data.get('visibility') is not None:
                    point.field("visibility", round(float(data['visibility']), 1))

                points.append(point)

            if points:
                self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                self.logger.info(f"Wrote {len(points)} weather forecast points to InfluxDB")
                return True

            return False

        except Exception as e:
            self.logger.error(f"Failed to write weather forecast points: {str(e)}")
            return False

    def delete_future_weather_forecasts(self) -> bool:
        """
        Delete only FUTURE weather forecast points for this house.

        Past forecast points are kept as history (one per hour).
        Future points are deleted before writing fresh forecast data.

        This gives us:
        - Historical record of what forecasts predicted for past times
        - Always the latest forecast for future times
        - No accumulation of multiple overlapping forecast series

        Returns:
            True if delete succeeded, False otherwise
        """
        if not self.enabled:
            return False

        try:
            delete_api = self.client.delete_api()

            # Only delete future forecasts (from now to +7 days)
            # Past forecasts remain as historical record
            start = datetime.now(timezone.utc)
            stop = datetime.now(timezone.utc) + timedelta(days=7)

            predicate = f'_measurement="weather_forecast_hourly" AND house_id="{self.house_id}"'

            delete_api.delete(
                start=start,
                stop=stop,
                predicate=predicate,
                bucket=self.bucket,
                org=self.org
            )

            self.logger.info("Deleted future forecast points (past forecasts kept as history)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete future weather forecasts: {str(e)}")
            return False

    def write_control_decision(self, decision: Dict) -> bool:
        """
        Write heating control decision to InfluxDB for analysis

        Args:
            decision: Dictionary with control decision
                {
                    'reduce_heating': bool,
                    'reason': str,
                    'confidence': float,
                    'forecast_change': float,
                    'current_indoor': float,
                    'solar_factor': str,
                    'cloud_condition': str
                }

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not decision:
            return False

        try:
            point = Point("heating_control") \
                .tag("house_id", self.house_id) \
                .tag("action", "reduce" if decision['reduce_heating'] else "maintain") \
                .tag("solar_factor", decision.get('solar_factor', 'unknown')) \
                .tag("cloud_condition", decision.get('cloud_condition', 'unknown')) \
                .field("reduce_heating", 1 if decision['reduce_heating'] else 0) \
                .field("confidence", round(float(decision['confidence']), 2)) \
                .field("forecast_change", round(float(decision.get('forecast_change', 0)), 2)) \
                .field("current_indoor", round(float(decision.get('current_indoor', 0)), 2)) \
                .time(datetime.utcnow(), WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"Failed to write control decision to InfluxDB: {str(e)}")
            return False

    def write_weather_observation(self, observation: Dict) -> bool:
        """
        Write current weather observation to InfluxDB.

        Args:
            observation: Dictionary with observation data
                {
                    'station_name': str,
                    'station_id': int,
                    'distance_km': float,
                    'temperature': float,
                    'wind_speed': float (optional),
                    'humidity': float (optional),
                    'timestamp': datetime
                }

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not observation:
            return False

        try:
            point = Point("weather_observation") \
                .tag("house_id", self.house_id) \
                .tag("station_name", observation.get('station_name', 'unknown')) \
                .tag("station_id", str(observation.get('station_id', 0))) \
                .field("temperature", round(float(observation['temperature']), 2)) \
                .field("distance_km", round(float(observation.get('distance_km', 0)), 2)) \
                .time(observation.get('timestamp', datetime.utcnow()), WritePrecision.S)

            # Optional fields
            if 'wind_speed' in observation and observation['wind_speed'] is not None:
                point.field("wind_speed", round(float(observation['wind_speed']), 2))
            if 'humidity' in observation and observation['humidity'] is not None:
                point.field("humidity", round(float(observation['humidity']), 2))

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"Failed to write weather observation to InfluxDB: {str(e)}")
            self._log_influx_error("Weather observation write failed", e, "write_weather_observation")
            return False

    def write_thermal_coefficient(self, coefficient: float, learning_period_hours: int) -> bool:
        """
        Write learned thermal coefficient to InfluxDB

        The thermal coefficient represents how quickly the building responds
        to outdoor temperature changes. This is learned over time.

        Args:
            coefficient: Thermal response coefficient (°C indoor per °C outdoor per hour)
            learning_period_hours: Number of hours of data used for learning

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled:
            return False

        try:
            point = Point("thermal_learning") \
                .tag("house_id", self.house_id) \
                .field("thermal_coefficient", round(float(coefficient), 2)) \
                .field("learning_period_hours", int(learning_period_hours)) \
                .time(datetime.utcnow(), WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"Failed to write thermal coefficient to InfluxDB: {str(e)}")
            return False

    def write_heat_curve_baseline(self, curve_values: Dict[int, float]) -> bool:
        """
        Store heat curve baseline Y-values to InfluxDB.
        Called before entering reduction mode to save current HomeSide values.

        Args:
            curve_values: Dictionary mapping index (64-73) to Y-value (supply temp)
                {64: 40.0, 65: 38.0, ..., 73: 23.34}

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not curve_values:
            return False

        try:
            point = Point("heat_curve_baseline") \
                .tag("house_id", self.house_id) \
                .time(datetime.utcnow(), WritePrecision.S)

            # Store each Y-value as a field
            for index, value in curve_values.items():
                point.field(f"y_{index}", round(float(value), 2))

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.info(f"Stored heat curve baseline: {len(curve_values)} points")
            return True

        except Exception as e:
            self.logger.error(f"Failed to write heat curve baseline: {str(e)}")
            return False

    def read_heat_curve_baseline(self) -> Optional[Dict[int, float]]:
        """
        Read the most recent heat curve baseline from InfluxDB.

        Returns:
            Dictionary mapping index (64-73) to Y-value, or None if not found
        """
        if not self.enabled:
            return None

        try:
            query_api = self.client.query_api()

            # Query for the most recent baseline
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -30d)
                |> filter(fn: (r) => r["_measurement"] == "heat_curve_baseline")
                |> filter(fn: (r) => r["house_id"] == "{self.house_id}")
                |> last()
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

            tables = query_api.query(query, org=self.org)

            if not tables or len(tables) == 0:
                return None

            # Extract values from the result
            curve_values = {}
            for table in tables:
                for record in table.records:
                    values = record.values
                    for key, value in values.items():
                        if key.startswith('y_') and value is not None:
                            index = int(key.replace('y_', ''))
                            curve_values[index] = float(value)

            return curve_values if curve_values else None

        except Exception as e:
            self.logger.error(f"Failed to read heat curve baseline: {str(e)}")
            return None

    def write_heat_curve_adjustment(
        self,
        action: str,
        adjusted_points: Dict[int, float],
        delta: float,
        reason: str,
        forecast_change: float = 0.0,
        duration_hours: float = 0.0
    ) -> bool:
        """
        Log a heat curve adjustment event.

        Args:
            action: "reduce" when entering reduction mode, "restore" when exiting
            adjusted_points: Dictionary of {index: new_value} for affected points
            delta: The adjustment delta applied (negative for reduction)
            reason: Human-readable reason for the adjustment
            forecast_change: Forecasted outdoor temp change that triggered this
            duration_hours: Expected duration of the adjustment

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled:
            return False

        try:
            point = Point("heat_curve_adjustment") \
                .tag("house_id", self.house_id) \
                .tag("action", action) \
                .field("delta", round(float(delta), 2)) \
                .field("points_adjusted", len(adjusted_points)) \
                .field("forecast_change", round(float(forecast_change), 2)) \
                .field("duration_hours", round(float(duration_hours), 2)) \
                .field("reason", reason) \
                .time(datetime.utcnow(), WritePrecision.S)

            # Log which points were adjusted and their new values
            for index, value in adjusted_points.items():
                point.field(f"point_{index}", round(float(value), 2))

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.info(f"Logged heat curve {action}: {len(adjusted_points)} points, delta={delta}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to write heat curve adjustment: {str(e)}")
            return False

    def write_thermal_data_point(self, data: Dict) -> bool:
        """
        Write a thermal data point to InfluxDB for persistence.

        This allows the thermal analyzer to restore historical data on restart.

        Args:
            data: Dictionary with thermal data
                {
                    'timestamp': datetime or ISO string,
                    'room_temperature': float,
                    'outdoor_temperature': float,
                    'supply_temp': float (optional),
                    'electric_heater': bool (optional)
                }

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not data:
            return False

        try:
            timestamp = data.get('timestamp')
            if isinstance(timestamp, str):
                # Parse ISO string to datetime
                if timestamp.endswith('Z'):
                    timestamp = timestamp.replace('Z', '+00:00')
                from datetime import datetime as dt
                timestamp = dt.fromisoformat(timestamp)

            point = Point("thermal_history") \
                .tag("house_id", self.house_id) \
                .field("room_temperature", round(float(data['room_temperature']), 2)) \
                .field("outdoor_temperature", round(float(data['outdoor_temperature']), 2)) \
                .time(timestamp, WritePrecision.S)

            # Optional fields
            if 'supply_temp' in data and data['supply_temp'] is not None:
                point.field("supply_temp", round(float(data['supply_temp']), 2))
            if 'electric_heater' in data and data['electric_heater'] is not None:
                point.field("electric_heater", 1 if data['electric_heater'] else 0)
            if 'return_temp' in data and data['return_temp'] is not None:
                point.field("return_temp", round(float(data['return_temp']), 2))

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"Failed to write thermal data point: {str(e)}")
            self._log_influx_error("Thermal data point write failed", e, "write_thermal_data_point")
            return False

    def delete_old_forecasts(self) -> bool:
        """
        Delete old forecast points that are in the past.

        Only deletes forecasts for PAST times (already resolved), not future times.
        This allows multiple predictions for the same future time to coexist,
        enabling lead-time accuracy comparison (24h vs 12h vs 3h predictions).

        Returns:
            True if delete succeeded, False otherwise
        """
        if not self.enabled:
            return False

        try:
            delete_api = self.client.delete_api()

            # Only delete PAST forecasts (from -48h to now)
            # Keep future forecasts to allow lead-time accuracy comparison
            start = datetime.now(timezone.utc) - timedelta(hours=48)
            stop = datetime.now(timezone.utc)

            # Delete predicate: measurement and house_id
            predicate = f'_measurement="temperature_forecast" AND house_id="{self.house_id}"'

            delete_api.delete(
                start=start,
                stop=stop,
                predicate=predicate,
                bucket=self.bucket,
                org=self.org
            )

            self.logger.info("Deleted past forecast points (keeping future forecasts for lead-time comparison)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete old forecasts: {str(e)}")
            return False

    def write_forecast_points(self, forecast_data: List[Dict]) -> bool:
        """
        Write hourly forecast points to InfluxDB with FUTURE timestamps.

        Each forecast point contains temperature predictions at a future time,
        along with lead_time_hours to enable accuracy comparison at different
        prediction horizons (24h, 12h, 3h).

        Args:
            forecast_data: List of forecast dictionaries:
                [
                    {
                        'timestamp': datetime (future time),
                        'forecast_type': str ('outdoor_temp', 'supply_temp_baseline',
                                             'supply_temp_ml', 'indoor_temp'),
                        'value': float (temperature in Celsius),
                        'lead_time_hours': float (hours from now to forecast time)
                    },
                    ...
                ]

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not forecast_data:
            return False

        try:
            # Use current time as the "forecast generation time" tag
            forecast_time = datetime.now(timezone.utc).isoformat()

            points = []
            for data in forecast_data:
                timestamp = data.get('timestamp')
                forecast_type = data.get('forecast_type')
                value = data.get('value')
                lead_time_hours = data.get('lead_time_hours', 0.0)

                if timestamp is None or forecast_type is None or value is None:
                    continue

                point = Point("temperature_forecast") \
                    .tag("house_id", self.house_id) \
                    .tag("forecast_type", forecast_type) \
                    .tag("forecast_time", forecast_time) \
                    .field("value", round(float(value), 2)) \
                    .field("lead_time_hours", round(float(lead_time_hours), 1)) \
                    .time(timestamp, WritePrecision.S)

                points.append(point)

            if points:
                self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                self.logger.info(f"Wrote {len(points)} forecast points to InfluxDB (with lead_time_hours)")
                return True

            return False

        except Exception as e:
            self.logger.error(f"Failed to write forecast points: {str(e)}")
            return False

    def write_learned_parameters(self, profile_data: Dict) -> bool:
        """
        Write learned parameters history to InfluxDB.

        Tracks how the learning algorithm adapts over time.
        Useful for debugging and visualizing algorithm behavior.

        Args:
            profile_data: Dictionary containing learned parameters:
                - thermal_coefficient
                - thermal_coefficient_confidence
                - total_samples
                - hourly_bias (dict of hour -> bias)

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled:
            return False

        try:
            point = Point("learned_parameters") \
                .tag("house_id", self.house_id) \
                .field("thermal_coefficient", profile_data.get("thermal_coefficient") or 0.0) \
                .field("confidence", profile_data.get("thermal_coefficient_confidence", 0.0)) \
                .field("total_samples", profile_data.get("total_samples", 0)) \
                .time(datetime.now(timezone.utc), WritePrecision.S)

            # Add hourly bias values as fields
            hourly_bias = profile_data.get("hourly_bias", {})
            for hour, bias in hourly_bias.items():
                point = point.field(f"bias_{hour}", bias)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.debug("Wrote learned parameters to InfluxDB")
            return True

        except Exception as e:
            self.logger.error(f"Failed to write learned parameters: {str(e)}")
            return False

    def write_forecast_accuracy(
        self,
        predicted: float,
        actual: float,
        error: float,
        hour: int,
        outdoor: float
    ) -> bool:
        """
        Write forecast accuracy measurement to InfluxDB.

        Tracks how accurate our predictions are, enabling:
        - Visualization of prediction quality over time
        - Analysis of which hours have systematic bias
        - Debugging of the forecasting algorithm

        Args:
            predicted: What we predicted the temp would be
            actual: What the temp actually was
            error: actual - predicted (positive = underestimate)
            hour: Hour of day (0-23)
            outdoor: Outdoor temperature at measurement time

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled:
            return False

        try:
            point = Point("forecast_accuracy") \
                .tag("house_id", self.house_id) \
                .tag("hour", f"{hour:02d}") \
                .field("predicted", round(float(predicted), 2)) \
                .field("actual", round(float(actual), 2)) \
                .field("error", round(float(error), 3)) \
                .field("outdoor", round(float(outdoor), 1)) \
                .time(datetime.now(timezone.utc), WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.debug(f"Wrote forecast accuracy: predicted={predicted:.1f}, actual={actual:.1f}, error={error:.2f}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to write forecast accuracy: {str(e)}")
            self._log_influx_error("Forecast accuracy write failed", e, "write_forecast_accuracy")
            return False

    def write_energy_forecast(self, forecast_points: list) -> bool:
        """
        Write energy forecast points to InfluxDB.

        Args:
            forecast_points: List of EnergyForecastPoint objects with:
                - timestamp: Target time for this prediction
                - heating_power_kw: Predicted heating power
                - heating_energy_kwh: Predicted energy for this hour
                - outdoor_temp, effective_temp, wind_effect, solar_effect
                - lead_time_hours: Hours from forecast generation

        Returns:
            True if write succeeded, False otherwise
        """
        if not self.enabled or not forecast_points:
            return False

        try:
            points = []
            for fp in forecast_points:
                point = Point("energy_forecast") \
                    .tag("house_id", self.house_id) \
                    .field("heating_power_kw", round(float(fp.heating_power_kw), 3)) \
                    .field("heating_energy_kwh", round(float(fp.heating_energy_kwh), 3)) \
                    .field("outdoor_temp", round(float(fp.outdoor_temp), 1)) \
                    .field("effective_temp", round(float(fp.effective_temp), 1)) \
                    .field("wind_effect", round(float(fp.wind_effect), 2)) \
                    .field("solar_effect", round(float(fp.solar_effect), 2)) \
                    .field("lead_time_hours", round(float(fp.lead_time_hours), 1)) \
                    .time(fp.timestamp, WritePrecision.S)

                points.append(point)

            if points:
                self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                self.logger.info(f"Wrote {len(points)} energy forecast points to InfluxDB")
                return True

            return False

        except Exception as e:
            self.logger.error(f"Failed to write energy forecast: {str(e)}")
            return False

    def delete_future_energy_forecasts(self) -> bool:
        """
        Delete future energy forecast points (keep history, update future).

        Returns:
            True if delete succeeded, False otherwise
        """
        if not self.enabled:
            return False

        try:
            delete_api = self.client.delete_api()

            start = datetime.now(timezone.utc)
            stop = datetime.now(timezone.utc) + timedelta(days=7)

            predicate = f'_measurement="energy_forecast" AND house_id="{self.house_id}"'

            delete_api.delete(
                start=start,
                stop=stop,
                predicate=predicate,
                bucket=self.bucket,
                org=self.org
            )

            return True

        except Exception as e:
            self.logger.error(f"Failed to delete energy forecasts: {str(e)}")
            return False

    def read_thermal_history(self, days: int = 7) -> list:
        """
        Read thermal history data from InfluxDB.

        Used to restore thermal analyzer state on startup.

        Args:
            days: Number of days of history to read (default 7)

        Returns:
            List of dictionaries with thermal data, sorted by timestamp
        """
        if not self.enabled:
            return []

        try:
            query_api = self.client.query_api()

            # Query raw field data (pivot doesn't work well across different house_ids)
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "thermal_history")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            # Group data by timestamp
            data_by_time = {}
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    field = record.get_field()
                    value = record.get_value()

                    if timestamp not in data_by_time:
                        data_by_time[timestamp] = {'timestamp': timestamp.isoformat()}

                    data_by_time[timestamp][field] = value

            # Convert to list and filter valid entries
            data_points = []
            for timestamp in sorted(data_by_time.keys()):
                data = data_by_time[timestamp]

                # Only include if we have required fields
                if 'room_temperature' in data and 'outdoor_temperature' in data:
                    data_point = {
                        'timestamp': data['timestamp'],
                        'room_temperature': float(data['room_temperature']),
                        'outdoor_temperature': float(data['outdoor_temperature']),
                    }

                    # Optional fields
                    if data.get('supply_temp') is not None:
                        data_point['supply_temp'] = float(data['supply_temp'])
                    if data.get('electric_heater') is not None:
                        data_point['electric_heater'] = bool(data['electric_heater'])
                    if data.get('return_temp') is not None:
                        data_point['return_temp'] = float(data['return_temp'])

                    data_points.append(data_point)

            self.logger.info(f"Read {len(data_points)} thermal history points from InfluxDB")
            return data_points

        except Exception as e:
            self.logger.error(f"Failed to read thermal history: {str(e)}")
            return []

    def get_last_data_timestamps(self) -> Dict[str, Optional[datetime]]:
        """
        Get the timestamp of the last data point for each measurement type.

        Used on startup to check for stale data.

        Returns:
            Dictionary with measurement names as keys and last timestamp as values:
            {
                'heating_system': datetime or None,
                'weather_forecast': datetime or None,
                'temperature_forecast': datetime or None
            }
        """
        if not self.enabled:
            return {
                'heating_system': None,
                'weather_forecast': None,
                'temperature_forecast': None
            }

        results = {}
        measurements = ['heating_system', 'weather_forecast', 'temperature_forecast']

        try:
            query_api = self.client.query_api()
            # Escape forward slashes for Flux regex
            escaped_house_id = self.house_id.replace('/', '\\/')

            for measurement in measurements:
                try:
                    query = f'''
                        from(bucket: "{self.bucket}")
                        |> range(start: -30d)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                        |> filter(fn: (r) => r["house_id"] =~ /{escaped_house_id}/)
                        |> filter(fn: (r) => r["_field"] == "room_temperature" or r["_field"] == "temperature")
                        |> sort(columns: ["_time"], desc: true)
                        |> first()
                    '''

                    tables = query_api.query(query, org=self.org)

                    last_time = None
                    for table in tables:
                        for record in table.records:
                            record_time = record.get_time()
                            if record_time and (last_time is None or record_time > last_time):
                                last_time = record_time

                    results[measurement] = last_time

                except Exception as e:
                    self.logger.warning(f"Failed to get last timestamp for {measurement}: {e}")
                    results[measurement] = None

            return results

        except Exception as e:
            self.logger.error(f"Failed to get last data timestamps: {str(e)}")
            return {m: None for m in measurements}

    def close(self):
        """Close InfluxDB client connection"""
        if self.client:
            self.client.close()

#!/usr/bin/env python3
"""
InfluxDB Client Module
Stores heating system metrics in InfluxDB for time-series analysis
and visualization
"""

import time

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.delete_api import DeleteApi
from typing import Dict, List, Optional, Tuple
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
        seq_logger=None,
        settings: dict = None
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
            settings: Optional dict with influxdb settings from settings.json
        """
        self.enabled = enabled
        self.house_id = house_id
        self.logger = logger
        self.seq_logger = seq_logger
        self._consecutive_failures = 0
        self._circuit_open_time = None   # When circuit breaker opened

        # Circuit breaker settings (from settings.json "influxdb" section)
        influx_settings = settings or {}
        self._write_timeout_ms = influx_settings.get('write_timeout_ms', 5000)
        self._circuit_breaker_threshold = influx_settings.get('circuit_breaker_threshold', 3)
        self._circuit_cooldown = influx_settings.get('circuit_breaker_cooldown_seconds', 60)

        # Store connection params for reconnect
        self._url = url
        self._token = token
        self._org = org
        self._bucket = bucket

        if not self.enabled:
            self.logger.info("InfluxDB writing disabled")
            self.client = None
            self.write_api = None
            return

        try:
            self.client = InfluxDBClient(url=url, token=token, org=org, timeout=self._write_timeout_ms)
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

        # Open circuit breaker when threshold is first hit
        if self._consecutive_failures == self._circuit_breaker_threshold and self._circuit_open_time is None:
            self._circuit_open_time = time.monotonic()
            self.logger.warning(
                f"Circuit breaker OPEN after {self._consecutive_failures} failures — "
                f"skipping writes for {self._circuit_cooldown}s"
            )
            if self.seq_logger:
                self.seq_logger.log(
                    "InfluxDB circuit breaker opened after {Failures} consecutive failures",
                    level='Warning',
                    properties={
                        'EventType': 'InfluxDBCircuitOpen',
                        'ConsecutiveFailures': self._consecutive_failures,
                        'CooldownSeconds': self._circuit_cooldown,
                        'HouseId': self.house_id
                    }
                )

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
            self._circuit_open_time = None

    def _should_write(self) -> bool:
        """
        Circuit breaker guard — call at the top of every write/delete method.

        Returns True if the write should proceed, False to skip.
        After 3 consecutive failures the circuit opens for 60s, then
        a reconnect is attempted.  If the reconnect succeeds the circuit
        closes; otherwise it stays open for another 60s.
        """
        if not self.enabled:
            return False

        # Circuit is closed — allow writes
        if self._consecutive_failures < self._circuit_breaker_threshold:
            return True

        # Circuit is open — check if cooldown has elapsed
        if self._circuit_open_time is None:
            self._circuit_open_time = time.monotonic()

        elapsed = time.monotonic() - self._circuit_open_time
        if elapsed < self._circuit_cooldown:
            return False  # Still in cooldown, skip silently

        # Cooldown elapsed — attempt reconnect
        if self._reconnect():
            return True   # Reconnected, allow writes
        else:
            # Reconnect failed — extend cooldown
            self._circuit_open_time = time.monotonic()
            return False

    def _reconnect(self) -> bool:
        """
        Close the old client and create a fresh one.
        Returns True if the new client passes a health check.
        """
        self.logger.info("Attempting InfluxDB reconnect...")
        try:
            if self.client:
                try:
                    self.client.close()
                except Exception:
                    pass

            self.client = InfluxDBClient(
                url=self._url, token=self._token, org=self._org, timeout=self._write_timeout_ms
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

            health = self.client.health()
            if health.status == "pass":
                self._consecutive_failures = 0
                self._circuit_open_time = None
                self.logger.info("InfluxDB reconnected successfully")
                if self.seq_logger:
                    self.seq_logger.log(
                        "InfluxDB reconnected after circuit breaker",
                        level='Information',
                        properties={
                            'EventType': 'InfluxDBReconnected',
                            'HouseId': self.house_id
                        }
                    )
                return True
            else:
                self.logger.warning(f"InfluxDB reconnect health check failed: {health.status}")
                return False
        except Exception as e:
            self.logger.warning(f"InfluxDB reconnect failed: {e}")
            return False

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
        if not self._should_write() or not data:
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
        if not self._should_write() or not forecast:
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
        if not self._should_write() or not forecast_data:
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
        if not self._should_write():
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
        if not self._should_write() or not decision:
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
        if not self._should_write() or not observation:
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
        if not self._should_write():
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
        if not self._should_write() or not curve_values:
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
        if not self._should_write():
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
        if not self._should_write() or not data:
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
        if not self._should_write() or not forecast_data:
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
        if not self._should_write():
            return False

        from write_throttle import WriteThrottle
        if not WriteThrottle.get().allow("learned_parameters", self.house_id, 3600):
            return True

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
        if not self._should_write():
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
        if not self._should_write() or not forecast_points:
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

    def delete_future_forecasts(self) -> bool:
        """
        Delete future temperature forecast points (keep history, update future).

        This prevents the "curtain" effect where multiple predictions for the
        same future timestamp accumulate and cause visual artifacts in charts.

        Returns:
            True if delete succeeded, False otherwise
        """
        if not self._should_write():
            return False

        try:
            delete_api = self.client.delete_api()

            start = datetime.now(timezone.utc)
            stop = datetime.now(timezone.utc) + timedelta(days=7)

            predicate = f'_measurement="temperature_forecast" AND house_id="{self.house_id}"'

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
            self.logger.error(f"Failed to delete future forecasts: {str(e)}")
            return False

    def delete_future_energy_forecasts(self) -> bool:
        """
        Delete future energy forecast points (keep history, update future).

        Returns:
            True if delete succeeded, False otherwise
        """
        if not self._should_write():
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

            for measurement in measurements:
                try:
                    query = f'''
                        from(bucket: "{self.bucket}")
                        |> range(start: -30d)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                        |> filter(fn: (r) => r["house_id"] == "{self.house_id}")
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

    def write_shared_weather_forecast(self, forecast_data: List[Dict], lat: float, lon: float) -> bool:
        """
        Write weather forecast to shared cache (not house-specific).

        Used for caching SMHI forecasts when multiple houses share the same location.
        Other houses can read this cache to avoid duplicate API calls.

        Args:
            forecast_data: List of forecast dictionaries from SMHI
            lat: Latitude used for the forecast
            lon: Longitude used for the forecast

        Returns:
            True if write succeeded, False otherwise
        """
        if not self._should_write() or not forecast_data:
            return False

        try:
            # Round coordinates to 3 decimal places for consistent cache key
            location_key = f"{lat:.3f},{lon:.3f}"

            points = []
            for data in forecast_data:
                time_str = data.get('time')
                if not time_str:
                    continue

                target_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))

                point = Point("weather_forecast_shared") \
                    .tag("location", location_key) \
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
                self.logger.info(f"Wrote {len(points)} shared weather forecast points (location: {location_key})")
                return True

            return False

        except Exception as e:
            self.logger.error(f"Failed to write shared weather forecast: {str(e)}")
            return False

    def read_shared_weather_forecast(self, lat: float, lon: float, max_age_minutes: int = 120) -> Optional[List[Dict]]:
        """
        Read weather forecast from shared cache if recent enough.

        Args:
            lat: Latitude to look up
            lon: Longitude to look up
            max_age_minutes: Maximum age of cached forecast (default 2 hours)

        Returns:
            List of forecast dictionaries if cache hit, None if miss or stale
        """
        if not self.enabled:
            return None

        try:
            query_api = self.client.query_api()
            location_key = f"{lat:.3f},{lon:.3f}"

            # Query for future forecast points from this location
            # Check if the most recent write was within max_age_minutes
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: now(), stop: 7d)
                |> filter(fn: (r) => r["_measurement"] == "weather_forecast_shared")
                |> filter(fn: (r) => r["location"] == "{location_key}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            forecast_points = []
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    values = record.values

                    point = {
                        'time': timestamp.isoformat(),
                        'temp': values.get('temperature'),
                        'hour': values.get('lead_time_hours', 0)
                    }

                    # Add optional fields if present
                    if values.get('cloud_cover') is not None:
                        point['cloud_cover'] = values['cloud_cover']
                    if values.get('wind_speed') is not None:
                        point['wind_speed'] = values['wind_speed']
                    if values.get('wind_gust') is not None:
                        point['wind_gust'] = values['wind_gust']
                    if values.get('wind_direction') is not None:
                        point['wind_direction'] = values['wind_direction']
                    if values.get('humidity') is not None:
                        point['humidity'] = values['humidity']
                    if values.get('precipitation') is not None:
                        point['precipitation'] = values['precipitation']
                    if values.get('visibility') is not None:
                        point['visibility'] = values['visibility']

                    forecast_points.append(point)

            if not forecast_points:
                return None

            # Check if we have enough future data (at least 12 hours)
            if len(forecast_points) < 12:
                self.logger.info(f"Shared weather cache has only {len(forecast_points)} points, fetching fresh")
                return None

            # Check the lead_time_hours of the first point to see how fresh the data is
            # If the first point has lead_time > max_age_minutes/60, the cache is stale
            first_lead_time = forecast_points[0].get('hour', 999)
            if first_lead_time > max_age_minutes / 60:
                self.logger.info(f"Shared weather cache is stale (first point lead_time: {first_lead_time:.1f}h)")
                return None

            self.logger.info(f"Using shared weather cache: {len(forecast_points)} points (location: {location_key})")
            return forecast_points

        except Exception as e:
            self.logger.error(f"Failed to read shared weather forecast: {str(e)}")
            return None

    def write_shared_weather_observation(self, observation: Dict, lat: float, lon: float) -> bool:
        """
        Write weather observation to shared cache (for effective_temp calculation).

        Args:
            observation: Dictionary with observation data (temperature, wind, humidity, etc.)
            lat: Latitude used for the observation
            lon: Longitude used for the observation

        Returns:
            True if write succeeded, False otherwise
        """
        if not self._should_write() or not observation:
            return False

        try:
            location_key = f"{lat:.3f},{lon:.3f}"

            point = Point("weather_observation_shared") \
                .tag("location", location_key) \
                .tag("station_name", observation.get('station_name', 'unknown')) \
                .field("temperature", round(float(observation['temperature']), 2)) \
                .field("distance_km", round(float(observation.get('distance_km', 0)), 2)) \
                .time(observation.get('timestamp', datetime.now(timezone.utc)), WritePrecision.S)

            if observation.get('wind_speed') is not None:
                point.field("wind_speed", round(float(observation['wind_speed']), 2))
            if observation.get('humidity') is not None:
                point.field("humidity", round(float(observation['humidity']), 2))

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            return True

        except Exception as e:
            self.logger.error(f"Failed to write shared weather observation: {str(e)}")
            return False

    def read_shared_weather_observation(self, lat: float, lon: float, max_age_minutes: int = 30) -> Optional[Dict]:
        """
        Read weather observation from shared cache if recent enough.

        Args:
            lat: Latitude to look up
            lon: Longitude to look up
            max_age_minutes: Maximum age of cached observation (default 30 min)

        Returns:
            Dictionary with observation data if cache hit, None if miss or stale
        """
        if not self.enabled:
            return None

        try:
            query_api = self.client.query_api()
            location_key = f"{lat:.3f},{lon:.3f}"

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{max_age_minutes}m)
                |> filter(fn: (r) => r["_measurement"] == "weather_observation_shared")
                |> filter(fn: (r) => r["location"] == "{location_key}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"], desc: true)
                |> limit(n: 1)
            '''

            tables = query_api.query(query, org=self.org)

            for table in tables:
                for record in table.records:
                    values = record.values
                    timestamp = record.get_time()

                    observation = {
                        'station_name': values.get('station_name', 'unknown'),
                        'temperature': values.get('temperature'),
                        'distance_km': values.get('distance_km', 0),
                        'timestamp': timestamp
                    }

                    if values.get('wind_speed') is not None:
                        observation['wind_speed'] = values['wind_speed']
                    if values.get('humidity') is not None:
                        observation['humidity'] = values['humidity']

                    self.logger.info(f"Using shared weather observation: {observation['temperature']:.1f}°C from {observation['station_name']}")
                    return observation

            return None

        except Exception as e:
            self.logger.error(f"Failed to read shared weather observation: {str(e)}")
            return None

    def delete_old_shared_weather_forecasts(self) -> bool:
        """
        Delete past shared weather forecast points (keep future only).

        Returns:
            True if delete succeeded, False otherwise
        """
        if not self._should_write():
            return False

        try:
            delete_api = self.client.delete_api()

            # Delete forecasts from the past
            start = datetime(2020, 1, 1, tzinfo=timezone.utc)
            stop = datetime.now(timezone.utc)

            predicate = '_measurement="weather_forecast_shared"'

            delete_api.delete(
                start=start,
                stop=stop,
                predicate=predicate,
                bucket=self.bucket,
                org=self.org
            )

            return True

        except Exception as e:
            self.logger.error(f"Failed to delete old shared weather forecasts: {str(e)}")
            return False

    def write_solar_event(self, event_data: dict) -> bool:
        """
        Write a detected solar heating event to InfluxDB.

        Solar events are periods when heating demand dropped due to solar gain.
        Used for solar coefficient learning (ML2 model).

        Args:
            event_data: Dictionary from SolarEvent.to_dict():
                - timestamp: Event start time
                - end_timestamp: Event end time
                - duration_minutes: Event duration
                - avg_supply_return_delta: Average supply-return delta
                - avg_outdoor_temp, avg_indoor_temp
                - avg_cloud_cover, avg_sun_elevation, avg_wind_speed
                - implied_solar_coefficient_ml2: Back-calculated coefficient
                - observations_count, peak_sun_elevation

        Returns:
            True if write succeeded, False otherwise
        """
        if not self._should_write() or not event_data:
            return False

        try:
            # Parse timestamp
            timestamp = event_data.get('timestamp')
            if isinstance(timestamp, str):
                if timestamp.endswith('Z'):
                    timestamp = timestamp.replace('Z', '+00:00')
                timestamp = datetime.fromisoformat(timestamp)

            point = Point("solar_event_ml2") \
                .tag("house_id", self.house_id) \
                .field("duration_minutes", round(float(event_data.get('duration_minutes', 0)), 1)) \
                .field("avg_supply_return_delta", round(float(event_data.get('avg_supply_return_delta', 0)), 2)) \
                .field("avg_outdoor_temp", round(float(event_data.get('avg_outdoor_temp', 0)), 1)) \
                .field("avg_indoor_temp", round(float(event_data.get('avg_indoor_temp', 0)), 1)) \
                .field("avg_cloud_cover", round(float(event_data.get('avg_cloud_cover', 0)), 1)) \
                .field("avg_sun_elevation", round(float(event_data.get('avg_sun_elevation', 0)), 1)) \
                .field("avg_wind_speed", round(float(event_data.get('avg_wind_speed', 0)), 1)) \
                .field("implied_solar_coefficient_ml2", round(float(event_data.get('implied_solar_coefficient_ml2', 0)), 1)) \
                .field("observations_count", int(event_data.get('observations_count', 0))) \
                .field("peak_sun_elevation", round(float(event_data.get('peak_sun_elevation', 0)), 1)) \
                .time(timestamp, WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.info(
                f"Wrote solar event: {event_data.get('duration_minutes', 0):.0f}min, "
                f"coeff={event_data.get('implied_solar_coefficient_ml2', 0):.1f}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to write solar event: {str(e)}")
            self._log_influx_error("Solar event write failed", e, "write_solar_event")
            return False

    def write_weather_coefficients_ml2(self, coefficients: dict) -> bool:
        """
        Write learned weather coefficients to InfluxDB for tracking over time.

        Args:
            coefficients: Dictionary with ML2 coefficients:
                - solar_coefficient_ml2: Learned solar coefficient
                - wind_coefficient_ml2: Wind coefficient (fixed)
                - solar_confidence_ml2: Learning confidence
                - total_solar_events: Total events detected

        Returns:
            True if write succeeded, False otherwise
        """
        if not self._should_write() or not coefficients:
            return False

        from write_throttle import WriteThrottle
        if not WriteThrottle.get().allow("weather_coefficients_ml2", self.house_id, 3600):
            return True

        try:
            point = Point("weather_coefficients_ml2") \
                .tag("house_id", self.house_id) \
                .field("solar_coefficient_ml2", round(float(coefficients.get('solar_coefficient_ml2', 6.0)), 1)) \
                .field("wind_coefficient_ml2", round(float(coefficients.get('wind_coefficient_ml2', 0.15)), 2)) \
                .field("solar_confidence_ml2", round(float(coefficients.get('solar_confidence_ml2', 0)), 2)) \
                .field("total_solar_events", int(coefficients.get('total_solar_events', 0))) \
                .time(datetime.now(timezone.utc), WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.info(
                f"Wrote ML2 coefficients: solar={coefficients.get('solar_coefficient_ml2', 0):.1f}, "
                f"confidence={coefficients.get('solar_confidence_ml2', 0):.0%}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to write weather coefficients: {str(e)}")
            self._log_influx_error("Weather coefficients write failed", e, "write_weather_coefficients_ml2")
            return False

    def write_thermal_timing_ml2(self, timing: dict) -> bool:
        """
        Write learned thermal response timing to InfluxDB.

        Args:
            timing: Dictionary with timing data:
                - heat_up_lag_minutes_ml2: Lag for rising effective temp
                - cool_down_lag_minutes_ml2: Lag for falling effective temp
                - confidence_ml2: Learning confidence
                - total_transitions: Total transitions measured

        Returns:
            True if write succeeded, False otherwise
        """
        if not self._should_write() or not timing:
            return False

        from write_throttle import WriteThrottle
        if not WriteThrottle.get().allow("thermal_timing_ml2", self.house_id, 3600):
            return True

        try:
            point = Point("thermal_timing_ml2") \
                .tag("house_id", self.house_id) \
                .field("heat_up_lag_minutes", round(float(timing.get('heat_up_lag_minutes_ml2', 60)), 1)) \
                .field("cool_down_lag_minutes", round(float(timing.get('cool_down_lag_minutes_ml2', 90)), 1)) \
                .field("confidence", round(float(timing.get('confidence_ml2', 0)), 2)) \
                .field("total_transitions", int(timing.get('total_transitions', 0))) \
                .time(datetime.now(timezone.utc), WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.info(
                f"Wrote ML2 thermal timing: heat_up={timing.get('heat_up_lag_minutes_ml2', 60):.0f}min, "
                f"cool_down={timing.get('cool_down_lag_minutes_ml2', 90):.0f}min"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to write thermal timing: {str(e)}")
            self._log_influx_error("Thermal timing write failed", e, "write_thermal_timing_ml2")
            return False

    def write_solar_early_warning(self, warning: dict) -> bool:
        """
        Write solar early warning event to InfluxDB.

        Used to track when predictive solar detection triggered.

        Args:
            warning: Dictionary with warning data:
                - start_time: When warning was triggered
                - outdoor_rise: Temperature rise from baseline
                - estimated_lead_time_minutes: Expected lead time
                - confidence: Detection confidence

        Returns:
            True if write succeeded, False otherwise
        """
        if not self._should_write() or not warning:
            return False

        try:
            timestamp = warning.get('start_time', datetime.now(timezone.utc))
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

            point = Point("solar_early_warning_ml2") \
                .tag("house_id", self.house_id) \
                .field("outdoor_rise", round(float(warning.get('outdoor_rise', 0)), 1)) \
                .field("lead_time_minutes", round(float(warning.get('estimated_lead_time_minutes', 60)), 1)) \
                .field("confidence", round(float(warning.get('confidence', 0)), 2)) \
                .time(timestamp, WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.info(
                f"Wrote solar early warning: +{warning.get('outdoor_rise', 0):.1f}°C rise, "
                f"lead_time={warning.get('estimated_lead_time_minutes', 60):.0f}min"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to write solar early warning: {str(e)}")
            self._log_influx_error("Solar early warning write failed", e, "write_solar_early_warning")
            return False

    def write_thermal_lag_measurement(self, lag: dict) -> bool:
        """
        Write a thermal lag measurement to InfluxDB.

        Args:
            lag: Dictionary with lag measurement:
                - type: 'rising' or 'falling'
                - lag_minutes: Measured lag time
                - effective_temp_change: Effective temp change that triggered
                - indoor_temp_change: Indoor temp response
                - confidence: Measurement confidence

        Returns:
            True if write succeeded, False otherwise
        """
        if not self._should_write() or not lag:
            return False

        try:
            point = Point("thermal_lag_ml2") \
                .tag("house_id", self.house_id) \
                .tag("transition_type", lag.get('type', 'unknown')) \
                .field("lag_minutes", round(float(lag.get('lag_minutes', 0)), 1)) \
                .field("effective_temp_change", round(float(lag.get('effective_temp_change', 0)), 1)) \
                .field("indoor_temp_change", round(float(lag.get('indoor_temp_change', 0)), 1)) \
                .field("confidence", round(float(lag.get('confidence', 0)), 2)) \
                .time(datetime.now(timezone.utc), WritePrecision.S)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            self.logger.debug(
                f"Wrote thermal lag: {lag.get('type', 'unknown')} {lag.get('lag_minutes', 0):.0f}min"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to write thermal lag: {str(e)}")
            self._log_influx_error("Thermal lag write failed", e, "write_thermal_lag_measurement")
            return False

    def read_solar_events(self, days: int = 30) -> List[dict]:
        """
        Read historical solar events from InfluxDB.

        Used for visualization and coefficient recalculation.

        Args:
            days: Number of days of history to read

        Returns:
            List of solar event dictionaries
        """
        if not self.enabled:
            return []

        try:
            query_api = self.client.query_api()

            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -{days}d)
                |> filter(fn: (r) => r["_measurement"] == "solar_event_ml2")
                |> filter(fn: (r) => r["house_id"] == "{self.house_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
            '''

            tables = query_api.query(query, org=self.org)

            events = []
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    if timestamp:
                        events.append({
                            'timestamp': timestamp.isoformat(),
                            'duration_minutes': record.values.get('duration_minutes'),
                            'avg_supply_return_delta': record.values.get('avg_supply_return_delta'),
                            'avg_outdoor_temp': record.values.get('avg_outdoor_temp'),
                            'avg_indoor_temp': record.values.get('avg_indoor_temp'),
                            'avg_cloud_cover': record.values.get('avg_cloud_cover'),
                            'avg_sun_elevation': record.values.get('avg_sun_elevation'),
                            'avg_wind_speed': record.values.get('avg_wind_speed'),
                            'implied_solar_coefficient_ml2': record.values.get('implied_solar_coefficient_ml2'),
                            'observations_count': record.values.get('observations_count'),
                            'peak_sun_elevation': record.values.get('peak_sun_elevation'),
                        })

            self.logger.info(f"Read {len(events)} solar events from InfluxDB")
            return events

        except Exception as e:
            self.logger.error(f"Failed to read solar events: {str(e)}")
            return []

    def close(self):
        """Close InfluxDB client connection"""
        if self.client:
            self.client.close()


class EnergyWriter:
    """
    Centralized energy data writer with value-aware deduplication.

    Entity-agnostic: works with any entity type (house, building) and
    any measurement (energy_meter, energy_consumption).
    """

    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=5_000)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def get_existing_data(self, tag_filters: Dict[str, str], measurement: str,
                          start: str, stop: str) -> Dict[datetime, Dict[str, float]]:
        """
        Query existing data for dedup.

        Returns dict mapping timestamp -> {field: numeric_value} for the given
        measurement and tag filters.
        """
        filter_lines = [f'r._measurement == "{measurement}"']
        for tag, value in tag_filters.items():
            filter_lines.append(f'r["{tag}"] == "{value}"')

        query = f'''
from(bucket: "{self.bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => {" and ".join(filter_lines)})
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        existing = {}
        try:
            query_api = self.client.query_api()
            for table in query_api.query(query, org=self.org):
                for record in table.records:
                    ts = record.get_time()
                    fields = {
                        k: v for k, v in record.values.items()
                        if isinstance(v, (int, float)) and not isinstance(v, bool)
                    }
                    existing[ts] = fields
        except Exception:
            pass  # Fail gracefully — write everything if dedup query fails

        return existing

    @staticmethod
    def records_match(new_record: Dict, existing: Dict[str, float],
                      tolerance: float = 0.001) -> bool:
        """Check if all numeric fields in new_record match existing values."""
        for field, new_val in new_record.items():
            if field == 'timestamp':
                continue
            if not isinstance(new_val, (int, float)) or isinstance(new_val, bool):
                continue
            old_val = existing.get(field)
            if old_val is None:
                return False
            if abs(float(new_val) - float(old_val)) > tolerance:
                return False
        return True

    def write_energy_records(
        self,
        records: List[Dict],
        entity_id: str,
        tag_name: str,
        measurement: str = "energy_meter",
        extra_tags: Optional[Dict[str, str]] = None
    ) -> Tuple[int, int, int]:
        """
        Write energy records with value-aware deduplication.

        Returns:
            Tuple of (new_count, skipped_count, updated_count)
        """
        if not records:
            return 0, 0, 0

        timestamps = [r['timestamp'] for r in records]
        min_time = min(timestamps)
        max_time = max(timestamps)
        start = (min_time - timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
        stop = (max_time + timedelta(hours=1)).isoformat().replace('+00:00', 'Z')

        tag_filters = {tag_name: entity_id}
        if extra_tags:
            tag_filters.update(extra_tags)

        existing = self.get_existing_data(tag_filters, measurement, start, stop)

        to_write = []
        skipped = 0
        updated = 0
        for record in records:
            ts = record['timestamp']
            if ts in existing:
                if self.records_match(record, existing[ts]):
                    skipped += 1
                else:
                    to_write.append(record)
                    updated += 1
            else:
                to_write.append(record)

        if not to_write:
            return 0, skipped, 0

        points = []
        for record in to_write:
            point = Point(measurement) \
                .tag(tag_name, entity_id) \
                .time(record['timestamp'], WritePrecision.S)
            if extra_tags:
                for k, v in extra_tags.items():
                    point = point.tag(k, v)
            for field, value in record.items():
                if field == 'timestamp':
                    continue
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    point = point.field(field, value)
            points.append(point)

        batch_size = 1000
        for i in range(0, len(points), batch_size):
            self.write_api.write(bucket=self.bucket, org=self.org, record=points[i:i + batch_size])

        return len(to_write) - updated, skipped, updated

    def close(self):
        """Close InfluxDB client connection."""
        if self.client:
            self.client.close()

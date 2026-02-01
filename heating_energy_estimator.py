#!/usr/bin/env python3
"""
Heating Energy Estimator

Estimates heating energy based on the temperature difference between
indoor temperature and effective outdoor temperature.

The core formula:
    heating_power = k × (T_indoor - T_effective_outdoor)

Where k is the building's heat loss coefficient (kW/°C).

Usage:
    python heating_energy_estimator.py                     # Analyze last 7 days
    python heating_energy_estimator.py --days 30           # Analyze last 30 days
    python heating_energy_estimator.py --write             # Write results to InfluxDB
    python heating_energy_estimator.py --k 0.15            # Override heat loss coefficient

Environment:
    INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Add parent dir for energy_models
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from energy_models import get_weather_model
from energy_models.weather_energy_model import WeatherConditions

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


@dataclass
class HeatingEstimate:
    """Single heating energy estimate."""
    timestamp: datetime
    indoor_temp: float
    outdoor_temp: float
    effective_outdoor_temp: float
    temp_difference: float  # indoor - effective_outdoor
    estimated_power_kw: float
    estimated_energy_kwh: float  # For the measurement interval
    hot_water_temp: Optional[float] = None
    hot_water_active: bool = False  # True if hot water temp indicates DHW usage
    wind_effect: float = 0.0
    solar_effect: float = 0.0

    @property
    def timestamp_swedish(self) -> str:
        if self.timestamp.tzinfo is None:
            ts = self.timestamp.replace(tzinfo=timezone.utc)
        else:
            ts = self.timestamp
        return ts.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d %H:%M')


class HeatingEnergyEstimator:
    """Estimates heating energy from temperature data."""

    # Default building heat loss coefficient (kW per °C difference)
    # Typical values: 0.1-0.3 for well-insulated homes
    DEFAULT_K = 0.15  # kW/°C

    # Hot water temp threshold to detect DHW usage
    DHW_TEMP_THRESHOLD = 40.0  # °C
    DHW_TEMP_RISE = 5.0  # °C above baseline

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        heat_loss_k: float = None,
        latitude: float = 58.41,
        longitude: float = 15.62
    ):
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.heat_loss_k = heat_loss_k or self.DEFAULT_K
        self.latitude = latitude
        self.longitude = longitude

        self.client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org
        )
        self.query_api = self.client.query_api()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

        # Weather model for effective temperature
        self.weather_model = get_weather_model('simple')

    def fetch_heating_data(self, house_id: str, days: int = 7) -> List[Dict]:
        """Fetch heating system data from InfluxDB."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{days}d)
            |> filter(fn: (r) => r["_measurement"] == "heating_system")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> filter(fn: (r) =>
                r["_field"] == "room_temperature" or
                r["_field"] == "outdoor_temperature" or
                r["_field"] == "hot_water_temp" or
                r["_field"] == "supply_temp" or
                r["_field"] == "return_temp"
            )
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query, org=self.influx_org)

        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    'timestamp': record.get_time(),
                    'room_temperature': record.values.get('room_temperature'),
                    'outdoor_temperature': record.values.get('outdoor_temperature'),
                    'hot_water_temp': record.values.get('hot_water_temp'),
                    'supply_temp': record.values.get('supply_temp'),
                    'return_temp': record.values.get('return_temp'),
                })

        print(f"Fetched {len(results)} heating data points")
        return results

    def fetch_weather_data(self, house_id: str, days: int = 7) -> Dict[str, Dict]:
        """Fetch weather observation data (wind, humidity) from InfluxDB."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{days}d)
            |> filter(fn: (r) => r["_measurement"] == "weather_observation")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query, org=self.influx_org)

        # Index by rounded timestamp for joining
        weather_by_time = {}
        for table in tables:
            for record in table.records:
                ts = record.get_time()
                # Round to nearest 15 minutes for joining
                rounded = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)
                weather_by_time[rounded.isoformat()] = {
                    'wind_speed': record.values.get('wind_speed', 3.0),
                    'humidity': record.values.get('humidity', 60.0),
                }

        print(f"Fetched {len(weather_by_time)} weather observations")
        return weather_by_time

    def fetch_cloud_cover(self, house_id: str, days: int = 7) -> Dict[str, float]:
        """Fetch cloud cover data from weather_forecast."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{days}d)
            |> filter(fn: (r) => r["_measurement"] == "weather_forecast")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> filter(fn: (r) => r["_field"] == "avg_cloud_cover")
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query, org=self.influx_org)

        cloud_by_time = {}
        for table in tables:
            for record in table.records:
                ts = record.get_time()
                # Use this cloud cover for the next few hours
                for h in range(3):
                    future = ts + timedelta(hours=h)
                    rounded = future.replace(minute=0, second=0, microsecond=0)
                    cloud_by_time[rounded.isoformat()] = record.get_value() or 4.0

        return cloud_by_time

    def calculate_effective_temp(
        self,
        timestamp: datetime,
        outdoor_temp: float,
        wind_speed: float = 3.0,
        humidity: float = 60.0,
        cloud_cover: float = 4.0
    ) -> Tuple[float, float, float]:
        """
        Calculate effective outdoor temperature.

        Returns: (effective_temp, wind_effect, solar_effect)
        """
        conditions = WeatherConditions(
            timestamp=timestamp,
            temperature=outdoor_temp,
            wind_speed=wind_speed,
            humidity=humidity,
            cloud_cover=cloud_cover,
            latitude=self.latitude,
            longitude=self.longitude
        )

        result = self.weather_model.effective_temperature(conditions)
        return result.effective_temp, result.wind_effect, result.solar_effect

    def estimate_heating(
        self,
        house_id: str,
        days: int = 7
    ) -> List[HeatingEstimate]:
        """
        Estimate heating energy for a house.

        Args:
            house_id: House identifier
            days: Days of history to analyze

        Returns:
            List of heating estimates
        """
        # Fetch all data
        heating_data = self.fetch_heating_data(house_id, days)
        weather_data = self.fetch_weather_data(house_id, days)
        cloud_data = self.fetch_cloud_cover(house_id, days)

        if not heating_data:
            print("No heating data available")
            return []

        # Calculate hot water baseline (lower quartile when not in use)
        hw_temps = [d['hot_water_temp'] for d in heating_data if d.get('hot_water_temp')]
        if hw_temps:
            sorted_hw = sorted(hw_temps)
            hw_baseline = sorted_hw[len(sorted_hw) // 4] if len(sorted_hw) > 4 else sorted_hw[0]
        else:
            hw_baseline = 25.0

        print(f"Hot water baseline temperature: {hw_baseline:.1f}°C")

        estimates = []
        measurement_interval_hours = 0.25  # 15 minutes

        for data in heating_data:
            ts = data['timestamp']
            indoor = data.get('room_temperature')
            outdoor = data.get('outdoor_temperature')
            hw_temp = data.get('hot_water_temp')

            if indoor is None or outdoor is None:
                continue

            # Get weather conditions for this timestamp
            ts_key = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0).isoformat()
            ts_hour_key = ts.replace(minute=0, second=0, microsecond=0).isoformat()

            weather = weather_data.get(ts_key, {'wind_speed': 3.0, 'humidity': 60.0})
            cloud = cloud_data.get(ts_hour_key, 4.0)

            # Calculate effective outdoor temperature
            eff_temp, wind_effect, solar_effect = self.calculate_effective_temp(
                timestamp=ts,
                outdoor_temp=outdoor,
                wind_speed=weather['wind_speed'],
                humidity=weather['humidity'],
                cloud_cover=cloud
            )

            # Temperature difference drives heating need
            temp_diff = indoor - eff_temp

            # Estimate heating power: P = k × ΔT
            # Only when outdoor is colder than indoor
            if temp_diff > 0:
                estimated_power = self.heat_loss_k * temp_diff
            else:
                estimated_power = 0.0

            # Energy for this interval
            estimated_energy = estimated_power * measurement_interval_hours

            # Detect hot water usage
            hw_active = False
            if hw_temp is not None:
                hw_active = hw_temp > (hw_baseline + self.DHW_TEMP_RISE)

            estimates.append(HeatingEstimate(
                timestamp=ts,
                indoor_temp=indoor,
                outdoor_temp=outdoor,
                effective_outdoor_temp=round(eff_temp, 2),
                temp_difference=round(temp_diff, 2),
                estimated_power_kw=round(estimated_power, 3),
                estimated_energy_kwh=round(estimated_energy, 4),
                hot_water_temp=hw_temp,
                hot_water_active=hw_active,
                wind_effect=round(wind_effect, 2),
                solar_effect=round(solar_effect, 2),
            ))

        return estimates

    def print_summary(self, estimates: List[HeatingEstimate]):
        """Print summary statistics."""
        if not estimates:
            print("No estimates to summarize")
            return

        total_energy = sum(e.estimated_energy_kwh for e in estimates)
        avg_power = sum(e.estimated_power_kw for e in estimates) / len(estimates)
        max_power = max(e.estimated_power_kw for e in estimates)

        dhw_events = sum(1 for e in estimates if e.hot_water_active)

        # Daily breakdown
        daily = {}
        for e in estimates:
            day = e.timestamp.strftime('%Y-%m-%d')
            if day not in daily:
                daily[day] = {'energy': 0, 'dhw_events': 0, 'count': 0}
            daily[day]['energy'] += e.estimated_energy_kwh
            daily[day]['dhw_events'] += 1 if e.hot_water_active else 0
            daily[day]['count'] += 1

        print("\n" + "="*70)
        print("HEATING ENERGY ESTIMATION SUMMARY")
        print("="*70)
        print(f"Heat loss coefficient (k): {self.heat_loss_k} kW/°C")
        print(f"Period: {estimates[0].timestamp_swedish} to {estimates[-1].timestamp_swedish}")
        print(f"Data points: {len(estimates)}")
        print(f"\nTotal estimated heating energy: {total_energy:.2f} kWh")
        print(f"Average heating power: {avg_power:.2f} kW")
        print(f"Peak heating power: {max_power:.2f} kW")
        print(f"Hot water events detected: {dhw_events}")

        print("\n" + "-"*70)
        print("DAILY BREAKDOWN")
        print("-"*70)
        print(f"{'Date':<12} {'Energy (kWh)':<15} {'DHW Events':<12} {'Samples'}")
        print("-"*70)
        for day in sorted(daily.keys()):
            d = daily[day]
            print(f"{day:<12} {d['energy']:>10.2f}     {d['dhw_events']:>6}       {d['count']}")

        # Show some sample data points
        print("\n" + "-"*70)
        print("SAMPLE DATA POINTS (last 10)")
        print("-"*70)
        print(f"{'Time':<18} {'In°C':<6} {'Out°C':<7} {'Eff°C':<7} {'ΔT':<6} {'Power':<7} {'HW°C':<6} {'DHW'}")
        print("-"*70)
        for e in estimates[-10:]:
            dhw_flag = "YES" if e.hot_water_active else ""
            hw = f"{e.hot_water_temp:.1f}" if e.hot_water_temp else "-"
            print(f"{e.timestamp_swedish:<18} {e.indoor_temp:<6.1f} {e.outdoor_temp:<7.1f} "
                  f"{e.effective_outdoor_temp:<7.1f} {e.temp_difference:<6.1f} "
                  f"{e.estimated_power_kw:<7.2f} {hw:<6} {dhw_flag}")

    def write_to_influx(self, house_id: str, estimates: List[HeatingEstimate]) -> int:
        """Write estimates to InfluxDB."""
        points = []

        for e in estimates:
            point = Point("heating_energy_estimate") \
                .tag("house_id", house_id) \
                .tag("method", "temp_difference") \
                .time(e.timestamp, WritePrecision.S) \
                .field("indoor_temp", e.indoor_temp) \
                .field("outdoor_temp", e.outdoor_temp) \
                .field("effective_outdoor_temp", e.effective_outdoor_temp) \
                .field("temp_difference", e.temp_difference) \
                .field("estimated_power_kw", e.estimated_power_kw) \
                .field("estimated_energy_kwh", e.estimated_energy_kwh) \
                .field("wind_effect", e.wind_effect) \
                .field("solar_effect", e.solar_effect) \
                .field("hot_water_active", 1 if e.hot_water_active else 0)

            if e.hot_water_temp is not None:
                point = point.field("hot_water_temp", e.hot_water_temp)

            points.append(point)

        if points:
            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            print(f"\nWrote {len(points)} estimates to InfluxDB (measurement: heating_energy_estimate)")

        return len(points)

    def close(self):
        if self.client:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(description='Estimate heating energy from temperature data')
    parser.add_argument('--days', type=int, default=7, help='Days of history to analyze')
    parser.add_argument('--house', type=str, default='HEM_FJV_Villa_149', help='House ID')
    parser.add_argument('--k', type=float, help='Heat loss coefficient (kW/°C), default 0.15')
    parser.add_argument('--lat', type=float, default=58.41, help='Latitude for solar calculations')
    parser.add_argument('--lon', type=float, default=15.62, help='Longitude for solar calculations')
    parser.add_argument('--write', action='store_true', help='Write results to InfluxDB')
    args = parser.parse_args()

    # Get configuration
    influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.getenv('INFLUXDB_BUCKET', 'heating')

    if not influx_token:
        print("ERROR: INFLUXDB_TOKEN not set")
        sys.exit(1)

    estimator = HeatingEnergyEstimator(
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        heat_loss_k=args.k,
        latitude=args.lat,
        longitude=args.lon
    )

    try:
        print(f"Analyzing heating for {args.house} (last {args.days} days)")
        print(f"Heat loss coefficient: {estimator.heat_loss_k} kW/°C")
        print()

        estimates = estimator.estimate_heating(args.house, args.days)

        if estimates:
            estimator.print_summary(estimates)

            if args.write:
                estimator.write_to_influx(args.house, estimates)
            else:
                print("\n(Use --write to save results to InfluxDB)")

    finally:
        estimator.close()


if __name__ == '__main__':
    main()

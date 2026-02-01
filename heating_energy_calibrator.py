#!/usr/bin/env python3
"""
Heating Energy Calibrator

Calibrates the heat loss coefficient (k) using real energy consumption data
and correlates with temperature differences to separate heating from DHW.

The core relationship:
    heating_power = k × (T_indoor - T_effective_outdoor)

When actual_energy > estimated_heating_energy:
    → The excess is likely DHW (domestic hot water)
    → Confirmed by hot_water_temp peaks

Usage:
    python heating_energy_calibrator.py                      # Analyze available data
    python heating_energy_calibrator.py --calibrate          # Calculate optimal k
    python heating_energy_calibrator.py --write              # Write separated energy to InfluxDB
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from zoneinfo import ZoneInfo
import statistics

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from energy_models import get_weather_model
from energy_models.weather_energy_model import WeatherConditions

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


@dataclass
class DailyEnergyAnalysis:
    """Analysis for a single day."""
    date: str
    actual_energy_kwh: float
    avg_indoor_temp: float
    avg_outdoor_temp: float
    avg_effective_outdoor_temp: float
    avg_temp_difference: float
    degree_hours: float  # ΔT × hours with data
    estimated_heating_kwh: float  # Using calibrated k
    excess_energy_kwh: float  # actual - estimated (likely DHW)
    dhw_events: int  # Hot water temp peaks
    data_points: int
    k_implied: float  # k that would explain this day's consumption


class HeatingEnergyCalibrator:
    """Calibrates heating model using real energy data."""

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        latitude: float = 58.41,
        longitude: float = 15.62
    ):
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.latitude = latitude
        self.longitude = longitude

        self.client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org
        )
        self.query_api = self.client.query_api()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

        self.weather_model = get_weather_model('simple')

    def fetch_daily_energy(self, house_id: str, start_date: str = "2025-12-01") -> Dict[str, float]:
        """Fetch daily energy consumption from energy_consumption measurement."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_date})
            |> filter(fn: (r) => r["_measurement"] == "energy_consumption")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: 1d, fn: sum, createEmpty: false)
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query, org=self.influx_org)

        daily_energy = {}
        for table in tables:
            for record in table.records:
                date = record.get_time().strftime('%Y-%m-%d')
                daily_energy[date] = record.get_value()

        print(f"Fetched {len(daily_energy)} days of energy data")
        return daily_energy

    def fetch_heating_data(self, house_id: str, start_date: str = "2025-12-01") -> List[Dict]:
        """Fetch heating system data."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_date})
            |> filter(fn: (r) => r["_measurement"] == "heating_system")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> filter(fn: (r) =>
                r["_field"] == "room_temperature" or
                r["_field"] == "outdoor_temperature" or
                r["_field"] == "hot_water_temp"
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
                })

        print(f"Fetched {len(results)} heating data points")
        return results

    def fetch_weather_data(self, house_id: str, start_date: str = "2025-12-01") -> Dict[str, Dict]:
        """Fetch weather observation data."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_date})
            |> filter(fn: (r) => r["_measurement"] == "weather_observation")
            |> filter(fn: (r) => r["house_id"] =~ /{house_id}/)
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query, org=self.influx_org)

        weather_by_time = {}
        for table in tables:
            for record in table.records:
                ts = record.get_time()
                rounded = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)
                weather_by_time[rounded.isoformat()] = {
                    'wind_speed': record.values.get('wind_speed') or 3.0,
                    'humidity': record.values.get('humidity') or 60.0,
                }

        return weather_by_time

    def calculate_effective_temp(
        self,
        timestamp: datetime,
        outdoor_temp: float,
        wind_speed: float = 3.0,
        humidity: float = 60.0,
        cloud_cover: float = 4.0
    ) -> float:
        """Calculate effective outdoor temperature."""
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
        return result.effective_temp

    def analyze(
        self,
        house_id: str,
        start_date: str = "2025-12-01",
        calibrated_k: float = None
    ) -> Tuple[List[DailyEnergyAnalysis], float]:
        """
        Analyze energy consumption and correlate with temperatures.

        Returns:
            Tuple of (daily_analyses, calibrated_k)
        """
        # Fetch all data
        daily_energy = self.fetch_daily_energy(house_id, start_date)
        heating_data = self.fetch_heating_data(house_id, start_date)
        weather_data = self.fetch_weather_data(house_id, start_date)

        if not daily_energy:
            print("No energy data available")
            return [], 0.0

        if not heating_data:
            print("No heating data available")
            return [], 0.0

        # Calculate hot water baseline
        hw_temps = [d['hot_water_temp'] for d in heating_data if d.get('hot_water_temp')]
        if hw_temps:
            sorted_hw = sorted(hw_temps)
            hw_baseline = sorted_hw[len(sorted_hw) // 4]
        else:
            hw_baseline = 25.0

        print(f"Hot water baseline: {hw_baseline:.1f}°C")

        # Group heating data by day
        daily_heating = {}
        for d in heating_data:
            date = d['timestamp'].strftime('%Y-%m-%d')
            if date not in daily_heating:
                daily_heating[date] = []
            daily_heating[date].append(d)

        # Calculate daily analyses
        analyses = []
        k_values = []

        for date in sorted(daily_energy.keys()):
            if date not in daily_heating:
                continue

            day_data = daily_heating[date]
            actual_energy = daily_energy[date]

            # Calculate temperature metrics
            indoor_temps = []
            outdoor_temps = []
            effective_temps = []
            dhw_events = 0

            for d in day_data:
                indoor = d.get('room_temperature')
                outdoor = d.get('outdoor_temperature')
                hw_temp = d.get('hot_water_temp')

                if indoor is None or outdoor is None:
                    continue

                indoor_temps.append(indoor)
                outdoor_temps.append(outdoor)

                # Get weather for effective temp
                ts = d['timestamp']
                ts_key = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0).isoformat()
                weather = weather_data.get(ts_key, {'wind_speed': 3.0, 'humidity': 60.0})

                eff_temp = self.calculate_effective_temp(
                    timestamp=ts,
                    outdoor_temp=outdoor,
                    wind_speed=weather['wind_speed'],
                    humidity=weather['humidity']
                )
                effective_temps.append(eff_temp)

                # Count DHW events
                if hw_temp and hw_temp > (hw_baseline + 5.0):
                    dhw_events += 1

            if not indoor_temps:
                continue

            avg_indoor = statistics.mean(indoor_temps)
            avg_outdoor = statistics.mean(outdoor_temps)
            avg_effective = statistics.mean(effective_temps)
            avg_diff = avg_indoor - avg_effective

            # Degree-hours (ΔT × hours of data)
            hours_of_data = len(day_data) * 0.25  # 15-min intervals
            degree_hours = avg_diff * hours_of_data

            # Calculate implied k for this day
            # k = Energy / degree_hours
            if degree_hours > 0:
                k_implied = actual_energy / degree_hours
                k_values.append(k_implied)
            else:
                k_implied = 0

            analyses.append(DailyEnergyAnalysis(
                date=date,
                actual_energy_kwh=actual_energy,
                avg_indoor_temp=round(avg_indoor, 1),
                avg_outdoor_temp=round(avg_outdoor, 1),
                avg_effective_outdoor_temp=round(avg_effective, 1),
                avg_temp_difference=round(avg_diff, 1),
                degree_hours=round(degree_hours, 1),
                estimated_heating_kwh=0,  # Will be filled after k calibration
                excess_energy_kwh=0,
                dhw_events=dhw_events,
                data_points=len(day_data),
                k_implied=round(k_implied, 4)
            ))

        # Calibrate k using median of implied values (robust to outliers)
        if k_values:
            if calibrated_k is None:
                calibrated_k = statistics.median(k_values)

            # Update estimates with calibrated k
            for a in analyses:
                a.estimated_heating_kwh = round(calibrated_k * a.degree_hours, 1)
                a.excess_energy_kwh = round(a.actual_energy_kwh - a.estimated_heating_kwh, 1)

        return analyses, calibrated_k

    def print_results(self, analyses: List[DailyEnergyAnalysis], calibrated_k: float):
        """Print analysis results."""
        if not analyses:
            print("No analysis results")
            return

        print("\n" + "=" * 100)
        print("HEATING ENERGY CALIBRATION RESULTS")
        print("=" * 100)
        print(f"Calibrated heat loss coefficient (k): {calibrated_k:.4f} kW/°C")
        print(f"Analysis period: {analyses[0].date} to {analyses[-1].date}")
        print(f"Days analyzed: {len(analyses)}")

        total_actual = sum(a.actual_energy_kwh for a in analyses)
        total_estimated = sum(a.estimated_heating_kwh for a in analyses)
        total_excess = sum(a.excess_energy_kwh for a in analyses)

        print(f"\nTotals:")
        print(f"  Actual consumption:    {total_actual:>8.1f} kWh")
        print(f"  Estimated heating:     {total_estimated:>8.1f} kWh")
        print(f"  Excess (likely DHW):   {total_excess:>8.1f} kWh ({100*total_excess/total_actual:.1f}%)")

        print("\n" + "-" * 100)
        print(f"{'Date':<12} {'Actual':>8} {'Est.Heat':>9} {'Excess':>8} {'ΔT':>6} {'Deg-Hr':>7} {'k_day':>7} {'DHW':>5} {'Indoor':>7} {'OutEff':>7}")
        print("-" * 100)

        for a in analyses:
            excess_pct = f"{a.excess_energy_kwh:+.1f}"
            print(f"{a.date:<12} {a.actual_energy_kwh:>8.1f} {a.estimated_heating_kwh:>9.1f} {excess_pct:>8} "
                  f"{a.avg_temp_difference:>6.1f} {a.degree_hours:>7.1f} {a.k_implied:>7.4f} "
                  f"{a.dhw_events:>5} {a.avg_indoor_temp:>7.1f} {a.avg_effective_outdoor_temp:>7.1f}")

        # Summary statistics
        k_values = [a.k_implied for a in analyses if a.k_implied > 0]
        if k_values:
            print("\n" + "-" * 100)
            print("K-VALUE STATISTICS (per day):")
            print(f"  Median:  {statistics.median(k_values):.4f} kW/°C")
            print(f"  Mean:    {statistics.mean(k_values):.4f} kW/°C")
            print(f"  Min:     {min(k_values):.4f} kW/°C")
            print(f"  Max:     {max(k_values):.4f} kW/°C")
            if len(k_values) > 1:
                print(f"  StdDev:  {statistics.stdev(k_values):.4f} kW/°C")

        # Interpretation
        print("\n" + "=" * 100)
        print("INTERPRETATION:")
        print("-" * 100)
        print(f"• The calibrated k = {calibrated_k:.4f} kW/°C means:")
        print(f"  - For every 1°C difference (indoor - effective outdoor), the house needs {calibrated_k:.2f} kW")
        print(f"  - At ΔT = 25°C (typical winter), heating power ≈ {calibrated_k * 25:.1f} kW")
        print(f"\n• Days with high excess energy likely have more hot water usage")
        print(f"• Days with negative excess: k might be too high, or heating was reduced")

    def write_to_influx(self, house_id: str, analyses: List[DailyEnergyAnalysis], k: float) -> int:
        """Write separated energy values to InfluxDB."""
        points = []

        for a in analyses:
            # Parse date to timestamp (midnight)
            ts = datetime.strptime(a.date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

            point = Point("energy_separated") \
                .tag("house_id", house_id) \
                .tag("method", "k_calibration") \
                .time(ts, WritePrecision.S) \
                .field("actual_energy_kwh", a.actual_energy_kwh) \
                .field("heating_energy_kwh", a.estimated_heating_kwh) \
                .field("dhw_energy_kwh", max(0, a.excess_energy_kwh)) \
                .field("excess_energy_kwh", a.excess_energy_kwh) \
                .field("avg_temp_difference", a.avg_temp_difference) \
                .field("degree_hours", a.degree_hours) \
                .field("k_value", k) \
                .field("k_implied", a.k_implied) \
                .field("dhw_events", a.dhw_events) \
                .field("avg_indoor_temp", a.avg_indoor_temp) \
                .field("avg_effective_outdoor_temp", a.avg_effective_outdoor_temp)

            points.append(point)

        if points:
            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            print(f"\nWrote {len(points)} daily records to InfluxDB (measurement: energy_separated)")

        return len(points)

    def close(self):
        if self.client:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(description='Calibrate heating energy model with real data')
    parser.add_argument('--house', type=str, default='HEM_FJV_Villa_149', help='House ID')
    parser.add_argument('--start', type=str, default='2025-12-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--k', type=float, help='Override k value (otherwise calibrated from data)')
    parser.add_argument('--lat', type=float, default=58.41, help='Latitude')
    parser.add_argument('--lon', type=float, default=15.62, help='Longitude')
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

    calibrator = HeatingEnergyCalibrator(
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        latitude=args.lat,
        longitude=args.lon
    )

    try:
        analyses, calibrated_k = calibrator.analyze(
            house_id=args.house,
            start_date=args.start,
            calibrated_k=args.k
        )

        if analyses:
            calibrator.print_results(analyses, calibrated_k)

            if args.write:
                calibrator.write_to_influx(args.house, analyses, calibrated_k)
            else:
                print("\n(Use --write to save results to InfluxDB)")

    finally:
        calibrator.close()


if __name__ == '__main__':
    main()

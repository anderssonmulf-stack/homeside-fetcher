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
    dhw_events: int  # Actual DHW usage events (transitions to hot)
    dhw_minutes: int  # Minutes with elevated hot water temp
    data_points: int
    data_coverage: float  # Fraction of expected data points (0.0 to 1.0)
    k_implied: float  # k that would explain this day's consumption

# Minimum data coverage to include day in calibration
MIN_DATA_COVERAGE = 0.8  # 80% = ~77 data points out of 96 expected
EXPECTED_POINTS_PER_DAY = 96  # 24 hours × 4 samples/hour (15-min intervals)

# K calibration percentiles
# Lower percentile = find days with minimal DHW (heating-only baseline)
# Higher percentile = accounts for some DHW in "typical" day
K_PERCENTILE_HEATING_ONLY = 15  # 15th percentile for heating-only estimate (minimal DHW days)
K_PERCENTILE_TYPICAL = 50  # 50th percentile (median) for typical day


class HeatingEnergyCalibrator:
    """Calibrates heating model using real energy data."""

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        latitude: float = 58.41,
        longitude: float = 15.62,
        solar_coefficient: float = None,
        wind_coefficient: float = None
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

        model_kwargs = {}
        if solar_coefficient is not None:
            model_kwargs['solar_coefficient'] = solar_coefficient
        if wind_coefficient is not None:
            model_kwargs['wind_coefficient'] = wind_coefficient
        self.weather_model = get_weather_model('simple', **model_kwargs)

    def fetch_daily_energy(self, house_id: str, start_date: str = "2025-12-01") -> Dict[str, float]:
        """
        Fetch daily energy consumption.

        Queries energy_meter first (reliable imported Dropbox data),
        falls back to energy_consumption (legacy) if no data found.
        """
        # First try energy_meter (imported from Dropbox - reliable)
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_date})
            |> filter(fn: (r) => r["_measurement"] == "energy_meter")
            |> filter(fn: (r) => r["house_id"] == "{house_id}")
            |> filter(fn: (r) => r["_field"] == "consumption")
            |> aggregateWindow(every: 1d, fn: sum, createEmpty: false)
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query, org=self.influx_org)

        daily_energy = {}
        for table in tables:
            for record in table.records:
                date = record.get_time().strftime('%Y-%m-%d')
                daily_energy[date] = record.get_value()

        if daily_energy:
            return daily_energy

        # Fallback to energy_consumption (legacy measurement)
        query_legacy = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_date})
            |> filter(fn: (r) => r["_measurement"] == "energy_consumption")
            |> filter(fn: (r) => r["house_id"] == "{house_id}")
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: 1d, fn: sum, createEmpty: false)
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query_legacy, org=self.influx_org)

        for table in tables:
            for record in table.records:
                date = record.get_time().strftime('%Y-%m-%d')
                daily_energy[date] = record.get_value()

        return daily_energy

    def fetch_heating_data(self, house_id: str, start_date: str = "2025-12-01") -> List[Dict]:
        """Fetch heating system data."""
        # Use regex anchors for exact match to avoid matching multiple houses
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_date})
            |> filter(fn: (r) => r["_measurement"] == "heating_system")
            |> filter(fn: (r) => r["house_id"] == "{house_id}")
            |> filter(fn: (r) =>
                r["_field"] == "room_temperature" or
                r["_field"] == "outdoor_temperature" or
                r["_field"] == "hot_water_temp"
            )
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        '''

        tables = self.query_api.query(query, org=self.influx_org)

        # Deduplicate by rounded timestamp (to nearest minute)
        # Some data has 1-second duplicate entries
        seen_times = set()
        results = []
        duplicates = 0

        for table in tables:
            for record in table.records:
                ts = record.get_time()
                # Round to nearest minute for deduplication
                rounded_ts = ts.replace(second=0, microsecond=0)
                ts_key = rounded_ts.isoformat()

                if ts_key in seen_times:
                    duplicates += 1
                    continue

                seen_times.add(ts_key)
                results.append({
                    'timestamp': ts,
                    'room_temperature': record.values.get('room_temperature'),
                    'outdoor_temperature': record.values.get('outdoor_temperature'),
                    'hot_water_temp': record.values.get('hot_water_temp'),
                })

        return results, duplicates

    def fetch_weather_data(self, house_id: str, start_date: str = "2025-12-01") -> Dict[str, Dict]:
        """Fetch weather observation data."""
        # Use regex anchors for exact match to avoid matching multiple houses
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_date})
            |> filter(fn: (r) => r["_measurement"] == "weather_observation")
            |> filter(fn: (r) => r["house_id"] == "{house_id}")
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
        calibrated_k: float = None,
        k_percentile: int = K_PERCENTILE_HEATING_ONLY,
        debug: bool = False,
        quiet: bool = False
    ) -> Tuple[List[DailyEnergyAnalysis], float]:
        """
        Analyze energy consumption and correlate with temperatures.

        Returns:
            Tuple of (daily_analyses, calibrated_k)
        """
        # Fetch all data
        daily_energy = self.fetch_daily_energy(house_id, start_date)
        heating_data, duplicates_removed = self.fetch_heating_data(house_id, start_date)
        weather_data = self.fetch_weather_data(house_id, start_date)

        if not quiet:
            print(f"Fetched {len(daily_energy)} days of energy data")
            print(f"Fetched {len(heating_data)} heating data points (removed {duplicates_removed} duplicates)")

        if not daily_energy:
            if not quiet:
                print("No energy data available")
            return [], 0.0

        if not heating_data:
            if not quiet:
                print("No heating data available")
            return [], 0.0

        # Calculate hot water baseline
        hw_temps = [d['hot_water_temp'] for d in heating_data if d.get('hot_water_temp')]
        if hw_temps:
            sorted_hw = sorted(hw_temps)
            hw_baseline = sorted_hw[len(sorted_hw) // 4]
        else:
            hw_baseline = 25.0

        if not quiet:
            print(f"Hot water baseline: {hw_baseline:.1f}°C")

        # Group heating data by day
        daily_heating = {}
        for d in heating_data:
            date = d['timestamp'].strftime('%Y-%m-%d')
            if date not in daily_heating:
                daily_heating[date] = []
            daily_heating[date].append(d)

        if debug and not quiet:
            print("\nDEBUG: Data points per day:")
            for date in sorted(daily_heating.keys()):
                count = len(daily_heating[date])
                coverage = count / EXPECTED_POINTS_PER_DAY * 100
                flag = " ← OVER!" if count > EXPECTED_POINTS_PER_DAY else ""
                print(f"  {date}: {count} points ({coverage:.0f}%){flag}")

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

            # Track DHW state for transition detection
            dhw_threshold = hw_baseline + 5.0
            prev_dhw_active = False
            dhw_minutes = 0

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

                # Count DHW events (transitions from cold to hot)
                # and track minutes with elevated hot water temp
                if hw_temp and hw_temp > dhw_threshold:
                    dhw_minutes += 15  # Each sample = 15 minutes
                    if not prev_dhw_active:
                        dhw_events += 1  # New DHW usage event started
                        prev_dhw_active = True
                else:
                    prev_dhw_active = False

            if not indoor_temps:
                continue

            avg_indoor = statistics.mean(indoor_temps)
            avg_outdoor = statistics.mean(outdoor_temps)
            avg_effective = statistics.mean(effective_temps)
            avg_diff = avg_indoor - avg_effective

            # Calculate data coverage (what fraction of expected points we have)
            data_coverage = len(day_data) / EXPECTED_POINTS_PER_DAY

            # Degree-hours (ΔT × hours of data)
            hours_of_data = len(day_data) * 0.25  # 15-min intervals
            degree_hours = avg_diff * hours_of_data

            # Calculate implied k for this day
            # k = Energy / degree_hours
            if degree_hours > 0:
                k_implied = actual_energy / degree_hours
                # Only use days with sufficient data coverage for k calibration
                if data_coverage >= MIN_DATA_COVERAGE:
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
                dhw_minutes=dhw_minutes,
                data_points=len(day_data),
                data_coverage=round(data_coverage, 2),
                k_implied=round(k_implied, 4)
            ))

        # Calibrate k using percentile of implied values
        # Lower percentile (25th) approximates heating-only days
        # Higher percentile (50th median) represents typical day including some DHW
        if k_values:
            if calibrated_k is None:
                sorted_k = sorted(k_values)
                percentile_idx = int(len(sorted_k) * k_percentile / 100)
                calibrated_k = sorted_k[min(percentile_idx, len(sorted_k) - 1)]
                if debug and not quiet:
                    print(f"DEBUG: Using {k_percentile}th percentile, index {percentile_idx} of {len(sorted_k)}")
                    print(f"DEBUG: k values (sorted): {[f'{k:.4f}' for k in sorted_k]}")

            # Update estimates with calibrated k
            for a in analyses:
                a.estimated_heating_kwh = round(calibrated_k * a.degree_hours, 1)
                a.excess_energy_kwh = round(a.actual_energy_kwh - a.estimated_heating_kwh, 1)

        return analyses, calibrated_k

    def print_results(self, analyses: List[DailyEnergyAnalysis], calibrated_k: float, k_percentile: int = K_PERCENTILE_HEATING_ONLY):
        """Print analysis results."""
        if not analyses:
            print("No analysis results")
            return

        print("\n" + "=" * 120)
        print("HEATING ENERGY CALIBRATION RESULTS")
        print("=" * 120)
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

        # Count days used for calibration
        days_used = sum(1 for a in analyses if a.data_coverage >= MIN_DATA_COVERAGE)
        print(f"Days used for k calibration: {days_used} (>={MIN_DATA_COVERAGE*100:.0f}% data coverage)")

        print("\n" + "-" * 120)
        print(f"{'Date':<12} {'Actual':>8} {'Est.Heat':>9} {'Excess':>8} {'ΔT':>6} {'Deg-Hr':>7} {'k_day':>7} {'DHW':>4} {'DHW':>5} {'Cover':>6} {'Indoor':>7} {'OutEff':>7} {'Used':>5}")
        print(f"{'':<12} {'kWh':>8} {'kWh':>9} {'kWh':>8} {'°C':>6} {'':>7} {'kW/°C':>7} {'evts':>4} {'min':>5} {'%':>6} {'°C':>7} {'°C':>7} {'':<5}")
        print("-" * 120)

        for a in analyses:
            excess_pct = f"{a.excess_energy_kwh:+.1f}"
            used_marker = "✓" if a.data_coverage >= MIN_DATA_COVERAGE else ""
            print(f"{a.date:<12} {a.actual_energy_kwh:>8.1f} {a.estimated_heating_kwh:>9.1f} {excess_pct:>8} "
                  f"{a.avg_temp_difference:>6.1f} {a.degree_hours:>7.1f} {a.k_implied:>7.4f} "
                  f"{a.dhw_events:>4} {a.dhw_minutes:>5} {a.data_coverage*100:>5.0f}% "
                  f"{a.avg_indoor_temp:>7.1f} {a.avg_effective_outdoor_temp:>7.1f} {used_marker:>5}")

        # Summary statistics - only for days used in calibration
        k_values_used = [a.k_implied for a in analyses if a.k_implied > 0 and a.data_coverage >= MIN_DATA_COVERAGE]
        k_values_all = [a.k_implied for a in analyses if a.k_implied > 0]

        if k_values_used:
            sorted_k = sorted(k_values_used)
            pct_idx = int(len(sorted_k) * k_percentile / 100)
            k_at_pct = sorted_k[min(pct_idx, len(sorted_k) - 1)]
            p25_idx = int(len(sorted_k) * 0.25)
            p75_idx = int(len(sorted_k) * 0.75)
            k_p25 = sorted_k[min(p25_idx, len(sorted_k) - 1)]
            k_p75 = sorted_k[min(p75_idx, len(sorted_k) - 1)]

            print("\n" + "-" * 120)
            print(f"K-VALUE STATISTICS (days with >={MIN_DATA_COVERAGE*100:.0f}% coverage):")
            print(f"  P{k_percentile}:    {k_at_pct:.4f} kW/°C  ← USED (heating-only baseline)")
            print(f"  Median:  {statistics.median(k_values_used):.4f} kW/°C")
            print(f"  P75:     {k_p75:.4f} kW/°C")
            print(f"  Mean:    {statistics.mean(k_values_used):.4f} kW/°C")
            print(f"  Min:     {min(k_values_used):.4f} kW/°C")
            print(f"  Max:     {max(k_values_used):.4f} kW/°C")
            if len(k_values_used) > 1:
                print(f"  StdDev:  {statistics.stdev(k_values_used):.4f} kW/°C")

        if len(k_values_all) > len(k_values_used):
            print(f"\n  (Excluded {len(k_values_all) - len(k_values_used)} partial days from calibration)")

        # Interpretation
        print("\n" + "=" * 120)
        print("INTERPRETATION:")
        print("-" * 120)
        print(f"• The calibrated k = {calibrated_k:.4f} kW/°C means:")
        print(f"  - For every 1°C difference (indoor - effective outdoor), the house needs {calibrated_k:.2f} kW")
        print(f"  - At ΔT = 25°C (typical winter), heating power ≈ {calibrated_k * 25:.1f} kW")
        print(f"\n• Days with high excess energy likely have more hot water usage")
        print(f"• Days with negative excess: k might be too high, or heating was reduced")
        print(f"\n• DHW events = transitions from cold to hot water (actual usage events)")
        print(f"• DHW minutes = total time hot water was elevated (may include multiple events)")

    def write_to_influx(self, house_id: str, analyses: List[DailyEnergyAnalysis], k: float) -> int:
        """Write separated energy values to InfluxDB.

        Only writes days with sufficient data coverage (>=80%) and excludes
        the current day (incomplete data).

        Deletes existing energy_separated records for this house before writing
        to prevent duplicates from different methods or re-runs.
        """
        from write_throttle import WriteThrottle
        if not WriteThrottle.get().allow("energy_separated", house_id, 3600):
            return 0

        points = []
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        skipped_coverage = 0
        skipped_today = 0

        for a in analyses:
            # Skip days with insufficient data coverage
            if a.data_coverage < MIN_DATA_COVERAGE:
                skipped_coverage += 1
                continue

            # Skip today (incomplete day)
            if a.date == today:
                skipped_today += 1
                continue

            # Parse date to timestamp (midnight)
            ts = datetime.strptime(a.date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

            point = Point("energy_separated") \
                .tag("house_id", house_id) \
                .tag("method", "k_calibration") \
                .time(ts, WritePrecision.S) \
                .field("actual_energy_kwh", float(a.actual_energy_kwh)) \
                .field("heating_energy_kwh", float(a.estimated_heating_kwh)) \
                .field("dhw_energy_kwh", float(max(0, a.excess_energy_kwh))) \
                .field("excess_energy_kwh", float(a.excess_energy_kwh)) \
                .field("avg_temp_difference", float(a.avg_temp_difference)) \
                .field("degree_hours", float(a.degree_hours)) \
                .field("k_value", float(k)) \
                .field("k_implied", float(a.k_implied)) \
                .field("dhw_events", int(a.dhw_events)) \
                .field("dhw_minutes", int(a.dhw_minutes)) \
                .field("data_coverage", float(a.data_coverage)) \
                .field("avg_indoor_temp", float(a.avg_indoor_temp)) \
                .field("avg_effective_outdoor_temp", float(a.avg_effective_outdoor_temp))

            points.append(point)

        if points:
            # Delete existing energy_separated records for this house in the date range
            # to prevent duplicates from different methods or re-runs
            dates = [a.date for a in analyses if a.data_coverage >= MIN_DATA_COVERAGE and a.date != today]
            if dates:
                start = datetime.strptime(min(dates), '%Y-%m-%d').replace(tzinfo=timezone.utc) - timedelta(hours=1)
                stop = datetime.strptime(max(dates), '%Y-%m-%d').replace(tzinfo=timezone.utc) + timedelta(hours=25)
                delete_api = self.client.delete_api()
                delete_api.delete(
                    start=start,
                    stop=stop,
                    predicate=f'_measurement="energy_separated" AND house_id="{house_id}"',
                    bucket=self.influx_bucket,
                    org=self.influx_org
                )

            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=points)
            print(f"\nWrote {len(points)} daily records to InfluxDB (measurement: energy_separated)")
            if skipped_coverage > 0:
                print(f"  Skipped {skipped_coverage} days with <{MIN_DATA_COVERAGE*100:.0f}% data coverage")
            if skipped_today > 0:
                print(f"  Skipped today (incomplete)")
        else:
            print(f"\nNo records to write (all {len(analyses)} days had insufficient coverage or are incomplete)")

        return len(points)

    def close(self):
        if self.client:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(description='Calibrate heating energy model with real data')
    parser.add_argument('--house', type=str, default='HEM_FJV_Villa_149', help='House ID')
    parser.add_argument('--start', type=str, default='2025-12-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--k', type=float, help='Override k value (otherwise calibrated from data)')
    parser.add_argument('--percentile', type=int, default=15,
                        help='Percentile for k calibration (default 15 = heating-only baseline)')
    parser.add_argument('--lat', type=float, default=58.41, help='Latitude')
    parser.add_argument('--lon', type=float, default=15.62, help='Longitude')
    parser.add_argument('--write', action='store_true', help='Write results to InfluxDB')
    parser.add_argument('--debug', action='store_true', help='Show debug info about data points')
    parser.add_argument('--compare', action='store_true', help='Compare different percentiles')
    args = parser.parse_args()

    # Get configuration
    influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.getenv('INFLUXDB_TOKEN')
    influx_org = os.getenv('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.getenv('INFLUXDB_BUCKET', 'heating')

    if not influx_token:
        print("ERROR: INFLUXDB_TOKEN not set")
        sys.exit(1)

    # Load per-building weather coefficients from profile
    solar_coeff = None
    wind_coeff = None
    try:
        from customer_profile import CustomerProfile
        profile = CustomerProfile.load(args.house, profiles_dir='profiles')
        wc = profile.learned.weather_coefficients
        if wc.solar_coefficient_ml2 is not None:
            solar_coeff = wc.solar_coefficient_ml2
        if wc.wind_coefficient_ml2 is not None:
            wind_coeff = wc.wind_coefficient_ml2
        if solar_coeff or wind_coeff:
            print(f"Using learned weather coefficients: solar={solar_coeff}, wind={wind_coeff}")
    except Exception:
        pass  # Use defaults if profile not found

    calibrator = HeatingEnergyCalibrator(
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        latitude=args.lat,
        longitude=args.lon,
        solar_coefficient=solar_coeff,
        wind_coefficient=wind_coeff
    )

    try:
        analyses, calibrated_k = calibrator.analyze(
            house_id=args.house,
            start_date=args.start,
            calibrated_k=args.k,
            k_percentile=args.percentile,
            debug=args.debug
        )

        if analyses:
            calibrator.print_results(analyses, calibrated_k, args.percentile)

            if args.compare:
                print("\n" + "=" * 120)
                print("PERCENTILE COMPARISON")
                print("-" * 120)
                print(f"{'Percentile':>12} {'k (kW/°C)':>10} {'Est Heat':>10} {'DHW':>10} {'DHW %':>8} {'Neg Days':>10}")
                print("-" * 120)

                for pct in [10, 15, 20, 25, 30, 40, 50]:
                    test_analyses, test_k = calibrator.analyze(
                        house_id=args.house,
                        start_date=args.start,
                        k_percentile=pct,
                        debug=False,
                        quiet=True
                    )
                    total_actual = sum(a.actual_energy_kwh for a in test_analyses)
                    total_estimated = sum(a.estimated_heating_kwh for a in test_analyses)
                    total_excess = total_actual - total_estimated
                    excess_pct = 100 * total_excess / total_actual if total_actual > 0 else 0
                    neg_days = sum(1 for a in test_analyses if a.excess_energy_kwh < 0)

                    marker = " ← " if pct == args.percentile else ""
                    print(f"{pct:>10}th {test_k:>10.4f} {total_estimated:>10.1f} {total_excess:>10.1f} {excess_pct:>7.1f}% {neg_days:>10}{marker}")

                print("-" * 120)
                print("Note: Lower percentile = less DHW estimated (purer heating baseline)")
                print("      Aim for minimal negative excess days while maintaining realistic DHW %")

            if args.write:
                calibrator.write_to_influx(args.house, analyses, calibrated_k)
            else:
                print("\n(Use --write to save results to InfluxDB)")

    finally:
        calibrator.close()


if __name__ == '__main__':
    main()

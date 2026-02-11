#!/usr/bin/env python3
"""
K-Value Recalibrator

Automatically recalibrates the building heat loss coefficient (k) every 72h
using properly separated heating-only energy data.

Stores k-value history in InfluxDB for visualization of convergence.

Usage:
    python k_recalibrator.py --house HEM_FJV_Villa_149
    python k_recalibrator.py --all                        # All enabled houses
    python k_recalibrator.py --house HEM_FJV_Villa_149 --dry-run

Environment:
    INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET
"""

import os
import sys
import argparse
import statistics
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from customer_profile import CustomerProfile, find_profile_for_client_id
from energy_models.weather_energy_model import SimpleWeatherModel, WeatherConditions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Minimum data requirements
MIN_DAYS_FOR_CALIBRATION = 3  # Need at least 3 days
MIN_DATA_COVERAGE = 0.7  # 70% of expected data points per day
K_PERCENTILE = 15  # Use 15th percentile (days with minimal DHW)


@dataclass
class CalibrationResult:
    """Result of a k-value calibration run."""
    house_id: str
    timestamp: datetime
    k_value: float
    k_median: float
    k_stddev: float
    days_used: int
    total_days: int
    avg_outdoor_temp: float
    confidence: float  # 0-1 based on data quality
    method: str = "heating_only_15pct"


class KRecalibrator:
    """Recalibrates k-value from separated heating energy data."""

    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        dry_run: bool = False
    ):
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.dry_run = dry_run

        self.client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org
        )
        self.query_api = self.client.query_api()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.weather_model = SimpleWeatherModel()

    def fetch_separated_energy(self, house_id: str, days: int = 30) -> List[Dict]:
        """Fetch daily separated heating energy from InfluxDB."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{days}d)
            |> filter(fn: (r) => r["_measurement"] == "energy_separated")
            |> filter(fn: (r) => r["house_id"] == "{house_id}")
            |> filter(fn: (r) => r["_field"] == "heating_energy_kwh" or r["_field"] == "total_energy_kwh" or r["_field"] == "confidence")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        '''

        try:
            tables = self.query_api.query(query, org=self.influx_org)
            results = []
            for table in tables:
                for record in table.records:
                    results.append({
                        'date': record.get_time().strftime('%Y-%m-%d'),
                        'timestamp': record.get_time(),
                        'heating_kwh': record.values.get('heating_energy_kwh', 0),
                        'total_kwh': record.values.get('total_energy_kwh', 0),
                        'confidence': record.values.get('confidence', 0.5),
                    })
            return results
        except Exception as e:
            logger.error(f"Failed to fetch separated energy: {e}")
            return []

    def fetch_daily_temps(self, house_id: str, days: int = 30) -> Dict[str, Dict]:
        """Fetch daily average temperatures."""
        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{days}d)
            |> filter(fn: (r) => r["_measurement"] == "heating_system")
            |> filter(fn: (r) => r["house_id"] == "{house_id}")
            |> filter(fn: (r) => r["_field"] == "room_temperature" or r["_field"] == "outdoor_temperature")
            |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        try:
            tables = self.query_api.query(query, org=self.influx_org)
            results = {}
            for table in tables:
                for record in table.records:
                    date = record.get_time().strftime('%Y-%m-%d')
                    results[date] = {
                        'indoor': record.values.get('room_temperature'),
                        'outdoor': record.values.get('outdoor_temperature'),
                    }
            return results
        except Exception as e:
            logger.error(f"Failed to fetch temps: {e}")
            return {}

    def calculate_k(
        self,
        house_id: str,
        days: int = 30
    ) -> Optional[CalibrationResult]:
        """
        Calculate k-value from separated heating energy.

        k = heating_energy / (delta_T * hours)

        Uses 15th percentile of daily k values to find "pure heating" days.
        """
        # Fetch data
        energy_data = self.fetch_separated_energy(house_id, days)
        temp_data = self.fetch_daily_temps(house_id, days)

        if not energy_data:
            logger.warning(f"No separated energy data for {house_id}")
            return None

        if not temp_data:
            logger.warning(f"No temperature data for {house_id}")
            return None

        # Calculate daily k values
        daily_k_values = []
        outdoor_temps = []

        for day in energy_data:
            date = day['date']
            if date not in temp_data:
                continue

            temps = temp_data[date]
            indoor = temps.get('indoor')
            outdoor = temps.get('outdoor')
            heating_kwh = day.get('heating_kwh', 0)

            if indoor is None or outdoor is None or heating_kwh <= 0:
                continue

            # Calculate degree-hours for the day
            delta_t = indoor - outdoor
            if delta_t <= 0:
                continue  # No heating needed

            degree_hours = delta_t * 24  # Full day

            # k = energy / degree_hours (kWh / °C·h = kW/°C)
            k_implied = heating_kwh / degree_hours

            if k_implied > 0 and k_implied < 1.0:  # Sanity check
                daily_k_values.append({
                    'date': date,
                    'k': k_implied,
                    'heating_kwh': heating_kwh,
                    'delta_t': delta_t,
                    'outdoor': outdoor,
                    'confidence': day.get('confidence', 0.5),
                })
                outdoor_temps.append(outdoor)

        if len(daily_k_values) < MIN_DAYS_FOR_CALIBRATION:
            logger.warning(f"Insufficient data: {len(daily_k_values)} days (need {MIN_DAYS_FOR_CALIBRATION})")
            return None

        # Use 15th percentile to find "pure heating" days (minimal DHW contamination)
        k_values = [d['k'] for d in daily_k_values]
        sorted_k = sorted(k_values)

        percentile_idx = max(0, int(len(sorted_k) * K_PERCENTILE / 100))
        k_calibrated = sorted_k[percentile_idx]

        # Statistics
        k_median = statistics.median(k_values)
        k_stddev = statistics.stdev(k_values) if len(k_values) > 1 else 0
        avg_outdoor = sum(outdoor_temps) / len(outdoor_temps)

        # Confidence based on data quality
        confidence = min(1.0, len(daily_k_values) / 14)  # Max at 2 weeks
        if k_stddev > 0:
            cv = k_stddev / k_median  # Coefficient of variation
            confidence *= max(0.5, 1.0 - cv)  # Lower if high variation

        logger.info(f"Calibration for {house_id}:")
        logger.info(f"  k (15th pct): {k_calibrated:.4f} kW/°C")
        logger.info(f"  k (median):   {k_median:.4f} kW/°C")
        logger.info(f"  k (stddev):   {k_stddev:.4f} kW/°C")
        logger.info(f"  Days used:    {len(daily_k_values)}")
        logger.info(f"  Confidence:   {confidence:.1%}")

        return CalibrationResult(
            house_id=house_id,
            timestamp=datetime.now(timezone.utc),
            k_value=k_calibrated,
            k_median=k_median,
            k_stddev=k_stddev,
            days_used=len(daily_k_values),
            total_days=len(energy_data),
            avg_outdoor_temp=avg_outdoor,
            confidence=confidence,
        )

    def write_k_history(self, result: CalibrationResult) -> bool:
        """Write k-value to history for tracking convergence."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would write k={result.k_value:.4f} to history")
            return True

        from write_throttle import WriteThrottle
        if not WriteThrottle.get().allow("k_calibration_history", result.house_id, 3600):
            logger.info(f"Throttled k-history write for {result.house_id}")
            return True

        try:
            point = Point("k_calibration_history") \
                .tag("house_id", result.house_id) \
                .tag("method", result.method) \
                .field("k_value", round(result.k_value, 5)) \
                .field("k_median", round(result.k_median, 5)) \
                .field("k_stddev", round(result.k_stddev, 5)) \
                .field("days_used", result.days_used) \
                .field("total_days", result.total_days) \
                .field("avg_outdoor_temp", round(result.avg_outdoor_temp, 1)) \
                .field("confidence", round(result.confidence, 3)) \
                .time(result.timestamp, WritePrecision.S)

            self.write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
            logger.info(f"Wrote k-value history: k={result.k_value:.4f}")
            return True

        except Exception as e:
            logger.error(f"Failed to write k-value history: {e}")
            return False

    def update_profile(self, profile: CustomerProfile, result: CalibrationResult) -> bool:
        """Update customer profile with new k-value."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update profile k from {profile.energy_separation.heat_loss_k} to {result.k_value:.4f}")
            return True

        try:
            old_k = profile.energy_separation.heat_loss_k
            profile.energy_separation.heat_loss_k = round(result.k_value, 5)
            profile.energy_separation.calibration_date = result.timestamp.strftime('%Y-%m-%d')
            profile.energy_separation.calibration_days = result.days_used
            profile.save()

            logger.info(f"Updated profile: k {old_k} -> {result.k_value:.4f}")
            return True

        except Exception as e:
            logger.error(f"Failed to update profile: {e}")
            return False

    def recalibrate(
        self,
        profile: CustomerProfile,
        days: int = 30,
        update_profile: bool = True
    ) -> Optional[CalibrationResult]:
        """
        Run full recalibration for a house.

        Args:
            profile: Customer profile
            days: Days of history to use
            update_profile: Whether to update the profile with new k

        Returns:
            CalibrationResult or None if failed
        """
        house_id = profile.customer_id

        # Check if energy separation is enabled
        if not profile.energy_separation.enabled:
            logger.warning(f"{house_id}: Energy separation not enabled")
            return None

        # Calculate new k
        result = self.calculate_k(house_id, days)
        if not result:
            return None

        # Write to history
        self.write_k_history(result)

        # Update profile if requested
        if update_profile:
            self.update_profile(profile, result)

        return result


def recalibrate_house(
    house_id: str,
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    profiles_dir: str = "profiles",
    days: int = 30,
    dry_run: bool = False,
    update_profile: bool = True
) -> Optional[CalibrationResult]:
    """
    Convenience function to recalibrate a single house.

    Can be called from the main fetcher.
    """
    # Find profile
    profile = None
    for filename in os.listdir(profiles_dir):
        if filename.endswith('.json') and '_signals.json' not in filename and house_id in filename:
            try:
                profile = CustomerProfile.load(
                    filename.replace('.json', ''),
                    profiles_dir=profiles_dir
                )
                break
            except:
                pass

    if not profile:
        # Try finding by client ID (short form like HEM_FJV_Villa_149)
        profile = find_profile_for_client_id(house_id, profiles_dir=profiles_dir)

    if not profile:
        logger.error(f"No profile found for {house_id}")
        return None

    # Run recalibration
    recalibrator = KRecalibrator(
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        dry_run=dry_run
    )

    return recalibrator.recalibrate(profile, days=days, update_profile=update_profile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Recalibrate k-value from separated energy data')
    parser.add_argument('--house', type=str, help='House ID to recalibrate')
    parser.add_argument('--all', action='store_true', help='Recalibrate all enabled houses')
    parser.add_argument('--days', type=int, default=30, help='Days of history to use')
    parser.add_argument('--dry-run', action='store_true', help='Do not write changes')
    parser.add_argument('--no-update', action='store_true', help='Do not update profile')
    args = parser.parse_args()

    # Get InfluxDB config from environment
    influx_url = os.environ.get('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.environ.get('INFLUXDB_TOKEN')
    influx_org = os.environ.get('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.environ.get('INFLUXDB_BUCKET', 'heating')

    if not influx_token:
        print("Error: INFLUXDB_TOKEN environment variable required")
        sys.exit(1)

    recalibrator = KRecalibrator(
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        dry_run=args.dry_run
    )

    profiles_dir = "profiles"
    results = []

    if args.house:
        # Single house
        profile = CustomerProfile.load(args.house, profiles_dir=profiles_dir)
        result = recalibrator.recalibrate(
            profile,
            days=args.days,
            update_profile=not args.no_update
        )
        if result:
            results.append(result)

    elif args.all:
        # All enabled houses
        for filename in os.listdir(profiles_dir):
            if not filename.endswith('.json') or '_signals.json' in filename:
                continue
            try:
                house_id = filename.replace('.json', '')
                profile = CustomerProfile.load(house_id, profiles_dir=profiles_dir)
                if profile.energy_separation.enabled:
                    result = recalibrator.recalibrate(
                        profile,
                        days=args.days,
                        update_profile=not args.no_update
                    )
                    if result:
                        results.append(result)
            except Exception as e:
                logger.warning(f"Failed to process {filename}: {e}")

    else:
        print("Specify --house HOUSE_ID or --all")
        sys.exit(1)

    # Summary
    print(f"\n{'='*60}")
    print(f"Recalibration Summary: {len(results)} house(s)")
    for r in results:
        print(f"  {r.house_id}: k={r.k_value:.4f} kW/°C ({r.days_used} days, {r.confidence:.0%} confidence)")

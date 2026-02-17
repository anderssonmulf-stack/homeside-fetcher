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
        dry_run: bool = False,
        entity_tag: str = "house_id",
        measurement: str = "heating_system",
        field_mapping: dict = None,
        assumed_indoor_temp: float = None,
    ):
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        self.dry_run = dry_run
        self.entity_tag = entity_tag
        self.measurement = measurement
        self.field_mapping = field_mapping or {}
        self.assumed_indoor_temp = assumed_indoor_temp

        self.client = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org,
            timeout=5_000
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
            |> filter(fn: (r) => r["{self.entity_tag}"] == "{house_id}")
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
        """Fetch daily average temperatures.

        Uses self.measurement and self.field_mapping to query the correct
        InfluxDB measurement. For buildings with assumed_indoor_temp,
        only queries outdoor temperature.
        """
        # Resolve actual field names from mapping
        outdoor_field = self.field_mapping.get('outdoor_temperature', 'outdoor_temperature')
        indoor_field = self.field_mapping.get('room_temperature', 'room_temperature')

        if self.assumed_indoor_temp is not None:
            # Buildings: only query outdoor temp
            field_filter = f'r["_field"] == "{outdoor_field}"'
        else:
            field_filter = f'r["_field"] == "{indoor_field}" or r["_field"] == "{outdoor_field}"'

        query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: -{days}d)
            |> filter(fn: (r) => r["_measurement"] == "{self.measurement}")
            |> filter(fn: (r) => r["{self.entity_tag}"] == "{house_id}")
            |> filter(fn: (r) => {field_filter})
            |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        try:
            tables = self.query_api.query(query, org=self.influx_org)
            results = {}
            for table in tables:
                for record in table.records:
                    date = record.get_time().strftime('%Y-%m-%d')
                    outdoor = record.values.get(outdoor_field)
                    indoor = record.values.get(indoor_field)
                    if self.assumed_indoor_temp is not None:
                        indoor = self.assumed_indoor_temp
                    results[date] = {
                        'indoor': indoor,
                        'outdoor': outdoor,
                    }

            # Fallback: supplement sparse outdoor temps with SMHI weather data
            if self.assumed_indoor_temp is not None and len(results) < days // 2:
                logger.info(f"Sparse outdoor temps ({len(results)} days), supplementing with SMHI weather")
                weather_query = f'''
                    from(bucket: "{self.influx_bucket}")
                    |> range(start: -{days}d)
                    |> filter(fn: (r) => r["_measurement"] == "weather_observation")
                    |> filter(fn: (r) => r["house_id"] == "{house_id}")
                    |> filter(fn: (r) => r["_field"] == "temperature")
                    |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
                '''
                weather_tables = self.query_api.query(weather_query, org=self.influx_org)
                for table in weather_tables:
                    for record in table.records:
                        date = record.get_time().strftime('%Y-%m-%d')
                        if date not in results:
                            results[date] = {
                                'indoor': self.assumed_indoor_temp,
                                'outdoor': record.get_value(),
                            }
                logger.info(f"After SMHI supplement: {len(results)} days")

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

            k_max = 50.0 if self.entity_tag == "building_id" else 1.0
            if k_implied > 0 and k_implied < k_max:  # Sanity check
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
                .tag(self.entity_tag, result.house_id) \
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


def recalibrate_entity(
    entity_id: str,
    entity_type: str,
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    days: int = 30,
    dry_run: bool = False,
    update_config: bool = True,
    profiles_dir: str = "profiles",
    buildings_dir: str = "buildings",
) -> Optional[CalibrationResult]:
    """
    Unified recalibration for houses and buildings.

    Args:
        entity_id: House ID (e.g. HEM_FJV_Villa_149) or building ID (e.g. TE236_HEM_Kontor)
        entity_type: "house" or "building"
        influx_url, influx_token, influx_org, influx_bucket: InfluxDB connection
        days: Days of history to use
        dry_run: If True, don't write changes
        update_config: Whether to update profile/config with new k
        profiles_dir: Directory for house profiles
        buildings_dir: Directory for building configs

    Returns:
        CalibrationResult or None if failed
    """
    import json

    if entity_type == "building":
        config_path = os.path.join(buildings_dir, f'{entity_id}.json')
        if not os.path.exists(config_path):
            logger.error(f"Building config not found: {config_path}")
            return None

        with open(config_path) as f:
            config = json.load(f)

        es = config.get('energy_separation', {})
        if not es.get('enabled'):
            logger.warning(f"{entity_id}: Energy separation not enabled")
            return None

        field_mapping = es.get('field_mapping', {
            'outdoor_temperature': 'outdoor_temp_fvc',
            'hot_water_temp': 'vv1_hot_water_temp',
        })
        assumed_indoor_temp = es.get('assumed_indoor_temp', 21.0)

        recalibrator = KRecalibrator(
            influx_url=influx_url,
            influx_token=influx_token,
            influx_org=influx_org,
            influx_bucket=influx_bucket,
            dry_run=dry_run,
            entity_tag="building_id",
            measurement="building_system",
            field_mapping=field_mapping,
            assumed_indoor_temp=assumed_indoor_temp,
        )

        result = recalibrator.calculate_k(entity_id, days)
        if not result:
            return None

        recalibrator.write_k_history(result)

        if update_config and not dry_run:
            _update_building_config(config_path, config, result)

        return result

    else:
        # House mode
        profile = None
        for filename in os.listdir(profiles_dir):
            if filename.endswith('.json') and '_signals.json' not in filename and entity_id in filename:
                try:
                    profile = CustomerProfile.load(
                        filename.replace('.json', ''),
                        profiles_dir=profiles_dir
                    )
                    break
                except:
                    pass

        if not profile:
            profile = find_profile_for_client_id(entity_id, profiles_dir=profiles_dir)

        if not profile:
            logger.error(f"No profile found for {entity_id}")
            return None

        recalibrator = KRecalibrator(
            influx_url=influx_url,
            influx_token=influx_token,
            influx_org=influx_org,
            influx_bucket=influx_bucket,
            dry_run=dry_run,
        )

        return recalibrator.recalibrate(profile, days=days, update_profile=update_config)


def recalibrate_house(
    house_id: str,
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    profiles_dir: str = "profiles",
    days: int = 30,
    dry_run: bool = False,
    update_profile: bool = True,
    entity_tag: str = "house_id"
) -> Optional[CalibrationResult]:
    """Thin wrapper around recalibrate_entity() for backward compatibility."""
    return recalibrate_entity(
        entity_id=house_id,
        entity_type="house",
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        days=days,
        dry_run=dry_run,
        update_config=update_profile,
        profiles_dir=profiles_dir,
    )


def recalibrate_building(
    building_id: str,
    influx_url: str,
    influx_token: str,
    influx_org: str,
    influx_bucket: str,
    buildings_dir: str = "buildings",
    days: int = 30,
    dry_run: bool = False,
    update_config: bool = True,
) -> Optional[CalibrationResult]:
    """Thin wrapper around recalibrate_entity() for backward compatibility."""
    return recalibrate_entity(
        entity_id=building_id,
        entity_type="building",
        influx_url=influx_url,
        influx_token=influx_token,
        influx_org=influx_org,
        influx_bucket=influx_bucket,
        days=days,
        dry_run=dry_run,
        update_config=update_config,
        buildings_dir=buildings_dir,
    )


def _update_building_config(config_path: str, config: dict, result: CalibrationResult):
    """Write updated k-value back to building JSON config."""
    import json

    try:
        es = config.get('energy_separation', {})
        old_k = es.get('heat_loss_k')
        es['heat_loss_k'] = round(result.k_value, 5)
        es['calibration_date'] = result.timestamp.strftime('%Y-%m-%d')
        es['calibration_days'] = result.days_used
        config['energy_separation'] = es

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

        logger.info(f"Updated building config: k {old_k} -> {result.k_value:.4f}")
    except Exception as e:
        logger.error(f"Failed to update building config: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Recalibrate k-value from separated energy data')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--house', type=str, help='House ID to recalibrate')
    group.add_argument('--building', type=str, help='Building ID to recalibrate')
    group.add_argument('--all', action='store_true', help='Recalibrate all enabled houses')
    parser.add_argument('--days', type=int, default=30, help='Days of history to use')
    parser.add_argument('--dry-run', action='store_true', help='Do not write changes')
    parser.add_argument('--no-update', action='store_true', help='Do not update profile/config')
    args = parser.parse_args()

    # Get InfluxDB config from environment
    influx_url = os.environ.get('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.environ.get('INFLUXDB_TOKEN')
    influx_org = os.environ.get('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.environ.get('INFLUXDB_BUCKET', 'heating')

    if not influx_token:
        print("Error: INFLUXDB_TOKEN environment variable required")
        sys.exit(1)

    results = []

    if args.building:
        # Single building
        result = recalibrate_entity(
            entity_id=args.building,
            entity_type="building",
            influx_url=influx_url,
            influx_token=influx_token,
            influx_org=influx_org,
            influx_bucket=influx_bucket,
            days=args.days,
            dry_run=args.dry_run,
            update_config=not args.no_update,
        )
        if result:
            results.append(result)

    elif args.house:
        # Single house
        result = recalibrate_entity(
            entity_id=args.house,
            entity_type="house",
            influx_url=influx_url,
            influx_token=influx_token,
            influx_org=influx_org,
            influx_bucket=influx_bucket,
            days=args.days,
            dry_run=args.dry_run,
            update_config=not args.no_update,
        )
        if result:
            results.append(result)

    elif args.all:
        # All enabled houses
        profiles_dir = "profiles"
        recalibrator = KRecalibrator(
            influx_url=influx_url,
            influx_token=influx_token,
            influx_org=influx_org,
            influx_bucket=influx_bucket,
            dry_run=args.dry_run
        )
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
        print("Specify --house HOUSE_ID, --building BUILDING_ID, or --all")
        sys.exit(1)

    # Summary
    print(f"\n{'='*60}")
    print(f"Recalibration Summary: {len(results)} entity(s)")
    for r in results:
        print(f"  {r.house_id}: k={r.k_value:.4f} kW/°C ({r.days_used} days, {r.confidence:.0%} confidence)")

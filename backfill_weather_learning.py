#!/usr/bin/env python3
"""
Backfill Weather Learning (ML2)

Processes historical data from InfluxDB to bootstrap solar coefficient learning.
Instead of waiting for new solar events, this script analyzes existing data.

Usage:
    # Dry run - see what events would be detected
    python3 backfill_weather_learning.py --house-id HEM_FJV_Villa_149 --dry-run

    # Process and update profile
    python3 backfill_weather_learning.py --house-id HEM_FJV_Villa_149

    # Process specific date range
    python3 backfill_weather_learning.py --house-id HEM_FJV_Villa_149 --days 90

    # Process all houses
    python3 backfill_weather_learning.py --all
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

from influxdb_client import InfluxDBClient

# Configure logging for backfill script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from customer_profile import CustomerProfile, find_profile_for_client_id
from weather_sensitivity_learner import WeatherSensitivityLearner, SolarEvent


def get_historical_data(
    client: InfluxDBClient,
    org: str,
    bucket: str,
    house_id: str,
    days: int = 90
) -> List[Dict]:
    """
    Query historical heating and weather data from InfluxDB.

    Returns combined data with all fields needed for solar event detection.
    """
    query_api = client.query_api()

    # Query heating system data
    heating_query = f'''
        from(bucket: "{bucket}")
        |> range(start: -{days}d)
        |> filter(fn: (r) => r["_measurement"] == "heating_system")
        |> filter(fn: (r) => r["house_id"] == "{house_id}")
        |> filter(fn: (r) =>
            r["_field"] == "supply_temp" or
            r["_field"] == "return_temp" or
            r["_field"] == "room_temperature" or
            r["_field"] == "outdoor_temperature"
        )
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
    '''

    heating_tables = query_api.query(heating_query, org=org)

    # Build heating data by timestamp
    heating_by_time = {}
    for table in heating_tables:
        for record in table.records:
            ts = record.get_time()
            if ts:
                ts_key = ts.isoformat()
                heating_by_time[ts_key] = {
                    'timestamp': ts,
                    'supply_temp': record.values.get('supply_temp'),
                    'return_temp': record.values.get('return_temp'),
                    'room_temp': record.values.get('room_temperature'),
                    'outdoor_temp': record.values.get('outdoor_temperature'),
                }

    # Query weather observations for wind and humidity
    weather_query = f'''
        from(bucket: "{bucket}")
        |> range(start: -{days}d)
        |> filter(fn: (r) => r["_measurement"] == "weather_observation")
        |> filter(fn: (r) => r["house_id"] == "{house_id}")
        |> filter(fn: (r) =>
            r["_field"] == "wind_speed" or
            r["_field"] == "humidity" or
            r["_field"] == "temperature"
        )
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
    '''

    weather_tables = query_api.query(weather_query, org=org)

    # Build weather data by hour (for matching with heating data)
    weather_by_hour = {}
    for table in weather_tables:
        for record in table.records:
            ts = record.get_time()
            if ts:
                # Round to hour for matching
                hour_key = ts.replace(minute=0, second=0, microsecond=0).isoformat()
                weather_by_hour[hour_key] = {
                    'wind_speed': record.values.get('wind_speed', 3.0),
                    'humidity': record.values.get('humidity', 60.0),
                }

    # Query cloud cover from weather forecast
    cloud_query = f'''
        from(bucket: "{bucket}")
        |> range(start: -{days}d)
        |> filter(fn: (r) => r["_measurement"] == "weather_forecast")
        |> filter(fn: (r) => r["house_id"] == "{house_id}")
        |> filter(fn: (r) => r["_field"] == "avg_cloud_cover")
        |> sort(columns: ["_time"])
    '''

    cloud_tables = query_api.query(cloud_query, org=org)

    # Build cloud cover by hour
    cloud_by_hour = {}
    for table in cloud_tables:
        for record in table.records:
            ts = record.get_time()
            value = record.get_value()
            if ts and value is not None:
                hour_key = ts.replace(minute=0, second=0, microsecond=0).isoformat()
                cloud_by_hour[hour_key] = value

    # Combine all data
    combined = []
    last_wind = 3.0
    last_cloud = 4.0

    for ts_key in sorted(heating_by_time.keys()):
        heating = heating_by_time[ts_key]
        ts = heating['timestamp']

        # Skip if missing required fields
        if None in [heating['supply_temp'], heating['return_temp'],
                    heating['room_temp'], heating['outdoor_temp']]:
            continue

        # Find matching weather data
        hour_key = ts.replace(minute=0, second=0, microsecond=0).isoformat()
        weather = weather_by_hour.get(hour_key, {})

        wind_speed = weather.get('wind_speed', last_wind)
        if wind_speed is not None:
            last_wind = wind_speed

        cloud_cover = cloud_by_hour.get(hour_key, last_cloud)
        if cloud_cover is not None:
            last_cloud = cloud_cover

        combined.append({
            'timestamp': ts,
            'supply_temp': heating['supply_temp'],
            'return_temp': heating['return_temp'],
            'room_temp': heating['room_temp'],
            'outdoor_temp': heating['outdoor_temp'],
            'wind_speed': wind_speed or 3.0,
            'cloud_cover': cloud_cover or 4.0,
        })

    return combined


def process_house(
    client: InfluxDBClient,
    org: str,
    bucket: str,
    profile: CustomerProfile,
    days: int = 90,
    dry_run: bool = False,
    verbose: bool = False
) -> Dict:
    """
    Process historical data for a single house and detect solar events.

    Args:
        client: InfluxDB client
        org: InfluxDB organization
        bucket: InfluxDB bucket
        profile: Customer profile
        days: Days of history to process
        dry_run: If True, don't write to InfluxDB or update profile
        verbose: Print detailed output

    Returns:
        Dict with processing results
    """
    house_id = profile.customer_id
    heat_loss_k = profile.energy_separation.heat_loss_k or 0.05

    print(f"\n{'='*60}")
    print(f"Processing: {profile.friendly_name} ({house_id})")
    print(f"  Heat loss k: {heat_loss_k:.4f} kW/°C")
    print(f"  Days: {days}")
    print(f"  Dry run: {dry_run}")
    print(f"{'='*60}")

    # Get location from environment or profile
    # Note: In a real deployment, these would come from the profile or config
    latitude = float(os.environ.get('LATITUDE', '58.41'))
    longitude = float(os.environ.get('LONGITUDE', '15.62'))

    # Load existing coefficients from profile
    existing_coefficients = profile.learned.weather_coefficients

    # Initialize learner
    learner = WeatherSensitivityLearner(
        heat_loss_k=heat_loss_k,
        latitude=latitude,
        longitude=longitude,
        coefficients=existing_coefficients,
    )

    # Query historical data
    print(f"\nQuerying {days} days of historical data...")
    data = get_historical_data(client, org, bucket, house_id, days)
    print(f"  Found {len(data)} data points")

    if not data:
        print("  No data found!")
        return {'events': 0, 'error': 'No data'}

    # Process each observation
    detected_events: List[SolarEvent] = []

    for i, obs in enumerate(data):
        event = learner.process_historical_observation(
            timestamp=obs['timestamp'],
            supply_temp=obs['supply_temp'],
            return_temp=obs['return_temp'],
            room_temp=obs['room_temp'],
            outdoor_temp=obs['outdoor_temp'],
            cloud_cover=obs['cloud_cover'],
            wind_speed=obs['wind_speed']
        )

        if event:
            detected_events.append(event)
            if verbose:
                print(f"  Event #{len(detected_events)}: {event.timestamp.strftime('%Y-%m-%d %H:%M')} "
                      f"({event.duration_minutes:.0f}min, coeff={event.implied_solar_coefficient_ml2:.1f})")

    # Check for final event (in case data ends during an event)
    if learner.current_event_start is not None:
        final_event = learner._finalize_event()
        if final_event:
            detected_events.append(final_event)

    print(f"\nDetected {len(detected_events)} solar events")

    if not detected_events:
        print("  No solar events detected (building may have poor solar exposure)")
        return {'events': 0, 'coefficient': existing_coefficients.solar_coefficient_ml2}

    # Calculate statistics
    coefficients = [e.implied_solar_coefficient_ml2 for e in detected_events]
    coefficients.sort()
    median_coeff = coefficients[len(coefficients) // 2]
    mean_coeff = sum(coefficients) / len(coefficients)
    min_coeff = min(coefficients)
    max_coeff = max(coefficients)

    print(f"\nImplied solar coefficients:")
    print(f"  Median: {median_coeff:.1f}")
    print(f"  Mean:   {mean_coeff:.1f}")
    print(f"  Range:  {min_coeff:.1f} - {max_coeff:.1f}")

    # Event details
    total_duration = sum(e.duration_minutes for e in detected_events)
    avg_duration = total_duration / len(detected_events)
    print(f"\nEvent statistics:")
    print(f"  Total events: {len(detected_events)}")
    print(f"  Total duration: {total_duration:.0f} minutes ({total_duration/60:.1f} hours)")
    print(f"  Average duration: {avg_duration:.0f} minutes")

    # Sample events
    if verbose and detected_events:
        print(f"\nSample events:")
        for event in detected_events[:5]:
            print(f"  {event.timestamp.strftime('%Y-%m-%d %H:%M')}: "
                  f"{event.duration_minutes:.0f}min, sun={event.avg_sun_elevation:.1f}°, "
                  f"cloud={event.avg_cloud_cover:.1f}, coeff={event.implied_solar_coefficient_ml2:.1f}")

    if dry_run:
        print("\n[DRY RUN] Would update:")
        print(f"  solar_coefficient_ml2: {existing_coefficients.solar_coefficient_ml2:.1f} -> {median_coeff:.1f}")
        print(f"  total_solar_events: {existing_coefficients.total_solar_events} -> {len(detected_events)}")
        return {
            'events': len(detected_events),
            'old_coefficient': existing_coefficients.solar_coefficient_ml2,
            'new_coefficient': median_coeff,
            'dry_run': True
        }

    # Update learner with all events for coefficient calculation
    learner.coefficients.total_solar_events = len(detected_events)
    learner.coefficients.events_since_last_update = len(detected_events)
    learner.detected_events = detected_events

    # Calculate final coefficient
    if learner.should_update_coefficients():
        updated = learner.update_coefficients()
    else:
        # Force update for backfill
        updated = learner.update_coefficients()

    # Write events to InfluxDB
    from influx_writer import InfluxDBWriter
    influx = InfluxDBWriter(
        url=os.environ.get('INFLUXDB_URL', 'http://influxdb:8086'),
        token=os.environ.get('INFLUXDB_TOKEN', ''),
        org=org,
        bucket=bucket,
        house_id=house_id,
        logger=logger,
        enabled=True
    )

    print(f"\nWriting {len(detected_events)} events to InfluxDB...")
    for event in detected_events:
        influx.write_solar_event(event.to_dict())

    # Write updated coefficients
    influx.write_weather_coefficients_ml2(updated.to_dict())
    influx.close()

    # Update profile
    profile.learned.weather_coefficients = updated
    profile.save()

    print(f"\nProfile updated:")
    print(f"  solar_coefficient_ml2: {updated.solar_coefficient_ml2:.1f}")
    print(f"  solar_confidence_ml2: {updated.solar_confidence_ml2:.0%}")
    print(f"  total_solar_events: {updated.total_solar_events}")

    return {
        'events': len(detected_events),
        'old_coefficient': existing_coefficients.solar_coefficient_ml2,
        'new_coefficient': updated.solar_coefficient_ml2,
        'confidence': updated.solar_confidence_ml2,
    }


def main():
    parser = argparse.ArgumentParser(
        description='Backfill solar learning from historical data'
    )
    parser.add_argument(
        '--house-id',
        help='House ID to process (e.g., HEM_FJV_Villa_149)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all houses with energy_separation enabled'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Days of history to process (default: 90)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Analyze only, do not write to InfluxDB or update profiles'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print detailed event information'
    )
    parser.add_argument(
        '--profiles-dir',
        default='profiles',
        help='Directory containing customer profiles'
    )

    args = parser.parse_args()

    if not args.house_id and not args.all:
        parser.error('Either --house-id or --all is required')

    # InfluxDB connection
    influx_url = os.environ.get('INFLUXDB_URL', 'http://influxdb:8086')
    influx_token = os.environ.get('INFLUXDB_TOKEN', '')
    influx_org = os.environ.get('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.environ.get('INFLUXDB_BUCKET', 'heating')

    print(f"Connecting to InfluxDB: {influx_url}")
    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)

    # Get profiles to process
    profiles_to_process = []

    if args.all:
        # Find all profiles with energy_separation enabled
        for filename in os.listdir(args.profiles_dir):
            if not filename.endswith('.json'):
                continue
            try:
                profile = CustomerProfile.load_by_path(
                    os.path.join(args.profiles_dir, filename)
                )
                if profile.energy_separation.enabled:
                    profiles_to_process.append(profile)
            except Exception as e:
                print(f"Warning: Failed to load {filename}: {e}")
    else:
        # Load specific profile
        try:
            profile = CustomerProfile.load(args.house_id, args.profiles_dir)
            profiles_to_process.append(profile)
        except FileNotFoundError:
            print(f"Error: Profile not found: {args.house_id}")
            sys.exit(1)

    if not profiles_to_process:
        print("No profiles to process")
        sys.exit(0)

    print(f"\nFound {len(profiles_to_process)} profile(s) to process")

    # Process each profile
    results = {}
    for profile in profiles_to_process:
        try:
            result = process_house(
                client=client,
                org=influx_org,
                bucket=influx_bucket,
                profile=profile,
                days=args.days,
                dry_run=args.dry_run,
                verbose=args.verbose
            )
            results[profile.customer_id] = result
        except Exception as e:
            print(f"Error processing {profile.customer_id}: {e}")
            results[profile.customer_id] = {'error': str(e)}

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    for house_id, result in results.items():
        if 'error' in result:
            print(f"  {house_id}: ERROR - {result['error']}")
        else:
            events = result.get('events', 0)
            old_coeff = result.get('old_coefficient', 6.0)
            new_coeff = result.get('new_coefficient', old_coeff)
            print(f"  {house_id}: {events} events, coeff {old_coeff:.1f} -> {new_coeff:.1f}")

    client.close()


if __name__ == '__main__':
    main()

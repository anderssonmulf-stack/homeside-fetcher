import time
import os
import json
import logging
from datetime import datetime, timezone, timedelta

from homeside_api import HomeSideAPI
from smhi_weather import SMHIWeather
from influx_writer import InfluxDBWriter
from thermal_analyzer import ThermalAnalyzer
from heat_curve_controller import HeatCurveController
from control_homeside import HomeSideControl
from seq_logger import SeqLogger
from customer_profile import CustomerProfile, find_profile_for_client_id
from temperature_forecaster import TemperatureForecaster
from energy_models.weather_energy_model import SimpleWeatherModel, WeatherConditions
from gap_filler import fill_gaps_on_startup, run_daily_gap_fill
from energy_forecaster import EnergyForecaster, format_energy_forecast
from k_recalibrator import recalibrate_house
from energy_importer import EnergyImporter
from dropbox_client import create_client_from_env
from weather_sensitivity_learner import WeatherSensitivityLearner


def check_data_staleness(influx, settings: dict, logger) -> dict:
    """
    Check if data in InfluxDB is stale compared to expected intervals.

    Args:
        influx: InfluxDBWriter instance
        settings: Application settings with data_collection intervals
        logger: Logger instance

    Returns:
        Dictionary with staleness info for each measurement type
    """
    if not influx:
        return {}

    data_settings = settings.get('data_collection', {})
    intervals = {
        'heating_system': data_settings.get('heating_data_interval_minutes', 15),
        'weather_forecast': data_settings.get('weather_forecast_interval_minutes', 120),
        'temperature_forecast': data_settings.get('ml_forecast_interval_minutes', 120)
    }

    friendly_names = {
        'heating_system': 'Heating data',
        'weather_forecast': 'Weather forecast',
        'temperature_forecast': 'ML temperature forecast'
    }

    now = datetime.now(timezone.utc)
    staleness_info = {}

    try:
        last_timestamps = influx.get_last_data_timestamps()

        for measurement, last_time in last_timestamps.items():
            expected_interval = intervals.get(measurement, 15)
            friendly_name = friendly_names.get(measurement, measurement)

            if last_time is None:
                logger.info(f"[STALENESS] {friendly_name}: No historical data found")
                print(f"ℹ️  {friendly_name}: No historical data found")
                staleness_info[measurement] = {
                    'last_time': None,
                    'age_minutes': None,
                    'expected_interval': expected_interval,
                    'is_stale': True,
                    'missing': True
                }
            else:
                # Ensure timezone awareness
                if last_time.tzinfo is None:
                    last_time = last_time.replace(tzinfo=timezone.utc)

                age = now - last_time
                age_minutes = age.total_seconds() / 60
                is_stale = age_minutes > expected_interval * 2  # Stale if > 2x expected interval

                staleness_info[measurement] = {
                    'last_time': last_time,
                    'age_minutes': age_minutes,
                    'expected_interval': expected_interval,
                    'is_stale': is_stale,
                    'missing': False
                }

                if is_stale:
                    hours = int(age_minutes // 60)
                    mins = int(age_minutes % 60)
                    age_str = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"
                    logger.info(
                        f"[STALENESS] {friendly_name}: Last data {age_str} ago "
                        f"(expected every {expected_interval}m)"
                    )
                    print(f"ℹ️  {friendly_name}: Last data {age_str} ago (expected every {expected_interval}m)")
                else:
                    mins = int(age_minutes)
                    print(f"✓ {friendly_name}: Last data {mins}m ago (OK)")

    except Exception as e:
        logger.warning(f"Failed to check data staleness: {e}")
        print(f"⚠ Failed to check data staleness: {e}")

    return staleness_info


def load_settings(settings_path: str = 'settings.json') -> dict:
    """
    Load application settings from JSON file.

    Returns default settings if file not found.
    """
    defaults = {
        'weather': {
            'forecast_interval_minutes': 120,
            'observation_enabled': True,
            'nearest_station_cache_hours': 24
        },
        'heating': {
            'target_indoor_temp': 22.0,
            'temp_margin': 0.5
        },
        'data_collection': {
            'heating_data_interval_minutes': 15,
            'weather_forecast_interval_minutes': 120,
            'ml_forecast_interval_minutes': 120,
            'failure_error_threshold_minutes': 120
        }
    }

    try:
        # Try multiple paths (local and docker)
        paths = [settings_path, f'/app/{settings_path}']
        for path in paths:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    settings = json.load(f)
                # Merge with defaults (settings override defaults)
                for key, value in defaults.items():
                    if key not in settings:
                        settings[key] = value
                    elif isinstance(value, dict):
                        for k, v in value.items():
                            if k not in settings[key]:
                                settings[key][k] = v
                return settings
    except Exception as e:
        print(f"Could not load settings.json: {e}, using defaults")

    return defaults


def generate_forecast_points(
    weather,
    heat_curve,
    thermal,
    current_indoor: float,
    current_outdoor: float,
    logger,
    forecast_hours: int = 72
) -> list:
    """
    Generate forecast points for visualization.

    Creates hourly forecast data for the configured forecast horizon:
    - outdoor_temp: From SMHI weather forecast
    - supply_temp_baseline: Expected supply temp from original heat curve
    - supply_temp_ml: Expected supply temp from ML-adjusted curve
    - indoor_temp: Predicted indoor temp using thermal analysis

    Args:
        weather: SMHIWeather instance
        heat_curve: HeatCurveController instance (optional)
        thermal: ThermalAnalyzer instance
        current_indoor: Current indoor temperature
        current_outdoor: Current outdoor temperature
        logger: Logger instance

    Returns:
        List of forecast point dictionaries ready for InfluxDB
    """
    from datetime import datetime, timedelta, timezone

    forecast_points = []

    # Get hourly weather forecast
    if not weather:
        return forecast_points

    hourly_forecasts = weather.get_forecast(hours_ahead=forecast_hours)
    if not hourly_forecasts:
        logger.warning("No weather forecast available for forecast generation")
        return forecast_points

    # Track indoor temp prediction (cumulative from current)
    predicted_indoor = current_indoor

    for forecast in hourly_forecasts:
        # Parse forecast time
        forecast_time_str = forecast.get('time')
        if not forecast_time_str:
            continue

        forecast_time = datetime.fromisoformat(
            forecast_time_str.replace('Z', '+00:00')
        )
        forecast_outdoor = forecast.get('temp')

        if forecast_outdoor is None:
            continue

        # 1. Outdoor temperature forecast
        forecast_points.append({
            'timestamp': forecast_time,
            'forecast_type': 'outdoor_temp',
            'value': forecast_outdoor
        })

        # 2. Supply temperature forecasts (from heat curve)
        if heat_curve:
            baseline_supply, current_supply = heat_curve.get_supply_temps_for_outdoor(
                forecast_outdoor
            )
            if baseline_supply is not None:
                forecast_points.append({
                    'timestamp': forecast_time,
                    'forecast_type': 'supply_temp_baseline',
                    'value': baseline_supply
                })
            if current_supply is not None:
                forecast_points.append({
                    'timestamp': forecast_time,
                    'forecast_type': 'supply_temp_ml',
                    'value': current_supply
                })

        # 3. Indoor temperature forecast (using thermal prediction)
        # Predict incrementally - each hour affects the next
        hours_ahead = forecast.get('hour', 1)
        if hours_ahead <= 1:
            hours_step = hours_ahead
        else:
            hours_step = 1  # Predict in 1-hour steps

        predicted_temp = thermal.predict_temperature_change(
            current_indoor=predicted_indoor,
            forecast_outdoor=forecast_outdoor,
            hours_ahead=hours_step,
            heating_active=True  # Assume heating is active
        )

        if predicted_temp is not None:
            predicted_indoor = predicted_temp
            forecast_points.append({
                'timestamp': forecast_time,
                'forecast_type': 'indoor_temp',
                'value': predicted_indoor
            })

    if forecast_points:
        logger.info(f"Generated {len(forecast_points)} forecast points for next 24h")

    return forecast_points


def run_energy_pipeline(
    config: dict,
    customer_profile,
    seq_logger,
    logger,
    profiles_dir: str = "profiles",
    calibration_days: int = 30
) -> dict:
    """
    Run the daily energy data pipeline:
    1. Import energy data from Dropbox
    2. Fill weather gaps + backfill effective_temp
    3. Separate heating vs DHW energy
    4. Recalibrate k-values

    Args:
        config: Application config with InfluxDB settings
        customer_profile: CustomerProfile instance (or None for all houses)
        seq_logger: SeqLogger instance
        logger: Python logger
        profiles_dir: Directory containing customer profiles
        calibration_days: Days of data for k-value calibration

    Returns:
        Dict with results from each step
    """
    results = {
        'import': {'success': False, 'files': 0, 'records': 0},
        'gap_fill': {'success': False, 'weather_written': 0, 'effective_temp_written': 0},
        'separation': {'success': False, 'houses': 0, 'records': 0},
        'calibration': {'success': False, 'houses': 0}
    }

    print("\n" + "="*50)
    print("🔄 DAILY ENERGY PIPELINE")
    print("="*50)

    # Step 1: Import energy data from Dropbox
    print("\n📥 Step 1: Importing energy data from Dropbox...")
    try:
        dropbox_client = create_client_from_env()
        if not dropbox_client:
            print("ℹ Dropbox not configured - skipping import (handled by main fetcher)")
            seq_logger.log(
                "Energy pipeline: Dropbox not configured - import handled by main fetcher",
                level='Info'
            )
        else:
            importer = EnergyImporter(
                dropbox_client=dropbox_client,
                influx_url=config.get('influxdb_url'),
                influx_token=config.get('influxdb_token'),
                influx_org=config.get('influxdb_org'),
                influx_bucket=config.get('influxdb_bucket'),
                profiles_dir=profiles_dir,
                dry_run=False
            )
            import_result = importer.run(sync_meters=True)
            importer.close()

            results['import'] = {
                'success': True,
                'files': import_result.get('files', 0),
                'records': import_result.get('records', 0),
                'errors': import_result.get('errors', [])
            }

            if import_result.get('records', 0) > 0:
                print(f"✓ Imported {import_result['records']} records from {import_result['files']} file(s)")
                seq_logger.log(
                    "Energy import: {Records} records from {Files} file(s)",
                    level='Information',
                    properties={
                        'Records': import_result['records'],
                        'Files': import_result['files']
                    }
                )
            else:
                print("ℹ No new energy files to import")

    except Exception as e:
        logger.error(f"Energy import failed: {e}")
        print(f"❌ Import failed: {e}")
        seq_logger.log(
            "Energy import failed: {Error}",
            level='Error',
            properties={'Error': str(e)}
        )

    # Step 2: Fill weather gaps + backfill effective_temp
    print("\n🌤 Step 2: Filling data gaps...")
    try:
        gap_result = run_daily_gap_fill(
            influx_url=config.get('influxdb_url'),
            influx_token=config.get('influxdb_token'),
            influx_org=config.get('influxdb_org'),
            influx_bucket=config.get('influxdb_bucket'),
            house_id=customer_profile.customer_id if customer_profile else None,
            latitude=config.get('latitude'),
            longitude=config.get('longitude'),
            profile=customer_profile,
            logger=logger,
            hours_back=48
        )
        results['gap_fill'] = {
            'success': True,
            'weather_written': gap_result.get('weather_written', 0),
            'effective_temp_written': gap_result.get('effective_temp_written', 0)
        }
        total_gap = gap_result.get('weather_written', 0) + gap_result.get('effective_temp_written', 0)
        if total_gap > 0:
            print(f"✓ Gap fill: {gap_result['weather_written']} weather + {gap_result['effective_temp_written']} effective_temp")
        else:
            print("ℹ No data gaps to fill")
    except Exception as e:
        logger.error(f"Gap fill failed: {e}")
        print(f"❌ Gap fill failed: {e}")
        results['gap_fill'] = {'success': False, 'weather_written': 0, 'effective_temp_written': 0}

    # Step 3: Update energy separation (heating vs DHW breakdown) — own house only
    print("\n⚡ Step 3: Updating energy separation...")
    separation_houses = 0
    separation_records = 0
    try:
        if customer_profile and customer_profile.energy_separation.enabled:
            from heating_energy_calibrator import run_energy_separation
            result = run_energy_separation(
                entity_id=customer_profile.customer_id,
                entity_type="house",
                influx_url=config['influxdb_url'],
                influx_token=config['influxdb_token'],
                influx_org=config['influxdb_org'],
                influx_bucket=config['influxdb_bucket'],
                profile=customer_profile,
                latitude=config.get('latitude'),
                longitude=config.get('longitude'),
                logger=logger,
                seq=seq_logger,
            )
            if result:
                written, used_k = result
                separation_records += written
                separation_houses += 1
                print(f"  ✓ {customer_profile.friendly_name}: {written} days written (k={used_k:.4f})")
            else:
                print(f"  ℹ {customer_profile.friendly_name}: no data to separate")

        results['separation'] = {
            'success': True,
            'houses': separation_houses,
            'records': separation_records
        }

        if separation_houses > 0:
            print(f"✓ Separated energy for {separation_houses} house(s), {separation_records} total records")
        else:
            print("ℹ No houses ready for energy separation")

    except Exception as e:
        logger.error(f"Energy separation failed: {e}")
        print(f"❌ Energy separation failed: {e}")
        seq_logger.log(
            "Energy separation failed: {Error}",
            level='Error',
            properties={'Error': str(e)}
        )
        results['separation'] = {'success': False, 'houses': 0, 'records': 0}

    # Step 4: Recalibrate k-values (runs after energy import) — own house only
    print("\n📊 Step 4: Recalibrating k-values...")
    if results['import']['success']:
        calibrated_count = 0
        try:
            if customer_profile and customer_profile.energy_separation.enabled:
                result = recalibrate_house(
                    house_id=customer_profile.customer_id,
                    influx_url=config['influxdb_url'],
                    influx_token=config['influxdb_token'],
                    influx_org=config['influxdb_org'],
                    influx_bucket=config['influxdb_bucket'],
                    profiles_dir=profiles_dir,
                    days=calibration_days,
                    update_profile=True
                )
                if result:
                    calibrated_count += 1
                    print(f"  ✓ {customer_profile.friendly_name}: k={result.k_value:.4f} kW/°C ({result.confidence:.0%} confidence)")
                    seq_logger.log(
                        "K-value calibrated for {House}: {KValue} kW/°C",
                        level='Information',
                        properties={
                            'House': customer_profile.friendly_name,
                            'HouseId': customer_profile.customer_id,
                            'KValue': round(result.k_value, 5),
                            'Confidence': round(result.confidence, 2),
                            'DaysUsed': result.days_used
                        }
                    )

            results['calibration'] = {
                'success': True,
                'houses': calibrated_count
            }

            if calibrated_count > 0:
                print(f"✓ Calibrated k-values for {calibrated_count} house(s)")
            else:
                print("ℹ No houses ready for k-value calibration")

        except Exception as e:
            logger.error(f"K-value calibration failed: {e}")
            print(f"❌ Calibration failed: {e}")
            seq_logger.log(
                "K-value calibration failed: {Error}",
                level='Error',
                properties={'Error': str(e)}
            )
    else:
        print("⏭ Skipping calibration (no new separation data)")

    # Summary
    gap = results['gap_fill']
    print("\n" + "-"*50)
    print("📋 Pipeline Summary:")
    print(f"  Import:      {results['import']['records']} records")
    print(f"  Gap fill:    {gap['weather_written']} weather + {gap['effective_temp_written']} effective_temp")
    print(f"  Separation:  {results['separation']['records']} records")
    print(f"  Calibration: {results['calibration'].get('houses', 0)} house(s)")
    print("="*50 + "\n")

    # Log overall pipeline result
    seq_logger.log(
        "Energy pipeline completed: import={ImportRecords}, gap_fill={GapWeather}+{GapEffTemp}, separation={SepRecords}, calibration={CalHouses}",
        level='Information',
        properties={
            'ImportRecords': results['import']['records'],
            'ImportFiles': results['import']['files'],
            'GapWeather': gap['weather_written'],
            'GapEffTemp': gap['effective_temp_written'],
            'SepRecords': results['separation']['records'],
            'SepHouses': results['separation']['houses'],
            'CalHouses': results['calibration'].get('houses', 0),
            'PipelineSuccess': all([
                results['import']['success'] or results['import']['records'] == 0,
                results['separation']['success'],
                results['calibration']['success']
            ])
        }
    )

    return results


def check_dropbox_and_separate(
    config: dict,
    seq_logger,
    logger,
    customer_profile=None,
    profiles_dir: str = "profiles",
    calibration_days: int = 30
) -> int:
    """
    Check Dropbox for new energy files, import them, and immediately
    run energy separation + k-calibration for this subprocess's house.

    The Dropbox import is shared (idempotent — files deleted after import),
    but separation and calibration only process the calling subprocess's house.

    Called periodically (e.g. every hour) so new energy data gets
    processed without waiting for the daily 08:00 pipeline.

    Returns:
        Number of records imported (0 means nothing new).
    """
    try:
        dropbox_client = create_client_from_env()
        if not dropbox_client:
            return 0

        importer = EnergyImporter(
            dropbox_client=dropbox_client,
            influx_url=config.get('influxdb_url'),
            influx_token=config.get('influxdb_token'),
            influx_org=config.get('influxdb_org'),
            influx_bucket=config.get('influxdb_bucket'),
            profiles_dir=profiles_dir,
            dry_run=False
        )
        import_result = importer.run(sync_meters=True)
        importer.close()

        records = import_result.get('records', 0)
        if records == 0:
            return 0

        # New data imported — run separation + calibration for own house only
        print(f"\n📥 Dropbox import: {records} records from {import_result.get('files', 0)} file(s)")
        seq_logger.log(
            "Dropbox import (periodic): {Records} records from {Files} file(s)",
            level='Information',
            properties={'Records': records, 'Files': import_result.get('files', 0)}
        )

        # Energy separation — own house only
        separation_done = False
        if customer_profile and customer_profile.energy_separation.enabled:
            print("⚡ Running energy separation after import...")
            from heating_energy_calibrator import run_energy_separation
            sep_result = run_energy_separation(
                entity_id=customer_profile.customer_id,
                entity_type="house",
                influx_url=config['influxdb_url'],
                influx_token=config['influxdb_token'],
                influx_org=config['influxdb_org'],
                influx_bucket=config['influxdb_bucket'],
                profile=customer_profile,
                latitude=config.get('latitude'),
                longitude=config.get('longitude'),
                logger=logger,
                seq=seq_logger,
            )
            if sep_result:
                written, used_k = sep_result
                separation_done = True
                print(f"  ✓ {customer_profile.friendly_name}: {written} days separated (k={used_k:.4f})")

            # K-value recalibration — own house only
            if separation_done:
                print("📊 Running k-value recalibration after import...")
                try:
                    result = recalibrate_house(
                        house_id=customer_profile.customer_id,
                        influx_url=config['influxdb_url'],
                        influx_token=config['influxdb_token'],
                        influx_org=config['influxdb_org'],
                        influx_bucket=config['influxdb_bucket'],
                        profiles_dir=profiles_dir,
                        days=calibration_days,
                        update_profile=True
                    )
                    if result:
                        print(f"  ✓ {customer_profile.friendly_name}: k={result.k_value:.4f} kW/°C")
                except Exception as e:
                    logger.warning(f"Post-import calibration failed for {customer_profile.friendly_name}: {e}")

                print(f"✓ Post-import pipeline complete ({customer_profile.friendly_name})")
                seq_logger.log(
                    "Post-import separation+calibration done for {House}",
                    level='Information',
                    properties={'House': customer_profile.friendly_name}
                )

        return records

    except Exception as e:
        logger.warning(f"Dropbox check failed: {e}")
        return 0


def check_daily_tasks(
    settings: dict,
    last_run_dates: dict,
    config: dict,
    customer_profile,
    seq_logger,
    logger
) -> dict:
    """
    Check if any daily tasks are due and run them.

    Args:
        settings: Application settings with daily_tasks config
        last_run_dates: Dict tracking last run date for each task
        config: Application config
        customer_profile: CustomerProfile instance
        seq_logger: SeqLogger instance
        logger: Python logger

    Returns:
        Updated last_run_dates dict
    """
    from datetime import datetime, timezone
    import pytz

    daily_tasks = settings.get('daily_tasks', {})
    if not daily_tasks:
        return last_run_dates

    # Use local timezone for task scheduling
    try:
        local_tz = pytz.timezone('Europe/Stockholm')
    except:
        local_tz = timezone.utc

    now = datetime.now(local_tz)
    today = now.date()

    for task_name, task_config in daily_tasks.items():
        if not task_config.get('enabled', False):
            continue

        # Check if already run today
        last_run = last_run_dates.get(task_name)
        if last_run == today:
            continue

        # Parse scheduled time
        scheduled_time_str = task_config.get('time', '08:00')
        try:
            hour, minute = map(int, scheduled_time_str.split(':'))
            scheduled_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            logger.warning(f"Invalid time format for task {task_name}: {scheduled_time_str}")
            continue

        # Check if it's time to run
        if now >= scheduled_time:
            logger.info(f"Running daily task: {task_name}")
            print(f"\n⏰ Daily task due: {task_name} (scheduled {scheduled_time_str})")

            if task_name == 'energy_pipeline':
                calibration_days = settings.get('calibration', {}).get('k_calibration_days', 30)
                run_energy_pipeline(
                    config=config,
                    customer_profile=customer_profile,
                    seq_logger=seq_logger,
                    logger=logger,
                    profiles_dir="profiles",
                    calibration_days=calibration_days
                )

            # Mark as run for today
            last_run_dates[task_name] = today
            logger.info(f"Completed daily task: {task_name}")

    return last_run_dates


def monitor_heating_system(config):
    """Main monitoring function"""

    # Setup standard Python logging
    logging.basicConfig(
        level=config.get('log_level', 'INFO'),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()

    # Stagger startup so processes don't all hit InfluxDB at the same time
    # Must be before any InfluxDB work (gap filler, staleness check, etc.)
    poll_offset = config.get('poll_offset_seconds', 0)
    if poll_offset > 0:
        print(f"Startup delay: {poll_offset}s (staggered poll offset)")
        time.sleep(poll_offset)

    # Setup Seq structured logging (separate from Python logging)
    seq_logger = SeqLogger(
        client_id=config.get('clientid'),
        friendly_name=config.get('friendly_name'),
        username=config.get('username'),
        seq_url=config.get('seq_url'),
        seq_api_key=config.get('seq_api_key'),
        display_name_source=config.get('display_name_source', 'friendly_name')
    )

    if seq_logger.enabled:
        print(f"Seq logging enabled: {config['seq_url']}")
    else:
        print("Seq logging disabled (no SEQ_URL configured)")

    logger.info(f"HomeSide Fetcher starting (interval: {config['interval_minutes']} min)")

    # Get session token - try from config first, then Selenium
    session_token = config.get('session_token')

    if session_token:
        logger.info("Using session token from configuration")
        print("✓ Using session token from .env")
    elif config.get('username') and config.get('password'):
        logger.info("No session token in config, will use API authentication...")
        print("✓ No session token in .env, will use API authentication")
        # session_token will be None initially, API class will get it on first request
        session_token = None
    else:
        logger.error("No session token or credentials provided. Exiting.")
        print("ERROR: Either HOMESIDE_SESSION_TOKEN or HOMESIDE_USERNAME/PASSWORD must be set")
        return

    # Initialize API
    debug_mode = config.get('debug_mode', False)
    api = HomeSideAPI(
        session_token,
        config['clientid'],
        logger,
        username=config['username'],
        password=config['password'],
        debug_mode=debug_mode,
        seq_logger=seq_logger
    )

    # Load application settings
    settings = load_settings()
    forecast_interval_minutes = settings['weather']['forecast_interval_minutes']
    forecast_hours = settings['weather'].get('forecast_hours', 72)
    observation_enabled = settings['weather'].get('observation_enabled', True)
    target_indoor_temp = settings['heating']['target_indoor_temp']
    temp_margin = settings['heating']['temp_margin']

    # Initialize weather (SMHI observations + forecasts)
    weather = None
    if config.get('latitude') and config.get('longitude'):
        weather = SMHIWeather(
            config['latitude'],
            config['longitude'],
            logger,
            station_cache_hours=settings['weather']['nearest_station_cache_hours']
        )
        print(f"✓ Weather enabled (lat: {config['latitude']}, lon: {config['longitude']})")
        print(f"  - Observations: every {config['interval_minutes']} min")
        print(f"  - Forecast: {forecast_hours}h horizon, every {forecast_interval_minutes} min")
    else:
        print("⚠ Weather disabled (no location configured)")

    # Get BMS token (this will also auto-discover client ID if not set)
    if not api.get_bms_token():
        logger.error("Failed to get BMS token. Exiting.")
        print("Failed to get BMS token. Exiting.")
        return

    # Now we have the client ID (either from config or auto-discovered)
    print(f"✓ Using client ID: {api.clientid}")
    if api.house_name:
        print(f"✓ House name: {api.house_name}")

    # Initialize InfluxDB (after BMS token so we have the client ID)
    influx = None
    if config.get('influxdb_enabled'):
        influx = InfluxDBWriter(
            url=config.get('influxdb_url'),
            token=config.get('influxdb_token'),
            org=config.get('influxdb_org'),
            bucket=config.get('influxdb_bucket'),
            house_id=api.clientid.split('/')[-1],  # Use short form: HEM_FJV_Villa_XXX
            logger=logger,
            enabled=True,
            seq_logger=seq_logger,
            settings=settings.get('influxdb', {})
        )
        print(f"✓ InfluxDB enabled: {config.get('influxdb_url')}")

        # Check for stale data on startup
        print("\n--- Checking data freshness ---")
        staleness_info = check_data_staleness(influx, settings, logger)
        if any(info.get('is_stale') or info.get('missing') for info in staleness_info.values()):
            print("→ Starting immediate data collection due to stale/missing data")
        print()

        # Attempt to fill gaps in historical data
        print("--- Checking for data gaps ---")
        try:
            success = fill_gaps_on_startup(
                influx_url=config.get('influxdb_url'),
                influx_token=config.get('influxdb_token'),
                influx_org=config.get('influxdb_org'),
                influx_bucket=config.get('influxdb_bucket'),
                house_id=api.clientid.split('/')[-1],  # Use short form
                username=config.get('username'),
                password=config.get('password'),
                settings=settings,
                logger=logger,
                latitude=config.get('latitude'),
                longitude=config.get('longitude')
            )
            if success:
                print("✓ Gap filler completed")
        except Exception as e:
            logger.warning(f"Gap filler failed (non-critical): {e}")
            print(f"⚠ Gap filler skipped: {e}")
        print()
    else:
        print("⚠ InfluxDB disabled (INFLUXDB_ENABLED=false)")

    # Initialize thermal analyzer (with InfluxDB for persistence if available)
    thermal = ThermalAnalyzer(logger, min_samples=24, influx=influx)
    if influx and thermal.historical_data:
        print(f"✓ Thermal analyzer initialized ({len(thermal.historical_data)} historical points loaded)")
    else:
        print("✓ Thermal analyzer initialized")

    # Load customer profile and initialize forecasters
    customer_profile = None
    forecaster = None
    energy_forecaster = None
    try:
        customer_profile = find_profile_for_client_id(api.clientid, profiles_dir="profiles")
        if customer_profile:
            # Apply per-house variable overrides (e.g. use zone sensor instead of broken mean)
            if customer_profile.variable_overrides:
                for api_name, field_name in customer_profile.variable_overrides.items():
                    # Remove old mapping for this field_name
                    old_api = [k for k, v in api.field_mapping.items() if v == field_name]
                    for k in old_api:
                        del api.field_mapping[k]
                        if k in api.target_vars:
                            api.target_vars.remove(k)
                    # Add new mapping
                    api.field_mapping[api_name] = field_name
                    if api_name not in api.target_vars:
                        api.target_vars.append(api_name)
                    print(f"  Override: {field_name} → {api_name}")

            forecaster = TemperatureForecaster(customer_profile)
            print(f"✓ Customer profile loaded: {customer_profile.friendly_name}")
            status = customer_profile.get_status()
            print(f"  - Target temp: {status['target_temp']}°C")
            print(f"  - Learning: {status['learning_status']}")

            # Initialize energy forecaster if k-value is calibrated
            heat_loss_k = customer_profile.energy_separation.heat_loss_k
            if heat_loss_k:
                # Use from_profile to include ML2 learned coefficients
                energy_forecaster = EnergyForecaster.from_profile(
                    profile=customer_profile,
                    latitude=config.get('latitude', 58.41),
                    longitude=config.get('longitude', 15.62),
                    logger=logger
                )
                print(f"  - Energy forecast: k={heat_loss_k:.4f} kW/°C")
                # Show if using ML2 coefficients
                if energy_forecaster.solar_coefficient_ml2 and energy_forecaster.solar_confidence_ml2 >= 0.2:
                    print(f"    Using ML2 model: solar={energy_forecaster.solar_coefficient_ml2:.1f}")
            else:
                print("  - Energy forecast: Not calibrated (run heating_energy_calibrator.py)")

            # Show ML2 weather learning status
            weather_status = customer_profile.get_status()
            print(f"  - Weather ML2: {weather_status.get('weather_status_ml2', 'Not initialized')}")
            if weather_status.get('solar_coefficient_ml2', 6.0) > 6.0:
                print(f"    Solar coeff: {weather_status.get('solar_coefficient_ml2'):.1f} "
                      f"({weather_status.get('solar_events_total', 0)} events)")
        else:
            print("⚠ No customer profile found, using legacy forecaster")
    except Exception as e:
        logger.warning(f"Failed to load customer profile: {e}")
        print(f"⚠ Customer profile error: {e}, using legacy forecaster")

    # Initialize weather sensitivity learner (ML2)
    weather_learner = None
    if customer_profile and customer_profile.energy_separation.heat_loss_k:
        heat_loss_k = customer_profile.energy_separation.heat_loss_k
        lat = config.get('latitude', 58.41)
        lon = config.get('longitude', 15.62)
        weather_learner = WeatherSensitivityLearner(
            heat_loss_k=heat_loss_k,
            latitude=lat,
            longitude=lon,
            coefficients=customer_profile.learned.weather_coefficients,
            timing=customer_profile.learned.thermal_timing,
            logger=logger
        )
        print(f"✓ Weather sensitivity learner (ML2) initialized")
    else:
        print("⚠ Weather sensitivity learner disabled (requires calibrated k-value)")

    # Initialize heat curve controller
    heat_curve_enabled = config.get('heat_curve_enabled', False)
    heat_curve = None
    if influx:  # Requires InfluxDB for baseline storage
        heat_curve = HeatCurveController(
            api=api,
            influx=influx,
            logger=logger,
            debug_mode=debug_mode
        )
        if heat_curve_enabled:
            print("✓ Heat curve controller enabled (ACTIVE MODE)")
        else:
            print("✓ Heat curve controller enabled (SIMULATION MODE - no changes applied)")
    else:
        print("⚠ Heat curve controller disabled (requires InfluxDB)")

    # Curve control mode mapping for InfluxDB (0=manual, 1=adaptive, 2=intelligent)
    CURVE_MODE_MAP = {"manual": 0, "adaptive": 1, "intelligent": 2}

    # Initialize ML curve control (writes weather-adjusted curve to HomeSide)
    ml_curve_control = None
    prev_curve_mode = None
    if customer_profile:
        prev_curve_mode = customer_profile.heat_curve_control.curve_control_mode
        if prev_curve_mode == "intelligent":
            ml_curve_control = HomeSideControl(
                api=api,
                profile=customer_profile,
                logger=logger,
                seq_logger=seq_logger
            )
            print(f"✓ ML curve control enabled (interval: {customer_profile.heat_curve_control.ml_update_interval_minutes}min)")

            # If we were in control before a restart, resume by re-reading indices
            if customer_profile.heat_curve_control.in_control:
                print("  Resuming ML curve control from previous session...")
                ml_curve_control.read_baseline()  # Re-discover Cwl.Advise indices
        else:
            print(f"ℹ Curve control mode: {prev_curve_mode}")

    interval_minutes = settings.get('data_collection', {}).get('heating_data_interval_minutes', 5)
    print(f"Starting monitoring loop (interval: {interval_minutes} minutes, offset: {poll_offset}s, live-reloaded from settings.json)")
    print("Press Ctrl+C to stop\n")

    # Track forecast timing (fetch every forecast_interval_minutes)
    last_forecast_time = None
    # Cache for forecast data between updates
    cached_forecast_trend = None
    cached_recommendation = None

    # Track last prediction for accuracy measurement
    last_indoor_prediction = None  # (timestamp, predicted_value, outdoor_temp)

    # Track consecutive failures for error escalation
    first_failure_time = None  # Timestamp when consecutive failures started
    failure_error_threshold = settings.get('data_collection', {}).get('failure_error_threshold_minutes', 120)

    # Track k-value recalibration timing (every 72h by default)
    # Initialize to now so we don't fire immediately on container start
    last_recalibration_time = datetime.now(timezone.utc)
    calibration_settings = settings.get('calibration', {})
    recalibration_hours = calibration_settings.get('k_recalibration_hours', 72)
    recalibration_days = calibration_settings.get('k_calibration_days', 30)
    recalibration_enabled = calibration_settings.get('enabled', True)

    # Track ML curve control timing
    last_ml_curve_update = None

    # Track periodic Dropbox import check (every hour)
    last_dropbox_check_time = None
    dropbox_check_interval_hours = 1

    # Track daily task execution (by date)
    daily_task_last_run = {}

    # Show daily task schedule
    daily_tasks = settings.get('daily_tasks', {})
    if daily_tasks:
        print("Daily tasks configured:")
        for task_name, task_config in daily_tasks.items():
            status = "enabled" if task_config.get('enabled') else "disabled"
            print(f"  - {task_name}: {task_config.get('time', '08:00')} ({status})")
        print()

    iteration = 0
    try:
        while True:
            iteration += 1
            now = datetime.now(timezone.utc)
            print(f"\n--- Data Collection #{iteration} ---")
            if debug_mode:
                logger.info(f"Starting data collection #{iteration}")

            # Track if this iteration succeeds
            collection_succeeded = False

            # Fetch raw data
            raw_data = api.get_heating_data()

            # If data fetch failed, try refreshing BMS token and retry once
            if not raw_data:
                logger.info("Data fetch failed (normal retry behavior), refreshing BMS token...")
                print("ℹ️  Refreshing BMS token...")

                if api.get_bms_token():
                    logger.info("BMS token refreshed, retrying data fetch...")
                    print("✓ BMS token refreshed, retrying...")
                    raw_data = api.get_heating_data()

            if raw_data:
                # Extract key values
                extracted_data = api.extract_key_values(raw_data)

                # If no matching variables found, session token likely expired
                if not extracted_data:
                    logger.warning("No matching variables found, session token likely expired. Refreshing...")
                    print("⚠ Session token likely expired, refreshing...")

                    # Try refreshing session token via Selenium
                    if api.refresh_session_token():
                        # Get new BMS token with new session token
                        if api.get_bms_token(retry_on_auth_error=False):
                            logger.info("Tokens refreshed, retrying data fetch...")
                            print("✓ Tokens refreshed, retrying...")
                            # Retry data fetch with new tokens
                            raw_data = api.get_heating_data(retry_on_auth_error=False)
                            if raw_data:
                                extracted_data = api.extract_key_values(raw_data)

                if extracted_data:
                    # Display data
                    api.display_data(extracted_data)

                    # Track forecast accuracy (compare last prediction to actual)
                    if forecaster and last_indoor_prediction is not None:
                        pred_time, predicted, pred_outdoor = last_indoor_prediction
                        actual = extracted_data.get('room_temperature')
                        outdoor = extracted_data.get('outdoor_temperature', pred_outdoor)

                        if actual is not None:
                            error = actual - predicted
                            hour = now.hour

                            # Record for learning
                            forecaster.record_accuracy(
                                predicted=predicted,
                                actual=actual,
                                hour=hour,
                                outdoor=outdoor
                            )

                            # Write to InfluxDB for visualization
                            if influx:
                                influx.write_forecast_accuracy(
                                    predicted=predicted,
                                    actual=actual,
                                    error=error,
                                    hour=hour,
                                    outdoor=outdoor
                                )

                            logger.debug(
                                f"Forecast accuracy: predicted={predicted:.1f}, "
                                f"actual={actual:.1f}, error={error:+.2f}"
                            )

                    # Calculate baseline supply temp from heat curve based on raw outdoor temp
                    if heat_curve and 'outdoor_temperature' in extracted_data:
                        baseline_supply, _ = heat_curve.get_supply_temps_for_outdoor(
                            extracted_data['outdoor_temperature']
                        )
                        if baseline_supply is not None:
                            extracted_data['supply_temp_heat_curve'] = round(baseline_supply, 2)

                    # =====================================================
                    # WEATHER: Current observations (every iteration)
                    # Uses shared cache for neighbors with same coordinates
                    # Moved before InfluxDB write so effective_temp is included
                    # =====================================================
                    weather_obs = None
                    weather_obs_data = None  # Dict version for effective_temp calculation
                    lat = config.get('latitude')
                    lon = config.get('longitude')

                    if weather and observation_enabled:
                        # Try shared cache first (for effective_temp - shared among neighbors)
                        if influx and lat and lon:
                            cached_obs = influx.read_shared_weather_observation(lat, lon)
                            if cached_obs:
                                weather_obs_data = cached_obs
                                print(f"\n📦 Weather: {cached_obs['temperature']:.1f}°C (shared cache from {cached_obs['station_name']})")

                        # If no cache, fetch from SMHI
                        if not weather_obs_data:
                            weather_obs = weather.get_current_weather()
                            if weather_obs and weather_obs.temperature is not None:
                                weather_obs_data = {
                                    'station_name': weather_obs.station.name,
                                    'station_id': weather_obs.station.id,
                                    'distance_km': weather_obs.station.distance_km,
                                    'temperature': weather_obs.temperature,
                                    'wind_speed': weather_obs.wind_speed,
                                    'humidity': weather_obs.humidity,
                                    'timestamp': weather_obs.timestamp
                                }
                                # Write to house-specific and shared cache
                                if influx:
                                    influx.write_weather_observation(weather_obs_data)
                                    if lat and lon:
                                        influx.write_shared_weather_observation(weather_obs_data, lat, lon)
                                print(f"\n🌡️ Current Weather: {weather_obs.temperature:.1f}°C (from {weather_obs.station.name})")

                    # =====================================================
                    # EFFECTIVE TEMP: Calculate ML supply temp using effective temperature
                    # Uses shared weather data from cache or fresh SMHI fetch
                    # =====================================================
                    # Use effective_temp (accounts for wind, humidity, solar) for ML supply temp
                    # This gives a more accurate "perceived" outdoor temperature for heating control
                    if heat_curve and 'outdoor_temperature' in extracted_data:
                        outdoor_temp = extracted_data['outdoor_temperature']
                        effective_temp = outdoor_temp  # Default to raw outdoor temp

                        # Calculate effective temperature if we have weather observation data
                        # (from shared cache or fresh fetch)
                        if weather_obs_data and weather_obs_data.get('temperature') is not None:
                            # Use ML2 coefficients from profile if available
                            model_kwargs = {}
                            if customer_profile and customer_profile.learned.weather_coefficients:
                                wc = customer_profile.learned.weather_coefficients
                                if wc.solar_coefficient_ml2 is not None:
                                    model_kwargs['solar_coefficient'] = wc.solar_coefficient_ml2
                                if wc.wind_coefficient_ml2 is not None:
                                    model_kwargs['wind_coefficient'] = wc.wind_coefficient_ml2
                            weather_model = SimpleWeatherModel(**model_kwargs)
                            conditions = WeatherConditions(
                                timestamp=now,
                                temperature=outdoor_temp,  # Use HomeSide outdoor temp as base
                                wind_speed=weather_obs_data.get('wind_speed') or 3.0,
                                humidity=weather_obs_data.get('humidity') or 60.0,
                                cloud_cover=4.0,  # Default: partly cloudy (could be improved with forecast data)
                                latitude=lat,
                                longitude=lon
                            )
                            eff_result = weather_model.effective_temperature(conditions)
                            effective_temp = eff_result.effective_temp

                            # Store effective temp and its breakdown for analysis
                            extracted_data['effective_temp'] = round(effective_temp, 2)
                            extracted_data['effective_temp_wind_effect'] = round(eff_result.wind_effect, 2)
                            extracted_data['effective_temp_solar_effect'] = round(eff_result.solar_effect, 2)

                            if debug_mode:
                                logger.debug(
                                    f"Effective temp: {effective_temp:.1f}°C "
                                    f"(base: {outdoor_temp:.1f}°C, wind: {eff_result.wind_effect:+.1f}°C, "
                                    f"solar: {eff_result.solar_effect:+.1f}°C)"
                                )

                        # Calculate ML supply temp using effective temperature
                        _, current_supply = heat_curve.get_supply_temps_for_outdoor(effective_temp)
                        if current_supply is not None:
                            extracted_data['supply_temp_heat_curve_ml'] = round(current_supply, 2)

                        # =====================================================
                        # CURVE CONTROL MODE: Detect transitions & ML control
                        # =====================================================
                        if customer_profile:
                            # Re-read mode from profile (GUI may have changed it)
                            try:
                                profiles_dir = customer_profile._profiles_dir
                                fresh_profile = CustomerProfile.load(customer_profile.customer_id, profiles_dir)
                                current_mode = fresh_profile.heat_curve_control.curve_control_mode
                            except Exception:
                                current_mode = customer_profile.heat_curve_control.curve_control_mode

                            # Detect mode transition
                            if current_mode != prev_curve_mode:
                                logger.info(f"Curve control mode changed: {prev_curve_mode} -> {current_mode}")
                                print(f"\n🔄 Curve control mode: {prev_curve_mode} -> {current_mode}")

                                # Exiting intelligent mode: restore baseline
                                if prev_curve_mode == "intelligent" and ml_curve_control and customer_profile.heat_curve_control.in_control:
                                    ml_curve_control.exit_ml_control(reason=f"mode changed to {current_mode}")

                                # Sync the mode to our in-memory profile
                                customer_profile.heat_curve_control.curve_control_mode = current_mode
                                customer_profile.heat_curve_control.ml_enabled = (current_mode == "intelligent")

                                # Entering intelligent mode: create controller
                                if current_mode == "intelligent":
                                    ml_curve_control = HomeSideControl(
                                        api=api,
                                        profile=customer_profile,
                                        logger=logger,
                                        seq_logger=seq_logger
                                    )
                                    logger.info("ML curve control initialized for intelligent mode")

                                prev_curve_mode = current_mode

                            # ML curve control logic (only in intelligent mode)
                            if current_mode == "intelligent" and ml_curve_control:
                                ctrl = customer_profile.heat_curve_control
                                # Enter ML control on first iteration if not already in control
                                if not ctrl.in_control:
                                    if ml_curve_control.enter_ml_control(weather_model, conditions):
                                        last_ml_curve_update = now

                                # Periodic ML curve update
                                if ctrl.in_control:
                                    interval = ctrl.ml_update_interval_minutes
                                    should_update = (
                                        last_ml_curve_update is None or
                                        (now - last_ml_curve_update).total_seconds() >= interval * 60
                                    )

                                    # Reactive update: rewrite early if estimated supply shift changed significantly
                                    # ml_last_offset stores parallel supply shift; estimate current from outdoor offset
                                    current_outdoor_offset = effective_temp - outdoor_temp
                                    estimated_shift = -0.7 * current_outdoor_offset  # approx curve slope
                                    if ctrl.ml_last_offset is not None:
                                        shift_change = abs(estimated_shift - ctrl.ml_last_offset)
                                        if shift_change >= ctrl.ml_reactive_threshold:
                                            should_update = True

                                    if should_update:
                                        if ml_curve_control.update_ml_curve(weather_model, conditions):
                                            last_ml_curve_update = now

                    # Record curve control mode in extracted_data for InfluxDB
                    if customer_profile:
                        mode = customer_profile.heat_curve_control.curve_control_mode
                        extracted_data['curve_control_mode'] = CURVE_MODE_MAP.get(mode, 1)

                    # Add data to thermal analyzer for learning
                    thermal.add_data_point(extracted_data)

                    # Update forecaster with thermal coefficient from analyzer
                    if forecaster and customer_profile:
                        coeff = thermal.calculate_thermal_coefficient()
                        if coeff:
                            customer_profile.update_learned_params(
                                thermal_coefficient=coeff['coefficient'],
                                confidence=coeff['confidence']
                            )

                        # Check if it's time to update hourly bias
                        if forecaster.should_update_learning():
                            logger.info("Updating forecaster hourly bias...")
                            new_bias = forecaster.update_hourly_bias()
                            if new_bias and influx:
                                # Write learned parameters to InfluxDB for tracking
                                influx.write_learned_parameters({
                                    'thermal_coefficient': customer_profile.learned.thermal_coefficient,
                                    'thermal_coefficient_confidence': customer_profile.learned.thermal_coefficient_confidence,
                                    'total_samples': customer_profile.learned.total_samples,
                                    'hourly_bias': customer_profile.learned.hourly_bias
                                })

                    # Write to InfluxDB (includes supply_temp_heat_curve_ml from effective temp)
                    if influx:
                        influx.write_heating_data(extracted_data)

                    # =====================================================
                    # WEATHER SENSITIVITY LEARNING (ML2)
                    # Detect solar heating events and learn coefficients
                    # =====================================================
                    if weather_learner and all(k in extracted_data for k in
                                               ['supply_temp', 'return_temp', 'room_temperature', 'outdoor_temperature']):
                        # Get cloud cover from forecast trend if available
                        cloud_cover = 4.0  # Default: partly cloudy
                        if cached_forecast_trend and cached_forecast_trend.get('avg_cloud_cover') is not None:
                            cloud_cover = cached_forecast_trend['avg_cloud_cover']

                        # Get wind speed from weather observation
                        wind_speed = 3.0  # Default
                        if weather_obs_data and weather_obs_data.get('wind_speed') is not None:
                            wind_speed = weather_obs_data['wind_speed']

                        # Process observation for all ML2 learning systems
                        ml2_result = weather_learner.process_observation(
                            timestamp=now,
                            supply_temp=extracted_data['supply_temp'],
                            return_temp=extracted_data['return_temp'],
                            room_temp=extracted_data['room_temperature'],
                            outdoor_temp=extracted_data['outdoor_temperature'],
                            cloud_cover=cloud_cover,
                            wind_speed=wind_speed
                        )

                        # Handle solar event if one was just completed
                        solar_event = ml2_result.get('solar_event')
                        if solar_event:
                            print(f"☀️  Solar event detected: {solar_event.duration_minutes:.0f}min, "
                                  f"coeff={solar_event.implied_solar_coefficient_ml2:.1f}")

                            # Write to InfluxDB
                            if influx:
                                influx.write_solar_event(solar_event.to_dict())

                            # Log to Seq
                            seq_logger.log(
                                "Solar event detected: {Duration}min, implied_coeff={Coefficient}",
                                level='Information',
                                properties={
                                    'EventType': 'SolarEventDetected',
                                    'Duration': round(solar_event.duration_minutes, 0),
                                    'Coefficient': round(solar_event.implied_solar_coefficient_ml2, 1),
                                    'AvgOutdoorTemp': round(solar_event.avg_outdoor_temp, 1),
                                    'AvgSunElevation': round(solar_event.avg_sun_elevation, 1),
                                    'AvgCloudCover': round(solar_event.avg_cloud_cover, 1),
                                }
                            )

                        # Handle solar early warning (predictive detection)
                        early_warning = ml2_result.get('early_warning')
                        if early_warning:
                            print(f"⚡ Solar early warning: +{early_warning['outdoor_rise']:.1f}°C, "
                                  f"~{early_warning['estimated_lead_time_minutes']:.0f}min lead time")

                            # Write to InfluxDB
                            if influx:
                                influx.write_solar_early_warning(early_warning)

                            seq_logger.log(
                                "Solar early warning: +{Rise}°C rise, lead_time={LeadTime}min",
                                level='Information',
                                properties={
                                    'EventType': 'SolarEarlyWarning',
                                    'OutdoorRise': round(early_warning['outdoor_rise'], 1),
                                    'LeadTimeMinutes': round(early_warning['estimated_lead_time_minutes'], 0),
                                    'Confidence': round(early_warning['confidence'], 2),
                                }
                            )

                        # Handle thermal lag measurement
                        thermal_lag = ml2_result.get('thermal_lag')
                        if thermal_lag:
                            print(f"🕐 Thermal lag measured: {thermal_lag['type']} "
                                  f"{thermal_lag['lag_minutes']:.0f}min")

                            # Write to InfluxDB
                            if influx:
                                influx.write_thermal_lag_measurement(thermal_lag)

                            seq_logger.log(
                                "Thermal lag measured: {Type} {Lag}min",
                                level='Information',
                                properties={
                                    'EventType': 'ThermalLagMeasured',
                                    'TransitionType': thermal_lag['type'],
                                    'LagMinutes': round(thermal_lag['lag_minutes'], 0),
                                    'EffectiveTempChange': round(thermal_lag['effective_temp_change'], 1),
                                    'IndoorTempChange': round(thermal_lag['indoor_temp_change'], 1),
                                }
                            )

                        # Log heating adjustment recommendation (for future use)
                        heating_adj = ml2_result.get('heating_adjustment')
                        if heating_adj and heating_adj.get('action') != 'maintain':
                            print(f"💡 ML2 heating recommendation: {heating_adj['action']} "
                                  f"({heating_adj['reason']})")

                        # Check if it's time to update coefficients
                        if weather_learner.should_update_coefficients():
                            updated_coeffs = weather_learner.update_coefficients()

                            # Update profile
                            customer_profile.learned.weather_coefficients = updated_coeffs
                            customer_profile.save()

                            # Write to InfluxDB for tracking
                            if influx:
                                influx.write_weather_coefficients_ml2(updated_coeffs.to_dict())

                            print(f"📈 ML2 solar coefficient updated: {updated_coeffs.solar_coefficient_ml2:.1f} "
                                  f"({updated_coeffs.solar_confidence_ml2:.0%} confidence)")

                            seq_logger.log(
                                "ML2 coefficient updated: solar={SolarCoeff}, confidence={Confidence}",
                                level='Information',
                                properties={
                                    'EventType': 'WeatherCoefficientsUpdated',
                                    'SolarCoeff': round(updated_coeffs.solar_coefficient_ml2, 1),
                                    'WindCoeff': round(updated_coeffs.wind_coefficient_ml2, 2),
                                    'Confidence': round(updated_coeffs.solar_confidence_ml2, 2),
                                    'TotalEvents': updated_coeffs.total_solar_events,
                                }
                            )

                        # Check if it's time to update thermal timing
                        if weather_learner.should_update_timing():
                            # Update profile with new timing
                            customer_profile.learned.thermal_timing = weather_learner.timing
                            customer_profile.save()

                            # Write to InfluxDB
                            if influx:
                                influx.write_thermal_timing_ml2(weather_learner.timing.to_dict())

                            print(f"📈 ML2 thermal timing updated: heat_up={weather_learner.timing.heat_up_lag_minutes_ml2:.0f}min, "
                                  f"cool_down={weather_learner.timing.cool_down_lag_minutes_ml2:.0f}min")

                    # =====================================================
                    # WEATHER: Forecast (every forecast_interval_minutes)
                    # =====================================================
                    forecast_trend = cached_forecast_trend
                    recommendation = cached_recommendation

                    # Check if we should fetch new forecast
                    should_fetch_forecast = (
                        weather and
                        (last_forecast_time is None or
                         (now - last_forecast_time).total_seconds() >= forecast_interval_minutes * 60)
                    )

                    if should_fetch_forecast:
                        forecast_trend = weather.get_temp_trend(hours_ahead=forecast_hours)
                        if forecast_trend:
                            last_forecast_time = now
                            cached_forecast_trend = forecast_trend

                            # Write forecast summary to InfluxDB
                            if influx:
                                influx.write_forecast_data(forecast_trend)

                                # Generate and write detailed forecast points for visualization
                                influx.delete_future_forecasts()  # Delete future temp forecasts (prevents "curtain" effect)
                                influx.delete_future_weather_forecasts()

                                # Use new forecaster if available, otherwise legacy
                                if forecaster:
                                    # Get hourly weather forecast - try shared cache first
                                    hourly_forecast = None
                                    lat = config.get('latitude')
                                    lon = config.get('longitude')

                                    if lat and lon:
                                        # Check shared cache first (avoids duplicate SMHI API calls for neighbors)
                                        hourly_forecast = influx.read_shared_weather_forecast(lat, lon)
                                        if hourly_forecast:
                                            print("📦 Using shared weather cache")

                                    if not hourly_forecast:
                                        # Fetch from SMHI API
                                        hourly_forecast = weather.get_forecast(hours_ahead=forecast_hours)
                                        # Write to shared cache for other houses
                                        if hourly_forecast and lat and lon:
                                            influx.delete_old_shared_weather_forecasts()
                                            influx.write_shared_weather_forecast(hourly_forecast, lat, lon)
                                    if hourly_forecast:
                                        # Store raw weather forecast for historical analysis
                                        influx.write_weather_forecast_points(hourly_forecast)

                                        # Generate energy forecast if calibrated
                                        if energy_forecaster:
                                            influx.delete_future_energy_forecasts()
                                            energy_points = energy_forecaster.generate_forecast(
                                                weather_forecast=hourly_forecast,
                                                current_indoor_temp=extracted_data.get('room_temperature')
                                            )
                                            if energy_points:
                                                influx.write_energy_forecast(energy_points)
                                                # Display summary
                                                summary_24h = energy_forecaster.get_summary(energy_points, hours=24)
                                                summary_72h = energy_forecaster.get_summary(energy_points, hours=72)
                                                print(format_energy_forecast(energy_points, summary_24h, summary_72h))

                                        forecast_points = forecaster.generate_forecast(
                                            current_indoor=extracted_data.get('room_temperature', 22.0),
                                            current_outdoor=extracted_data.get('outdoor_temperature', 0.0),
                                            weather_forecast=hourly_forecast,
                                            heat_curve=heat_curve
                                        )
                                        if forecast_points:
                                            # Convert to InfluxDB format
                                            influx_points = [p.to_influx_dict() for p in forecast_points]
                                            influx.write_forecast_points(influx_points)

                                            # Store first indoor prediction for accuracy tracking
                                            indoor_forecasts = [
                                                p for p in forecast_points
                                                if p.forecast_type == 'indoor_temp'
                                            ]
                                            if indoor_forecasts:
                                                first_indoor = indoor_forecasts[0]
                                                last_indoor_prediction = (
                                                    first_indoor.timestamp,
                                                    first_indoor.value,
                                                    extracted_data.get('outdoor_temperature', 0.0)
                                                )
                                else:
                                    # Legacy forecaster (fallback)
                                    # First store raw weather forecast for historical analysis
                                    # Try shared cache first
                                    hourly_forecast = None
                                    lat = config.get('latitude')
                                    lon = config.get('longitude')

                                    if lat and lon:
                                        hourly_forecast = influx.read_shared_weather_forecast(lat, lon)
                                        if hourly_forecast:
                                            print("📦 Using shared weather cache (legacy)")

                                    if not hourly_forecast:
                                        hourly_forecast = weather.get_forecast(hours_ahead=forecast_hours)
                                        if hourly_forecast and lat and lon:
                                            influx.delete_old_shared_weather_forecasts()
                                            influx.write_shared_weather_forecast(hourly_forecast, lat, lon)

                                    if hourly_forecast:
                                        influx.write_weather_forecast_points(hourly_forecast)

                                        # Generate energy forecast if calibrated
                                        if energy_forecaster:
                                            influx.delete_future_energy_forecasts()
                                            energy_points = energy_forecaster.generate_forecast(
                                                weather_forecast=hourly_forecast,
                                                current_indoor_temp=extracted_data.get('room_temperature')
                                            )
                                            if energy_points:
                                                influx.write_energy_forecast(energy_points)
                                                summary_24h = energy_forecaster.get_summary(energy_points, hours=24)
                                                summary_72h = energy_forecaster.get_summary(energy_points, hours=72)
                                                print(format_energy_forecast(energy_points, summary_24h, summary_72h))

                                    forecast_points = generate_forecast_points(
                                        weather=weather,
                                        heat_curve=heat_curve,
                                        thermal=thermal,
                                        current_indoor=extracted_data.get('room_temperature', 22.0),
                                        current_outdoor=extracted_data.get('outdoor_temperature', 0.0),
                                        logger=logger,
                                        forecast_hours=forecast_hours
                                    )
                                    if forecast_points:
                                        influx.write_forecast_points(forecast_points)

                            # Get heating recommendation
                            if 'room_temperature' in extracted_data:
                                recommendation = weather.should_reduce_heating(
                                    current_indoor_temp=extracted_data['room_temperature'],
                                    target_temp=target_indoor_temp,
                                    temp_margin=temp_margin
                                )
                                cached_recommendation = recommendation

                                # Write recommendation to InfluxDB
                                if influx:
                                    influx.write_control_decision(recommendation)

                            print(f"📊 Forecast updated (next update in {forecast_interval_minutes} min)")

                    # Consolidated weather & heating display
                    if forecast_trend:
                        print(f"\n📊 Weather Forecast (24h):")
                        print(f"  Temperature: {forecast_trend['current_temp']:.2f}°C {forecast_trend['trend_symbol']} {forecast_trend['change']:+.2f}°C")
                        print(f"  Range: {forecast_trend['min_temp']:.2f}°C - {forecast_trend['max_temp']:.2f}°C")
                        print(f"  Sky: {forecast_trend['cloud_condition']}", end="")
                        if forecast_trend.get('avg_cloud_cover') is not None:
                            print(f" ({forecast_trend['avg_cloud_cover']:.2f}/8 octas)")
                        else:
                            print()

                        if recommendation:
                            action_icon = "🔽" if recommendation['reduce_heating'] else "➡️"
                            solar_icon = "☀️" if recommendation.get('solar_factor') == 'high' else "⛅" if recommendation.get('solar_factor') == 'medium' else ""
                            print(f"\n🔥 Heating: {action_icon} {recommendation['reason']}")
                            print(f"  Confidence: {recommendation['confidence']:.0%}", end="")
                            if solar_icon:
                                print(f" {solar_icon}")
                            else:
                                print()

                        # Get thermal analyzer recommendation (for comparison/learning)
                        if forecast_trend:
                            thermal_rec = thermal.get_heating_recommendation(
                                current_indoor=extracted_data['room_temperature'],
                                forecast_outdoor_trend=forecast_trend,
                                target_temp=target_indoor_temp
                            )

                            if debug_mode:
                                print(f"\n🧠 Thermal Analysis:")
                                print(f"  Action: {thermal_rec['action']}")
                                print(f"  Reason: {thermal_rec['reason']}")
                                print(f"  Predicted temp: {thermal_rec.get('predicted_temp', 0):.2f}°C")

                    # Check if we have enough data for thermal coefficient
                    thermal_data = thermal.calculate_thermal_coefficient()
                    if thermal_data and debug_mode:
                        print(f"\n📈 Thermal Learning:")
                        print(f"  Coefficient: {thermal_data['coefficient']:.2f} °C/h/°C")
                        print(f"  Confidence: {thermal_data['confidence']:.0%}")
                        print(f"  Data points: {thermal_data['data_points']}")

                        # Write to InfluxDB
                        if influx:
                            influx.write_thermal_coefficient(
                                thermal_data['coefficient'],
                                thermal_data['data_points'] // 4  # Convert to hours
                            )

                    # Heat curve controller evaluation
                    curve_recommendation = None
                    if heat_curve and forecast_trend and 'room_temperature' in extracted_data:
                        # Check if any active adjustment has expired
                        heat_curve.check_expiration()

                        # Get recommendation from controller
                        curve_recommendation = heat_curve.should_reduce(
                            forecast_trend=forecast_trend,
                            current_indoor=extracted_data['room_temperature'],
                            target_indoor=target_indoor_temp
                        )

                        if curve_recommendation['reduce']:
                            mode_label = "ACTIVE" if heat_curve_enabled else "SIMULATION"
                            print(f"\n📉 Heat Curve [{mode_label}]:")
                            print(f"  Recommendation: Reduce by {abs(curve_recommendation['delta']):.1f}°C")
                            print(f"  Affected points: {curve_recommendation['affected_points']}")
                            print(f"  Duration: {curve_recommendation['duration_hours']:.0f} hours")
                            print(f"  Reason: {curve_recommendation['reason']}")
                            print(f"  Confidence: {curve_recommendation['confidence']:.0%}")

                            # Only apply changes if enabled
                            if heat_curve_enabled and not heat_curve.adjustment_active:
                                current_curve = heat_curve.read_current_curve()
                                if current_curve:
                                    heat_curve.enter_reduction_mode(
                                        current_curve=current_curve,
                                        affected_points=curve_recommendation['affected_points'],
                                        delta=curve_recommendation['delta'],
                                        duration_hours=curve_recommendation['duration_hours'],
                                        reason=curve_recommendation['reason'],
                                        forecast_change=forecast_trend.get('change', 0)
                                    )
                        elif debug_mode:
                            print(f"\n📉 Heat Curve: No reduction ({curve_recommendation['reason']})")

                    # Send consolidated data to Seq
                    seq_logger.log_data_collection(
                        iteration=iteration,
                        heating_data=extracted_data,
                        forecast=forecast_trend,
                        recommendation=recommendation,
                        curve_recommendation=curve_recommendation,
                        heat_curve_enabled=heat_curve_enabled,
                        curve_adjustment_active=heat_curve.adjustment_active if heat_curve else False,
                        session_info={
                            'last8': api.session_token[-8:] if api.session_token else 'N/A',
                            'source': api.session_token_source,
                            'updated_at': api.session_token_updated_at
                        }
                    )

                    # Mark this iteration as successful
                    collection_succeeded = True

            # Track consecutive failures
            if collection_succeeded:
                if first_failure_time is not None:
                    logger.info("Data collection recovered after previous failures")
                    print("✓ Data collection recovered")
                first_failure_time = None
            else:
                if first_failure_time is None:
                    first_failure_time = now
                    logger.warning("Data collection failed - starting failure tracking")
                    print("⚠ Data collection failed")
                else:
                    failure_duration = (now - first_failure_time).total_seconds() / 60
                    if failure_duration >= failure_error_threshold:
                        logger.error(
                            f"Data collection has been failing for {failure_duration:.0f} minutes "
                            f"(threshold: {failure_error_threshold} minutes)"
                        )
                        print(f"❌ ERROR: Data collection failing for {failure_duration:.0f} minutes!")
                    else:
                        logger.warning(
                            f"Data collection failed - consecutive failures for {failure_duration:.0f} minutes"
                        )
                        print(f"⚠ Data collection failed ({failure_duration:.0f}m of consecutive failures)")

            # Check and run daily scheduled tasks (energy pipeline)
            daily_task_last_run = check_daily_tasks(
                settings=settings,
                last_run_dates=daily_task_last_run,
                config=config,
                customer_profile=customer_profile,
                seq_logger=seq_logger,
                logger=logger
            )

            # Periodic Dropbox import check — picks up new energy files hourly
            # and immediately runs separation + calibration so data is fresh
            if (last_dropbox_check_time is None or
                (now - last_dropbox_check_time).total_seconds() >= dropbox_check_interval_hours * 3600):
                imported = check_dropbox_and_separate(
                    config=config,
                    seq_logger=seq_logger,
                    logger=logger,
                    customer_profile=customer_profile,
                    profiles_dir="profiles",
                    calibration_days=calibration_settings.get('k_calibration_days', 30)
                )
                last_dropbox_check_time = now
                if imported > 0 and customer_profile:
                    # Reload profile in case k-value was updated
                    customer_profile = find_profile_for_client_id(api.clientid, profiles_dir="profiles")
                    if energy_forecaster and customer_profile.energy_separation.heat_loss_k:
                        energy_forecaster.heat_loss_k = customer_profile.energy_separation.heat_loss_k

            # Check if k-value recalibration is due (every 72h) - fallback if daily pipeline hasn't run
            if (recalibration_enabled and customer_profile and
                customer_profile.energy_separation.enabled and
                (last_recalibration_time is None or
                 (now - last_recalibration_time).total_seconds() >= recalibration_hours * 3600)):
                try:
                    print("\n🔧 Running k-value recalibration...")
                    result = recalibrate_house(
                        house_id=customer_profile.customer_id,
                        influx_url=config['influxdb_url'],
                        influx_token=config['influxdb_token'],
                        influx_org=config['influxdb_org'],
                        influx_bucket=config['influxdb_bucket'],
                        profiles_dir="profiles",
                        days=recalibration_days,
                        update_profile=True
                    )
                    if result:
                        old_k = customer_profile.energy_separation.heat_loss_k
                        # Reload profile to get updated k
                        customer_profile = find_profile_for_client_id(api.clientid, profiles_dir="profiles")
                        new_k = customer_profile.energy_separation.heat_loss_k
                        print(f"✓ k-value recalibrated: {old_k:.4f} → {new_k:.4f} kW/°C")
                        print(f"  ({result.days_used} days, {result.confidence:.0%} confidence)")
                        # Update energy forecaster with new k
                        if energy_forecaster:
                            energy_forecaster.heat_loss_k = new_k
                        logger.info(f"Recalibrated k: {old_k:.4f} → {new_k:.4f} ({result.days_used} days)")
                    else:
                        print("⚠ Recalibration skipped (insufficient data)")
                    last_recalibration_time = now
                except Exception as e:
                    logger.warning(f"Recalibration failed: {e}")
                    print(f"⚠ Recalibration error: {e}")
                    last_recalibration_time = now  # Don't retry immediately

            # Re-read interval from settings.json (live reload — no restart needed)
            settings = load_settings()
            new_interval = settings.get('data_collection', {}).get('heating_data_interval_minutes', 5)
            if new_interval != interval_minutes:
                print(f"⚙ Poll interval changed: {interval_minutes} → {new_interval} min")
                interval_minutes = new_interval

            # Wait until next scheduled interval (fixed schedule, not relative)
            now = datetime.now(timezone.utc)
            # Calculate seconds until next interval boundary + per-house offset
            # The offset staggers writes so houses don't all hit InfluxDB at once
            minutes_past = now.minute % interval_minutes
            seconds_past = minutes_past * 60 + now.second + now.microsecond / 1_000_000
            sleep_seconds = (interval_minutes * 60) - seconds_past + poll_offset

            # If we're very close to the boundary (or past it due to offset), wait for the next one
            if sleep_seconds < 10:
                sleep_seconds += interval_minutes * 60

            next_run = now + timedelta(seconds=sleep_seconds)
            print(f"Next collection at {next_run.strftime('%H:%M:%S')} UTC ({sleep_seconds/60:.1f} min)...")
            if debug_mode:
                logger.info(f"Sleeping {sleep_seconds:.0f}s until next scheduled collection")
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
        print("\n\nMonitoring stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error ({type(e).__name__}): {str(e)}")
        print(f"Unexpected error: {e}")
    finally:
        # Exit ML curve control gracefully (restore original Yref)
        if ml_curve_control and customer_profile and customer_profile.heat_curve_control.in_control:
            print("Restoring original Yref before shutdown...")
            ml_curve_control.exit_ml_control(reason="shutdown")

        # Cleanup resources
        api.cleanup()


if __name__ == "__main__":
    # Load configuration from environment variables
    config = {
        'session_token': os.getenv('HOMESIDE_SESSION_TOKEN'),
        'username': os.getenv('HOMESIDE_USERNAME'),
        'password': os.getenv('HOMESIDE_PASSWORD'),
        'clientid': os.getenv('HOMESIDE_CLIENTID'),
        'friendly_name': os.getenv('FRIENDLY_NAME'),
        'display_name_source': os.getenv('DISPLAY_NAME_SOURCE', 'friendly_name'),
        'interval_minutes': int(os.getenv('POLL_INTERVAL_MINUTES', '5')),
        'seq_url': os.getenv('SEQ_URL'),
        'seq_api_key': os.getenv('SEQ_API_KEY'),
        'log_level': os.getenv('LOG_LEVEL', 'INFO'),
        'debug_mode': os.getenv('DEBUG_MODE', 'false').lower() == 'true',
        'influxdb_enabled': os.getenv('INFLUXDB_ENABLED', 'false').lower() == 'true',
        'influxdb_url': os.getenv('INFLUXDB_URL'),
        'influxdb_token': os.getenv('INFLUXDB_TOKEN'),
        'influxdb_org': os.getenv('INFLUXDB_ORG'),
        'influxdb_bucket': os.getenv('INFLUXDB_BUCKET'),
        'latitude': float(os.getenv('LATITUDE')) if os.getenv('LATITUDE') else None,
        'longitude': float(os.getenv('LONGITUDE')) if os.getenv('LONGITUDE') else None,
        'heat_curve_enabled': os.getenv('HEAT_CURVE_ENABLED', 'false').lower() == 'true',
        'poll_offset_seconds': int(os.getenv('POLL_OFFSET_SECONDS', '0')),
    }

    # Validate required config
    has_session_token = bool(config['session_token'])
    has_credentials = bool(config['username'] and config['password'])

    if not has_session_token and not has_credentials:
        print("ERROR: Either HOMESIDE_SESSION_TOKEN or HOMESIDE_USERNAME/PASSWORD must be set")
        print("")
        print("Option 1 - Use session token (recommended):")
        print("  Get it from browser: DevTools -> Application -> Local Storage -> currentToken")
        print("  Set: HOMESIDE_SESSION_TOKEN=your_token_here")
        print("")
        print("Option 2 - Use Selenium automation:")
        print("  Set: HOMESIDE_USERNAME=your_username")
        print("  Set: HOMESIDE_PASSWORD=your_password")
        exit(1)

    # Client ID is now auto-discovered, no need to require it
    if config['clientid']:
        print(f"✓ Using client ID from config: {config['clientid']}")
    else:
        print("✓ Client ID will be auto-discovered after login")

    monitor_heating_system(config)

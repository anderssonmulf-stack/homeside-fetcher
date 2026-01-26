import time
import os
import json
import logging
from datetime import datetime, timezone

from homeside_api import HomeSideAPI
from smhi_weather import SMHIWeather
from influx_writer import InfluxDBWriter
from thermal_analyzer import ThermalAnalyzer
from heat_curve_controller import HeatCurveController
from seq_logger import SeqLogger
from customer_profile import CustomerProfile, find_profile_for_client_id
from temperature_forecaster import TemperatureForecaster


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
    logger
) -> list:
    """
    Generate forecast points for visualization in Grafana.

    Creates hourly forecast data for the next 12 hours:
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

    hourly_forecasts = weather.get_forecast(hours_ahead=12)
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
        logger.info(f"Generated {len(forecast_points)} forecast points for next 12h")

    return forecast_points


def monitor_heating_system(config):
    """Main monitoring function"""

    # Setup standard Python logging
    logging.basicConfig(
        level=config.get('log_level', 'INFO'),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()

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
        print(f"  - Forecast: every {forecast_interval_minutes} min")
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
            house_id=api.clientid,  # Use auto-discovered client ID
            logger=logger,
            enabled=True
        )
        print(f"✓ InfluxDB enabled: {config.get('influxdb_url')}")
    else:
        print("⚠ InfluxDB disabled (INFLUXDB_ENABLED=false)")

    # Initialize thermal analyzer (with InfluxDB for persistence if available)
    thermal = ThermalAnalyzer(logger, min_samples=24, influx=influx)
    if influx and thermal.historical_data:
        print(f"✓ Thermal analyzer initialized ({len(thermal.historical_data)} historical points loaded)")
    else:
        print("✓ Thermal analyzer initialized")

    # Load customer profile and initialize forecaster
    customer_profile = None
    forecaster = None
    try:
        customer_profile = find_profile_for_client_id(api.clientid, profiles_dir="profiles")
        if customer_profile:
            forecaster = TemperatureForecaster(customer_profile)
            print(f"✓ Customer profile loaded: {customer_profile.friendly_name}")
            status = customer_profile.get_status()
            print(f"  - Target temp: {status['target_temp']}°C")
            print(f"  - Learning: {status['learning_status']}")
        else:
            print("⚠ No customer profile found, using legacy forecaster")
    except Exception as e:
        logger.warning(f"Failed to load customer profile: {e}")
        print(f"⚠ Customer profile error: {e}, using legacy forecaster")

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

    interval_minutes = config['interval_minutes']
    print(f"Starting monitoring loop (interval: {interval_minutes} minutes)")
    print("Press Ctrl+C to stop\n")

    # Track forecast timing (fetch every forecast_interval_minutes)
    last_forecast_time = None
    # Cache for forecast data between updates
    cached_forecast_trend = None
    cached_recommendation = None

    # Track last prediction for accuracy measurement
    last_indoor_prediction = None  # (timestamp, predicted_value, outdoor_temp)

    iteration = 0
    try:
        while True:
            iteration += 1
            now = datetime.now(timezone.utc)
            print(f"\n--- Data Collection #{iteration} ---")
            if debug_mode:
                logger.info(f"Starting data collection #{iteration}")

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

                    # Calculate expected supply temps from heat curve based on outdoor temp
                    # - supply_temp_heat_curve: From baseline (original) curve
                    # - supply_temp_heat_curve_ml: From current (possibly ML-adjusted) curve
                    if heat_curve and 'outdoor_temperature' in extracted_data:
                        baseline_supply, current_supply = heat_curve.get_supply_temps_for_outdoor(
                            extracted_data['outdoor_temperature']
                        )
                        if baseline_supply is not None:
                            extracted_data['supply_temp_heat_curve'] = round(baseline_supply, 2)
                        if current_supply is not None:
                            extracted_data['supply_temp_heat_curve_ml'] = round(current_supply, 2)

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

                    # Write to InfluxDB
                    if influx:
                        influx.write_heating_data(extracted_data)

                    # =====================================================
                    # WEATHER: Current observations (every iteration)
                    # =====================================================
                    weather_obs = None
                    if weather and observation_enabled:
                        weather_obs = weather.get_current_weather()
                        if weather_obs and weather_obs.temperature is not None:
                            # Write observation to InfluxDB
                            if influx:
                                influx.write_weather_observation({
                                    'station_name': weather_obs.station.name,
                                    'station_id': weather_obs.station.id,
                                    'distance_km': weather_obs.station.distance_km,
                                    'temperature': weather_obs.temperature,
                                    'wind_speed': weather_obs.wind_speed,
                                    'humidity': weather_obs.humidity,
                                    'timestamp': weather_obs.timestamp
                                })
                            print(f"\n🌡️ Current Weather: {weather_obs.temperature:.1f}°C (from {weather_obs.station.name})")

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
                        forecast_trend = weather.get_temp_trend(hours_ahead=12)
                        if forecast_trend:
                            last_forecast_time = now
                            cached_forecast_trend = forecast_trend

                            # Write forecast summary to InfluxDB
                            if influx:
                                influx.write_forecast_data(forecast_trend)

                                # Generate and write detailed forecast points for Grafana
                                influx.delete_old_forecasts()

                                # Use new forecaster if available, otherwise legacy
                                if forecaster:
                                    # Get hourly weather forecast
                                    hourly_forecast = weather.get_forecast(hours_ahead=12)
                                    if hourly_forecast:
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
                                    forecast_points = generate_forecast_points(
                                        weather=weather,
                                        heat_curve=heat_curve,
                                        thermal=thermal,
                                        current_indoor=extracted_data.get('room_temperature', 22.0),
                                        current_outdoor=extracted_data.get('outdoor_temperature', 0.0),
                                        logger=logger
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
                        print(f"\n📊 Weather Forecast (12h):")
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
                            print(f"  Affected points: {curve_recommendation['affected_indices']}")
                            print(f"  Duration: {curve_recommendation['duration_hours']:.0f} hours")
                            print(f"  Reason: {curve_recommendation['reason']}")
                            print(f"  Confidence: {curve_recommendation['confidence']:.0%}")

                            # Only apply changes if enabled
                            if heat_curve_enabled and not heat_curve.adjustment_active:
                                current_curve = heat_curve.read_current_curve()
                                if current_curve:
                                    heat_curve.enter_reduction_mode(
                                        current_curve=current_curve,
                                        affected_indices=curve_recommendation['affected_indices'],
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

            # Wait for next interval
            print(f"Next collection in {interval_minutes} minutes...")
            if debug_mode:
                logger.info(f"Waiting {interval_minutes} minutes for next collection")
            time.sleep(interval_minutes * 60)

    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
        print("\n\nMonitoring stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error ({type(e).__name__}): {str(e)}")
        print(f"Unexpected error: {e}")
    finally:
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
        'interval_minutes': int(os.getenv('POLL_INTERVAL_MINUTES', '15')),
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

import time
import os
import json
import logging
from datetime import datetime, timezone
import seqlog

from homeside_api import HomeSideAPI, send_to_seq_direct
from smhi_weather import SMHIWeather
from influx_writer import InfluxDBWriter
from thermal_analyzer import ThermalAnalyzer
from heat_curve_controller import HeatCurveController


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


def monitor_heating_system(config):
    """Main monitoring function"""

    # Setup logging
    if config.get('seq_url'):
        seqlog.log_to_seq(
            server_url=config['seq_url'],
            api_key=config.get('seq_api_key'),
            level=config.get('log_level', 'INFO'),
            batch_size=10,
            auto_flush_timeout=2,
            override_root_logger=True
        )
        print(f"Seq logging enabled: {config['seq_url']}")
    else:
        logging.basicConfig(
            level=config.get('log_level', 'INFO'),
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        print("Seq logging disabled (no SEQ_URL configured)")

    logger = logging.getLogger()
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
        debug_mode=debug_mode
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

    # Initialize thermal analyzer
    thermal = ThermalAnalyzer(logger, min_samples=24)
    print("✓ Thermal analyzer initialized")

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

                    # Add data to thermal analyzer for learning
                    thermal.add_data_point(extracted_data)

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

                            # Write forecast to InfluxDB
                            if influx:
                                influx.write_forecast_data(forecast_trend)

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

                    # Send consolidated data to Seq with structured properties
                    seq_properties = {
                        'EventType': 'DataCollected',
                        'Iteration': iteration,
                        'VariableCount': len(extracted_data) - 1,  # Exclude timestamp
                        'SessionTokenLast8': api.session_token[-8:] if api.session_token else 'N/A',
                        'SessionTokenUpdatedAt': api.session_token_updated_at,
                        'SessionTokenSource': api.session_token_source
                    }

                    # Add all heating values as properties (rounded to 2 decimals)
                    for key, value in extracted_data.items():
                        if key != 'timestamp':
                            property_name = ''.join(word.capitalize() for word in key.split('_'))
                            # Round numeric values to 2 decimals for Seq
                            if isinstance(value, (int, float)):
                                seq_properties[property_name] = round(float(value), 2)
                            else:
                                seq_properties[property_name] = value

                    # Add weather forecast data (rounded to 2 decimals)
                    if forecast_trend:
                        seq_properties['ForecastTrend'] = forecast_trend['trend']
                        seq_properties['ForecastTrendSymbol'] = forecast_trend['trend_symbol']
                        seq_properties['ForecastChange'] = round(float(forecast_trend['change']), 2)
                        seq_properties['ForecastCurrentTemp'] = round(float(forecast_trend['current_temp']), 2)
                        seq_properties['ForecastAvgTemp'] = round(float(forecast_trend['avg_temp']), 2)
                        seq_properties['ForecastCloudCondition'] = forecast_trend['cloud_condition']
                        if forecast_trend.get('avg_cloud_cover') is not None:
                            seq_properties['ForecastCloudCover'] = round(float(forecast_trend['avg_cloud_cover']), 2)

                    # Add heating recommendation (rounded to 2 decimals)
                    if recommendation:
                        seq_properties['HeatingAction'] = 'reduce' if recommendation['reduce_heating'] else 'maintain'
                        seq_properties['HeatingConfidence'] = round(float(recommendation['confidence']), 2)
                        seq_properties['HeatingReason'] = recommendation['reason']
                        seq_properties['SolarFactor'] = recommendation.get('solar_factor', 'unknown')

                    # Add heat curve recommendation (rounded to 2 decimals)
                    if curve_recommendation:
                        seq_properties['CurveReduce'] = curve_recommendation['reduce']
                        seq_properties['CurveDelta'] = round(float(curve_recommendation['delta']), 2)
                        seq_properties['CurveDuration'] = round(float(curve_recommendation['duration_hours']), 1)
                        seq_properties['CurveConfidence'] = round(float(curve_recommendation['confidence']), 2)
                        seq_properties['CurveReason'] = curve_recommendation['reason']
                        seq_properties['CurveAffectedPoints'] = ','.join(map(str, curve_recommendation['affected_indices']))
                        seq_properties['CurveModeActive'] = heat_curve_enabled
                        if heat_curve:
                            seq_properties['CurveAdjustmentActive'] = heat_curve.adjustment_active

                    # Create consolidated message
                    msg_parts = [f"Iteration #{iteration}"]
                    if forecast_trend:
                        msg_parts.append(f"{forecast_trend['trend_symbol']} {forecast_trend['change']:+.2f}°C")
                    if recommendation:
                        if recommendation['reduce_heating']:
                            # Only show confidence when recommending to reduce heating
                            msg_parts.append(f"🔽 {recommendation['confidence']:.0%}")
                        else:
                            msg_parts.append("➡️")
                    if curve_recommendation and curve_recommendation['reduce']:
                        mode_icon = "📉" if heat_curve_enabled else "📊"
                        msg_parts.append(f"{mode_icon} {curve_recommendation['delta']:+.1f}°C")

                    send_to_seq_direct(
                        f"🌡️ {' | '.join(msg_parts)}",
                        level='Information',
                        properties=seq_properties
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

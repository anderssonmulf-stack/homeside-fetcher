# Homeside Fetcher

A Python application that fetches heating system data from HomeSide district heating API, analyzes thermal dynamics, and integrates with weather forecasts to optimize heating control.

## Project Structure

### Core Application Files

| File | Purpose |
|------|---------|
| `HSF_Fetcher.py` | Main application entry point. Runs the monitoring loop, coordinates all modules, handles data collection every 15 minutes. |
| `homeside_api.py` | HomeSide API client. Handles authentication (session token, BMS token), fetches heating variables, and can write values back to the system. |
| `thermal_analyzer.py` | Learns building thermal dynamics over time. Calculates thermal coefficient (how the building responds to outdoor temp changes). Requires 24 data points (6 hours) minimum. |
| `heat_curve_controller.py` | Manages dynamic heat curve adjustments based on weather forecasts. Can reduce supply temperatures when outdoor temps are rising. |
| `smhi_weather.py` | Unified SMHI weather client for observations (nearest station) and forecasts (PMP3G API). Used for heating decisions. |
| `weather_forecast.py` | Legacy weather forecast module (replaced by smhi_weather.py but kept for reference). |
| `influx_writer.py` | InfluxDB client for storing time-series data (heating metrics, forecasts, thermal coefficients, heat curve baselines). |
| `seq_logger.py` | Centralized Seq structured logging with automatic client_id tagging for multi-site deployments. |
| `customer_profile.py` | Manages customer-specific settings and learned parameters. Each customer has a JSON profile in `profiles/`. |
| `temperature_forecaster.py` | Model C hybrid forecaster: combines physics-based prediction with historical learning for accurate temperature forecasts. |

### Configuration Files

| File | Purpose |
|------|---------|
| `settings.json` | Application settings (weather intervals, target indoor temp, margins) |
| `variables_config.json` | Maps HomeSide API variable names to friendly field names (room_temperature, outdoor_temperature, etc.) |
| `.env` | Environment configuration (credentials, API tokens, InfluxDB settings) - gitignored |
| `.env.example` | Template for .env file |
| `profiles/*.json` | Customer-specific profiles with settings and learned parameters |

### Docker Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Container image definition |
| `docker-compose.yml` | Container orchestration config |

### Grafana Dashboards

Located in `grafana/dashboards/`:

| File | Purpose |
|------|---------|
| `homeside-mobile.json` | Mobile-friendly dashboard with stat panels and forecast graphs |
| `homeside-admin.json` | Admin dashboard with multi-user support (house_id variable) and detailed analytics |

Provisioning configs in `grafana/provisioning/`:
- `dashboards/default.yaml` - Auto-loads dashboards from `/var/lib/grafana/dashboards`
- `datasources/influxdb.yaml` - Pre-configures InfluxDB connection

### Debug/Development Tools

| File | Purpose |
|------|---------|
| `debug_variables.py` | Debug tool for inspecting API variables |
| `dump_variables.py` | Dumps all variables from the API |
| `find_all_variables.py` | Discovers available variables in the system |

### Migration Scripts

| File | Purpose |
|------|---------|
| `migrate_seq_to_influx.py` | One-time migration: reads thermal data from Seq logs and writes to InfluxDB `thermal_history` measurement |

## External Scripts

### GitHub Commit Script
Located in `/opt/dev` - script for committing changes to GitHub.

### Backup Scripts
Located in `/backup_scripts`:
- `backup_to_nas_host.sh` - Backs up the project files to NAS
- `backup_docker_system.sh` - Backs up Docker containers and volumes to NAS

## Data Flow

1. `HSF_Fetcher.py` polls HomeSide API every 15 minutes via `homeside_api.py`
2. Extracted heating data is passed to `thermal_analyzer.py` for learning
3. Weather forecasts are fetched via `smhi_weather.py`
4. `heat_curve_controller.py` evaluates if heating should be reduced
5. All data is written to InfluxDB via `influx_writer.py`
6. Logs are sent to Seq for monitoring

## Key Variables Tracked

From `variables_config.json`:
- `room_temperature` - Average indoor temperature
- `outdoor_temperature` - Current outdoor temp
- `outdoor_temp_24h_avg` - 24-hour outdoor average
- `supply_temp` - Heating supply water temperature
- `return_temp` - Heating return water temperature
- `hot_water_temp` - Domestic hot water temperature
- `system_pressure` - Heating system pressure
- `electric_heater` - Electric backup heater status
- `heat_recovery` - Heat recovery system status
- `away_mode` - Forced absence mode
- `target_temp_setpoint` - User's target temperature
- `away_temp_setpoint` - Temperature when away

### Calculated Variables

- `supply_temp_heat_curve` - Expected supply temp from the **baseline** (original) heat curve. This is the curve before any ML adjustments.
- `supply_temp_heat_curve_ml` - Expected supply temp from the **current active** heat curve. When in reduction mode, this will be lower than the baseline.

Both are calculated by `HeatCurveController.get_supply_temps_for_outdoor()` using linear interpolation.

During normal operation: both values are the same.
During ML reduction mode: `supply_temp_heat_curve_ml` < `supply_temp_heat_curve`

The baseline curve is stored in InfluxDB (`heat_curve_baseline` measurement) when entering reduction mode, and restored when exiting.

## Seq Logging Architecture

Structured logging to Seq is handled by `seq_logger.py` which provides:

- **Automatic site tagging** - All log events include `ClientId`, `FriendlyName`, `Username`, and `DisplayName` for filtering multi-site deployments
- **Configurable display name** - Choose which identifier appears in log messages via `DISPLAY_NAME_SOURCE` env var:
  - `friendly_name` (default) - Human-readable name like "Daggis8"
  - `client_id` - Full or short client ID like "HEM_FJV_Villa_149"
  - `username` - HomeSide username
- **Consolidated data collection logs** - Single log event per collection with all heating data, forecast, recommendations, and thermal analysis
- **Concise message format** - `[Daggis8] #1 | 🏠22.7°C | 🌡️-0.4°C | ↓-2.8°C | ➡️`

Environment variables:
```bash
FRIENDLY_NAME=Daggis8                    # Human-readable site name
DISPLAY_NAME_SOURCE=friendly_name        # Which name in log messages
```

Usage in code:
```python
from seq_logger import SeqLogger

seq_logger = SeqLogger(
    client_id="38/Account/HEM_FJV_149/...",
    friendly_name="Daggis8",
    username="FC 2000232581",
    display_name_source='friendly_name'
)
seq_logger.log_data_collection(iteration=1, heating_data={...}, forecast={...})
seq_logger.log_error("Something went wrong", error=exception)
```

## Thermal Data Persistence

The thermal analyzer now persists data to InfluxDB (`thermal_history` measurement) and restores it on startup. This solves the "Insufficient data for thermal coefficient" problem after restarts.

### How it works

1. **On data collection**: Each data point is written to InfluxDB via `influx_writer.py:write_thermal_data_point()`
2. **On startup**: Historical data (last 7 days) is loaded via `influx_writer.py:read_thermal_history()`
3. **Result**: Thermal coefficient calculation works immediately after restart

### One-time migration from Seq logs

If you have historical data in Seq but not yet in InfluxDB, run the migration script:

```bash
# First, do a dry run to see what would be migrated
python migrate_seq_to_influx.py --seq-url http://seq:80 --dry-run

# Then run the actual migration
python migrate_seq_to_influx.py --seq-url http://seq:80

# Optionally specify days to look back
python migrate_seq_to_influx.py --seq-url http://seq:80 --days 14
```

Note: Use `--seq-url http://seq:80` because Seq's events API is on port 80 (web interface), not port 5341 (ingestion).

This reads `EventType='DataCollected'` events from Seq and writes them to the `thermal_history` measurement in InfluxDB.

**Migration completed 2026-01-24**: 629 data points migrated from Seq to InfluxDB.

## Customer Profiles

Each customer has a JSON profile in `profiles/` containing all customer-specific settings and learned parameters. This centralizes configuration for maintainability and future GUI integration.

### Profile Structure

```json
{
  "schema_version": 1,
  "customer_id": "HEM_FJV_Villa_149",
  "friendly_name": "Daggis8",

  "building": {
    "description": "Well-insulated 1990s villa",
    "thermal_response": "medium"  // slow, medium, fast
  },

  "comfort": {
    "target_indoor_temp": 22.0,
    "acceptable_deviation": 1.0
  },

  "heating_system": {
    "response_time_minutes": 30,
    "max_supply_temp": 55
  },

  "learned": {
    "thermal_coefficient": 0.000084,
    "thermal_coefficient_confidence": 0.90,
    "hourly_bias": {"06": -0.2, "12": 0.0, "18": 0.1},
    "samples_since_last_update": 0,
    "total_samples": 500,
    "next_update_at_samples": 96,
    "updated_at": "2026-01-25T18:00:00Z"
  }
}
```

### Key Fields

| Section | Field | Purpose |
|---------|-------|---------|
| `building` | `thermal_response` | How quickly the building responds to heating (affects forecast rates) |
| `comfort` | `target_indoor_temp` | Thermostat setpoint - forecaster won't predict above this |
| `comfort` | `acceptable_deviation` | How much variation from target is acceptable |
| `learned` | `thermal_coefficient` | Auto-learned from thermal analyzer |
| `learned` | `hourly_bias` | Systematic prediction errors by hour (auto-learned) |

### Usage

```python
from customer_profile import CustomerProfile, find_profile_for_client_id

# Load by client ID (extracts customer_id from HomeSide path)
profile = find_profile_for_client_id("38/xxx/HEM_FJV_149/HEM_FJV_Villa_149")

# Access settings
print(profile.comfort.target_indoor_temp)  # 22.0

# Update learned parameters
profile.update_learned_params(thermal_coefficient=0.00009, confidence=0.92)
profile.save()
```

## Temperature Forecaster (Model C)

The forecaster uses a hybrid approach combining physics-based prediction with historical learning.

### How It Works

1. **Physics Model (Thermostat-Aware)**
   - If indoor < target: heating active, temp rises toward target
   - If indoor >= target: thermostat maintains, temp stabilizes
   - Rate limited by building's thermal response

2. **Historical Adjustment**
   - Tracks prediction errors by hour of day
   - Learns systematic biases (e.g., "mornings typically 0.2°C lower than predicted")
   - Applies corrections weighted by confidence

3. **Learning Schedule**
   - First update: after 24 samples (~6 hours)
   - Second update: after 48 samples (~12 hours)
   - Then: every 96 samples (~24 hours / daily)

### Key Insight

The thermostat is the dominant factor. Physics determines *how fast* temperature changes, but the setpoint determines *where it ends up*. This prevents unrealistic forecasts like "indoor will rise to 25°C" when the thermostat is set to 22°C.

### Forecast Types Generated

| Type | Description |
|------|-------------|
| `outdoor_temp` | From SMHI weather forecast |
| `indoor_temp` | Model C prediction (physics + learning) |
| `supply_temp_baseline` | Expected supply temp from original heat curve |
| `supply_temp_ml` | Expected supply temp from ML-adjusted curve |

### Explainability (GUI-Ready)

Each indoor forecast includes an explanation:

```python
{
  "value": 21.8,
  "explanation": {
    "physics_base": 22.1,
    "physics_reasoning": "Below target, heating active",
    "hourly_adjustment": -0.3,
    "adjustment_reasoning": "Historical data shows -0.3°C bias at 08:00",
    "confidence": 0.85,
    "factors": [
      {"name": "Target setpoint", "value": 22.0, "impact": "high"},
      {"name": "Outdoor forecast", "value": -3.0, "impact": "medium"}
    ]
  }
}
```

## InfluxDB Measurements

### Core Measurements

| Measurement | Purpose |
|-------------|---------|
| `heating_system` | All heating variables (room_temp, supply_temp, etc.) |
| `weather_observation` | SMHI weather station readings |
| `weather_forecast` | SMHI forecast summary |
| `thermal_history` | Historical data for thermal analyzer persistence |
| `heat_curve_baseline` | Stored heat curve for restoration |

### Forecast Measurements

| Measurement | Purpose |
|-------------|---------|
| `temperature_forecast` | Future temperature predictions (12h ahead) |
| `forecast_accuracy` | Predicted vs actual comparisons for learning |
| `learned_parameters` | History of learned params (for tracking adaptation) |

### Querying Forecasts

```flux
// Get indoor temperature forecasts for next 12h
from(bucket: "heating")
  |> range(start: now(), stop: now() + 12h)
  |> filter(fn: (r) => r._measurement == "temperature_forecast")
  |> filter(fn: (r) => r.forecast_type == "indoor_temp")

// Check forecast accuracy
from(bucket: "heating")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "forecast_accuracy")
  |> filter(fn: (r) => r._field == "error")
```

## Grafana Dashboards

### Mobile Dashboard (`homeside-mobile.json`)

Optimized for phone viewing with large stat panels:
- Room, Target, Outdoor, Supply, Hot Water, Pressure stats
- Temperature History & Forecast (24h past + 12h future)
- Supply Temperature Forecast
- Outdoor Temperature Sources (HomeSide vs SMHI)

### Admin Dashboard (`homeside-admin.json`)

Multi-user dashboard with `house_id` variable selector:
- All stat panels from mobile
- Room vs Outdoor Temperature & Forecast
- Supply & Return Temperatures & Forecast
- Supply Temp vs Heat Curve Deviation
- Supply - Return Delta
- Hot Water Temperature
- System Pressure

### Forecast Visualization

- **Historical data**: Solid lines
- **Forecast data**: Dashed lines (same colors as historical)
- **Time range**: Default `now-24h` to `now+12h`

### Dashboard Provisioning

Dashboards are auto-loaded via Grafana provisioning:

```yaml
# docker-compose.yml
volumes:
  - ./grafana/provisioning:/etc/grafana/provisioning
  - ./grafana/dashboards:/var/lib/grafana/dashboards
```

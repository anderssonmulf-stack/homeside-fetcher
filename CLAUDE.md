# BVPro

A Python application that monitors heating systems for residential houses (via HomeSide API) and commercial buildings (via Arrigo BMS API), analyzes thermal dynamics, and integrates with weather forecasts to optimize heating control.

## Architecture Overview

| | Houses (Residential) | Buildings (Commercial) |
|---|---|---|
| **API** | HomeSide district heating | Arrigo BMS (GraphQL) |
| **Fetcher** | `HSF_Fetcher.py` | `building_fetcher.py` |
| **Config** | `profiles/*.json` (CustomerProfile) | `buildings/*.json` (plain JSON) |
| **Deployment** | Docker containers via `docker-compose.yml` | Subprocesses via orchestrator |
| **Credentials** | Per-container env vars | `.env`: `BUILDING_<id>_USERNAME/PASSWORD` |
| **InfluxDB tag** | `house_id` | `building_id` |
| **Measurement** | `heating_system` | `building_system` |
| **Onboard** | `add_customer.py` | `add_building.py` |
| **Offboard** | `remove_customer.py` | `remove_customer.py --type building` |

Shared: energy separation (`heating_energy_calibrator.py`), k-value recalibration (`k_recalibrator.py`), bootstrap (`gap_filler.py`), Dropbox energy import, InfluxDB, Seq logging, web GUI.

## Project Structure

### Core Files

| File | Purpose |
|------|---------|
| `HSF_Fetcher.py` | House fetcher: polls HomeSide API every 5 min, coordinates all modules |
| `building_fetcher.py` | Building fetcher: polls Arrigo BMS API, writes signals to InfluxDB |
| `arrigo_api.py` | Arrigo BMS API client (auth, signal discovery, reads) |
| `homeside_api.py` | HomeSide API client (auth, fetch/write heating variables) |
| `thermal_analyzer.py` | Learns building thermal dynamics (k-coefficient). Needs 24 data points min |
| `heat_curve_controller.py` | Dynamic heat curve adjustments based on weather forecasts |
| `smhi_weather.py` | SMHI weather client: observations (nearest station) + forecasts (PMP3G) |
| `influx_writer.py` | InfluxDB writer for all time-series data |
| `seq_logger.py` | Seq structured logging with auto client_id tagging |
| `customer_profile.py` | Customer settings and learned parameters (`profiles/*.json`) |
| `temperature_forecaster.py` | Model C hybrid forecaster (physics + historical learning) |
| `dropbox_client.py` | Dropbox OAuth client with token refresh |
| `dropbox_sync.py` | Syncs meter request file to Dropbox |
| `energy_importer.py` | Imports energy data from Dropbox into InfluxDB |

### Configuration

| File | Purpose |
|------|---------|
| `settings.json` | App settings (weather intervals, target temp, margins) |
| `variables_config.json` | HomeSide API variable name → field name mapping |
| `.env` / `.env.example` | Credentials, API tokens, InfluxDB settings |
| `profiles/*.json` | House-specific profiles with learned parameters |
| `buildings/*.json` | Building configs with Arrigo connection + signal mapping |
| `offboarded.json` | Soft-offboarded entities pending InfluxDB purge |

### Admin Scripts

| File | Purpose |
|------|---------|
| `add_customer.py` | Add houses (creates profile, updates docker-compose, deploys) |
| `add_building.py` | Add buildings (discovers Arrigo signals, creates config) |
| `remove_customer.py` | Remove houses/buildings: hard or soft (30-day grace) |
| `gap_filler.py` | Unified 7-phase bootstrap for houses and buildings |
| `import_historical_data.py` | Legacy house bootstrap (superseded by gap_filler) |

## Data Flow

**Houses:** `HSF_Fetcher` → HomeSide API → `thermal_analyzer` + `smhi_weather` → `heat_curve_controller` → InfluxDB + Seq

**Buildings:** `building_fetcher` → Arrigo API → InfluxDB + Seq. Daily energy separation at 08:00, k-recalibration every 72h.

## Adding Houses / Buildings

Use the automated scripts. Manual steps are in `docs/ONBOARDING.md`.

```bash
# Add house (interactive)
python3 add_customer.py

# Add house (non-interactive)
python3 add_customer.py --non-interactive --username FC2000233091 --password "pass" --name "House Name" --lat 56.67 --lon 12.86

# Add building (interactive)
python3 add_building.py

# Add building (non-interactive)
python3 add_building.py --non-interactive --host exodrift05.systeminstallation.se --username "user" --password "pass" --name "Building" --building-id SITE_Name --lat 56.67 --lon 12.86 --bootstrap
```

## Removing Houses / Buildings

See `docs/OFFBOARDING.md` for full details.

```bash
python3 remove_customer.py HEM_FJV_Villa_99                          # hard remove house
python3 remove_customer.py TE236_HEM_Kontor --type building          # hard remove building
python3 remove_customer.py HEM_FJV_Villa_99 --soft                   # soft offboard (30-day grace)
python3 remove_customer.py HEM_FJV_Villa_99 --soft --days 60         # custom grace period
```

## Bootstrap Pipeline

7-phase pipeline in `gap_filler.py`: Arrigo data → weather → energy → sanity checks → InfluxDB → calibration → backtest. See `docs/BOOTSTRAP.md`.

```bash
python3 gap_filler.py --bootstrap --house-id TE236_HEM_Kontor --days 90
python3 gap_filler.py --bootstrap --house-id TE236_HEM_Kontor --days 90 --dry-run
```

## Dropbox Energy Exchange

Automated energy data exchange with the energy company via Dropbox. See `docs/DROPBOX_ENERGY.md`.

```bash
python3 dropbox_sync.py          # sync meter requests
python3 energy_importer.py       # import energy data
```

## Backup & Commit Scripts

All scripts in `backup_scripts/` use `backup_include.conf` as single source of truth.

```bash
bash backup_scripts/backup_to_nas_host.sh        # codebase backup
bash backup_scripts/backup_docker_system.sh       # full Docker backup
bash backup_scripts/git_commit.sh "message"       # git commit
bash backup_scripts/git_commit.sh -p "message"    # commit + push
```

Edit `backup_scripts/backup_include.conf` to modify what gets backed up/committed.

## Key Variables

From `variables_config.json`: `room_temperature`, `outdoor_temperature`, `outdoor_temp_24h_avg`, `supply_temp`, `return_temp`, `hot_water_temp`, `system_pressure`, `electric_heater`, `heat_recovery`, `away_mode`, `target_temp_setpoint`, `away_temp_setpoint`.

Calculated: `supply_temp_heat_curve` (baseline curve), `supply_temp_heat_curve_ml` (ML-adjusted curve). Both from `HeatCurveController.get_supply_temps_for_outdoor()`. During ML reduction: `_ml` < baseline.

## Thermal Data Persistence

Thermal analyzer persists to InfluxDB (`thermal_history` measurement) and restores on startup:
- Write: `influx_writer.py:write_thermal_data_point()`
- Read: `influx_writer.py:read_thermal_history()` (last 7 days on startup)

## Customer Profiles

Each house has a JSON profile in `profiles/`. Key fields:

| Section | Field | Purpose |
|---------|-------|---------|
| `building.thermal_response` | `slow`/`medium`/`fast` | Building response speed |
| `comfort.target_indoor_temp` | float | Thermostat setpoint |
| `comfort.acceptable_deviation` | float | Acceptable temp variation |
| `learned.thermal_coefficient` | float | Auto-learned from thermal analyzer |
| `learned.hourly_bias` | dict | Prediction errors by hour (auto-learned) |

```python
from customer_profile import CustomerProfile, find_profile_for_client_id
profile = find_profile_for_client_id("38/xxx/HEM_FJV_149/HEM_FJV_Villa_149")
profile.update_learned_params(thermal_coefficient=0.00009, confidence=0.92)
profile.save()
```

## Temperature Forecaster (Model C)

Hybrid physics + learning approach:
1. **Physics:** If indoor < target → heating active, temp rises toward target. If >= target → thermostat maintains. Rate limited by `thermal_response`.
2. **Learning:** Tracks prediction errors by hour, applies corrections weighted by confidence.
3. **Schedule:** First update at 24 samples (~6h), then 48 (~12h), then every 96 (~24h).

Key insight: thermostat setpoint is the dominant factor. Physics determines *speed*, setpoint determines *destination*.

Forecast types: `outdoor_temp` (SMHI), `indoor_temp` (Model C), `supply_temp_baseline`, `supply_temp_ml`.

## InfluxDB Measurements

| Measurement | Purpose |
|-------------|---------|
| `heating_system` | House heating variables — tagged by `house_id` |
| `building_system` | Building BMS signals — tagged by `building_id` |
| `weather_observation` | SMHI weather station readings |
| `weather_forecast` | SMHI forecast summary |
| `thermal_history` | Thermal analyzer persistence data |
| `heat_curve_baseline` | Stored heat curve for restoration |
| `temperature_forecast` | 24h predictions with `lead_time_hours` |
| `forecast_accuracy` | Predicted vs actual comparisons |
| `learned_parameters` | History of learned params |

## Web Infrastructure

Public at **bvpro.hem.se**. Security: GeoIP (Sweden only) → rate limiting → fail2ban → HTTPS → Flask/Gunicorn. See `docs/WEB_INFRASTRUCTURE.md`.

## Settings GUI (webgui/)

Flask app for customer self-service and admin management.

### Key Files

| File | Purpose |
|------|---------|
| `app.py` | Flask routes: dashboard, house settings, admin |
| `auth.py` | Authentication, bcrypt, role-based access |
| `influx_reader.py` | InfluxDB queries for real-time data |
| `email_service.py` | SMTP notifications (send.one.com:587) |

### User Roles

| Role | Capabilities |
|------|--------------|
| `admin` | Full access, approve users, manage all houses |
| `user` | View and edit assigned houses |
| `viewer` | View-only access to assigned houses |
| `pending` | Awaiting admin approval |

### Plotly Chart Pattern

All charts in `house_graphs.html` / `building_graphs.html` must follow this pattern:

```javascript
function updateMyChart() {
    const chartDiv = document.getElementById('my-chart');
    const summaryDiv = document.getElementById('my-summary');
    chartDiv.innerHTML = '<div class="loading-spinner">Loading...</div>';
    summaryDiv.innerHTML = '';

    fetch(`/api/house/${houseId}/my-endpoint?days=${days}`)
        .then(r => r.ok ? r.json() : { data: [], error: 'Failed' })
        .then(result => {
            const data = result.data || [];
            if (data.length === 0) {
                chartDiv.innerHTML = '<div class="no-data-message">No data available.</div>';
                return;
            }
            const traces = [...];
            const layout = {
                height: 350,
                margin: { l: 60, r: 30, t: 30, b: 80 },
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: '#ffffff',
                font: { family: 'system-ui, -apple-system, sans-serif' },
                dragmode: 'pan',
                hovermode: 'x unified',
            };
            // CRITICAL: Clear spinner BEFORE Plotly renders
            chartDiv.innerHTML = '';
            Plotly.newPlot('my-chart', traces, layout, chartConfig)
                .then(() => { summaryDiv.innerHTML = `...`; })
                .catch(err => {
                    console.error('Plotly render error:', err);
                    chartDiv.innerHTML = '<div class="no-data-message">Failed to render.</div>';
                });
        })
        .catch(error => {
            console.error('Failed to load data:', error);
            chartDiv.innerHTML = '<div class="no-data-message">Failed to load data.</div>';
        });
}
```

Key: clear spinner with `chartDiv.innerHTML = ''` before `Plotly.newPlot()`. Always `.catch()` on the Plotly promise. Use `chartConfig` (global) for consistent config.

### InfluxDB Reader Pattern

All query methods in `influx_reader.py` must follow:

```python
def get_my_data(self, house_id: str, days: int = 30) -> dict:
    self._ensure_connection()
    if not self.client:
        return {'data': [], 'error': 'No connection'}
    try:
        query_api = self.client.query_api()  # NOT self.query_api
        # ... build and execute query
    except Exception as e:
        print(f"Failed to query: {e}")
        return {'data': [], 'error': str(e)}
```

Key: call `self._ensure_connection()` first, check `self.client`, create `query_api` via `self.client.query_api()`.

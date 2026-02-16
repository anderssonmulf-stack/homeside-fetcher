# BVPro

A Python application that monitors heating systems for residential houses (via HomeSide API) and commercial buildings (via Arrigo BMS API), analyzes thermal dynamics, and integrates with weather forecasts to optimize heating control.

## Architecture Overview

A single Docker container (`SvenskEB-orchestrator`) manages all fetcher subprocesses. The `orchestrator.py` scans `profiles/` and `buildings/` every 60 seconds, spawning or stopping subprocesses as configs are added or removed. No docker-compose editing is needed to add/remove entities.

| | Houses (Residential) | Buildings (Commercial) |
|---|---|---|
| **API** | HomeSide district heating | Arrigo BMS (GraphQL) |
| **Fetcher** | `HSF_Fetcher.py` | `building_fetcher.py` |
| **Config** | `profiles/*.json` (CustomerProfile) | `buildings/*.json` (plain JSON) |
| **Credentials** | `.env`: `HOUSE_<customer_id>_USERNAME/PASSWORD` | `.env`: `BUILDING_<id>_USERNAME/PASSWORD` |
| **InfluxDB tag** | `house_id` | `building_id` |
| **Measurement** | `heating_system` | `building_system` |
| **Onboard** | `add_customer.py` | `add_building.py` |
| **Offboard** | `remove_customer.py` | `remove_customer.py --type building` |
| **Poll interval** | 5 min (configurable via `settings.json`) | 5 min (configurable per building) |

Shared across both: energy separation (`heating_energy_calibrator.py`), k-value recalibration (`k_recalibrator.py`), Dropbox energy import, InfluxDB, Seq logging, web GUI.

## Project Structure

### Core Files

| File | Purpose |
|------|---------|
| `orchestrator.py` | Single container that manages all fetcher subprocesses, scans profiles/buildings every 60s |
| `HSF_Fetcher.py` | House fetcher: polls HomeSide API, runs thermal analysis, energy pipeline |
| `building_fetcher.py` | Building fetcher: polls Arrigo BMS API, writes signals to InfluxDB |
| `arrigo_api.py` | Arrigo BMS API client (auth, signal discovery, GraphQL) |
| `homeside_api.py` | HomeSide API client (session token, BMS token, read/write variables) |
| `thermal_analyzer.py` | Learns building thermal dynamics. Requires 24 data points (6 hours) minimum |
| `heat_curve_controller.py` | Dynamic heat curve adjustments based on weather forecasts |
| `smhi_weather.py` | SMHI weather client for observations and forecasts |
| `influx_writer.py` | InfluxDB write client for all time-series data |
| `seq_logger.py` | Structured Seq logging with automatic site tagging |
| `customer_profile.py` | Customer profile management (JSON in `profiles/`) |
| `temperature_forecaster.py` | Model C hybrid forecaster (physics + historical learning) |
| `dropbox_client.py` | Dropbox OAuth client with automatic token refresh |
| `dropbox_sync.py` | Syncs meter request file to Dropbox |
| `energy_importer.py` | Imports energy data files from Dropbox into InfluxDB |
| `heating_energy_calibrator.py` | Energy separation: splits total energy into heating vs DHW |
| `k_recalibrator.py` | K-value (heat loss coefficient) recalibration |
| `gap_filler.py` | Fills gaps in weather observation and heating_system data |

### Configuration

| File | Purpose |
|------|---------|
| `settings.json` | Application settings (weather intervals, poll interval, margins) |
| `variables_config.json` | Maps HomeSide API variable names to field names |
| `.env` | All credentials, API tokens, InfluxDB settings (gitignored) |
| `profiles/*.json` | Per-house profiles with settings and learned parameters |
| `buildings/*.json` | Per-building config with Arrigo connection, signal mapping |
| `offboarded.json` | Tracks soft-offboarded entities pending InfluxDB purge (gitignored) |

### Admin Scripts

| File | Purpose |
|------|---------|
| `add_customer.py` | Add new house: creates profile, adds credentials to `.env`, auto-detected by orchestrator |
| `add_building.py` | Add new building: discovers Arrigo signals, creates config, adds credentials |
| `remove_customer.py` | Remove house/building: hard (immediate) or soft (30-day grace period) |
| `import_historical_data.py` | Bootstrap house data from Arrigo GraphQL API |
| `building_import_historical_data.py` | Bootstrap building data from Arrigo history |

## Data Flow

### Houses

1. `HSF_Fetcher.py` polls HomeSide API every 5 minutes via `homeside_api.py`
2. Heating data passed to `thermal_analyzer.py` for learning
3. Weather forecasts fetched via `smhi_weather.py`
4. `heat_curve_controller.py` evaluates if heating should be reduced
5. All data written to InfluxDB via `influx_writer.py`

### Buildings

1. `building_fetcher.py` polls Arrigo BMS API every 5 minutes via `arrigo_api.py`
2. Configured signals written to InfluxDB (`building_system` measurement)
3. Daily at 08:00: energy separation runs via `run_energy_separation()`
4. Every 72h: k-value recalibration via `recalibrate_entity()`

### Energy Pipeline (Daily at 08:00)

1. Import meter data from Dropbox → `energy_importer.py`
2. Energy separation (heating/DHW split) → `heating_energy_calibrator.py`
3. K-value recalibration → `k_recalibrator.py`
4. Hourly Dropbox check via `check_dropbox_and_separate()` triggers separation on new imports

## Adding New Houses

```bash
python3 add_customer.py  # Interactive
python3 add_customer.py --non-interactive --username FC2000233091 --password "xxx" --name "House Name" --lat 56.67 --lon 12.86
```

The script creates a profile JSON and appends credentials to `.env`. The orchestrator auto-detects within 60 seconds.

After adding, have the customer register at `https://bvpro.hem.se/register`, then approve them in the admin panel.

## Adding New Buildings

```bash
python3 add_building.py  # Interactive — discovers Arrigo signals automatically
python3 add_building.py --non-interactive --host exodrift05.systeminstallation.se --username "user" --password "xxx" --name "Building Name" --building-id SITE_Name
```

The script creates `buildings/<id>.json` and adds credentials to `.env`. The orchestrator auto-detects within 60 seconds.

To enable energy separation: set `energy_separation.enabled: true` in the building config, configure `meter_ids` and `field_mapping`.

## Removing Houses & Buildings

```bash
python3 remove_customer.py HEM_FJV_Villa_99              # Hard remove (immediate)
python3 remove_customer.py TE236_HEM_Kontor --type building  # Remove building
python3 remove_customer.py HEM_FJV_Villa_99 --soft        # Soft: 30-day grace period
python3 remove_customer.py HEM_FJV_Villa_99 --dry-run     # Preview
```

Soft offboard: config/credentials removed immediately (orchestrator stops subprocess within 60s), InfluxDB data purged after grace period via `offboarded.json`.

## Critical Patterns

### Timezone: Swedish Days

Energy meter timestamps are in UTC. **Always group by Swedish date** (CET/CEST), not UTC.

- `aggregateWindow(every: 1d)` in Flux uses UTC boundaries → **wrong day grouping**
- Fix: fetch hourly data in Flux, group by Swedish date in Python: `ts.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d')`
- For Flux-side: `import "timezone"` + `option location = timezone.location(name: "Europe/Stockholm")`
- Energy timestamps represent start-of-period (hour 00:00Z = Swedish hour 01:00 CET)
- `energy_separated` records stored at UTC midnight of the Swedish date label

### InfluxDB Duplicate Prevention

When writing to InfluxDB, always **delete existing records** for the same entity + date range before writing.

- InfluxDB uses `measurement + tags + timestamp` as unique key
- Different tag values (e.g. different `method` tags) create **separate series**, causing duplicates for the same logical data
- Pattern: `client.delete_api().delete(start, stop, predicate, bucket, org)` before `write_api.write()`

### Plotly Charts (webgui)

When adding/modifying charts in `house_graphs.html` or `building_graphs.html`:
1. Clear loading spinner with `chartDiv.innerHTML = ''` **before** `Plotly.newPlot()`
2. Always use `.catch()` on the Plotly promise
3. Include standard layout: `height: 350`, `paper_bgcolor: 'rgba(0,0,0,0)'`, `plot_bgcolor: '#ffffff'`, `dragmode: 'pan'`

### InfluxDB Reader Pattern (webgui)

All query methods in `influx_reader.py`:
1. Call `self._ensure_connection()` at start
2. Check `if not self.client` and return early with `{'data': [], 'error': '...'}`
3. Create `query_api` via `self.client.query_api()` (NOT `self.query_api`)

## Key Variables Tracked

From `variables_config.json`:
- `room_temperature`, `outdoor_temperature`, `outdoor_temp_24h_avg`
- `supply_temp`, `return_temp`, `hot_water_temp`
- `system_pressure`, `electric_heater`, `heat_recovery`
- `away_mode`, `target_temp_setpoint`, `away_temp_setpoint`

Calculated: `supply_temp_heat_curve` (baseline) and `supply_temp_heat_curve_ml` (ML-adjusted). During reduction mode, `_ml` < baseline.

## InfluxDB Measurements

| Measurement | Tags | Purpose |
|-------------|------|---------|
| `heating_system` | `house_id` | House heating variables |
| `building_system` | `building_id` | Building BMS signals |
| `energy_meter` | `house_id`, `meter_id` | Raw hourly energy consumption from Dropbox import |
| `energy_separated` | `house_id` or `building_id` | Daily heating/DHW split, k-value |
| `k_value_history` | `house_id` or `building_id` | K-value recalibration history |
| `weather_observation` | `house_id` | SMHI weather station readings |
| `weather_forecast` | `house_id` | SMHI forecast summary |
| `thermal_history` | `house_id` | Thermal analyzer persistence data |
| `heat_curve_baseline` | `house_id` | Stored heat curve for restoration |
| `temperature_forecast` | `house_id` | 24h ahead predictions with `lead_time_hours` |
| `forecast_accuracy` | `house_id` | Predicted vs actual comparisons |

## Thermal Data Persistence

The thermal analyzer persists to InfluxDB (`thermal_history`) and restores on startup. This solves the "Insufficient data for thermal coefficient" problem after container restarts.

## Temperature Forecaster (Model C)

Hybrid physics + learning approach:
- **Physics**: thermostat-aware — if indoor < target, heating active; if >= target, stabilizes
- **Learning**: tracks prediction errors by hour, applies corrections weighted by confidence
- **Key insight**: thermostat is dominant factor; setpoint determines where temp ends up
- **Schedule**: first update at 24 samples (~6h), then daily at 96 samples

## Dropbox Energy Data Exchange

Automated energy data exchange: this server writes meter request file (`BVPro_DH.csv`) to Dropbox, work server exports hourly energy data (`energy_*.txt`), this server imports and deletes.

Setup: `python3 setup_dropbox_auth.py`, then add `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN` to `.env`. Add meter IDs to profiles via web GUI or directly in JSON.

## Web GUI (webgui/)

Flask app at **bvpro.hem.se** behind nginx reverse proxy with GeoIP (Sweden only), rate limiting, fail2ban, Let's Encrypt HTTPS.

### Key Files

| File | Purpose |
|------|---------|
| `app.py` | Flask routes: dashboard, house/building detail, graphs, admin, API endpoints |
| `auth.py` | User auth, bcrypt passwords, role-based access (admin/user/viewer/pending) |
| `influx_reader.py` | InfluxDB query methods for all web GUI data |
| `ai_assistant.py` | AI chat assistant (Anthropic API with tool_use) |
| `ai_tools.py` | Tool functions for AI assistant (InfluxDB queries, profile reads) |
| `email_service.py` | SMTP notifications via one.com |
| `theme.py` | Multi-deployment theming (SITE_THEME env var) |
| `audit.py` | Audit logging for settings changes |

### Deployment

Service: `svenskeb-gui.service` (systemd). Restart: `sudo systemctl restart svenskeb-gui`

Logs: `sudo journalctl -u svenskeb-gui -f`

### Docker

InfluxDB URL inside containers: `http://influxdb:8086`, from host: `http://localhost:8086`

Rebuild + deploy: `docker compose build && docker compose up -d`

### Backup Scripts

All in `backup_scripts/`, configured via `backup_include.conf`. Run: `bash backup_scripts/backup_to_nas_host.sh`

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
| `influx_writer.py` | InfluxDB client for storing time-series data (heating metrics, forecasts, thermal coefficients, heat curve baselines). Logs write failures to Seq. |
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
| `docker-compose.yml` | Container orchestration config. Defines one fetcher service per customer (multi-house support). |

### Grafana Dashboards

Located in `grafana/dashboards/`:

| File | Purpose |
|------|---------|
| `homeside-mobile.json` | Mobile-friendly dashboard with stat panels and forecast graphs |
| `homeside-admin.json` | Admin dashboard with multi-user support (house_id variable) and detailed analytics |
| `homeside-2.0.json` | Improved admin dashboard with split temperature graphs and clean legends |
| `homeside-3.0.json` | Latest dashboard with **friendly names** in house selector (e.g., "Daggis8" instead of technical ID) |

Provisioning configs in `grafana/provisioning/`:
- `dashboards/default.yaml` - Auto-loads dashboards from `/var/lib/grafana/dashboards`
- `datasources/influxdb.yaml` - Pre-configures InfluxDB connection

### Debug/Development Tools

| File | Purpose |
|------|---------|
| `debug_variables.py` | Debug tool for inspecting API variables |
| `dump_variables.py` | Dumps all variables from the API |
| `find_all_variables.py` | Discovers available variables in the system |

### Admin Scripts

| File | Purpose |
|------|---------|
| `add_customer.py` | Interactive script to add new customers (creates profile, updates docker-compose, deploys) |
| `migrate_seq_to_influx.py` | Migrates historical data from Seq logs to InfluxDB (for data recovery) |
| `import_historical_data.py` | Fetches historical data from Arrigo GraphQL API to bootstrap thermal analyzer for new houses |

### Migration Scripts

| File | Purpose |
|------|---------|
| `migrate_seq_to_influx.py` | One-time migration: reads thermal data from Seq logs and writes to InfluxDB `thermal_history` measurement |

## Historical Data Import (New Customers)

When adding a new customer, you can import 3 months of historical data from the Arrigo API to immediately calculate thermal coefficients instead of waiting 6+ hours for the thermal analyzer to collect enough data.

### Usage

```bash
# Dry run - see what would be imported
python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90 --dry-run

# Import 3 months of data
python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90

# List available signals
python3 import_historical_data.py --username FC2000232581 --password "xxx" --list-signals
```

### How It Works

The script:
1. Authenticates with HomeSide API (same as the fetcher)
2. Gets BMS token for Arrigo access
3. Attempts to fetch historical data via GraphQL
4. Maps Arrigo signal names to our field names:
   - "Ute Temperatur" -> `outdoor_temperature`
   - "Medeltemperatur Rum" -> `room_temperature`
   - "Tillopp värme" -> `supply_temp`
   - "Retur värme" -> `return_temp`
5. Writes data to InfluxDB (`thermal_history` and `heating_system` measurements)

### Known Limitations

The Arrigo GraphQL API endpoint varies by installation:
- `exodrift10.systeminstallation.se/arrigo/api/graphql` - some installations
- Other server hostnames may be used

If automatic discovery fails, you can specify the server:
```bash
python3 import_historical_data.py --username FC... --password "..." --arrigo-host exodrift10.systeminstallation.se
```

### Signal Name Mapping

| Arrigo Signal | Field Name |
|---------------|------------|
| Ute Temperatur | outdoor_temperature |
| Medeltemperatur Rum | room_temperature |
| Tillopp värme, FC... | supply_temp |
| Retur värme, FC... | return_temp |
| Tappvarmvatten, FC... | hot_water_temp |
| Framledning Börvärde | supply_setpoint |

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

## Adding New Customers

### Quick Method (Recommended)

Use the automated script:

```bash
cd /opt/dev/homeside-fetcher
python3 add_customer.py
```

The script will:
1. Prompt for customer details (username, password, name, location)
2. Create the customer profile JSON
3. Add the fetcher service to docker-compose.yml
4. Build and deploy the container

For non-interactive use:
```bash
python3 add_customer.py --non-interactive \
    --username FC2000233091 \
    --password "password" \
    --name "House Name" \
    --lat 56.67 \
    --lon 12.86
```

### Manual Method

To add a new customer/house manually, follow these steps:

### Step 1: Get Customer Information

Collect the following from the customer:
- **HomeSide username** (e.g., `FC2000233091`)
- **HomeSide password**
- **Friendly name** for the house (e.g., `Glansen`, `Daggis8`)
- **Location coordinates** (latitude/longitude for weather forecasts)

You can find the coordinates using Google Maps (right-click on location).

### Step 2: Create Customer Profile

Create a new profile file in `profiles/` named `HEM_FJV_Villa_XX.json`:

```json
{
  "schema_version": 1,
  "customer_id": "HEM_FJV_Villa_XX",
  "friendly_name": "CustomerName",
  "building": {
    "description": "Description of the building",
    "thermal_response": "medium"
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
    "thermal_coefficient": null,
    "thermal_coefficient_confidence": 0.0,
    "hourly_bias": {},
    "samples_since_last_update": 0,
    "total_samples": 0,
    "next_update_at_samples": 24,
    "updated_at": null
  }
}
```

**Note:** The `customer_id` must match the last segment of the HomeSide client ID (auto-discovered on first run).

### Step 3: Add Fetcher Service to docker-compose.yml

Add a new service for the customer in `docker-compose.yml`:

```yaml
  # New customer: FriendlyName
  homeside-fetcher-customername:
    build: .
    container_name: homeside-fetcher-HEM_FJV_Villa_XX
    restart: unless-stopped
    environment:
      - HOMESIDE_USERNAME=FCXXXXXXXXX
      - HOMESIDE_PASSWORD=password
      - HOMESIDE_CLIENTID=
      - FRIENDLY_NAME=CustomerName
      - DISPLAY_NAME_SOURCE=friendly_name
      - POLL_INTERVAL_MINUTES=15
      - SEQ_URL=http://seq:5341
      - SEQ_API_KEY=your-seq-api-key
      - LOG_LEVEL=INFO
      - DEBUG_MODE=false
      - INFLUXDB_URL=http://influxdb:8086
      - INFLUXDB_TOKEN=homeside_token_2026_secret
      - INFLUXDB_ORG=homeside
      - INFLUXDB_BUCKET=heating
      - INFLUXDB_ENABLED=true
      - LATITUDE=XX.XX
      - LONGITUDE=XX.XX
    volumes:
      - ./profiles:/app/profiles
    networks:
      - dryckesmail_beer-network
    depends_on:
      - influxdb
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

**Important:** The `volumes` section mounts the profiles directory so the fetcher can access customer profiles.

### Step 4: Build and Deploy

```bash
cd /opt/dev/homeside-fetcher

# Rebuild the fetcher image
docker compose build

# Start all services (including the new fetcher)
docker compose up -d

# Check the logs to verify it started correctly
docker logs -f homeside-fetcher-HEM_FJV_Villa_XX
```

### Step 5: Verify Setup

Check the startup logs for:
```
✓ Customer profile loaded: CustomerName
```

If you see `⚠ No customer profile found`, verify:
1. The profile filename matches `HEM_FJV_Villa_XX.json`
2. The `customer_id` in the profile matches the auto-discovered client ID
3. The profiles volume is mounted correctly

### Step 6: Add User to Web GUI (svenskeb.se)

1. Have the customer register at `https://svenskeb.se/register`
2. Admin receives email notification
3. Admin logs in and approves the user at `/admin/users`
4. Assign the customer's house(s) during approval

### Finding the Client ID

If you need to find the exact client ID format:
1. Start the fetcher with `HOMESIDE_CLIENTID=` (empty)
2. Check the logs for: `✓ Using client ID: 38/xxx/HEM_FJV_XX/HEM_FJV_Villa_XX`
3. The last segment (`HEM_FJV_Villa_XX`) is what the profile filename should match

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
| `temperature_forecast` | Future temperature predictions (24h ahead) with lead_time tracking |
| `forecast_accuracy` | Predicted vs actual comparisons for learning |
| `learned_parameters` | History of learned params (for tracking adaptation) |

### Lead-Time Accuracy Tracking

Each forecast point includes a `lead_time_hours` field indicating how far ahead the prediction was made. This enables comparing forecast accuracy at different horizons (24h vs 12h vs 3h) to find the optimal prediction window.

**How it works:**
- Forecasts are generated every 2 hours for the next 24 hours
- Each forecast point is tagged with `lead_time_hours` (hours from generation to target time)
- Multiple predictions for the same future time accumulate (not deleted)
- When the target time arrives, compare all predictions to find which lead time was most accurate

### Querying Forecasts

```flux
// Get indoor temperature forecasts for next 24h
from(bucket: "heating")
  |> range(start: now(), stop: now() + 24h)
  |> filter(fn: (r) => r._measurement == "temperature_forecast")
  |> filter(fn: (r) => r.forecast_type == "indoor_temp")

// Check forecast accuracy
from(bucket: "heating")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "forecast_accuracy")
  |> filter(fn: (r) => r._field == "error")

// Compare accuracy by lead time (e.g., 24h vs 12h vs 3h predictions)
from(bucket: "heating")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "temperature_forecast")
  |> filter(fn: (r) => r._field == "lead_time_hours")
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

### 2.0 Dashboard (`homeside-2.0.json`)

Improved version of the admin dashboard:
- **Separate graphs** for Indoor and Outdoor temperatures (split from combined view)
- **Clean legends** - no duplicate entries (fixed using Flux `map()` and `group()`)
- Clear naming: "Heat Curve" instead of "Expected (baseline)"

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

## Web Infrastructure

The system is exposed publicly via **svenskeb.se** (Svensk EnergiBesparing) with multiple security layers.

### Domain & URLs

| URL | Purpose |
|-----|---------|
| `grafana.svenskeb.se` | Grafana dashboards (HTTPS, basic auth) |
| `svenskeb.se` | Reserved for future Settings GUI |

### nginx Reverse Proxy

Configuration files in `nginx/`:

| File | Purpose |
|------|---------|
| `grafana.svenskeb.se.conf` | Site config with auth, rate limiting, geo-blocking |
| `rate-limiting.conf` | Rate limit zones (login: 1 req/s, general: 10 req/s) |
| `geoip-sweden-only.conf` | GeoIP config to allow only Swedish IPs |
| `fail2ban-nginx.conf` | Auto-ban after 3 failed logins |
| `setup-nginx.sh` | Deployment script |

### Security Layers

```
Internet
    ↓
┌─────────────────────────────────────────┐
│  Router Port Forwarding                 │
│  └── 80, 443 → 192.168.86.9            │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  UFW Firewall                           │
│  └── Allow 80/tcp, 443/tcp             │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  nginx                                  │
│  ├── GeoIP (Sweden only)               │  ← Blocks non-SE countries
│  ├── Rate limiting                      │  ← 1 req/s login, 10 req/s general
│  ├── Connection limit (10/IP)          │
│  ├── HTTPS (Let's Encrypt)             │
│  └── Basic auth                         │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  fail2ban                               │
│  └── Ban IP after 3 failed logins      │  ← 1 hour ban
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  Grafana (localhost:3000)              │
└─────────────────────────────────────────┘
```

### Setup Instructions

**Initial setup (run once):**
```bash
# Install packages
sudo apt install nginx certbot python3-certbot-nginx apache2-utils fail2ban libnginx-mod-http-geoip geoip-database

# Deploy configs
sudo bash /opt/dev/homeside-fetcher/nginx/setup-nginx.sh

# Create user
sudo htpasswd -c /etc/nginx/.htpasswd USERNAME

# Open firewall
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Get SSL certificate
sudo certbot --nginx -d grafana.svenskeb.se

# Reload services
sudo systemctl reload nginx
sudo systemctl restart fail2ban
```

**Add more users:**
```bash
sudo htpasswd /etc/nginx/.htpasswd newuser
```

**Check fail2ban status:**
```bash
sudo fail2ban-client status nginx-http-auth
```

### Geo-Blocking

Only Swedish IPs (country code `SE`) are allowed. All other countries get connection dropped (HTTP 444).

The geo-blocking uses nginx's GeoIP module with the legacy GeoIP database (`/usr/share/GeoIP/GeoIP.dat`).

To allow additional countries, edit `/etc/nginx/conf.d/geoip-sweden-only.conf`:
```nginx
map $geoip_country_code $allowed_country {
    default 0;      # Block all by default
    SE      1;      # Sweden
    NO      1;      # Norway (example)
    ""      1;      # Local requests
}
```

### SSL Certificate Renewal

Let's Encrypt certificates auto-renew via certbot's systemd timer. Check status:
```bash
sudo certbot certificates
sudo systemctl status certbot.timer
```

## Settings GUI (webgui/)

A Flask-based web application at **svenskeb.se** for customer self-service and admin management.

### Architecture

```
Internet → nginx (svenskeb.se) → Gunicorn → Flask App
                                    ↓
                              InfluxDB (real-time data)
                              profiles/*.json (settings)
                              users.json (authentication)
```

### Files

| File | Purpose |
|------|---------|
| `app.py` | Flask application with routes for dashboard, house settings, user management |
| `auth.py` | User authentication, bcrypt password hashing, role-based access control |
| `audit.py` | Audit logging for tracking changes |
| `email_service.py` | SMTP email notifications via one.com (send.one.com:587) |
| `influx_reader.py` | Queries real-time heating data from InfluxDB |
| `create_admin.py` | CLI tool to create initial admin user |
| `svenskeb-gui.service` | systemd service file for production deployment |

### Templates

| Template | Purpose |
|----------|---------|
| `base.html` | Base layout with navigation and flash messages |
| `login.html` | Login form |
| `register.html` | User registration with HomeSide credentials |
| `dashboard.html` | User's house list |
| `house_detail.html` | Real-time data, settings, profile editing |
| `house_graphs.html` | Plotly charts for data visualization |
| `admin_users.html` | Admin: user approval, credential testing |
| `admin_edit_user.html` | Admin: edit user details |

### Plotly Chart Implementation Pattern

All charts in `house_graphs.html` must follow this pattern to avoid "Loading..." spinner issues:

```javascript
function updateMyChart() {
    const chartDiv = document.getElementById('my-chart');
    const summaryDiv = document.getElementById('my-summary');

    // 1. Show loading spinner
    chartDiv.innerHTML = '<div class="loading-spinner">Loading...</div>';
    summaryDiv.innerHTML = '';

    fetch(`/api/house/${houseId}/my-endpoint?days=${days}`)
        .then(r => r.ok ? r.json() : { data: [], error: 'Failed' })
        .then(result => {
            const data = result.data || [];

            // 2. Handle empty data
            if (data.length === 0) {
                chartDiv.innerHTML = '<div class="no-data-message">No data available.</div>';
                return;
            }

            // 3. Build traces and layout...
            const traces = [...];
            const layout = {
                height: 350,
                margin: { l: 60, r: 30, t: 30, b: 80 },
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: '#ffffff',
                font: { family: 'system-ui, -apple-system, sans-serif' },
                dragmode: 'pan',
                hovermode: 'x unified',
                // ... other options
            };

            // 4. CRITICAL: Clear loading spinner BEFORE Plotly renders
            chartDiv.innerHTML = '';

            // 5. Use Plotly with promise chain for error handling
            Plotly.newPlot('my-chart', traces, layout, chartConfig)
                .then(() => {
                    // 6. Update summary after chart renders
                    summaryDiv.innerHTML = `...`;
                })
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

**Key points:**
1. Always clear the loading spinner with `chartDiv.innerHTML = ''` before `Plotly.newPlot()`
2. Always use `.catch()` on the Plotly promise to handle render errors
3. Include standard layout properties: `height`, `paper_bgcolor`, `plot_bgcolor`, `font`, `dragmode`
4. Use `chartConfig` (defined globally) for consistent Plotly configuration

### InfluxDB Reader Pattern

All query methods in `influx_reader.py` must follow this pattern:

```python
def get_my_data(self, house_id: str, days: int = 30) -> dict:
    self._ensure_connection()
    if not self.client:
        return {'data': [], 'error': 'No connection'}

    try:
        query_api = self.client.query_api()  # Get query API from client
        # ... build and execute query
    except Exception as e:
        print(f"Failed to query: {e}")
        return {'data': [], 'error': str(e)}
```

**Key points:**
1. Call `self._ensure_connection()` at the start
2. Check `if not self.client` and return early
3. Create `query_api` via `self.client.query_api()` - NOT `self.query_api`

### User Roles

| Role | Capabilities |
|------|--------------|
| `admin` | Full access, approve users, manage all houses |
| `user` | View and edit assigned houses |
| `viewer` | View-only access to assigned houses |
| `pending` | Awaiting admin approval |

### Features

**User Registration & Onboarding:**
- Self-registration with HomeSide credentials
- Admin receives email notification
- Admin can test HomeSide credentials before approval
- Role selection (user/viewer/admin) during approval
- House assignment with friendly names

**Real-Time Dashboard:**
- Live data from InfluxDB (room temp, supply temp, etc.)
- Freshness indicator (green if < 16 minutes old)
- Swedish timezone display
- Editable friendly name and description

**Admin Panel:**
- Pulsing red badge shows pending user count
- Approve/reject pending registrations
- Test HomeSide API credentials
- Assign houses with friendly names
- Edit user roles and permissions

### Deployment

**systemd service** (`svenskeb-gui.service`):
```ini
[Unit]
Description=Svenskeb Settings GUI
After=network.target

[Service]
Type=simple
User=ulf
WorkingDirectory=/opt/dev/homeside-fetcher/webgui
EnvironmentFile=/opt/dev/homeside-fetcher/webgui/.env
Environment="PATH=/opt/dev/homeside-fetcher/webgui/venv/bin"
ExecStart=/opt/dev/homeside-fetcher/webgui/venv/bin/gunicorn -b 127.0.0.1:5000 -w 2 app:app
Restart=always

[Install]
WantedBy=multi-user.target
```

**Commands:**
```bash
# Install/update service
sudo cp webgui/svenskeb-gui.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable svenskeb-gui
sudo systemctl restart svenskeb-gui

# View logs
sudo journalctl -u svenskeb-gui -f

# Create admin user
cd /opt/dev/homeside-fetcher/webgui
source venv/bin/activate
python create_admin.py
```

**Environment variables** (`.env`):
```bash
SECRET_KEY=your-secret-key
SMTP_HOST=send.one.com
SMTP_PORT=587
SMTP_USER=info@svenskeb.se
SMTP_PASSWORD=your-password
ADMIN_EMAIL=admin@svenskeb.se
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your-token
INFLUXDB_ORG=homeside
INFLUXDB_BUCKET=heating
```

### nginx Configuration

Site config at `/etc/nginx/sites-available/svenskeb.se`:
```nginx
server {
    listen 443 ssl;
    server_name svenskeb.se www.svenskeb.se;

    # SSL via Let's Encrypt
    ssl_certificate /etc/letsencrypt/live/svenskeb.se/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/svenskeb.se/privkey.pem;

    # GeoIP Sweden-only
    if ($allowed_country = 0) { return 444; }

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

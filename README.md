# HomeSide Fetcher

A Python application that optimizes district heating by learning your building's thermal dynamics and adjusting heating based on weather forecasts. The primary goal is **energy savings by preventing overheating** - reducing supply temperatures when outdoor temperatures are rising.

## How It Works

1. **Collects heating data** from HomeSide district heating API every 15 minutes
2. **Learns thermal dynamics** - how your building responds to outdoor temperature changes
3. **Fetches weather forecasts** from SMHI to predict upcoming conditions
4. **Adjusts heat curves** - reduces supply temperatures when warming weather is forecast
5. **Stores time-series data** in InfluxDB for analysis and persistence

## Features

- Thermal coefficient learning (building heat response analysis)
- Weather-based heat curve optimization
- SMHI weather integration (observations and forecasts)
- InfluxDB time-series storage
- Seq structured logging for monitoring
- Docker deployment
- Multi-site support with automatic client discovery

## Setup Instructions

### 1. Configure Credentials

The application supports two authentication methods:

**Option A: Username/Password (Recommended)**
- Set `HOMESIDE_USERNAME` and `HOMESIDE_PASSWORD` in `.env`
- Tokens are automatically obtained and refreshed when they expire
- No manual intervention needed

**Option B: Manual Session Token (Legacy)**
- Get token from browser: Log in to HomeSide → F12 → Application → Local Storage → `currentToken`
- Set `HOMESIDE_SESSION_TOKEN` in `.env`
- Note: This token will expire and require manual refresh

### 2. Client ID (Auto-Discovered)

The client ID is automatically discovered after login. The application calls `/api/v2/housefidlist` to get your house list and extracts the client ID.

For multiple houses, set `HOMESIDE_CLIENTID` manually in `.env`:
- Format: `38/Account/HEM_FJV_149/HEM_FJV_Villa_149`

### 3. Configure Environment

```bash
cp .env.example .env
nano .env
```

Required settings:
- `HOMESIDE_USERNAME` - your HomeSide username
- `HOMESIDE_PASSWORD` - your HomeSide password

Optional settings:
- `INFLUXDB_URL`, `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET` - for data persistence
- `SEQ_URL`, `SEQ_API_KEY` - for structured logging
- `FRIENDLY_NAME` - human-readable site name for logs
- `HEAT_CURVE_ENABLED` - enable/disable heat curve adjustments

### 4. Run with Docker

```bash
docker compose up -d
```

### 5. View Logs

```bash
docker compose logs -f homeside-fetcher
```

## Data Collected

Key variables tracked from the heating system:

| Variable | Description |
|----------|-------------|
| `room_temperature` | Average indoor temperature |
| `outdoor_temperature` | Current outdoor temp |
| `outdoor_temp_24h_avg` | 24-hour outdoor average |
| `supply_temp` | Heating supply water temperature |
| `return_temp` | Heating return water temperature |
| `hot_water_temp` | Domestic hot water temperature |
| `electric_heater` | Electric backup heater status |
| `heat_recovery` | Heat recovery system status |
| `away_mode` | Forced absence mode |

## Configuration

| Environment Variable | Description | Required | Default |
|---------------------|-------------|----------|---------|
| HOMESIDE_USERNAME | HomeSide username | Yes | - |
| HOMESIDE_PASSWORD | HomeSide password | Yes | - |
| HOMESIDE_SESSION_TOKEN | Manual session token (legacy, optional) | No | - |
| HOMESIDE_CLIENTID | Client ID (auto-discovered if not set) | No | Auto |
| POLL_INTERVAL_MINUTES | Minutes between polls | No | 15 |
| INFLUXDB_URL | InfluxDB server URL | No | - |
| INFLUXDB_TOKEN | InfluxDB API token | No | - |
| INFLUXDB_ORG | InfluxDB organization | No | - |
| INFLUXDB_BUCKET | InfluxDB bucket name | No | - |
| SEQ_URL | Seq logging server URL | No | - |
| SEQ_API_KEY | Seq API key | No | - |
| FRIENDLY_NAME | Human-readable site name | No | - |
| HEAT_CURVE_ENABLED | Enable heat curve control | No | false |

## Token Management

**With username/password authentication (recommended):**
Tokens are automatically refreshed when they expire. No manual intervention needed.

**With manual session token (legacy):**
The session token will eventually expire. When you see authentication errors:
1. Log in to HomeSide again in your browser
2. Get the new `currentToken` value from Local Storage
3. Update `.env` with the new token
4. Restart the container: `docker compose restart`

## Troubleshooting

### "no authorization header sent" error
If using username/password: Check credentials are correct. The system will auto-retry.
If using manual token: Your session token has expired. Get a fresh one from the browser, or switch to username/password authentication for automatic refresh.

### "Method forbiden" error
Check that your clientid is correct.

### "Insufficient data for thermal coefficient"
The system needs at least 6 hours (24 data points) to learn your building's thermal dynamics. This data persists in InfluxDB across restarts.

### Container keeps restarting
Check logs: `docker compose logs homeside-fetcher`

## Project Structure

```
homeside-fetcher/
├── HSF_Fetcher.py           # Main application entry point
├── homeside_api.py          # HomeSide API client
├── thermal_analyzer.py      # Thermal dynamics learning
├── heat_curve_controller.py # Heat curve adjustments
├── smhi_weather.py          # SMHI weather integration
├── influx_writer.py         # InfluxDB client
├── seq_logger.py            # Seq logging
├── customer_profile.py      # Customer settings management
├── temperature_forecaster.py # Indoor temperature forecaster
├── profiles/                # Customer JSON profiles
├── settings.json            # Application settings
├── variables_config.json    # Variable name mappings
├── Dockerfile               # Docker image
├── docker-compose.yml       # Docker Compose config
├── .env.example             # Configuration template
├── webgui/                  # Settings GUI (Flask)
│   ├── app.py               # Flask application
│   ├── auth.py              # User authentication
│   ├── email_service.py     # Email notifications
│   ├── influx_reader.py     # InfluxDB queries
│   └── templates/           # HTML templates
├── nginx/                   # nginx configuration
└── README.md                # This file
```

## Development Status

- [x] HomeSide API authentication
- [x] Heating data collection
- [x] Thermal dynamics learning
- [x] Weather forecast integration (SMHI)
- [x] Heat curve optimization
- [x] InfluxDB persistence
- [x] Seq structured logging
- [x] Multi-site support
- [x] Public access with security (nginx, HTTPS, geo-blocking)
- [x] Settings GUI with user management (bvpro.hem.se)
- [x] Automatic token refresh (HomeSide API and Dropbox OAuth)
- [x] Energy data import from Dropbox with daily scheduling

### Potential Future Features

- [ ] MQTT publishing for home automation integration
- [ ] Home Assistant integration
- [ ] Mobile notifications

## Web Access

### Settings GUI

A Flask-based customer portal at **bvpro.hem.se** for:

- **User self-registration** - new users can register with their HomeSide credentials
- **Admin approval workflow** - admins receive email notifications and can test credentials
- **Real-time dashboard** - live heating data from InfluxDB
- **Settings management** - edit friendly names, descriptions, comfort settings
- **Role-based access** - admin, user, viewer roles

See `webgui/` folder for application code and `CLAUDE.md` for detailed setup instructions.

## License

Private project for personal use.

# HomeSide Fetcher

Fetches heating system data (SCADA variables) from HomeSide district heating system and publishes to MQTT.

## Features

- Polls HomeSide API for heating system variables (212+ data points)
- Logs to Seq for monitoring and debugging
- Publishes data to MQTT (ready for Homey integration)
- Runs as a Docker container
- Extracts key values: temperatures, heater status, alarms, etc.

## Setup Instructions

### 1. Get Your Session Token

The HomeSide API requires a session token from the browser:

1. Log in to https://homeside.systeminstallation.se/
2. Open **Developer Tools** (F12)
3. Go to **Application** tab
4. Expand **Local Storage** → click `homeside.systeminstallation.se`
5. Find the key `currentToken`
6. Copy the entire value (looks like: `"querykey":"GLl7FoLWjIb..."`
   - Copy just the value part after `"querykey":"`, not the whole object

### 2. Client ID (Auto-Discovered!)

**Good news: The client ID is now automatically discovered!**

After login, the application calls `/api/v2/housefidlist` to get your house list and automatically extracts the client ID. No manual configuration needed!

If you have multiple houses and want to specify which one to use, you can still set it manually:
- Format: `38/Account/HEM_FJV_149/HEM_FJV_Villa_149`
- Set `HOMESIDE_CLIENTID` in your `.env` file

### 3. Configure Environment

```bash
cd /opt/dev/homeside-fetcher
cp .env.example .env
nano .env
```

Update these values in `.env`:
- `HOMESIDE_SESSION_TOKEN` - from step 1
- `HOMESIDE_CLIENTID` - from step 2
- `SEQ_URL` - your Seq server URL (optional)
- `MQTT_BROKER` - your MQTT broker address (optional)

### 4. Run with Docker

```bash
docker compose up -d
```

### 5. View Logs

```bash
docker compose logs -f homeside-fetcher
```

## Data Collected

The fetcher collects 212+ variables including:

- **Room Temperature** (AI_ZON1_GT_RUM_1_Output)
- **Outdoor Temperature** (AI_GT_UTE_Output)
- **Outdoor 24h Average** (MEDEL_GT_UTE_24h_Average)
- **Supply Temperature** (AI_VS1_GT_TILL_1_Output)
- **Return Temperature** (AI_VS1_GT_RETUR_1_Output)
- **Electric Heater Status** (ELM1_AKTIV_Input)
- **Heat Recovery Status** (VMM1_AKTIV_Input)
- **Pool Status** (POOL_AKTIV_Input)
- **Burglar Alarm** (DI_INBOTTSLARM_AKTIV_Input)
- **Away Mode** (FORC_BORTALAGE)

## Configuration

| Environment Variable | Description | Required | Default |
|---------------------|-------------|----------|---------|
| HOMESIDE_SESSION_TOKEN | Session token from browser | No | - |
| HOMESIDE_CLIENTID | Client ID (auto-discovered if not set) | No | Auto |
| POLL_INTERVAL_MINUTES | Minutes between polls | No | 15 |
| SEQ_URL | Seq logging server URL | No | - |
| SEQ_API_KEY | Seq API key | No | - |
| LOG_LEVEL | Logging level | No | INFO |
| MQTT_BROKER | MQTT broker address | No | - |
| MQTT_PORT | MQTT broker port | No | 1883 |
| MQTT_TOPIC_PREFIX | MQTT topic prefix | No | homeside |

## Token Expiration

The session token from the browser will eventually expire. When you see authentication errors:

1. Log in to HomeSide again in your browser
2. Get the new `currentToken` value from Local Storage
3. Update `.env` with the new token
4. Restart the container: `docker compose restart`

## MQTT Integration (TODO)

MQTT publishing is prepared but not yet implemented. When enabled, it will publish to topics like:
- `homeside/temperature/room`
- `homeside/temperature/outdoor`
- `homeside/status/heater`
- etc.

## Troubleshooting

### "no authorization header sent" error
Your session token has expired. Get a fresh one from the browser.

### "Method forbiden" error
Check that your clientid is correct.

### Empty data `{}`
The BMS token or session token might be invalid. Try getting fresh tokens.

### Container keeps restarting
Check logs: `docker compose logs homeside-fetcher`
Make sure SESSION_TOKEN and CLIENTID are set in `.env`

## Development Status

- [x] Session token authentication
- [x] BMS token acquisition
- [x] Heating data retrieval (212 variables)
- [x] Data extraction and display
- [x] Seq logging integration
- [ ] MQTT publishing implementation
- [ ] Automatic token refresh
- [ ] Homey integration guide

## Project Structure

```
homeside-fetcher/
├── HSF_Fetcher.py         # Main application
├── requirements.txt       # Python dependencies
├── Dockerfile            # Docker image
├── docker-compose.yml    # Docker Compose config
├── .env                  # Your configuration (gitignored)
├── .env.example          # Configuration template
└── README.md             # This file
```

## API Flow

1. Use session token in Authorization header
2. Call `/housearrigobmsapi/getarrigobmstoken` to get BMS token + UIDs
3. Call `/housearrigobmsapi/getducvariables` with full payload:
   - `uid`, `clientid`, `extendUid`, `token` (BMS token)
4. Parse the 212 variables returned
5. Extract key heating metrics
6. Log to Seq / publish to MQTT

## License

Private project for personal use.

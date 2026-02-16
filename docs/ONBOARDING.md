# Building & House Onboarding Guide

Complete guide for onboarding new entities (buildings and houses) into BVPro, from initial configuration through to live data collection and display.

---

## Table of Contents

1. [Entity Types Overview](#entity-types-overview)
2. [Building Onboarding](#building-onboarding)
3. [House Onboarding](#house-onboarding)
4. [Energy Meter Data Flow](#energy-meter-data-flow)
5. [Web GUI User Setup](#web-gui-user-setup)
6. [Verification & Troubleshooting](#verification--troubleshooting)
7. [Offboarding](#offboarding)

---

## Entity Types Overview

BVPro supports two entity types:

| Aspect | House (Villa) | Building (Arrigo) | Building (EBO) |
|---|---|---|---|
| Config location | `profiles/HEM_FJV_Villa_*.json` | `buildings/*.json` | `buildings/*.json` |
| Data fetcher | `HSF_Fetcher.py` (HomeSide API) | `building_fetcher.py` (Arrigo) | `building_fetcher.py` (EBO) |
| Env prefix | `HOUSE_*` | `BUILDING_*` | Shared `credential_ref` |
| InfluxDB tag | `house_id` | `building_id` | `building_id` |
| InfluxDB measurement | `heating_system` | `building_system` | `building_system` |
| Signals | ~10 heating variables | 100-250+ BMS signals | 20-50+ BMS signals |
| Meter IDs | Single meter per house | Can have multiple meters | Can have multiple meters |
| Auth method | HomeSide username/password | Arrigo direct username/password | EBO SxWDigest (shared credential_ref) |
| Onboard script | `add_customer.py` | `add_building.py` | `add_ebo_building.py` |

---

## Building Onboarding

### Step 1: Create Building Configuration

Create `buildings/<building_id>.json`. Use `buildings/EXAMPLE.json.template` as a starting point.

```json
{
  "schema_version": 1,
  "building_id": "SITE_BuildingName",
  "friendly_name": "Building Name",
  "building_type": "commercial",
  "meter_ids": ["735999255020055028"],
  "connection": {
    "system": "arrigo",
    "host": "exodriftXX.systeminstallation.se",
    "account": "AccountName"
  },
  "sub_areas": [],
  "analog_signals": {},
  "digital_signals": {},
  "alarm_monitoring": {
    "enabled": false,
    "priorities": ["A", "B"],
    "poll_interval_minutes": 1
  },
  "poll_interval_minutes": 15
}
```

**Key fields:**
- `building_id` -- Unique identifier, used as InfluxDB tag and env var prefix
- `meter_ids` -- District heating meter ID(s) from the energy company
- `connection.host` -- The Arrigo BMS server hostname
- `connection.account` -- Arrigo credentials domain

### Step 2: Add Credentials to `.env`

```bash
BUILDING_<building_id>_USERNAME=username
BUILDING_<building_id>_PASSWORD=password
```

Example:
```bash
BUILDING_TE236_HEM_Kontor_USERNAME=Ulf Andersson
BUILDING_TE236_HEM_Kontor_PASSWORD=xxxx
```

### Step 3: Orchestrator Auto-Discovery (automatic)

The orchestrator (`orchestrator.py`) scans `buildings/` every 60 seconds:

1. Finds the new `.json` config file
2. Checks for matching `BUILDING_<building_id>_USERNAME` in environment
3. Spawns: `python building_fetcher.py --building <building_id>`
4. The fetcher authenticates against Arrigo BMS via `arrigo_api.py`
5. On first run, discovers available signals via GraphQL (can take 30-60s)
6. Writes discovered signals back to the config file
7. Begins polling every `poll_interval_minutes` (default: 15)

**No restart needed** -- the orchestrator picks up new buildings automatically.

### Step 4: Verify Data Collection

```bash
# Check orchestrator logs for the new building
docker logs -f homeside-orchestrator 2>&1 | grep "TE236"

# Query InfluxDB directly
influx query 'from(bucket: "heating")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "building_system")
  |> filter(fn: (r) => r.building_id == "TE236_HEM_Kontor")
  |> limit(n: 5)'
```

---

## EBO Building Onboarding

EBO (Schneider Electric) buildings use a different BMS API (SxWDigest) and a shared credential model. Multiple buildings can share one credential set via `credential_ref`.

### Step 1: Add Shared Credentials to `.env`

EBO uses named credential references. Add once per EBO server, then reference from multiple building configs:

```bash
# EBO credentials (shared across buildings on the same server)
EBO_HK_CRED1_USERNAME=username
EBO_HK_CRED1_PASSWORD=password
EBO_HK_CRED1_DOMAIN=        # Often empty for EBO
```

### Step 2: Browse the EBO Object Tree

Explore the EBO server to find your building's AS controller:

```bash
# Browse root level to find Enterprise Controllers
/srv/BVPro/webgui/venv/bin/python add_ebo_building.py \
    --base-url https://ebo.example.com \
    --credential-ref EBO_HK_CRED1 \
    --browse --site-path "/EC1"

# Browse deeper to find AS controllers
/srv/BVPro/webgui/venv/bin/python add_ebo_building.py \
    --base-url https://ebo.example.com \
    --credential-ref EBO_HK_CRED1 \
    --browse --site-path "/BuildingName AS3"
```

**EBO tree structure:**
- `/EC1` — Enterprise Controller (root)
  - `/ES1`, `/ES2` — Enterprise Servers
    - `Fastigheter med flera AS-x` — Buildings folder
- `/BuildingName AS3` — AS controllers are also at root level

### Step 3: Discover Signals and Create Config

Run discovery on the building's AS controller path:

```bash
/srv/BVPro/webgui/venv/bin/python add_ebo_building.py \
    --non-interactive \
    --base-url https://ebo.example.com \
    --credential-ref EBO_HK_CRED1 \
    --site-path "/BuildingName 20942 AS3" \
    --name "BuildingName" \
    --building-id HK_Building_20942 \
    --lat 56.67 --lon 12.86
```

This creates `buildings/HK_Building_20942.json` with discovered signals. The script also generates a `_signals.json` reference file containing all available signals and trend logs for future use.

### Step 4: Edit Configuration

After discovery, review and edit the config:

1. **Set `field_name`** for each signal you want to fetch (maps to InfluxDB field name)
2. **Set `fetch: true`** for signals to poll
3. **Add `trend_log` paths** for signals that need historical bootstrap (from `_signals.json`)
4. **Add heat curve signals** with `write_on_change: true` flag

```json
{
  "connection": {
    "system": "ebo",
    "base_url": "https://ebo.example.com",
    "credential_ref": "EBO_HK_CRED1"
  },
  "analog_signals": {
    "/Building AS3/VS1/IO Bus/VS1-GT1/Value": {
      "field_name": "vs1_supply_temp",
      "fetch": true,
      "trend_log": "/Building AS3/VS1/Trendloggar/VS1-GT1-Tillopp"
    },
    "/Building AS3/VS1/Variabler/GT1_X1/Value": {
      "field_name": "vs1_hc_x1",
      "fetch": true,
      "write_on_change": true
    }
  }
}
```

**EBO signal path formats:**
- **IO Bus:** `/Site/Subsystem/IO Bus/PointName/Value` — physical sensors (temperatures, pressures, valve positions)
- **Variabler:** `/Site/Subsystem/Variabler/Name/Value` — setpoints, heat curve breakpoints
- **Variabler Externa:** `/Site/Subsystem/Variabler Externa/Name/Value` — external references

### Step 5: Test Data Fetch

```bash
# Dry run -- verify signals can be read
/srv/BVPro/webgui/venv/bin/python building_fetcher.py \
    --building HK_Building_20942 --once --dry-run

# Real fetch -- write to InfluxDB
INFLUXDB_URL=http://localhost:8086 /srv/BVPro/webgui/venv/bin/python building_fetcher.py \
    --building HK_Building_20942 --once
```

### Step 6: Bootstrap Historical Data

```bash
INFLUXDB_URL=http://localhost:8086 /srv/BVPro/webgui/venv/bin/python gap_filler.py \
    --bootstrap --house-id HK_Building_20942 --days 90 --yes
```

The `--yes` flag skips the interactive sanity check prompt (expected warnings: heat curve signals and some setpoints have no trend logs).

### Step 7: Orchestrator Auto-Discovery (automatic)

The orchestrator picks up new EBO buildings the same as Arrigo:
1. Finds `buildings/<id>.json` with `connection.system: "ebo"`
2. Resolves credentials via `credential_ref`
3. Spawns `building_fetcher.py` which auto-detects EBO mode

**No restart needed.**

### EBO-Specific Notes

- **Shared credentials:** Use `credential_ref` in building config instead of per-building env vars. One credential set can serve all buildings on the same EBO server.
- **IO Bus batching:** The adapter reads IO Bus points by browsing the parent folder (not individual point paths). This is a single API call per subsystem.
- **Hex-encoded doubles:** EBO returns values as IEEE 754 hex strings. The adapter decodes these automatically.
- **Heat curves:** VS subsystems have breakpoint tables (X=outdoor temp, Y=supply temp). VS1 typically has 7 points, VS2 has 3 points. Always fetch these with `write_on_change: true`.
- **Signal reference file:** `buildings/<id>_signals.json` stores all discovered signals and trend logs so you don't need to re-browse the EBO tree when adding signals later.
- **venv required:** Admin scripts must run with `/srv/BVPro/webgui/venv/bin/python` since system Python is externally-managed.

---

## House Onboarding

### Quick Method (Recommended)

```bash
python3 add_customer.py
```

The interactive script prompts for all details. For non-interactive use:

```bash
python3 add_customer.py --non-interactive \
    --username FC2000233091 \
    --password "password" \
    --name "House Name" \
    --lat 56.67 \
    --lon 12.86
```

### Manual Method

#### 1. Create Profile

Create `profiles/HEM_FJV_Villa_XX.json`:

```json
{
  "schema_version": 1,
  "customer_id": "HEM_FJV_Villa_XX",
  "friendly_name": "HouseName",
  "building": {
    "description": "Description",
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

#### 2. Add to docker-compose.yml

Add a fetcher service (see `CLAUDE.md` for full service template).

#### 3. Build and Deploy

```bash
docker compose build && docker compose up -d
```

#### 4. Add Meter ID (for energy data)

Add the meter ID to the profile either:
- Via web GUI: House Detail > Building Information > Energy Meter IDs
- Directly in the JSON: `"meter_ids": ["735999255020057923"]`

#### 5. Optional: Import Historical Data

Bootstrap thermal analysis with 3 months of historical data:

```bash
python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90
```

---

## Energy Meter Data Flow

This is the process by which district heating energy data (consumption, volume, temperatures) flows into the system for **both** buildings and houses.

### Architecture

```
This Server (07:55 daily)           Dropbox /data/              Work Server (07:45 daily)
┌─────────────────────────┐     ┌──────────────────────┐     ┌─────────────────────────┐
│                         │     │                      │     │                         │
│ 1. dropbox_sync.py      │────▶│ BVPro_DH.csv      │◀────│ 3. Reads meter IDs      │
│    writes meter request │     │                      │     │    from request file     │
│                         │     │                      │     │                         │
│ 5. energy_importer.py   │◀────│ energy_YYYYMMDD.txt  │◀────│ 4. Exports hourly       │
│    imports to InfluxDB  │     │                      │     │    energy data           │
│    deletes file         │     │                      │     │                         │
└─────────────────────────┘     └──────────────────────┘     └─────────────────────────┘
```

### Step-by-Step: Meter ID to Data

#### 1. Meter Mapping Discovery

`customer_profile.py:build_meter_mapping()` scans **both** directories:

- `profiles/*.json` -- house meter IDs (tagged as `type: "house"`)
- `buildings/*.json` -- building meter IDs (tagged as `type: "building"`)

Each meter ID maps to its entity:
```python
{
  "735999255020055028": {
    "id": "TE236_HEM_Kontor",
    "type": "building",
    "friendly_name": "HEM Kontor TE236"
  },
  "735999255020057923": {
    "id": "HEM_FJV_Villa_149",
    "type": "house",
    "friendly_name": "Daggis8"
  }
}
```

#### 2. Request File Sync (`dropbox_sync.py`)

For each meter ID:
1. Queries InfluxDB for the last imported data point
2. Sets `from_datetime` to one hour after the last data point
3. Writes to Dropbox `/data/BVPro_DH.csv`:

```csv
meter_id;from_datetime;house_name
735999255020055028;2026-02-01 23:00;HEM Kontor TE236
735999255020057923;2026-02-01 23:00;Daggis8
```

#### 3. Work Server Exports Data

The BVPro work server reads `BVPro_DH.csv`, queries the energy company's system for each meter, and writes hourly data files back to Dropbox.

#### 4. Energy Import (`energy_importer.py`)

Downloads energy files from Dropbox and for each record:

1. Parses: `timestamp;serviceID;meterStand;consumption;volume;temperatureIn;temperatureOut`
2. Looks up `serviceID` (meter ID) in the meter mapping
3. Determines entity type: `building` or `house`
4. Writes to InfluxDB `energy_meter` measurement with the correct tag:

```
# Building example
energy_meter,building_id=TE236_HEM_Kontor,meter_id=735999255020055028
  consumption=3.0,volume=0.08,primary_temp_in=64.24,primary_temp_out=33.9,meter_reading=6920

# House example
energy_meter,house_id=HEM_FJV_Villa_149,meter_id=735999255020057923
  consumption=2.5,volume=0.06,primary_temp_in=62.10,primary_temp_out=31.5,meter_reading=4280
```

5. After successful import: deletes the energy file from Dropbox
6. Updates `BVPro_DH.csv` with new `from_datetime`

### Manual Energy Operations

```bash
# Sync meter requests to Dropbox
python3 dropbox_sync.py

# Dry run -- see what would be imported
python3 energy_importer.py --dry-run

# Import energy data
python3 energy_importer.py
```

---

## Web GUI User Setup

### 1. User Registration

Customer registers at `https://bvpro.hem.se/register` with their HomeSide credentials.

### 2. Admin Notification

Admin receives email at `admin@bvpro.hem.se`.

### 3. Admin Approval

1. Log in to `/admin/users`
2. Review pending registration (pulsing red badge indicates pending users)
3. Optionally test HomeSide credentials
4. Select role: `user`, `viewer`, or `admin`
5. Assign house(s)/building(s) with friendly names
6. Approve

### User Roles

| Role | Capabilities |
|---|---|
| `admin` | Full access, approve users, manage all entities |
| `user` | View and edit assigned houses/buildings |
| `viewer` | View-only access to assigned entities |
| `pending` | Awaiting admin approval |

---

## Verification & Troubleshooting

### Check Building Data is Flowing

```bash
# Orchestrator picking up the building
docker logs homeside-orchestrator 2>&1 | grep "<building_id>"

# Building fetcher running
docker logs homeside-orchestrator 2>&1 | grep "building_fetcher"
```

### Check Energy Data is Flowing

```bash
# Verify meter mapping includes the new meter
python3 -c "from customer_profile import build_meter_mapping; print(build_meter_mapping())"

# Check Dropbox request file
python3 dropbox_sync.py  # Updates request file

# Check for energy files to import
python3 energy_importer.py --dry-run
```

### Common Issues

| Problem | Cause | Fix |
|---|---|---|
| Orchestrator skips building | Missing credentials in `.env` | Add `BUILDING_*` or `credential_ref` credentials |
| No signals discovered (Arrigo) | Wrong Arrigo host or account | Verify `connection.host` and `connection.account` |
| No signals discovered (EBO) | Wrong site path or IO Bus structure | Browse tree with `--browse` to verify paths |
| EBO login fails | Wrong credentials or domain | Check `credential_ref` resolves correctly in `.env` |
| EBO values are `None` | Hex decoding failure or wrong path type | Verify path ends in `/Value`, check IO Bus vs direct |
| Energy data not imported | Meter ID not in any config | Add `meter_ids` to building/profile JSON |
| Energy data tagged wrong | Meter ID in both house and building config | Ensure each meter ID is only in one entity |
| "No customer profile found" (house) | Profile filename doesn't match client ID | Check logs for auto-discovered client ID |
| Bootstrap fails with EOFError | Running non-interactively without `--yes` | Add `--yes` flag to `gap_filler.py` |

---

## Offboarding

### Soft Remove (recommended, 30-day grace period)

```bash
# Building
python3 remove_customer.py TE236_HEM_Kontor --soft --type building

# House
python3 remove_customer.py HEM_FJV_Villa_99 --soft
```

Removes config and credentials immediately (orchestrator stops the subprocess within 60s). InfluxDB data is kept for 30 days, then purged automatically.

### Hard Remove (immediate)

```bash
# Building
python3 remove_customer.py TE236_HEM_Kontor --type building

# House
python3 remove_customer.py HEM_FJV_Villa_99
```

Immediately deletes all InfluxDB data, config files, and credentials.

### Cancel Pending Offboard

Remove the entry from `offboarded.json` > `pending_purge` array.

---

## InfluxDB Data Reference

### Measurements by Entity Type

| Measurement | Entity | Tags | Description |
|---|---|---|---|
| `building_system` | Building | `building_id` | Real-time BMS signals (temps, flow, pressure) |
| `building_alarms` | Building | `building_id` | BMS alarm counts and states |
| `heating_system` | House | `house_id` | Heating variables (room/supply/return temp) |
| `thermal_history` | House | `house_id` | Historical data for thermal analyzer |
| `energy_meter` | Both | `building_id` or `house_id`, `meter_id` | District heating energy data |
| `temperature_forecast` | House | `house_id` | 24h temperature predictions |
| `weather_observation` | Shared | `station_id` | SMHI weather station readings |
| `weather_forecast` | Shared | -- | SMHI forecast summary |

### Energy Data Fields

| Column | InfluxDB Field | Description |
|---|---|---|
| serviceID | `meter_id` (tag) | Energy meter ID |
| meterStand | `meter_reading` | Cumulative kWh |
| consumption | `consumption` | Hourly kWh |
| volume | `volume` | Hourly m³ |
| temperatureIn | `primary_temp_in` | Primary side inlet (from utility) |
| temperatureOut | `primary_temp_out` | Primary side outlet (to utility) |

**Note:** Primary side temperatures are from the district heating utility side, not the house-side supply/return temps.

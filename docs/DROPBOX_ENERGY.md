# Dropbox Energy Data Exchange

Automated district heating energy data exchange via Dropbox between this server and the energy company.

## Architecture

```
Personal Server (07:55)              Dropbox /data/                Work Server (07:45)
┌────────────────────┐            ┌──────────────────────┐        ┌────────────────────┐
│                    │            │                      │        │                    │
│ Writes request     │──write────▶│ BVPro_DH.csv      │◀──read─│ Reads meter IDs    │
│ (meter IDs + dates)│            │                      │        │                    │
│                    │◀──read─────│ energy_YYYYMMDD.txt  │◀─write─│ Exports hourly     │
│ Imports to InfluxDB│            │                      │        │ energy data        │
│ Deletes file       │            │                      │        │                    │
└────────────────────┘            └──────────────────────┘        └────────────────────┘
```

## Files in Dropbox `/data/`

| File | Direction | Format |
|------|-----------|--------|
| `BVPro_DH.csv` | This server writes | `meter_id;from_datetime;house_name` |
| `energy_*.txt` | Work server writes | Semicolon-separated energy data |

## Request File Format (`BVPro_DH.csv`)

```csv
meter_id;from_datetime;house_name
735999255020057923;2026-02-01 23:00;Daggis8
```

The `from_datetime` is automatically updated after each successful import to the hour after the last imported data point.

## Energy Data File Format (`energy_*.txt`)

```
timestamp;serviceID;meterStand;consumption;volume;temperatureIn;temperatureOut
2026-01-28T23:00:00Z;735999255020057923;6920;3;0.08;64.24;33.9
```

| Column | InfluxDB Field | Description |
|--------|----------------|-------------|
| serviceID | meter_id (tag) | Energy meter ID |
| meterStand | meter_reading | Cumulative kWh |
| consumption | consumption | Hourly kWh |
| volume | volume | Hourly m³ |
| temperatureIn | primary_temp_in | Primary side inlet (from utility) |
| temperatureOut | primary_temp_out | Primary side outlet (to utility) |

**Note:** Primary side temperatures are from the district heating utility side, NOT the house-side supply/return temps.

## Setup

1. **One-time Dropbox OAuth setup:**
   ```bash
   python3 setup_dropbox_auth.py
   ```

2. **Environment variables** (in `.env`):
   ```bash
   DROPBOX_APP_KEY=your_app_key
   DROPBOX_APP_SECRET=your_app_secret
   DROPBOX_REFRESH_TOKEN=your_refresh_token
   ```

3. **Add meter ID to customer profile:**
   - Via web GUI: House Detail → Building Information → Energy Meter IDs
   - Or directly in `profiles/HEM_FJV_Villa_XXX.json`: `"meter_ids": ["735999255020057923"]`

## Manual Usage

```bash
# Sync meter requests to Dropbox
python3 dropbox_sync.py

# Dry run - see what would be imported
python3 energy_importer.py --dry-run

# Import energy data
python3 energy_importer.py
```

## Behavior

- **Successful import:** Data written to InfluxDB, energy file deleted from Dropbox, request file updated with new from_datetime
- **No files:** Nothing happens, request file unchanged (work server will export on next run)
- **Parse errors:** File kept in Dropbox for investigation
- **Logging:** All events logged to Seq

# Unified Bootstrap Pipeline (gap_filler.py)

Both houses and buildings use the same 7-phase bootstrap pipeline in `gap_filler.py` (`ArrigoBootstrapper` class). The pipeline auto-detects entity type based on whether `buildings/<id>.json` exists.

## 7 Phases

| Phase | Description | Houses | Buildings |
|-------|-------------|--------|-----------|
| 1 | Fetch Arrigo signal history (5-min, cursor pagination) | Via HomeSide → BMS token → Arrigo | Direct Arrigo login |
| 2 | Fetch SMHI weather observations (~4 months) | Yes | Yes |
| 3 | Copy energy_meter data from source entity | Optional | Optional |
| 4 | Sanity checks (stale signals, coverage, range) | Core: outdoor, room, supply, return | Core: outdoor only |
| 5 | Write to InfluxDB | `heating_system` + `thermal_history` + `weather` + `energy` | `building_system` + `weather` + `energy` |
| 6 | Energy separation + k-recalibration | Yes (if meter data) | Yes (if meter data) |
| 7 | Backtest predictions | Indoor temp (Model C) | Energy (k × degree_hours) |

## Auth Differences

- **Houses:** `ArrigoHistoricalClient` authenticates via HomeSide API → BMS token → Arrigo GraphQL. Requires `--username`/`--password` (HomeSide credentials).
- **Buildings:** `ArrigoAPI` authenticates directly with Arrigo using host/username/password. Credentials resolved from CLI args or `.env` (`BUILDING_<id>_USERNAME/PASSWORD`). Host and location read from `buildings/<id>.json`.

## Usage

```bash
# Bootstrap a building (credentials/location from config + .env)
python3 gap_filler.py --bootstrap --house-id TE236_HEM_Kontor --days 90

# Bootstrap a building with explicit args
python3 gap_filler.py --bootstrap --house-id TE236_HEM_Kontor \
    --arrigo-host exodrift05.systeminstallation.se \
    --username "user" --password "pass" \
    --lat 56.67 --lon 12.86 --days 90 --resolution 5

# Bootstrap a house
python3 gap_filler.py --bootstrap --house-id HEM_FJV_Villa_149 \
    --username FC2000232581 --password "pass" \
    --lat 56.67 --lon 12.86 --days 90

# Dry run
python3 gap_filler.py --bootstrap --house-id TE236_HEM_Kontor --days 90 --dry-run
```

## Signal Metadata

Bootstrap saves signal metadata for audit:
- Houses: `profiles/<house_id>_signals.json`
- Buildings: `buildings/<building_id>_signals.json`

## Historical Data Import (Legacy)

The older `import_historical_data.py` script is superseded by `gap_filler.py --bootstrap` but still works for houses:

```bash
python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90
```

### Signal Name Mapping (Arrigo → InfluxDB)

| Arrigo Signal | Field Name |
|---------------|------------|
| Ute Temperatur | outdoor_temperature |
| Medeltemperatur Rum | room_temperature |
| Tillopp värme, FC... | supply_temp |
| Retur värme, FC... | return_temp |
| Tappvarmvatten, FC... | hot_water_temp |
| Framledning Börvärde | supply_setpoint |

The Arrigo GraphQL API endpoint varies by installation (e.g., `exodrift10.systeminstallation.se`). Use `--arrigo-host` if auto-discovery fails.

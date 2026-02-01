# Heating Energy Separation

## Status: PAUSED
**Paused on:** 2026-02-01
**Reason:** Waiting for more historical HomeSide data to be fetched
**Resume when:** Historical heating system data is available from Dec 2025 onwards

---

## Overview

This feature separates district heating energy into:
- **Space heating energy** (radiators, floor heating)
- **DHW energy** (domestic hot water / tap water)

## Approach

The core formula:
```
heating_power = k × (T_indoor - T_effective_outdoor)
```

Where:
- `k` = building heat loss coefficient (kW/°C)
- `T_indoor` = room temperature
- `T_effective_outdoor` = outdoor temp adjusted for wind, humidity, solar

**Logic:**
1. Calculate expected heating energy from temperature difference
2. Compare with actual energy consumption from district heating meter
3. Excess energy (actual > estimated) = likely DHW usage
4. Confirmed by `hot_water_temp` peaks (HomeSide on-demand DHW system)

---

## Files Created

| File | Purpose |
|------|---------|
| `energy_models/heating_energy_separator.py` | DHW event detection from hot water temp peaks |
| `energy_models/weather_energy_model.py` | Effective outdoor temperature calculation |
| `heating_energy_estimator.py` | Estimates heating from temp difference (uses assumed k) |
| `heating_energy_calibrator.py` | **Main tool** - Calibrates k from real energy data |
| `energy_separation_service.py` | Service to run separation for enabled houses |
| `customer_profile.py` | Added `energy_separation` config section |

---

## Current Findings (2026-02-01)

### Data Available
- **Energy consumption:** Dec 1, 2025 - Jan 29, 2026 (60 days, 1440 hourly records)
- **Heating system data:** Jan 18, 2026 onwards only (13 days overlap)
- **Measurement:** `energy_consumption` with `energy_type: fjv_total`

### Preliminary Calibration (13 days only)
```
Calibrated k = 0.0764 kW/°C

At ΔT = 25°C (typical winter):
  → Heating power ≈ 1.9 kW
  → Daily heating ≈ 45 kWh
```

### Issues Identified
1. **Partial days** (like Jan 18 with only 89 degree-hours) skew results
2. **1 kWh resolution** creates noise in hourly data
3. **High k variation** (0.04 - 0.40) due to DHW and data gaps
4. **Only 13 days** of overlap - need more data for reliable calibration

---

## How to Resume

### 1. After fetching historical HomeSide data

Once you have heating system data from Dec 2025:

```bash
# Run calibration with full date range
source webgui/venv/bin/activate
INFLUXDB_TOKEN=$(grep INFLUXDB_TOKEN webgui/.env | cut -d'=' -f2) \
INFLUXDB_URL=http://localhost:8086 \
python heating_energy_calibrator.py --house HEM_FJV_Villa_149 --start 2025-12-01
```

### 2. Filter out partial days

The script should be updated to exclude days with < 80 data points (< 20 hours of data).

### 3. Write results to InfluxDB

Once calibration looks good:

```bash
python heating_energy_calibrator.py --house HEM_FJV_Villa_149 --start 2025-12-01 --write
```

This writes to `energy_separated` measurement with fields:
- `actual_energy_kwh`
- `heating_energy_kwh`
- `dhw_energy_kwh`
- `excess_energy_kwh`
- `k_value`, `k_implied`
- `avg_temp_difference`, `degree_hours`

### 4. Enable per-house in profile

Add to customer profile JSON:
```json
{
  "energy_separation": {
    "enabled": true,
    "method": "homeside_ondemand_dhw",
    "dhw_temp_threshold": 45.0,
    "avg_dhw_power_kw": 25.0
  }
}
```

---

## Future Improvements

1. **Minimum data threshold** - Skip days with < 80 data points
2. **Hourly separation** - Currently daily, could do hourly for better DHW detection
3. **DHW energy estimation** - Use hot water temp peaks to estimate DHW energy directly
4. **Rolling calibration** - Update k as more data accumulates
5. **Grafana dashboard** - Visualize heating vs DHW split

---

## Data Structure

### Input: energy_consumption
```
measurement: energy_consumption
tags: house_id, energy_type (fjv_total)
fields: value (kWh per hour)
```

### Input: heating_system
```
measurement: heating_system
tags: house_id
fields: room_temperature, outdoor_temperature, hot_water_temp, supply_temp, return_temp
```

### Output: energy_separated
```
measurement: energy_separated
tags: house_id, method
fields:
  - actual_energy_kwh
  - heating_energy_kwh
  - dhw_energy_kwh
  - excess_energy_kwh
  - k_value
  - avg_temp_difference
  - degree_hours
  - dhw_events
```

---

## Contact

Questions about this feature: Check git history for context or search conversation logs.

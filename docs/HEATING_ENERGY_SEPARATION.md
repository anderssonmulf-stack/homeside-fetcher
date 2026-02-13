# Heating Energy Separation

## Status: ACTIVE
**Last calibration:** 2026-02-01
**House:** HEM_FJV_Villa_149 (Daggis8)

---

## Overview

This feature separates district heating energy into:
- **Space heating energy** (radiators, floor heating)
- **DHW energy** (domestic hot water / tap water)

## Calibration Results (Feb 2026)

| Metric | Value |
|--------|-------|
| Heat loss coefficient (k) | **0.0685 kW/°C** |
| Calibration method | 15th percentile of daily k values |
| Days analyzed | 12 (Jan 19-30, 2026) |
| Data coverage | 94-99% per day |
| Total consumption | 581 kWh |
| Estimated heating | 487 kWh (84%) |
| Estimated DHW | 94 kWh (16%) |

### Interpretation

At ΔT = 25°C (typical winter):
- Heating power ≈ 1.7 kW
- Daily heating ≈ 40 kWh

The 16% DHW is reasonable for a well-insulated house with efficient DHW system.

---

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
3. Excess energy (actual > estimated) = DHW usage

---

## Files

| File | Purpose |
|------|---------|
| `heating_energy_calibrator.py` | **Main tool** - Calibrates k from real energy data |
| `energy_models/weather_energy_model.py` | Effective outdoor temperature calculation |
| `energy_models/heating_energy_separator.py` | DHW event detection from hot water temp peaks |
| `heating_energy_estimator.py` | Estimates heating from temp difference |

---

## Usage

### Run calibration analysis

```bash
source webgui/venv/bin/activate
INFLUXDB_TOKEN=$(grep INFLUXDB_TOKEN webgui/.env | cut -d'=' -f2) \
INFLUXDB_URL=http://localhost:8086 \
python heating_energy_calibrator.py --house HEM_FJV_Villa_149 --start 2026-01-18
```

### Compare different percentiles

```bash
python heating_energy_calibrator.py --house HEM_FJV_Villa_149 --start 2026-01-18 --compare
```

### Write results to InfluxDB

```bash
python heating_energy_calibrator.py --house HEM_FJV_Villa_149 --start 2026-01-18 --write
```

### Command line options

| Option | Description |
|--------|-------------|
| `--house` | House ID (e.g., HEM_FJV_Villa_149) |
| `--start` | Start date for analysis (YYYY-MM-DD) |
| `--percentile` | Percentile for k calibration (default: 15) |
| `--k` | Override k value (skip auto-calibration) |
| `--compare` | Compare different percentiles |
| `--write` | Write results to InfluxDB |
| `--debug` | Show data point counts per day |

---

## InfluxDB Measurements

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
fields: room_temperature, outdoor_temperature, hot_water_temp
```

### Output: energy_separated
```
measurement: energy_separated
tags: house_id, method
fields:
  - actual_energy_kwh     (daily total)
  - heating_energy_kwh    (estimated heating)
  - dhw_energy_kwh        (excess = actual - heating)
  - excess_energy_kwh     (can be negative)
  - k_value               (calibrated k)
  - k_implied             (k if this day was heating-only)
  - avg_temp_difference   (indoor - effective outdoor)
  - degree_hours          (ΔT × hours)
  - dhw_events            (hot water usage transitions)
  - dhw_minutes           (minutes with elevated hot water)
  - data_coverage         (0.0 to 1.0+)
```

---

## Customer Profile

The calibrated k is stored in customer profiles:

```json
{
  "energy_separation": {
    "enabled": true,
    "method": "k_calibration",
    "heat_loss_k": 0.0685,
    "k_percentile": 15,
    "calibration_date": "2026-02-01",
    "calibration_days": 12,
    "dhw_percentage": 16.1
  }
}
```

---

## Technical Notes

### Percentile Selection

- **Lower percentile (10-15%)**: Finds "heating-only" days with minimal DHW
- **Higher percentile (50%)**: Median includes average DHW in estimate

We use 15th percentile because:
- Gives realistic DHW percentage (16% vs 6% at 25th percentile)
- Only 1 day with negative excess (vs 6 days at 50th percentile)
- Captures the minimum heating baseline

### Data Deduplication

The calibrator automatically removes duplicate records (1-second timestamp offsets)
that can occur from system restarts or data migration.

### Data Coverage Requirements

Days with <80% data coverage are excluded from k calibration to avoid
partial-day bias. Coverage >100% indicates overlapping data (automatically
handled by deduplication).

---

## Web GUI Integration

The energy separation data is displayed in Plotly graphs at:
**https://svenskeb.se/house/HEM_FJV_Villa_149/graphs**

We're using Plotly in the Flask web GUI for charting because:
- Good mobile experience
- Consistent styling with the rest of the web GUI
- Easy to customize per-house

### Planned Graph: Energy Separation

Add a stacked bar chart showing:
- **Heating energy** (blue) - estimated from k × degree-hours
- **DHW energy** (orange) - excess above heating estimate

Location: Below the existing "Energy Consumption" chart in `house_graphs.html`

API endpoint needed: `/api/house/<house_id>/energy-separated`

---

## Future Improvements

1. **Rolling calibration** - Update k as more data accumulates
2. **Hourly separation** - Currently daily, could do hourly for better DHW detection
3. **Multi-house comparison** - Compare k values between houses
4. **Auto-calibration** - Run calibration automatically when enough data exists

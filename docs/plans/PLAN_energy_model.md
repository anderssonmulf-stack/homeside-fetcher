# Energy Model Architecture Plan

## Goal

Decompose district heating energy consumption into:
1. **Tap water heating** - Energy used to heat domestic hot water
2. **Space heating** - Energy used to heat the building

And predict expected heating energy based on weather conditions.

## Why This Matters

- **Understand actual heating efficiency** - Compare predicted vs actual
- **Identify anomalies** - Leaks, insulation problems, system issues
- **Optimize heating** - Know how much energy *should* be used
- **Customer insights** - Show where energy goes

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Energy Analysis Pipeline                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Raw Data   │───▶│  Decomposer  │───▶│   Storage    │      │
│  │  (InfluxDB)  │    │              │    │  (InfluxDB)  │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│        │                    │                                    │
│        │             ┌──────┴──────┐                            │
│        │             ▼             ▼                            │
│        │      tap_water_kwh   heating_kwh                       │
│        │                           │                            │
│        ▼                           ▼                            │
│  ┌──────────────┐    ┌──────────────────────┐                  │
│  │   Weather    │───▶│  Heating Predictor   │                  │
│  │    Model     │    │  (expected vs actual) │                  │
│  └──────────────┘    └──────────────────────┘                  │
│        │                                                        │
│        ▼                                                        │
│  effective_outdoor_temp                                         │
│  (temp + wind + humidity + solar)                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Module 1: Tap Water Detection (DEFERRED)

**Status**: Not reliable for heat-exchanger systems (no tank).

For HomeSide systems with direct district heating:
- Hot water temp RISES when tap water is used (heat exchanger activates)
- No way to estimate volume/energy from temperature alone
- Would need flow meter for accurate measurement

**Future options**:
- Flow meter integration
- Learn typical patterns from energy data
- Manual input from user

---

## Module 2: Weather Energy Model

**File**: `energy_models/weather_energy_model.py`

**Purpose**: Calculate "effective outdoor temperature" that accounts for all weather factors affecting heating need.

**Input** (from SMHI or weather_observation):
- `temperature` (°C)
- `wind_speed` (m/s)
- `humidity` (%)
- `cloud_cover` (%) - proxy for solar radiation
- Optional: `solar_radiation` (W/m²) if available

**Output**:
- `effective_outdoor_temp` (°C) - The temperature it "feels like" for heating purposes

**Model**:
```
effective_temp = T_outdoor
               - wind_chill_factor(wind_speed)
               - humidity_factor(humidity, T_outdoor)
               + solar_gain_factor(solar_radiation, cloud_cover)
```

**Wind chill effect on buildings**:
- Not the same as human wind chill
- Increases convective heat loss from walls
- Empirical: `wind_effect = k_wind × sqrt(wind_speed)` where k_wind ≈ 0.5-1.5

**Humidity effect**:
- Higher humidity = better heat conduction in air
- Cold + humid feels colder
- Effect is small but measurable

**Solar gain**:
- Reduces heating need during day
- Depends on: cloud cover, sun angle, building orientation
- Simplified: `solar_effect = k_solar × (1 - cloud_cover/100) × daylight_factor`

**Strategy pattern**:
```python
class WeatherEnergyModel(Protocol):
    def effective_temperature(self,
                              temp: float,
                              wind: float,
                              humidity: float,
                              cloud_cover: float,
                              timestamp: datetime) -> float:
        ...

class SimpleWeatherModel(WeatherEnergyModel):
    """Basic model with empirical coefficients"""
    ...

class CalibratedWeatherModel(WeatherEnergyModel):
    """Model calibrated to specific building's response"""
    ...
```

---

## Module 3: Energy Decomposer

**File**: `energy_models/energy_decomposer.py`

**Purpose**: Split total district heating into tap water + space heating.

**Input**:
- Hourly total energy (kWh)
- Tap water estimates from Module 1
- Weather data

**Output**:
- `tap_water_kwh` per hour
- `heating_kwh` per hour

**Logic**:
```python
def decompose(total_kwh, tap_water_estimate, weather_data):
    # Tap water component
    tap_water_kwh = tap_water_estimate

    # Sanity check: tap water can't exceed total
    tap_water_kwh = min(tap_water_kwh, total_kwh * 0.8)

    # Remaining is space heating
    heating_kwh = total_kwh - tap_water_kwh

    # Sanity check: heating should correlate with weather
    # If it doesn't, adjust estimates

    return tap_water_kwh, heating_kwh
```

---

## Module 4: Heating Predictor

**File**: `energy_models/heating_predictor.py`

**Purpose**: Predict expected heating energy based on weather and building characteristics.

**Input**:
- `effective_outdoor_temp` from weather model
- `indoor_target_temp` (from profile)
- Building characteristics (thermal coefficient, etc.)

**Output**:
- `expected_heating_kwh` per hour

**Model**:
```
ΔT = indoor_target - effective_outdoor_temp
expected_kwh = building_heat_loss_coefficient × ΔT × hours

Where heat_loss_coefficient is learned from:
- Historical energy data
- Known building parameters
- Or thermal_coefficient from existing thermal_analyzer
```

**Heating Degree Hours (HDH)**:
```
If effective_temp < base_temp (e.g., 17°C):
    HDH = base_temp - effective_temp
Else:
    HDH = 0  # No heating needed

expected_kwh = k × HDH
```

---

## Data Flow

```
Every hour (or on energy data import):

1. Fetch data for time period:
   - energy_consumption (hourly kWh)
   - hot_water_temp (15-min samples)
   - weather (temp, wind, humidity, clouds)

2. Run tap water detection:
   tap_water_kwh = TapWaterDetector.detect(hot_water_temps)

3. Calculate effective outdoor temp:
   eff_temp = WeatherModel.effective_temperature(weather)

4. Decompose energy:
   tap_kwh, heating_kwh = Decomposer.decompose(total, tap_water_kwh)

5. Predict expected heating:
   expected_kwh = HeatingPredictor.predict(eff_temp, building)

6. Store results:
   InfluxDB: energy_analysis measurement
     - tap_water_kwh
     - heating_kwh
     - expected_heating_kwh
     - effective_outdoor_temp
     - heating_efficiency = expected / actual
```

---

## File Structure

```
/opt/dev/homeside-fetcher/
├── energy_models/
│   ├── __init__.py
│   ├── base.py              # Protocol definitions
│   ├── tap_water_detector.py
│   ├── weather_energy_model.py
│   ├── energy_decomposer.py
│   ├── heating_predictor.py
│   └── models/
│       ├── homeside_tap_water.py    # HomeSide-specific detector
│       └── simple_weather.py        # Basic weather model
│
├── scripts/
│   └── run_energy_analysis.py       # Batch analysis script
│
└── (existing files)
```

---

## InfluxDB Measurements

New measurement: `energy_analysis`

| Field | Type | Description |
|-------|------|-------------|
| total_kwh | float | Total district heating energy |
| tap_water_kwh | float | Estimated tap water heating |
| heating_kwh | float | Estimated space heating |
| expected_heating_kwh | float | Predicted heating need |
| effective_outdoor_temp | float | Weather-adjusted outdoor temp |
| heating_efficiency | float | expected/actual ratio |

Tags: `house_id`, `model_version`

---

## Model Versioning

Important for comparing models over time:

```python
class ModelRegistry:
    """Track which models were used for each analysis"""

    def register_run(self, house_id, timestamp, models_used):
        # Store: tap_water_model=v1.2, weather_model=v2.0, etc.
        ...
```

This allows:
- Re-running analysis with new models
- Comparing model accuracy
- A/B testing improvements

---

## Implementation Order

### Phase 1: Foundation
1. Create `energy_models/` package structure
2. Define Protocol classes (interfaces)
3. Implement simple weather model
4. Store results in InfluxDB

### Phase 2: Tap Water Detection
1. Analyze hot_water_temp patterns in existing data
2. Implement HomeSide tap water detector
3. Validate against known usage patterns

### Phase 3: Energy Decomposition
1. Implement decomposer
2. Run on historical data
3. Visualize in webgui (new graph!)

### Phase 4: Heating Prediction
1. Implement heating predictor
2. Calibrate per building
3. Compare predicted vs actual

### Phase 5: Refinement
1. Tune model parameters
2. Add more weather factors
3. Learn building-specific coefficients

---

## Open Questions

1. **Tap water tank size** - Can we learn this from data patterns?
2. **Solar radiation data** - Does SMHI provide this? Or estimate from cloud cover?
3. **Building orientation** - Affects solar gain significantly
4. **Historical baseline** - How much data needed to calibrate?
5. **Model update frequency** - Daily? Weekly? After each import?

---

## Success Metrics

- **Tap water detection accuracy**: Compare estimated vs actual (if metered)
- **Heating prediction error**: RMSE of predicted vs actual kWh
- **Decomposition stability**: Consistent ratios over time
- **Model improvement**: Track error reduction with model updates

"""
Weather Energy Model

Calculates "effective outdoor temperature" that accounts for all weather
factors affecting building heat loss:
- Temperature (primary factor)
- Wind speed (increases convective heat loss)
- Humidity (affects heat conduction)
- Solar radiation (reduces heating need)

The effective temperature represents what the outdoor temperature would
need to be in calm, dry, overcast conditions to cause the same heat loss.
"""

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol

from astral import LocationInfo
from astral.sun import sun, elevation


@dataclass
class WeatherConditions:
    """Weather conditions for a single point in time."""
    timestamp: datetime
    temperature: float          # °C
    wind_speed: float = 0.0     # m/s
    humidity: float = 50.0      # % (0-100)
    cloud_cover: float = 8.0    # octas (0-8, 8 = fully overcast)

    # Location for solar calculations (optional)
    latitude: Optional[float] = None
    longitude: Optional[float] = None


@dataclass
class EffectiveTemperature:
    """Result of effective temperature calculation with breakdown."""
    effective_temp: float       # Final effective temperature (°C)
    base_temp: float            # Original outdoor temperature
    wind_effect: float          # Temperature adjustment from wind (negative = colder)
    humidity_effect: float      # Temperature adjustment from humidity
    solar_effect: float         # Temperature adjustment from sun (positive = warmer)

    # Detailed factors for debugging/explanation
    sun_elevation: Optional[float] = None  # Degrees above horizon
    solar_intensity: Optional[float] = None  # 0-1 estimated intensity

    def to_dict(self) -> dict:
        """Convert to dictionary for storage/display."""
        return {
            'effective_temp': round(self.effective_temp, 2),
            'base_temp': round(self.base_temp, 2),
            'wind_effect': round(self.wind_effect, 2),
            'humidity_effect': round(self.humidity_effect, 2),
            'solar_effect': round(self.solar_effect, 2),
            'sun_elevation': round(self.sun_elevation, 1) if self.sun_elevation else None,
            'solar_intensity': round(self.solar_intensity, 2) if self.solar_intensity else None,
        }


class WeatherEnergyModel(Protocol):
    """Protocol for weather energy models (strategy pattern)."""

    def effective_temperature(self, conditions: WeatherConditions) -> EffectiveTemperature:
        """
        Calculate effective outdoor temperature for heating purposes.

        Args:
            conditions: Current weather conditions

        Returns:
            EffectiveTemperature with breakdown of contributing factors
        """
        ...

    @property
    def model_version(self) -> str:
        """Model identifier for tracking which model was used."""
        ...


class SimpleWeatherModel(WeatherEnergyModel):
    """
    Simple weather model with empirical coefficients.

    Based on building heat loss physics:
    - Wind increases convective heat transfer from building envelope
    - Humidity affects thermal conductivity of air
    - Solar radiation provides free heating

    Coefficients can be tuned per building type or calibrated from data.
    """

    # Default coefficients (can be overridden per building)
    DEFAULT_WIND_COEFFICIENT = 0.56     # °C per sqrt(m/s) - reduced 20% from 0.7, suitable for brick/metal buildings
    DEFAULT_HUMIDITY_COEFFICIENT = 0.01  # °C per % humidity above 50%
    DEFAULT_SOLAR_COEFFICIENT = 6.0      # Max °C gain at full sun (doubled based on real-world observations)

    def __init__(
        self,
        wind_coefficient: float = DEFAULT_WIND_COEFFICIENT,
        humidity_coefficient: float = DEFAULT_HUMIDITY_COEFFICIENT,
        solar_coefficient: float = DEFAULT_SOLAR_COEFFICIENT,
    ):
        """
        Initialize the weather model with coefficients.

        Args:
            wind_coefficient: How much each sqrt(m/s) of wind cools (°C)
            humidity_coefficient: How much each % humidity above 50% cools (°C)
            solar_coefficient: Maximum temperature gain from full sun (°C)
        """
        self.wind_coefficient = wind_coefficient
        self.humidity_coefficient = humidity_coefficient
        self.solar_coefficient = solar_coefficient

    @property
    def model_version(self) -> str:
        return f"simple_v1.0_w{self.wind_coefficient}_h{self.humidity_coefficient}_s{self.solar_coefficient}"

    def effective_temperature(self, conditions: WeatherConditions) -> EffectiveTemperature:
        """
        Calculate effective outdoor temperature.

        Formula:
            effective = base_temp - wind_effect - humidity_effect + solar_effect

        Where:
            wind_effect = k_wind × sqrt(wind_speed)
            humidity_effect = k_humidity × max(0, humidity - 50)
            solar_effect = k_solar × solar_intensity × (1 - cloud_cover/8)
        """
        base_temp = conditions.temperature

        # Wind effect: increases with square root of wind speed
        # This follows convective heat transfer physics
        wind_effect = self.wind_coefficient * math.sqrt(max(0, conditions.wind_speed))

        # Humidity effect: cold + humid air conducts heat better
        # Only significant when humidity is above ~50%
        humidity_above_baseline = max(0, conditions.humidity - 50)
        humidity_effect = self.humidity_coefficient * humidity_above_baseline

        # Solar effect: depends on sun position and cloud cover
        solar_effect, sun_elev, solar_intensity = self._calculate_solar_effect(conditions)

        # Calculate effective temperature
        effective = base_temp - wind_effect - humidity_effect + solar_effect

        return EffectiveTemperature(
            effective_temp=effective,
            base_temp=base_temp,
            wind_effect=-wind_effect,  # Negative because it cools
            humidity_effect=-humidity_effect,  # Negative because it cools
            solar_effect=solar_effect,  # Positive because it warms
            sun_elevation=sun_elev,
            solar_intensity=solar_intensity,
        )

    def _calculate_solar_effect(self, conditions: WeatherConditions) -> tuple[float, Optional[float], Optional[float]]:
        """
        Calculate solar heating effect.

        Returns:
            (solar_effect_celsius, sun_elevation_degrees, solar_intensity_0_to_1)
        """
        # If no location provided, assume no solar effect
        if conditions.latitude is None or conditions.longitude is None:
            # Fallback: simple cloud-based estimate
            cloud_fraction = conditions.cloud_cover / 8.0
            solar_intensity = 1.0 - cloud_fraction
            solar_effect = self.solar_coefficient * solar_intensity * 0.5  # Assume mid-day average
            return solar_effect, None, solar_intensity

        try:
            # Calculate sun position
            location = LocationInfo(
                latitude=conditions.latitude,
                longitude=conditions.longitude
            )

            sun_elev = elevation(location.observer, conditions.timestamp)

            # No solar effect if sun is below horizon
            if sun_elev <= 0:
                return 0.0, sun_elev, 0.0

            # Solar intensity based on sun elevation
            # At 90° (noon in tropics): intensity = 1.0
            # At low angles: intensity drops due to longer path through atmosphere
            # Simplified model: intensity = sin(elevation)
            raw_intensity = math.sin(math.radians(sun_elev))

            # Reduce by cloud cover (0-8 octas)
            # 0 = clear, 8 = overcast
            cloud_fraction = conditions.cloud_cover / 8.0
            cloud_transmission = 1.0 - (cloud_fraction * 0.9)  # Clouds block up to 90%

            solar_intensity = raw_intensity * cloud_transmission

            # Convert to temperature effect
            solar_effect = self.solar_coefficient * solar_intensity

            return solar_effect, sun_elev, solar_intensity

        except Exception:
            # Fallback if astral calculation fails
            cloud_fraction = conditions.cloud_cover / 8.0
            solar_intensity = (1.0 - cloud_fraction) * 0.5
            solar_effect = self.solar_coefficient * solar_intensity
            return solar_effect, None, solar_intensity


class CalibratedWeatherModel(SimpleWeatherModel):
    """
    Weather model with coefficients calibrated to a specific building.

    Learns optimal coefficients from historical data by comparing
    predicted vs actual heating energy.
    """

    def __init__(
        self,
        building_id: str,
        wind_coefficient: float = SimpleWeatherModel.DEFAULT_WIND_COEFFICIENT,
        humidity_coefficient: float = SimpleWeatherModel.DEFAULT_HUMIDITY_COEFFICIENT,
        solar_coefficient: float = SimpleWeatherModel.DEFAULT_SOLAR_COEFFICIENT,
        calibration_date: Optional[datetime] = None,
        calibration_error: Optional[float] = None,
    ):
        super().__init__(wind_coefficient, humidity_coefficient, solar_coefficient)
        self.building_id = building_id
        self.calibration_date = calibration_date
        self.calibration_error = calibration_error

    @property
    def model_version(self) -> str:
        base = super().model_version
        cal_date = self.calibration_date.strftime('%Y%m%d') if self.calibration_date else 'uncal'
        return f"calibrated_{self.building_id}_{cal_date}_{base}"

    def to_dict(self) -> dict:
        """Serialize model parameters for storage."""
        return {
            'building_id': self.building_id,
            'wind_coefficient': self.wind_coefficient,
            'humidity_coefficient': self.humidity_coefficient,
            'solar_coefficient': self.solar_coefficient,
            'calibration_date': self.calibration_date.isoformat() if self.calibration_date else None,
            'calibration_error': self.calibration_error,
            'model_version': self.model_version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CalibratedWeatherModel':
        """Load model from stored parameters."""
        cal_date = None
        if data.get('calibration_date'):
            cal_date = datetime.fromisoformat(data['calibration_date'])

        return cls(
            building_id=data['building_id'],
            wind_coefficient=data.get('wind_coefficient', cls.DEFAULT_WIND_COEFFICIENT),
            humidity_coefficient=data.get('humidity_coefficient', cls.DEFAULT_HUMIDITY_COEFFICIENT),
            solar_coefficient=data.get('solar_coefficient', cls.DEFAULT_SOLAR_COEFFICIENT),
            calibration_date=cal_date,
            calibration_error=data.get('calibration_error'),
        )


# Singleton for default model
_default_model: Optional[WeatherEnergyModel] = None


def get_weather_model(
    model_type: str = 'simple',
    **kwargs
) -> WeatherEnergyModel:
    """
    Factory function to get a weather energy model.

    Args:
        model_type: 'simple' or 'calibrated'
        **kwargs: Model-specific parameters

    Returns:
        WeatherEnergyModel instance
    """
    global _default_model

    if model_type == 'simple':
        if not kwargs and _default_model is not None:
            return _default_model
        model = SimpleWeatherModel(**kwargs)
        if not kwargs:
            _default_model = model
        return model

    elif model_type == 'calibrated':
        if 'building_id' not in kwargs:
            raise ValueError("building_id required for calibrated model")
        return CalibratedWeatherModel(**kwargs)

    else:
        raise ValueError(f"Unknown model type: {model_type}")


# =============================================================================
# Utility functions for common operations
# =============================================================================

def calculate_heating_degree_hours(
    effective_temp: float,
    base_temp: float = 17.0,
    hours: float = 1.0
) -> float:
    """
    Calculate Heating Degree Hours (HDH).

    HDH is a measure of how much heating is needed.

    Args:
        effective_temp: Effective outdoor temperature (°C)
        base_temp: Temperature below which heating is needed (default 17°C)
        hours: Number of hours (default 1)

    Returns:
        Heating degree hours (always >= 0)
    """
    if effective_temp >= base_temp:
        return 0.0
    return (base_temp - effective_temp) * hours


def estimate_heating_energy(
    effective_temp: float,
    heat_loss_coefficient: float,
    indoor_temp: float = 21.0,
    hours: float = 1.0
) -> float:
    """
    Estimate heating energy needed.

    Args:
        effective_temp: Effective outdoor temperature (°C)
        heat_loss_coefficient: Building heat loss (kW/°C)
        indoor_temp: Target indoor temperature (°C)
        hours: Number of hours

    Returns:
        Estimated energy in kWh
    """
    delta_t = max(0, indoor_temp - effective_temp)
    power_kw = heat_loss_coefficient * delta_t
    return power_kw * hours

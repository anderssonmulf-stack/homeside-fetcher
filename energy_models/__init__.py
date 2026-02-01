"""
Energy Models Package

Modular components for energy analysis:
- Weather energy model: Calculate effective outdoor temperature
- Heating energy separator: Split district heating into space heating vs DHW
- Heating predictor: Predict expected heating energy (future)

Each module uses a strategy pattern for swappable implementations.
"""

from .weather_energy_model import (
    WeatherEnergyModel,
    SimpleWeatherModel,
    get_weather_model,
)

from .heating_energy_separator import (
    HomeSideOnDemandDHWSeparator,
    DHWEvent,
    EnergySeparationResult,
    get_energy_separator,
)

__all__ = [
    # Weather models
    'WeatherEnergyModel',
    'SimpleWeatherModel',
    'get_weather_model',
    # Energy separation
    'HomeSideOnDemandDHWSeparator',
    'DHWEvent',
    'EnergySeparationResult',
    'get_energy_separator',
]

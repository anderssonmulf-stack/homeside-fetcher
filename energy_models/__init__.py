"""
Energy Models Package

Modular components for energy analysis:
- Weather energy model: Calculate effective outdoor temperature
- Heating predictor: Predict expected heating energy
- Energy decomposer: Split total energy into components

Each module uses a strategy pattern for swappable implementations.
"""

from .weather_energy_model import (
    WeatherEnergyModel,
    SimpleWeatherModel,
    get_weather_model,
)

__all__ = [
    'WeatherEnergyModel',
    'SimpleWeatherModel',
    'get_weather_model',
]

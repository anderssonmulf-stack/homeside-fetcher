#!/usr/bin/env python3
"""
Energy Forecaster

Predicts heating energy consumption based on:
- Weather forecast (72h)
- Building heat loss coefficient (k)
- Effective outdoor temperature (wind, humidity, solar effects)

Core formula:
    heating_power = k × (T_indoor - T_effective_outdoor)
    heating_energy = heating_power × hours

Where k is calibrated from historical data (typically 0.05-0.15 kW/°C).

Example: k=0.0685, indoor=22°C, effective_outdoor=-5°C
    power = 0.0685 × (22 - (-5)) = 0.0685 × 27 = 1.85 kW
    energy/day = 1.85 × 24 = 44.4 kWh
"""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

from energy_models.weather_energy_model import WeatherConditions, SimpleWeatherModel

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


@dataclass
class EnergyForecastPoint:
    """Single energy forecast point."""
    timestamp: datetime
    outdoor_temp: float
    effective_temp: float
    wind_effect: float
    solar_effect: float
    humidity_effect: float
    heating_power_kw: float
    heating_energy_kwh: float  # For this hour
    lead_time_hours: float  # Hours from forecast generation

    # Weather details
    wind_speed: Optional[float] = None
    cloud_cover: Optional[float] = None
    humidity: Optional[float] = None

    @property
    def timestamp_swedish(self) -> str:
        if self.timestamp.tzinfo is None:
            ts = self.timestamp.replace(tzinfo=timezone.utc)
        else:
            ts = self.timestamp
        return ts.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d %H:%M')


@dataclass
class EnergyForecastSummary:
    """Summary of energy forecast for a period."""
    total_energy_kwh: float
    avg_power_kw: float
    peak_power_kw: float
    avg_outdoor_temp: float
    min_outdoor_temp: float
    hours: int


class EnergyForecaster:
    """
    Forecasts heating energy consumption based on weather forecast and building characteristics.

    Uses the calibrated heat loss coefficient (k) to predict:
    - Hourly heating power (kW)
    - Hourly heating energy (kWh)
    - 24h/72h energy totals

    Supports ML2 learned coefficients for solar/wind sensitivity.
    """

    def __init__(
        self,
        heat_loss_k: float,
        target_indoor_temp: float = 22.0,
        latitude: float = 58.41,
        longitude: float = 15.62,
        logger = None,
        solar_coefficient_ml2: Optional[float] = None,
        wind_coefficient_ml2: Optional[float] = None,
        solar_confidence_ml2: float = 0.0
    ):
        """
        Initialize energy forecaster.

        Args:
            heat_loss_k: Building heat loss coefficient (kW/°C)
            target_indoor_temp: Target indoor temperature (°C)
            latitude: Location latitude for solar calculations
            longitude: Location longitude for solar calculations
            logger: Optional logger instance
            solar_coefficient_ml2: Learned solar coefficient (ML2 model)
            wind_coefficient_ml2: Learned wind coefficient (ML2 model)
            solar_confidence_ml2: Confidence in learned ML2 coefficients (0-1)
        """
        self.heat_loss_k = heat_loss_k
        self.target_indoor_temp = target_indoor_temp
        self.latitude = latitude
        self.longitude = longitude
        self.logger = logger

        # ML2 learned coefficients
        self.solar_coefficient_ml2 = solar_coefficient_ml2
        self.wind_coefficient_ml2 = wind_coefficient_ml2
        self.solar_confidence_ml2 = solar_confidence_ml2

        # Weather model for effective temperature calculation
        # Use learned coefficients if available with sufficient confidence
        if solar_coefficient_ml2 is not None and solar_confidence_ml2 >= 0.3:
            self.weather_model = SimpleWeatherModel(
                solar_coefficient=solar_coefficient_ml2,
                wind_coefficient=wind_coefficient_ml2 or 0.15
            )
            if logger:
                logger.info(
                    f"Using ML2 weather model: solar={solar_coefficient_ml2:.1f}, "
                    f"wind={wind_coefficient_ml2 or 0.15:.2f}"
                )
        else:
            self.weather_model = SimpleWeatherModel()

    @classmethod
    def from_profile(cls, profile, latitude: float, longitude: float, logger=None) -> 'EnergyForecaster':
        """
        Create EnergyForecaster from a CustomerProfile with learned coefficients.

        Args:
            profile: CustomerProfile instance
            latitude: Location latitude
            longitude: Location longitude
            logger: Optional logger

        Returns:
            EnergyForecaster with ML2 coefficients if available
        """
        heat_loss_k = profile.energy_separation.heat_loss_k
        if not heat_loss_k:
            raise ValueError("Profile does not have calibrated heat_loss_k")

        weather_coeffs = profile.learned.weather_coefficients

        return cls(
            heat_loss_k=heat_loss_k,
            target_indoor_temp=profile.comfort.target_indoor_temp,
            latitude=latitude,
            longitude=longitude,
            logger=logger,
            solar_coefficient_ml2=weather_coeffs.solar_coefficient_ml2,
            wind_coefficient_ml2=weather_coeffs.wind_coefficient_ml2,
            solar_confidence_ml2=weather_coeffs.solar_confidence_ml2
        )

    def generate_forecast(
        self,
        weather_forecast: List[Dict],
        current_indoor_temp: Optional[float] = None
    ) -> List[EnergyForecastPoint]:
        """
        Generate hourly energy forecast from weather forecast.

        Args:
            weather_forecast: List of weather forecast points from SMHI:
                [{'time': str, 'temp': float, 'hour': float,
                  'wind_speed': float, 'humidity': float, 'cloud_cover': float}, ...]
            current_indoor_temp: Current indoor temp (defaults to target)

        Returns:
            List of EnergyForecastPoint for each forecast hour
        """
        if not weather_forecast:
            return []

        indoor_temp = current_indoor_temp or self.target_indoor_temp
        forecast_points = []

        for wp in weather_forecast:
            try:
                # Parse timestamp
                time_str = wp.get('time')
                if not time_str:
                    continue

                timestamp = datetime.fromisoformat(time_str.replace('Z', '+00:00'))

                # Get weather parameters
                outdoor_temp = wp.get('temp')
                if outdoor_temp is None:
                    continue

                wind_speed = wp.get('wind_speed', 0.0)
                humidity = wp.get('humidity', 80.0)  # Default to typical Nordic humidity
                cloud_cover = wp.get('cloud_cover', 4.0)  # Default to partly cloudy
                lead_time = wp.get('hour', 0.0)

                # Calculate effective outdoor temperature
                conditions = WeatherConditions(
                    temperature=outdoor_temp,
                    wind_speed=wind_speed or 0.0,
                    humidity=humidity or 80.0,
                    cloud_cover=cloud_cover or 4.0,
                    latitude=self.latitude,
                    longitude=self.longitude,
                    timestamp=timestamp
                )

                eff_result = self.weather_model.effective_temperature(conditions)
                effective_temp = eff_result.effective_temp

                # Calculate heating power: P = k × ΔT
                temp_diff = indoor_temp - effective_temp

                # Only positive heating (no cooling)
                if temp_diff > 0:
                    heating_power = self.heat_loss_k * temp_diff
                else:
                    heating_power = 0.0

                # Energy for 1 hour = power (kW) × 1 (h) = kWh
                heating_energy = heating_power

                forecast_points.append(EnergyForecastPoint(
                    timestamp=timestamp,
                    outdoor_temp=outdoor_temp,
                    effective_temp=effective_temp,
                    wind_effect=eff_result.wind_effect,
                    solar_effect=eff_result.solar_effect,
                    humidity_effect=eff_result.humidity_effect,
                    heating_power_kw=round(heating_power, 3),
                    heating_energy_kwh=round(heating_energy, 3),
                    lead_time_hours=round(lead_time, 1),
                    wind_speed=wind_speed,
                    cloud_cover=cloud_cover,
                    humidity=humidity
                ))

            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error processing forecast point: {e}")
                continue

        return forecast_points

    def get_summary(
        self,
        forecast_points: List[EnergyForecastPoint],
        hours: int = 24
    ) -> Optional[EnergyForecastSummary]:
        """
        Get summary statistics for forecast period.

        Args:
            forecast_points: List of forecast points
            hours: Number of hours to summarize (default 24)

        Returns:
            EnergyForecastSummary or None if insufficient data
        """
        if not forecast_points:
            return None

        # Limit to requested hours
        points = forecast_points[:hours]

        if not points:
            return None

        total_energy = sum(p.heating_energy_kwh for p in points)
        powers = [p.heating_power_kw for p in points]
        outdoor_temps = [p.outdoor_temp for p in points]

        return EnergyForecastSummary(
            total_energy_kwh=round(total_energy, 1),
            avg_power_kw=round(sum(powers) / len(powers), 2),
            peak_power_kw=round(max(powers), 2),
            avg_outdoor_temp=round(sum(outdoor_temps) / len(outdoor_temps), 1),
            min_outdoor_temp=round(min(outdoor_temps), 1),
            hours=len(points)
        )

    def get_daily_totals(
        self,
        forecast_points: List[EnergyForecastPoint]
    ) -> Dict[str, float]:
        """
        Get energy totals grouped by day.

        Returns:
            Dict of {date_str: total_kwh}
        """
        daily = {}

        for p in forecast_points:
            date_str = p.timestamp.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d')
            daily[date_str] = daily.get(date_str, 0.0) + p.heating_energy_kwh

        return {k: round(v, 1) for k, v in daily.items()}


def format_energy_forecast(
    forecast_points: List[EnergyForecastPoint],
    summary_24h: Optional[EnergyForecastSummary] = None,
    summary_72h: Optional[EnergyForecastSummary] = None
) -> str:
    """Format energy forecast for display."""
    lines = []

    if summary_24h:
        lines.append(f"⚡ Energy Forecast (24h): {summary_24h.total_energy_kwh:.1f} kWh")
        lines.append(f"   Avg power: {summary_24h.avg_power_kw:.2f} kW, Peak: {summary_24h.peak_power_kw:.2f} kW")
        lines.append(f"   Outdoor: avg {summary_24h.avg_outdoor_temp:.1f}°C, min {summary_24h.min_outdoor_temp:.1f}°C")

    if summary_72h:
        lines.append(f"⚡ Energy Forecast (72h): {summary_72h.total_energy_kwh:.1f} kWh")

    return '\n'.join(lines)

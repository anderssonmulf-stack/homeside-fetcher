#!/usr/bin/env python3
"""
Weather Forecast Module
Integrates with SMHI (Swedish Meteorological and Hydrological Institute) API
to fetch temperature forecasts for heating optimization
"""

import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from astral import LocationInfo
from astral.sun import sun


class WeatherForecast:
    """
    Fetches weather forecasts from SMHI API

    SMHI provides free, open weather data for Sweden with hourly forecasts
    up to 10 days ahead. We use this to predict temperature trends for
    optimizing heating schedules.
    """

    def __init__(self, latitude: float, longitude: float, logger):
        """
        Initialize weather forecast client

        Args:
            latitude: Latitude for location (e.g., 58.59 for Linköping)
            longitude: Longitude for location (e.g., 16.19 for Linköping)
            logger: Logger instance for debugging
        """
        self.latitude = latitude
        self.longitude = longitude
        self.logger = logger
        self.base_url = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2"

    def get_forecast(self, hours_ahead: int = 12) -> Optional[List[Dict]]:
        """
        Get temperature forecast for the next N hours

        Args:
            hours_ahead: Number of hours to fetch forecast for (default 12)

        Returns:
            List of forecast data points with timestamp and temperature,
            or None if fetch failed

        Example return:
            [
                {'time': '2026-01-18T12:00:00Z', 'temp': 5.2, 'hour': 1},
                {'time': '2026-01-18T13:00:00Z', 'temp': 5.8, 'hour': 2},
                ...
            ]
        """
        try:
            # SMHI API endpoint for point forecasts
            url = f"{self.base_url}/geotype/point/lon/{self.longitude}/lat/{self.latitude}/data.json"

            self.logger.info(f"Fetching SMHI forecast for lat={self.latitude}, lon={self.longitude}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Extract hourly temperature forecasts
            forecasts = []
            now = datetime.now(timezone.utc)
            cutoff_time = now + timedelta(hours=hours_ahead)

            for time_series in data.get('timeSeries', []):
                # Parse forecast timestamp
                valid_time_str = time_series.get('validTime')
                valid_time = datetime.fromisoformat(valid_time_str.replace('Z', '+00:00'))

                # Only include forecasts within our time window
                if valid_time > cutoff_time:
                    break

                if valid_time < now:
                    continue

                # Extract forecast parameters
                temp = None
                cloud_cover = None
                solar_rad = None

                for param in time_series.get('parameters', []):
                    param_name = param.get('name')
                    if param_name == 't':  # Temperature at 2m
                        temp = param.get('values', [None])[0]
                    elif param_name == 'tcc_mean':  # Total cloud cover (0-8 octas)
                        cloud_cover = param.get('values', [None])[0]
                    elif param_name == 'Wsymb2':  # Weather symbol (includes cloud info)
                        pass  # We use tcc_mean instead

                if temp is not None:
                    hours_from_now = (valid_time - now).total_seconds() / 3600
                    forecast_point = {
                        'time': valid_time_str,
                        'temp': temp,
                        'hour': round(hours_from_now, 1)
                    }

                    if cloud_cover is not None:
                        forecast_point['cloud_cover'] = cloud_cover

                    forecasts.append(forecast_point)

            if forecasts:
                self.logger.info(f"Retrieved {len(forecasts)} forecast points (next {hours_ahead}h)")
                return forecasts
            else:
                self.logger.warning("No forecast data available")
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch SMHI forecast: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing SMHI forecast: {str(e)}")
            return None

    def get_temp_trend(self, hours_ahead: int = 12) -> Optional[Dict]:
        """
        Analyze temperature trend for heating decisions

        Returns:
            Dictionary with trend analysis:
            {
                'current_temp': float,  # Current forecast temperature
                'avg_temp': float,      # Average temp over period
                'max_temp': float,      # Maximum temp in period
                'min_temp': float,      # Minimum temp in period
                'trend': str,           # 'rising', 'falling', or 'stable'
                'trend_symbol': str,    # '↑', '↓', or '→'
                'change': float,        # Expected temp change (degrees)
                'avg_cloud_cover': float,  # Average cloud cover (0-8 octas)
                'cloud_condition': str  # 'clear', 'partly cloudy', 'cloudy', 'overcast'
            }
        """
        forecasts = self.get_forecast(hours_ahead)
        if not forecasts:
            return None

        temps = [f['temp'] for f in forecasts]
        cloud_covers = [f.get('cloud_cover') for f in forecasts if 'cloud_cover' in f]

        # Calculate trend
        first_temp = temps[0]
        last_temp = temps[-1]
        temp_change = last_temp - first_temp

        # Determine trend (>1°C change = significant)
        if temp_change > 1.0:
            trend = 'rising'
            trend_symbol = '↑'
        elif temp_change < -1.0:
            trend = 'falling'
            trend_symbol = '↓'
        else:
            trend = 'stable'
            trend_symbol = '→'

        # Analyze cloud cover
        avg_cloud = sum(cloud_covers) / len(cloud_covers) if cloud_covers else None
        if avg_cloud is not None:
            if avg_cloud < 2:
                cloud_condition = 'clear'
            elif avg_cloud < 5:
                cloud_condition = 'partly cloudy'
            elif avg_cloud < 7:
                cloud_condition = 'cloudy'
            else:
                cloud_condition = 'overcast'
        else:
            cloud_condition = 'unknown'

        analysis = {
            'current_temp': first_temp,
            'avg_temp': sum(temps) / len(temps),
            'max_temp': max(temps),
            'min_temp': min(temps),
            'trend': trend,
            'trend_symbol': trend_symbol,
            'change': temp_change,
            'forecast_hours': hours_ahead,
            'avg_cloud_cover': avg_cloud,
            'cloud_condition': cloud_condition
        }

        self.logger.info(
            f"Temp trend: {trend} {trend_symbol} "
            f"(current: {first_temp:.1f}°C, "
            f"change: {temp_change:+.1f}°C over {hours_ahead}h, "
            f"clouds: {cloud_condition})"
        )

        return analysis

    def is_nighttime(self) -> bool:
        """
        Check if it's currently nighttime (sun is down) based on location

        Returns:
            True if sun is below horizon, False otherwise
        """
        try:
            # Create location info for astral calculations
            location = LocationInfo(
                name="Home",
                region="Sweden",
                timezone="Europe/Stockholm",
                latitude=self.latitude,
                longitude=self.longitude
            )

            # Get today's sunrise/sunset times
            s = sun(location.observer, date=datetime.now())
            sunrise = s['sunrise']
            sunset = s['sunset']

            # Get current time (timezone-aware)
            now = datetime.now(timezone.utc)

            # Check if we're before sunrise or after sunset
            is_night = now < sunrise or now > sunset

            return is_night

        except Exception as e:
            self.logger.warning(f"Failed to calculate sunrise/sunset: {str(e)}")
            # Fallback: simple time-based check (20:00 - 06:00 local time)
            now_hour = datetime.now().hour
            return now_hour >= 20 or now_hour < 6

    def should_reduce_heating(
        self,
        current_indoor_temp: float,
        target_temp: float = 21.0,
        temp_margin: float = 0.5
    ) -> Dict:
        """
        Determine if heating should be reduced based on forecast

        Logic:
        - If outdoor temperature is rising AND
        - Indoor temp is near or above target
        - Then reduce heating to save energy
        - Account for solar radiation affecting outdoor temp sensor

        Args:
            current_indoor_temp: Current indoor temperature
            target_temp: Desired indoor temperature
            temp_margin: Temperature margin for decision (°C)

        Returns:
            Dictionary with recommendation:
            {
                'reduce_heating': bool,
                'reason': str,
                'confidence': float,  # 0.0-1.0
                'solar_factor': str   # 'high', 'medium', 'low' - solar influence on sensor
            }
        """
        trend = self.get_temp_trend(hours_ahead=12)
        if not trend:
            return {
                'reduce_heating': False,
                'reason': 'No forecast data available',
                'confidence': 0.0,
                'solar_factor': 'unknown'
            }

        # Assess solar influence on outdoor temperature sensor
        # First check if it's nighttime - no solar influence when sun is down
        if self.is_nighttime():
            solar_factor = 'none'
            solar_adjustment = 0.0
        else:
            # During daytime: clear skies = high solar influence on SW-facing sensor
            cloud_cover = trend.get('avg_cloud_cover', 8)
            if cloud_cover < 2:
                solar_factor = 'high'
                solar_adjustment = 2.0  # Sensor reads 2°C higher in direct sun
            elif cloud_cover < 5:
                solar_factor = 'medium'
                solar_adjustment = 1.0
            else:
                solar_factor = 'low'
                solar_adjustment = 0.0

        # Decision logic
        indoor_above_target = current_indoor_temp >= (target_temp - temp_margin)
        outdoor_rising = trend['trend'] == 'rising'
        # Adjust temp change by solar influence (sensor shows higher temps in sun)
        actual_temp_change = trend['change'] - solar_adjustment
        outdoor_warming_significantly = actual_temp_change > 2.0

        if indoor_above_target and outdoor_rising:
            confidence = min(1.0, abs(actual_temp_change) / 5.0)  # Max confidence at 5°C change
            reason_parts = [f"Outdoor temp {trend['trend_symbol']} {trend['change']:+.1f}°C"]
            if solar_adjustment > 0:
                reason_parts.append(f"(~{actual_temp_change:+.1f}°C actual, {solar_factor} solar influence)")
            reason_parts.append(f"indoor at {current_indoor_temp:.1f}°C")

            return {
                'reduce_heating': True,
                'reason': ", ".join(reason_parts),
                'confidence': confidence,
                'forecast_change': trend['change'],
                'current_indoor': current_indoor_temp,
                'solar_factor': solar_factor,
                'cloud_condition': trend.get('cloud_condition', 'unknown')
            }
        elif indoor_above_target and outdoor_warming_significantly:
            # Even if not rising overall, significant warming warrants reduction
            return {
                'reduce_heating': True,
                'reason': f"Significant outdoor warming expected ({actual_temp_change:+.1f}°C after solar adjustment)",
                'confidence': 0.8,
                'forecast_change': trend['change'],
                'current_indoor': current_indoor_temp,
                'solar_factor': solar_factor,
                'cloud_condition': trend.get('cloud_condition', 'unknown')
            }
        else:
            return {
                'reduce_heating': False,
                'reason': f"Heating needed ({trend['trend_symbol']} {trend['trend']}, indoor: {current_indoor_temp:.1f}°C, {trend.get('cloud_condition', 'unknown')})",
                'confidence': 0.5,
                'forecast_change': trend.get('change', 0),
                'current_indoor': current_indoor_temp,
                'solar_factor': solar_factor,
                'cloud_condition': trend.get('cloud_condition', 'unknown')
            }

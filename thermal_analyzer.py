#!/usr/bin/env python3
"""
Thermal Analyzer Module
Learns building thermal dynamics over time to enable predictive heating control

For buildings with:
- Floor heating (slow response)
- Concrete foundation (high thermal mass)
- Brick exterior walls (good insulation)
- Expected thermal lag: 6-12 hours
"""

from typing import List, Dict, Optional
from datetime import datetime, timedelta, timezone
import statistics


def _ensure_timezone_aware(dt: datetime) -> datetime:
    """Ensure a datetime is timezone-aware (UTC if naive)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


class ThermalAnalyzer:
    """
    Analyzes and learns building thermal response characteristics

    The thermal coefficient represents how the building responds to
    outdoor temperature changes and heating inputs over time.
    """

    def __init__(self, logger, min_samples: int = 24, influx=None):
        """
        Initialize thermal analyzer

        Args:
            logger: Logger instance
            min_samples: Minimum data points needed for analysis (default 24 = 6 hours at 15min intervals)
            influx: Optional InfluxDBWriter instance for persistence
        """
        self.logger = logger
        self.min_samples = min_samples
        self.influx = influx
        self.historical_data = []

        # Load historical data from InfluxDB if available
        if self.influx:
            self._load_historical_data()

    def _load_historical_data(self):
        """Load historical thermal data from InfluxDB on startup."""
        try:
            history = self.influx.read_thermal_history(days=7)
            if history:
                for data in history:
                    # Parse timestamp and add to historical data
                    data_point = data.copy()
                    if isinstance(data['timestamp'], str):
                        dt = datetime.fromisoformat(
                            data['timestamp'].replace('Z', '+00:00')
                        )
                    else:
                        dt = data['timestamp']
                    # Ensure timezone-aware
                    data_point['timestamp_dt'] = _ensure_timezone_aware(dt)
                    self.historical_data.append(data_point)

                self.logger.info(
                    f"Loaded {len(self.historical_data)} historical data points from InfluxDB"
                )
        except Exception as e:
            self.logger.error(f"Failed to load thermal history: {str(e)}")

    def add_data_point(self, data: Dict):
        """
        Add a data point for thermal learning

        Args:
            data: Dictionary with heating system metrics
                {
                    'timestamp': ISO timestamp,
                    'room_temperature': float,
                    'outdoor_temperature': float,
                    'supply_temp': float,
                    'electric_heater': bool
                }
        """
        if not data:
            return

        # Only keep data points with required fields
        required_fields = ['timestamp', 'room_temperature', 'outdoor_temperature']
        if all(field in data for field in required_fields):
            # Store with parsed timestamp for easier analysis
            data_point = data.copy()
            if isinstance(data['timestamp'], str):
                dt = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            else:
                dt = data['timestamp']
            # Ensure timezone-aware
            data_point['timestamp_dt'] = _ensure_timezone_aware(dt)

            self.historical_data.append(data_point)

            # Persist to InfluxDB for restart recovery
            if self.influx:
                self.influx.write_thermal_data_point(data)

            # Keep last 7 days of data (672 points at 15min intervals)
            max_points = 672
            if len(self.historical_data) > max_points:
                self.historical_data = self.historical_data[-max_points:]

    def calculate_thermal_lag(self) -> Optional[float]:
        """
        Calculate the thermal lag - how long it takes for outdoor temperature
        changes to affect indoor temperature

        Returns:
            Thermal lag in hours, or None if insufficient data
        """
        if len(self.historical_data) < self.min_samples * 4:  # Need ~24 hours
            self.logger.info(f"Insufficient data for thermal lag analysis ({len(self.historical_data)}/{self.min_samples * 4})")
            return None

        # Look for correlation between outdoor temp changes and indoor temp changes
        # with various time offsets
        best_lag = 6.0  # Default assumption for floor heating + concrete

        # This is simplified - a full implementation would use cross-correlation
        # For now, return the expected lag for this building type
        self.logger.info(f"Thermal lag estimated at {best_lag} hours (floor heating + concrete)")
        return best_lag

    def calculate_thermal_coefficient(self) -> Optional[Dict]:
        """
        Calculate how quickly the building loses/gains heat

        The thermal coefficient represents:
        - Heat loss rate to outdoor environment
        - Effectiveness of heating system
        - Thermal mass effects

        Returns:
            Dictionary with thermal analysis or None if insufficient data
            {
                'coefficient': float,  # °C change per hour per °C outdoor difference
                'confidence': float,   # 0.0-1.0
                'samples': int,        # Number of data points used
                'avg_indoor': float,   # Average indoor temp
                'avg_outdoor': float   # Average outdoor temp
            }
        """
        if len(self.historical_data) < self.min_samples:
            self.logger.info(f"Insufficient data for thermal coefficient ({len(self.historical_data)}/{self.min_samples})")
            return None

        try:
            # Calculate temperature changes over time
            indoor_temps = [d['room_temperature'] for d in self.historical_data]
            outdoor_temps = [d['outdoor_temperature'] for d in self.historical_data]

            avg_indoor = statistics.mean(indoor_temps)
            avg_outdoor = statistics.mean(outdoor_temps)

            # Calculate rate of change
            temp_changes = []
            for i in range(1, len(self.historical_data)):
                prev = self.historical_data[i-1]
                curr = self.historical_data[i]

                # Time difference in hours
                time_diff = (curr['timestamp_dt'] - prev['timestamp_dt']).total_seconds() / 3600

                if time_diff > 0 and time_diff < 2:  # Valid interval (15 min to 2 hours)
                    # Indoor temperature change
                    indoor_delta = curr['room_temperature'] - prev['room_temperature']

                    # Outdoor temperature difference from indoor
                    outdoor_diff = (curr['outdoor_temperature'] + prev['outdoor_temperature']) / 2 - avg_indoor

                    # Heating status (if heater was on, heat is being added)
                    heating_active = curr.get('electric_heater', False) or prev.get('electric_heater', False)

                    # Calculate coefficient (simplified model)
                    # When heating is off, indoor temp changes toward outdoor temp
                    if not heating_active and abs(outdoor_diff) > 2:  # Significant difference
                        # Rate of change toward outdoor temp
                        coefficient = indoor_delta / (time_diff * outdoor_diff)
                        if abs(coefficient) < 0.5:  # Reasonable range
                            temp_changes.append(coefficient)

            if len(temp_changes) < 5:
                self.logger.info(f"Insufficient valid temperature changes ({len(temp_changes)})")
                return None

            # Average coefficient
            avg_coefficient = statistics.mean(temp_changes)

            # Confidence based on consistency
            if len(temp_changes) > 1:
                stdev = statistics.stdev(temp_changes)
                confidence = max(0.0, min(1.0, 1.0 - (stdev / 0.1)))  # Lower stdev = higher confidence
            else:
                confidence = 0.3

            result = {
                'coefficient': abs(avg_coefficient),  # Use absolute value
                'confidence': confidence,
                'samples': len(temp_changes),
                'avg_indoor': avg_indoor,
                'avg_outdoor': avg_outdoor,
                'data_points': len(self.historical_data)
            }

            self.logger.info(
                f"Thermal coefficient: {result['coefficient']:.6f} °C/h/°C "
                f"(confidence: {result['confidence']:.2f}, samples: {result['samples']})"
            )

            return result

        except Exception as e:
            self.logger.error(f"Error calculating thermal coefficient: {str(e)}")
            return None

    def predict_temperature_change(
        self,
        current_indoor: float,
        forecast_outdoor: float,
        hours_ahead: float,
        heating_active: bool = False
    ) -> Optional[float]:
        """
        Predict how indoor temperature will change based on learned characteristics

        Args:
            current_indoor: Current indoor temperature
            forecast_outdoor: Forecast outdoor temperature
            hours_ahead: Hours to predict into the future
            heating_active: Whether heating will be active

        Returns:
            Predicted indoor temperature, or None if cannot predict
        """
        thermal_data = self.calculate_thermal_coefficient()
        if not thermal_data:
            return None

        coefficient = thermal_data['coefficient']

        # Temperature difference (driving force)
        temp_diff = forecast_outdoor - current_indoor

        # Calculate natural temperature change (without heating)
        natural_change = coefficient * temp_diff * hours_ahead

        # If heating is active, it counteracts the natural change
        # This is simplified - actual heating effect depends on many factors
        if heating_active:
            # Heating typically maintains or raises temp by ~1-2°C over natural drift
            heating_effect = 1.5 * hours_ahead / 6.0  # Scale by time
            predicted_temp = current_indoor + natural_change + heating_effect
        else:
            predicted_temp = current_indoor + natural_change

        self.logger.info(
            f"Temp prediction: {current_indoor:.1f}°C → {predicted_temp:.1f}°C "
            f"in {hours_ahead}h (outdoor: {forecast_outdoor:.1f}°C, heating: {heating_active})"
        )

        return predicted_temp

    def get_heating_recommendation(
        self,
        current_indoor: float,
        forecast_outdoor_trend: Dict,
        target_temp: float = 21.0
    ) -> Dict:
        """
        Get heating recommendation based on learned thermal characteristics

        Args:
            current_indoor: Current indoor temperature
            forecast_outdoor_trend: Forecast trend from weather module
            target_temp: Target indoor temperature

        Returns:
            Dictionary with recommendation
            {
                'action': str,  # 'reduce', 'maintain', 'increase'
                'reason': str,
                'confidence': float,
                'predicted_temp': float  # Predicted temp if recommendation followed
            }
        """
        # Default to maintain if we can't analyze
        if not forecast_outdoor_trend:
            return {
                'action': 'maintain',
                'reason': 'No forecast data available',
                'confidence': 0.0,
                'predicted_temp': current_indoor
            }

        # Predict temperature without heating
        forecast_avg = forecast_outdoor_trend.get('avg_temp', current_indoor - 5)
        predicted_no_heat = self.predict_temperature_change(
            current_indoor,
            forecast_avg,
            hours_ahead=6.0,
            heating_active=False
        )

        # Predict temperature with heating
        predicted_with_heat = self.predict_temperature_change(
            current_indoor,
            forecast_avg,
            hours_ahead=6.0,
            heating_active=True
        )

        # Decision logic
        outdoor_rising = forecast_outdoor_trend.get('trend') == 'rising'
        temp_change = forecast_outdoor_trend.get('change', 0)

        if predicted_no_heat and predicted_no_heat >= (target_temp - 0.5):
            # Even without heating, temp will stay acceptable
            return {
                'action': 'reduce',
                'reason': f"Natural temp will be {predicted_no_heat:.1f}°C (outdoor rising {temp_change:+.1f}°C)",
                'confidence': 0.8 if outdoor_rising else 0.6,
                'predicted_temp': predicted_no_heat
            }
        elif current_indoor < (target_temp - 1.0):
            # Below target, need heating
            return {
                'action': 'increase',
                'reason': f"Below target ({current_indoor:.1f}°C < {target_temp:.1f}°C)",
                'confidence': 0.9,
                'predicted_temp': predicted_with_heat or current_indoor + 1.0
            }
        else:
            # Maintain current heating
            return {
                'action': 'maintain',
                'reason': f"At target ({current_indoor:.1f}°C ≈ {target_temp:.1f}°C)",
                'confidence': 0.7,
                'predicted_temp': predicted_with_heat or current_indoor
            }

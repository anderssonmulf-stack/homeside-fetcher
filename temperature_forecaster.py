"""
Temperature Forecaster - Model C (Hybrid)

Combines physics-based prediction with historical learning:
1. Physics model: Thermostat-aware prediction based on thermal coefficient
2. Historical adjustment: Corrects systematic biases by hour of day
3. Confidence weighting: Trusts physics more when history is limited

Key principle: The thermostat is the dominant factor. Physics determines
*how fast* temperature changes, but the setpoint determines *where it ends up*.

All predictions include explanations for GUI transparency.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict

from customer_profile import CustomerProfile


@dataclass
class ForecastExplanation:
    """
    Detailed explanation of how a forecast was calculated.
    Designed for GUI display and debugging.
    """
    physics_base: float
    physics_reasoning: str
    hourly_adjustment: float
    adjustment_reasoning: str
    confidence: float
    confidence_reasoning: str
    factors: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ForecastPoint:
    """A single forecast point with value and explanation."""
    timestamp: datetime
    forecast_type: str  # indoor_temp, outdoor_temp, supply_temp_baseline, supply_temp_ml
    value: float
    explanation: Optional[ForecastExplanation] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "timestamp": self.timestamp.isoformat(),
            "forecast_type": self.forecast_type,
            "value": round(self.value, 2)
        }
        if self.explanation:
            result["explanation"] = self.explanation.to_dict()
        return result

    def to_influx_dict(self) -> Dict[str, Any]:
        """Format for InfluxDB writer."""
        return {
            "timestamp": self.timestamp,
            "forecast_type": self.forecast_type,
            "value": self.value
        }


class TemperatureForecaster:
    """
    Hybrid temperature forecaster (Model C).

    Usage:
        profile = CustomerProfile.load("HEM_FJV_Villa_149")
        forecaster = TemperatureForecaster(profile)

        # Generate forecasts
        points = forecaster.generate_forecast(
            current_indoor=22.1,
            current_outdoor=-2.0,
            weather_forecast=[...],
            heat_curve=heat_curve_controller
        )

        # Record actual vs predicted for learning
        forecaster.record_accuracy(
            predicted=22.1,
            actual=21.8,
            hour=8,
            outdoor=-2.0
        )
    """

    # Thermal response rates (°C per hour) based on building type
    THERMAL_RESPONSE_RATES = {
        "slow": {"heating": 0.3, "cooling": 0.1},    # Heavy masonry, well-insulated
        "medium": {"heating": 0.5, "cooling": 0.2},  # Typical villa
        "fast": {"heating": 0.8, "cooling": 0.3}     # Light construction
    }

    def __init__(self, profile: CustomerProfile):
        """
        Initialize forecaster with customer profile.

        Args:
            profile: CustomerProfile instance with settings and learned params
        """
        self.profile = profile
        self.logger = logging.getLogger(__name__)

        # Get thermal response rates for this building
        response_type = profile.building.thermal_response
        self.response_rates = self.THERMAL_RESPONSE_RATES.get(
            response_type,
            self.THERMAL_RESPONSE_RATES["medium"]
        )

        # Accuracy tracking for learning (in-memory buffer)
        self._accuracy_buffer: List[Dict[str, Any]] = []

    def generate_forecast(
        self,
        current_indoor: float,
        current_outdoor: float,
        weather_forecast: List[Dict[str, Any]],
        heat_curve=None
    ) -> List[ForecastPoint]:
        """
        Generate temperature forecasts for the next 12 hours.

        Args:
            current_indoor: Current indoor temperature
            current_outdoor: Current outdoor temperature
            weather_forecast: List of hourly forecasts with 'time' and 'temp' keys
            heat_curve: Optional HeatCurveController for supply temp forecasts

        Returns:
            List of ForecastPoint objects ready for InfluxDB and GUI
        """
        forecast_points = []
        predicted_indoor = current_indoor

        for forecast in weather_forecast:
            # Parse forecast time
            time_str = forecast.get('time')
            if not time_str:
                continue

            forecast_time = datetime.fromisoformat(
                time_str.replace('Z', '+00:00')
            )
            forecast_outdoor = forecast.get('temp')

            if forecast_outdoor is None:
                continue

            # 1. Outdoor temperature (direct from weather)
            forecast_points.append(ForecastPoint(
                timestamp=forecast_time,
                forecast_type='outdoor_temp',
                value=forecast_outdoor
            ))

            # 2. Supply temperature forecasts (from heat curve)
            if heat_curve:
                baseline_supply, ml_supply = heat_curve.get_supply_temps_for_outdoor(
                    forecast_outdoor
                )
                if baseline_supply is not None:
                    forecast_points.append(ForecastPoint(
                        timestamp=forecast_time,
                        forecast_type='supply_temp_baseline',
                        value=baseline_supply
                    ))
                if ml_supply is not None:
                    forecast_points.append(ForecastPoint(
                        timestamp=forecast_time,
                        forecast_type='supply_temp_ml',
                        value=ml_supply
                    ))

            # 3. Indoor temperature forecast (Model C)
            indoor_forecast = self._predict_indoor(
                current_indoor=predicted_indoor,
                outdoor_temp=forecast_outdoor,
                forecast_time=forecast_time
            )

            forecast_points.append(indoor_forecast)

            # Update for next iteration (chain predictions)
            predicted_indoor = indoor_forecast.value

        self.logger.info(
            f"Generated {len(forecast_points)} forecast points "
            f"(indoor: {current_indoor:.1f} -> {predicted_indoor:.1f})"
        )

        return forecast_points

    def _predict_indoor(
        self,
        current_indoor: float,
        outdoor_temp: float,
        forecast_time: datetime
    ) -> ForecastPoint:
        """
        Predict indoor temperature using Model C (physics + historical).

        Model C Logic:
        1. Physics model predicts based on thermostat behavior
        2. Historical bias corrects systematic errors
        3. Confidence weights the adjustment
        """
        target = self.profile.comfort.target_indoor_temp
        acceptable_dev = self.profile.comfort.acceptable_deviation
        hour = forecast_time.hour

        # --- Step 1: Physics-based prediction ---
        physics_prediction, physics_reasoning = self._physics_model(
            current_indoor=current_indoor,
            outdoor_temp=outdoor_temp,
            target_temp=target,
            acceptable_deviation=acceptable_dev
        )

        # --- Step 2: Historical adjustment ---
        hour_key = f"{hour:02d}"
        hourly_bias = self.profile.learned.hourly_bias.get(hour_key, 0.0)

        if hourly_bias != 0:
            adjustment_reasoning = (
                f"Historical data shows {hourly_bias:+.2f} C bias at {hour:02d}:00 "
                f"(based on past observations)"
            )
        else:
            adjustment_reasoning = f"No historical data for {hour:02d}:00 yet"

        # --- Step 3: Confidence weighting ---
        confidence = self.profile.learned.thermal_coefficient_confidence
        total_samples = self.profile.learned.total_samples

        if total_samples < 24:
            confidence_reasoning = f"Learning phase ({total_samples}/24 initial samples)"
            effective_confidence = 0.0  # Don't apply adjustments yet
        elif confidence < 0.5:
            confidence_reasoning = f"Low confidence ({confidence:.0%}), limited adjustment"
            effective_confidence = confidence * 0.5
        else:
            confidence_reasoning = f"Good confidence ({confidence:.0%})"
            effective_confidence = confidence

        # Apply adjustment weighted by confidence
        adjusted_prediction = physics_prediction + (hourly_bias * effective_confidence)

        # Clamp to reasonable bounds
        min_temp = target - acceptable_dev - 1.0
        max_temp = target + acceptable_dev + 0.5
        final_prediction = max(min_temp, min(max_temp, adjusted_prediction))

        # --- Build explanation ---
        factors = [
            {
                "name": "Target setpoint",
                "value": target,
                "unit": "C",
                "impact": "high"
            },
            {
                "name": "Outdoor forecast",
                "value": outdoor_temp,
                "unit": "C",
                "impact": "medium"
            },
            {
                "name": "Current indoor",
                "value": current_indoor,
                "unit": "C",
                "impact": "medium"
            }
        ]

        if hourly_bias != 0:
            factors.append({
                "name": f"Hour {hour:02d}:00 bias",
                "value": hourly_bias,
                "unit": "C",
                "impact": "low" if abs(hourly_bias) < 0.2 else "medium"
            })

        explanation = ForecastExplanation(
            physics_base=round(physics_prediction, 2),
            physics_reasoning=physics_reasoning,
            hourly_adjustment=round(hourly_bias * effective_confidence, 2),
            adjustment_reasoning=adjustment_reasoning,
            confidence=round(effective_confidence, 2),
            confidence_reasoning=confidence_reasoning,
            factors=factors
        )

        return ForecastPoint(
            timestamp=forecast_time,
            forecast_type='indoor_temp',
            value=round(final_prediction, 2),
            explanation=explanation
        )

    def _physics_model(
        self,
        current_indoor: float,
        outdoor_temp: float,
        target_temp: float,
        acceptable_deviation: float
    ) -> Tuple[float, str]:
        """
        Thermostat-aware physics model.

        Key insight: The thermostat dominates behavior.
        - Below target: heating active, temperature rises toward target
        - At/above target: heating cycles, temperature stabilizes
        - Heat loss depends on indoor-outdoor delta

        Args:
            current_indoor: Current indoor temperature
            outdoor_temp: Forecasted outdoor temperature
            target_temp: Thermostat setpoint
            acceptable_deviation: Acceptable deviation from target

        Returns:
            Tuple of (predicted_temp, reasoning_string)
        """
        thermal_coeff = self.profile.learned.thermal_coefficient
        heating_rate = self.response_rates["heating"]
        cooling_rate = self.response_rates["cooling"]

        # Calculate heat loss pressure
        temp_delta = current_indoor - outdoor_temp
        heat_loss_pressure = temp_delta * 0.02  # Simplified heat loss factor

        if current_indoor < target_temp - acceptable_deviation:
            # Well below target: heating strongly active
            rise = heating_rate - heat_loss_pressure
            rise = max(0.1, rise)  # Always some progress when heating
            predicted = current_indoor + rise

            # Don't overshoot target
            predicted = min(predicted, target_temp)

            reasoning = (
                f"Below target ({current_indoor:.1f} < {target_temp:.1f}), "
                f"heating active, rising ~{rise:.1f} C/h"
            )

        elif current_indoor < target_temp:
            # Slightly below target: heating active but approaching setpoint
            gap = target_temp - current_indoor
            rise = min(heating_rate * 0.5, gap)  # Slow approach
            predicted = current_indoor + rise

            reasoning = (
                f"Approaching target ({current_indoor:.1f} -> {target_temp:.1f}), "
                f"heating modulating"
            )

        elif current_indoor <= target_temp + acceptable_deviation:
            # At target: thermostat cycling, stable
            # Small drift toward equilibrium based on outdoor
            if outdoor_temp < current_indoor - 10:
                drift = -0.1  # Slight cooling pressure
            else:
                drift = 0.0

            predicted = current_indoor + drift

            # Thermostat prevents dropping below target
            predicted = max(predicted, target_temp - acceptable_deviation * 0.5)

            reasoning = (
                f"At target ({current_indoor:.1f} ~ {target_temp:.1f}), "
                f"thermostat maintaining"
            )

        else:
            # Above target: heating off, cooling down
            cooling = cooling_rate + heat_loss_pressure * 0.5
            predicted = current_indoor - cooling

            # Don't drop below target
            predicted = max(predicted, target_temp)

            reasoning = (
                f"Above target ({current_indoor:.1f} > {target_temp:.1f}), "
                f"cooling toward setpoint"
            )

        return predicted, reasoning

    def record_accuracy(
        self,
        predicted: float,
        actual: float,
        hour: int,
        outdoor: float
    ) -> None:
        """
        Record prediction accuracy for learning.

        Called after each data collection to compare last prediction
        with actual measurement.

        Args:
            predicted: What we predicted
            actual: What actually happened
            hour: Hour of day (0-23)
            outdoor: Outdoor temperature at time of measurement
        """
        error = actual - predicted  # Positive = we underestimated

        self._accuracy_buffer.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hour": hour,
            "predicted": predicted,
            "actual": actual,
            "error": error,
            "outdoor": outdoor
        })

        # Keep buffer bounded
        if len(self._accuracy_buffer) > 1000:
            self._accuracy_buffer = self._accuracy_buffer[-500:]

    def should_update_learning(self) -> bool:
        """Check if it's time to update learned parameters."""
        return self.profile.record_sample()

    def update_hourly_bias(self) -> Dict[str, float]:
        """
        Calculate and update hourly bias from accuracy buffer.

        Returns:
            Dictionary of updated hourly biases
        """
        if len(self._accuracy_buffer) < 10:
            self.logger.info("Not enough accuracy data to update bias")
            return {}

        # Group errors by hour
        hourly_errors: Dict[int, List[float]] = {}
        for record in self._accuracy_buffer:
            hour = record["hour"]
            if hour not in hourly_errors:
                hourly_errors[hour] = []
            hourly_errors[hour].append(record["error"])

        # Calculate mean bias per hour (minimum 3 samples)
        new_bias = {}
        for hour, errors in hourly_errors.items():
            if len(errors) >= 3:
                mean_error = sum(errors) / len(errors)
                hour_key = f"{hour:02d}"

                # Blend with existing bias (80% new, 20% old for smoothing)
                old_bias = self.profile.learned.hourly_bias.get(hour_key, 0.0)
                blended = 0.8 * mean_error + 0.2 * old_bias

                # Only store if significant
                if abs(blended) > 0.05:
                    new_bias[hour_key] = round(blended, 3)

                self.logger.debug(
                    f"Hour {hour:02d}: mean_error={mean_error:.2f}, "
                    f"old={old_bias:.2f}, new={blended:.2f}"
                )

        # Update profile
        self.profile.learned.hourly_bias.update(new_bias)
        self.profile.learned.updated_at = datetime.now(timezone.utc).isoformat()
        self.profile.save()

        self.logger.info(
            f"Updated hourly bias for {len(new_bias)} hours "
            f"(total samples: {self.profile.learned.total_samples})"
        )

        # Clear buffer after update
        self._accuracy_buffer = []

        return new_bias

    def get_accuracy_stats(self) -> Dict[str, Any]:
        """Get accuracy statistics for GUI display."""
        if not self._accuracy_buffer:
            return {"status": "No data yet"}

        errors = [r["error"] for r in self._accuracy_buffer]
        abs_errors = [abs(e) for e in errors]

        return {
            "samples": len(self._accuracy_buffer),
            "mean_error": round(sum(errors) / len(errors), 3),
            "mean_absolute_error": round(sum(abs_errors) / len(abs_errors), 3),
            "max_error": round(max(errors), 2),
            "min_error": round(min(errors), 2)
        }

    def get_status(self) -> Dict[str, Any]:
        """Get forecaster status for GUI display."""
        profile_status = self.profile.get_status()
        accuracy_stats = self.get_accuracy_stats()

        return {
            **profile_status,
            "accuracy": accuracy_stats,
            "thermal_response": self.profile.building.thermal_response,
            "response_rates": self.response_rates
        }

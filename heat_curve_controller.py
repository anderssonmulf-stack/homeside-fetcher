#!/usr/bin/env python3
"""
Heat Curve Controller
Manages dynamic adjustment of the heating curve based on weather forecasts
to optimize energy usage while maintaining indoor comfort.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Tuple
import json


# Heat curve X-axis mapping: index -> outdoor temperature (°C)
# These are READ-ONLY values from HomeSide
CURVE_X_AXIS = {
    54: -30,  # X_1
    55: -25,  # X_2
    56: -20,  # X_3
    57: -15,  # X_4
    58: -10,  # X_5
    59: -5,   # X_6
    60: 0,    # X_7
    61: 5,    # X_8
    62: 10,   # X_9
    63: 15,   # X_10
}

# Heat curve Y-axis mapping: index -> corresponds to X-axis outdoor temp
# These are the WRITABLE supply temperature values
CURVE_Y_INDICES = {
    64: -30,  # Y_1 -> supply temp when outdoor is -30°C
    65: -25,  # Y_2 -> supply temp when outdoor is -25°C
    66: -20,  # Y_3 -> supply temp when outdoor is -20°C
    67: -15,  # Y_4 -> supply temp when outdoor is -15°C
    68: -10,  # Y_5 -> supply temp when outdoor is -10°C
    69: -5,   # Y_6 -> supply temp when outdoor is -5°C
    70: 0,    # Y_7 -> supply temp when outdoor is 0°C
    71: 5,    # Y_8 -> supply temp when outdoor is +5°C
    72: 10,   # Y_9 -> supply temp when outdoor is +10°C
    73: 15,   # Y_10 -> supply temp when outdoor is +15°C
}

# Reverse mapping: outdoor temp -> Y-axis index
OUTDOOR_TO_Y_INDEX = {v: k for k, v in CURVE_Y_INDICES.items()}


class HeatCurveController:
    """
    Controls heat curve adjustments based on weather forecasts.

    The controller:
    1. Monitors weather forecasts for rising outdoor temperatures
    2. When conditions are right, reduces supply temperatures on relevant curve points
    3. Stores baseline values before adjustment
    4. Restores baseline when reduction period ends
    """

    def __init__(self, api, influx, logger, debug_mode: bool = False):
        """
        Initialize the heat curve controller.

        Args:
            api: HomeSideAPI instance for reading/writing curve values
            influx: InfluxDBWriter instance for storing baselines
            logger: Logger instance
            debug_mode: Enable verbose logging
        """
        self.api = api
        self.influx = influx
        self.logger = logger
        self.debug_mode = debug_mode

        # Adjustment state
        self.adjustment_active = False
        self.adjustment_started_at: Optional[datetime] = None
        self.adjustment_expires_at: Optional[datetime] = None
        self.adjusted_indices: List[int] = []
        self.adjustment_delta: float = 0.0

        # Configuration
        self.min_forecast_change = 2.0  # Minimum outdoor temp rise to trigger reduction (°C)
        self.max_adjustment = 3.0  # Maximum supply temp reduction (°C)
        self.adjustment_ratio = 0.5  # Supply temp reduction per °C outdoor rise
        self.min_supply_temp = 20.0  # Never reduce below this (°C)

    def read_current_curve(self) -> Optional[Dict[int, float]]:
        """
        Read current Y-axis values from the HomeSide API.

        Returns:
            Dictionary mapping index (64-73) to current supply temp value
        """
        try:
            raw_data = self.api.get_heating_data()
            if not raw_data or 'variables' not in raw_data:
                self.logger.error("Failed to read heating data for curve values")
                return None

            curve_values = {}
            for var in raw_data['variables']:
                path = var.get('path', '')
                value = var.get('value')

                # Check if this is a Y-axis curve point
                if path.startswith('Cwl.Advise.A[') and value is not None:
                    try:
                        index = int(path.replace('Cwl.Advise.A[', '').replace(']', ''))
                        if 64 <= index <= 73:
                            curve_values[index] = float(value)
                    except (ValueError, TypeError):
                        continue

            if len(curve_values) == 10:
                if self.debug_mode:
                    self.logger.info(f"Read heat curve: {curve_values}")
                return curve_values
            else:
                self.logger.warning(f"Incomplete heat curve data: got {len(curve_values)}/10 points")
                return curve_values if curve_values else None

        except Exception as e:
            self.logger.error(f"Error reading heat curve: {str(e)}")
            return None

    def get_affected_indices(
        self,
        current_outdoor: float,
        forecast_outdoor: float
    ) -> List[int]:
        """
        Determine which curve Y-indices should be adjusted based on
        current and forecasted outdoor temperature range.

        Args:
            current_outdoor: Current outdoor temperature (°C)
            forecast_outdoor: Forecasted outdoor temperature (°C)

        Returns:
            List of Y-axis indices (64-73) that fall within the outdoor temp range
        """
        # Get the range of outdoor temps we care about
        min_temp = min(current_outdoor, forecast_outdoor)
        max_temp = max(current_outdoor, forecast_outdoor)

        # Add some margin to catch nearby curve points
        margin = 2.5  # Half the spacing between curve points (5°C / 2)
        min_temp -= margin
        max_temp += margin

        affected = []
        for index, outdoor_temp in CURVE_Y_INDICES.items():
            if min_temp <= outdoor_temp <= max_temp:
                affected.append(index)

        # Sort by index
        affected.sort()

        if self.debug_mode:
            self.logger.info(
                f"Outdoor range {current_outdoor:.1f}°C -> {forecast_outdoor:.1f}°C "
                f"affects indices: {affected}"
            )

        return affected

    def calculate_adjustment(
        self,
        forecast_change: float,
        current_indoor: float,
        target_indoor: float = 21.0
    ) -> Tuple[float, str]:
        """
        Calculate the recommended supply temperature adjustment.

        Args:
            forecast_change: Expected outdoor temperature change (°C)
            current_indoor: Current indoor temperature (°C)
            target_indoor: Target indoor temperature (°C)

        Returns:
            Tuple of (adjustment_delta, reason)
            adjustment_delta is negative for reduction
        """
        # Don't adjust if forecast isn't rising enough
        if forecast_change < self.min_forecast_change:
            return 0.0, f"Forecast change {forecast_change:+.1f}°C below threshold"

        # Don't adjust if indoor is already below target
        if current_indoor < target_indoor - 0.5:
            return 0.0, f"Indoor temp {current_indoor:.1f}°C below target"

        # Calculate adjustment proportional to forecast change
        # More outdoor warming = more we can reduce supply temp
        raw_adjustment = forecast_change * self.adjustment_ratio

        # Cap at maximum adjustment
        adjustment = min(raw_adjustment, self.max_adjustment)

        # Return as negative (reduction)
        reason = (
            f"Outdoor rising {forecast_change:+.1f}°C, "
            f"indoor at {current_indoor:.1f}°C (target {target_indoor:.1f}°C)"
        )

        return -adjustment, reason

    def should_reduce(
        self,
        forecast_trend: Dict,
        current_indoor: float,
        target_indoor: float = 21.0
    ) -> Dict:
        """
        Determine if heat curve should be reduced and by how much.

        Args:
            forecast_trend: Weather forecast trend dict from weather module
            current_indoor: Current indoor temperature
            target_indoor: Target indoor temperature

        Returns:
            Dictionary with recommendation:
            {
                'reduce': bool,
                'delta': float (negative for reduction),
                'duration_hours': float,
                'affected_indices': List[int],
                'reason': str,
                'confidence': float
            }
        """
        if not forecast_trend:
            return {
                'reduce': False,
                'delta': 0.0,
                'duration_hours': 0,
                'affected_indices': [],
                'reason': 'No forecast data',
                'confidence': 0.0
            }

        forecast_change = forecast_trend.get('change', 0)
        trend = forecast_trend.get('trend', 'stable')
        current_outdoor = forecast_trend.get('current_temp', 0)

        # Only reduce on rising temperatures
        if trend != 'rising':
            return {
                'reduce': False,
                'delta': 0.0,
                'duration_hours': 0,
                'affected_indices': [],
                'reason': f'Outdoor not rising (trend: {trend})',
                'confidence': 0.0
            }

        # Calculate adjustment
        delta, reason = self.calculate_adjustment(
            forecast_change,
            current_indoor,
            target_indoor
        )

        if delta >= 0:  # No reduction needed
            return {
                'reduce': False,
                'delta': 0.0,
                'duration_hours': 0,
                'affected_indices': [],
                'reason': reason,
                'confidence': 0.0
            }

        # Determine affected curve points
        forecast_outdoor = current_outdoor + forecast_change
        affected_indices = self.get_affected_indices(current_outdoor, forecast_outdoor)

        if not affected_indices:
            return {
                'reduce': False,
                'delta': 0.0,
                'duration_hours': 0,
                'affected_indices': [],
                'reason': 'No curve points in forecast range',
                'confidence': 0.0
            }

        # Duration based on forecast window (typically 12 hours)
        duration_hours = forecast_trend.get('forecast_hours', 12)

        # Confidence based on forecast change magnitude
        confidence = min(1.0, abs(forecast_change) / 5.0)

        return {
            'reduce': True,
            'delta': delta,
            'duration_hours': duration_hours,
            'affected_indices': affected_indices,
            'reason': reason,
            'confidence': confidence
        }

    def enter_reduction_mode(
        self,
        current_curve: Dict[int, float],
        affected_indices: List[int],
        delta: float,
        duration_hours: float,
        reason: str,
        forecast_change: float = 0.0
    ) -> bool:
        """
        Enter heat curve reduction mode.

        1. Store current curve values as baseline (if different from stored)
        2. Apply reduction to affected points
        3. Log the adjustment

        Args:
            current_curve: Current Y-values from HomeSide
            affected_indices: Which indices to adjust
            delta: Adjustment delta (negative for reduction)
            duration_hours: How long to maintain reduction
            reason: Human-readable reason
            forecast_change: Forecasted outdoor temp change

        Returns:
            True if successfully entered reduction mode
        """
        if self.adjustment_active:
            self.logger.warning("Already in reduction mode, skipping")
            return False

        try:
            # Read stored baseline from InfluxDB
            stored_baseline = self.influx.read_heat_curve_baseline()

            # Compare with current values and update if different
            if stored_baseline != current_curve:
                self.logger.info("Heat curve changed since last baseline, storing new baseline")
                self.influx.write_heat_curve_baseline(current_curve)

            # Calculate new values for affected points
            adjusted_values = {}
            for index in affected_indices:
                if index in current_curve:
                    current_val = current_curve[index]
                    new_val = max(self.min_supply_temp, current_val + delta)
                    adjusted_values[index] = new_val

            # Apply adjustments via API
            success_count = 0
            for index, new_value in adjusted_values.items():
                path = f"Cwl.Advise.A[{index}]"
                if self.api.write_value(path, new_value):
                    success_count += 1
                    if self.debug_mode:
                        old_val = current_curve.get(index, 0)
                        self.logger.info(f"Adjusted {path}: {old_val:.1f} -> {new_value:.1f}")
                else:
                    self.logger.error(f"Failed to write {path}")

            if success_count == 0:
                self.logger.error("Failed to apply any curve adjustments")
                return False

            # Log the adjustment to InfluxDB
            self.influx.write_heat_curve_adjustment(
                action="reduce",
                adjusted_points=adjusted_values,
                delta=delta,
                reason=reason,
                forecast_change=forecast_change,
                duration_hours=duration_hours
            )

            # Update state
            self.adjustment_active = True
            self.adjustment_started_at = datetime.now(timezone.utc)
            self.adjustment_expires_at = self.adjustment_started_at + timedelta(hours=duration_hours)
            self.adjusted_indices = affected_indices
            self.adjustment_delta = delta

            self.logger.info(
                f"Entered reduction mode: {success_count} points adjusted by {delta:.1f}°C "
                f"for {duration_hours:.1f} hours"
            )
            print(f"🔽 Heat curve reduced: {success_count} points by {delta:.1f}°C for {duration_hours:.0f}h")

            return True

        except Exception as e:
            self.logger.error(f"Error entering reduction mode: {str(e)}")
            return False

    def exit_reduction_mode(self, reason: str = "Duration expired") -> bool:
        """
        Exit heat curve reduction mode and restore baseline values.

        Args:
            reason: Why we're exiting (for logging)

        Returns:
            True if successfully restored baseline
        """
        if not self.adjustment_active:
            return True  # Already not in reduction mode

        try:
            # Read baseline from InfluxDB
            baseline = self.influx.read_heat_curve_baseline()
            if not baseline:
                self.logger.error("No baseline found in database, cannot restore")
                return False

            # Restore baseline values for adjusted indices
            restored_values = {}
            success_count = 0

            for index in self.adjusted_indices:
                if index in baseline:
                    path = f"Cwl.Advise.A[{index}]"
                    baseline_val = baseline[index]

                    if self.api.write_value(path, baseline_val):
                        restored_values[index] = baseline_val
                        success_count += 1
                        if self.debug_mode:
                            self.logger.info(f"Restored {path} to {baseline_val:.1f}")
                    else:
                        self.logger.error(f"Failed to restore {path}")

            # Log the restoration
            self.influx.write_heat_curve_adjustment(
                action="restore",
                adjusted_points=restored_values,
                delta=-self.adjustment_delta,  # Inverse of reduction
                reason=reason,
                forecast_change=0.0,
                duration_hours=0.0
            )

            # Clear state
            self.adjustment_active = False
            self.adjustment_started_at = None
            self.adjustment_expires_at = None
            self.adjusted_indices = []
            self.adjustment_delta = 0.0

            self.logger.info(f"Exited reduction mode: {success_count} points restored ({reason})")
            print(f"➡️ Heat curve restored: {success_count} points ({reason})")

            return True

        except Exception as e:
            self.logger.error(f"Error exiting reduction mode: {str(e)}")
            return False

    def check_expiration(self) -> bool:
        """
        Check if current adjustment has expired and restore if needed.

        Returns:
            True if adjustment was expired and restored
        """
        if not self.adjustment_active:
            return False

        if datetime.now(timezone.utc) >= self.adjustment_expires_at:
            self.logger.info("Heat curve adjustment expired")
            return self.exit_reduction_mode(reason="Duration expired")

        return False

    def get_status(self) -> Dict:
        """
        Get current controller status.

        Returns:
            Dictionary with current state information
        """
        status = {
            'adjustment_active': self.adjustment_active,
            'adjusted_indices': self.adjusted_indices,
            'adjustment_delta': self.adjustment_delta,
        }

        if self.adjustment_active:
            status['started_at'] = self.adjustment_started_at.isoformat()
            status['expires_at'] = self.adjustment_expires_at.isoformat()
            remaining = (self.adjustment_expires_at - datetime.now(timezone.utc)).total_seconds() / 3600
            status['remaining_hours'] = max(0, remaining)

        return status

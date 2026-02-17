#!/usr/bin/env python3
"""
Heat Curve Controller
Manages dynamic adjustment of the heating curve based on weather forecasts
to optimize energy usage while maintaining indoor comfort.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Tuple
import json


# Standard outdoor temperatures for curve points 1-10 (same for all HomeSide installations)
# Point 1 = coldest (-30°C), Point 10 = warmest (+15°C)
# NOTE: The Cwl.Advise.A[] indices for these points differ between installations,
# so they must be discovered dynamically from variable names (CurveAdaptation_Y_*).
CURVE_OUTDOOR_TEMPS = {
    1: -30, 2: -25, 3: -20, 4: -15, 5: -10,
    6: -5,  7: 0,   8: 5,   9: 10,  10: 15,
}


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

        # Discovered Cwl.Advise.A indices for Y-axis points (point_num -> advise_index)
        # Populated on first read_current_curve() call
        self._y_advise_indices: Dict[int, int] = {}

        # Adjustment state
        self.adjustment_active = False
        self.adjustment_started_at: Optional[datetime] = None
        self.adjustment_expires_at: Optional[datetime] = None
        self.adjusted_points: List[int] = []
        self.adjustment_delta: float = 0.0

        # Configuration
        self.min_forecast_change = 2.0  # Minimum outdoor temp rise to trigger reduction (°C)
        self.max_adjustment = 3.0  # Maximum supply temp reduction (°C)
        self.adjustment_ratio = 0.5  # Supply temp reduction per °C outdoor rise
        self.min_supply_temp = 20.0  # Never reduce below this (°C)

    def read_current_curve(self) -> Optional[Dict[int, float]]:
        """
        Read current Y-axis values from the HomeSide API.
        Discovers Cwl.Advise.A indices dynamically by variable name
        (CurveAdaptation_Y_1 through CurveAdaptation_Y_10).

        Returns:
            Dictionary mapping point number (1-10) to current supply temp value
        """
        try:
            raw_data = self.api.get_heating_data()
            if not raw_data or 'variables' not in raw_data:
                self.logger.error("Failed to read heating data for curve values")
                return None

            curve_values = {}
            for var in raw_data['variables']:
                short_name = var.get('variable', '').split('.')[-1]
                path = var.get('path', '')
                value = var.get('value')

                # Match CurveAdaptation_Y_1 through CurveAdaptation_Y_10
                if 'CurveAdaptation_Y_' in short_name and value is not None:
                    try:
                        point_num = int(short_name.split('CurveAdaptation_Y_')[1])
                        if 1 <= point_num <= 10:
                            curve_values[point_num] = float(value)
                            # Cache Cwl.Advise index for this point (needed for writes)
                            if path.startswith('Cwl.Advise.A['):
                                advise_idx = int(path.replace('Cwl.Advise.A[', '').replace(']', ''))
                                self._y_advise_indices[point_num] = advise_idx
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

    def get_supply_temp_from_curve(
        self,
        outdoor_temp: float,
        curve_values: Dict[int, float] = None
    ) -> Optional[float]:
        """
        Calculate the expected supply temperature from the heat curve
        for a given outdoor temperature using linear interpolation.

        Args:
            outdoor_temp: Current outdoor temperature (°C)
            curve_values: Optional pre-fetched curve values (point_num -> supply_temp).
                          If None, reads from API.

        Returns:
            Interpolated supply temperature (°C), or None if curve unavailable

        Example:
            If outdoor temp is +2°C and:
            - Point 7 (0°C) = 30
            - Point 8 (+5°C) = 25
            Then returns: 30 + (2-0)/(5-0) * (25-30) = 28°C
        """
        if curve_values is None:
            curve_values = self.read_current_curve()

        return self._interpolate_curve(outdoor_temp, curve_values)

    def get_supply_temps_for_outdoor(
        self,
        outdoor_temp: float
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Get both baseline and current supply temperatures for a given outdoor temp.

        Returns:
            Tuple of (baseline_supply_temp, current_supply_temp):
            - baseline_supply_temp: From stored baseline (original curve)
            - current_supply_temp: From currently active curve (may be ML-adjusted)

        When not in reduction mode, both values are the same.
        When in reduction mode, current will be lower than baseline.
        """
        # Get current curve from API
        current_curve = self.read_current_curve()
        if not current_curve:
            return None, None

        # Calculate current supply temp (what's active now)
        current_supply = self._interpolate_curve(outdoor_temp, current_curve)

        # Get baseline from InfluxDB (original curve before any adjustments)
        baseline_curve = None
        if self.influx:
            baseline_curve = self.influx.read_heat_curve_baseline()

        # If no baseline stored, or not in adjustment mode, baseline = current
        if baseline_curve:
            baseline_supply = self._interpolate_curve(outdoor_temp, baseline_curve)
        else:
            baseline_supply = current_supply

        return baseline_supply, current_supply

    def _interpolate_curve(
        self,
        outdoor_temp: float,
        curve_values: Dict[int, float]
    ) -> Optional[float]:
        """
        Internal helper to interpolate supply temp from curve values.

        Args:
            outdoor_temp: Outdoor temperature to interpolate for
            curve_values: Dict mapping point number (1-10) to supply temp
        """
        if not curve_values:
            return None

        # Build sorted list of (outdoor_temp, supply_temp) points
        points = []
        for point_num, supply_temp in curve_values.items():
            if point_num in CURVE_OUTDOOR_TEMPS:
                curve_outdoor = CURVE_OUTDOOR_TEMPS[point_num]
                points.append((curve_outdoor, supply_temp))

        if not points:
            return None

        points.sort(key=lambda x: x[0])

        # Handle out-of-range temperatures
        if outdoor_temp <= points[0][0]:
            return points[0][1]
        if outdoor_temp >= points[-1][0]:
            return points[-1][1]

        # Find bracketing points and interpolate
        for i in range(len(points) - 1):
            x1, y1 = points[i]
            x2, y2 = points[i + 1]

            if x1 <= outdoor_temp <= x2:
                if x2 == x1:
                    return y1
                ratio = (outdoor_temp - x1) / (x2 - x1)
                return y1 + ratio * (y2 - y1)

        return None

    def get_affected_points(
        self,
        current_outdoor: float,
        forecast_outdoor: float
    ) -> List[int]:
        """
        Determine which curve points should be adjusted based on
        current and forecasted outdoor temperature range.

        Args:
            current_outdoor: Current outdoor temperature (°C)
            forecast_outdoor: Forecasted outdoor temperature (°C)

        Returns:
            List of point numbers (1-10) that fall within the outdoor temp range
        """
        # Get the range of outdoor temps we care about
        min_temp = min(current_outdoor, forecast_outdoor)
        max_temp = max(current_outdoor, forecast_outdoor)

        # Add some margin to catch nearby curve points
        margin = 2.5  # Half the spacing between curve points (5°C / 2)
        min_temp -= margin
        max_temp += margin

        affected = []
        for point_num, outdoor_temp in CURVE_OUTDOOR_TEMPS.items():
            if min_temp <= outdoor_temp <= max_temp:
                affected.append(point_num)

        affected.sort()

        if self.debug_mode:
            self.logger.info(
                f"Outdoor range {current_outdoor:.1f}°C -> {forecast_outdoor:.1f}°C "
                f"affects points: {affected}"
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
                'affected_points': List[int] (point numbers 1-10),
                'reason': str,
                'confidence': float
            }
        """
        if not forecast_trend:
            return {
                'reduce': False,
                'delta': 0.0,
                'duration_hours': 0,
                'affected_points': [],
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
                'affected_points': [],
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
                'affected_points': [],
                'reason': reason,
                'confidence': 0.0
            }

        # Determine affected curve points
        forecast_outdoor = current_outdoor + forecast_change
        affected_points = self.get_affected_points(current_outdoor, forecast_outdoor)

        if not affected_points:
            return {
                'reduce': False,
                'delta': 0.0,
                'duration_hours': 0,
                'affected_points': [],
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
            'affected_points': affected_points,
            'reason': reason,
            'confidence': confidence
        }

    def enter_reduction_mode(
        self,
        current_curve: Dict[int, float],
        affected_points: List[int],
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
            current_curve: Current Y-values {point_num: supply_temp}
            affected_points: Which point numbers (1-10) to adjust
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

        if not self._y_advise_indices:
            self.logger.error("No Cwl.Advise index mapping discovered yet, call read_current_curve() first")
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
            for point_num in affected_points:
                if point_num in current_curve:
                    current_val = current_curve[point_num]
                    new_val = max(self.min_supply_temp, current_val + delta)
                    adjusted_values[point_num] = new_val

            # Apply adjustments via API using discovered Cwl.Advise indices
            success_count = 0
            for point_num, new_value in adjusted_values.items():
                advise_idx = self._y_advise_indices.get(point_num)
                if advise_idx is None:
                    self.logger.error(f"No Cwl.Advise index for point {point_num}")
                    continue
                path = f"Cwl.Advise.A[{advise_idx}]"
                if self.api.write_value(path, new_value):
                    success_count += 1
                    if self.debug_mode:
                        old_val = current_curve.get(point_num, 0)
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
            self.adjusted_points = affected_points
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

            # Restore baseline values for adjusted points
            restored_values = {}
            success_count = 0

            for point_num in self.adjusted_points:
                if point_num in baseline:
                    advise_idx = self._y_advise_indices.get(point_num)
                    if advise_idx is None:
                        self.logger.error(f"No Cwl.Advise index for point {point_num}")
                        continue
                    path = f"Cwl.Advise.A[{advise_idx}]"
                    baseline_val = baseline[point_num]

                    if self.api.write_value(path, baseline_val):
                        restored_values[point_num] = baseline_val
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
            self.adjusted_points = []
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
            'adjusted_points': self.adjusted_points,
            'adjustment_delta': self.adjustment_delta,
        }

        if self.adjustment_active:
            status['started_at'] = self.adjustment_started_at.isoformat()
            status['expires_at'] = self.adjustment_expires_at.isoformat()
            remaining = (self.adjustment_expires_at - datetime.now(timezone.utc)).total_seconds() / 3600
            status['remaining_hours'] = max(0, remaining)

        return status

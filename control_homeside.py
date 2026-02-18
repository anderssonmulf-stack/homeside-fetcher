#!/usr/bin/env python3
"""
HomeSide Heat Curve Control

Takes control of a HomeSide house's heat curve by:
1. Reading and storing the current baseline (Cwl.Advise curve + adaption settings)
2. Writing a desired curve via Cwl.Advise.A[64-73]
3. Restoring the original baseline on exit

Architecture note:
- Cwl.Advise.A[64-73] is an override layer on top of Yref/CurveAdaptation.
  Writing to it does NOT change Yref values — it's a separate writable layer.
- Writing to KU_VS1_GT_TILL_1_Yref* directly causes API timeouts.
- Only Cwl.Advise.A[*] paths work for curve writes.

Separate from heat_curve_controller.py (which handles predictions/ML).
Each building system type gets its own control script.
"""

from datetime import datetime, timezone
from typing import Dict, Optional, Any
import logging


# Curve point index (1-10) maps to outdoor temperature
CURVE_OUTDOOR_TEMPS = {
    1: -30, 2: -25, 3: -20, 4: -15, 5: -10,
    6: -5,  7: 0,   8: 5,   9: 10,  10: 15,
}

# NOTE: Cwl.Advise.A[] indices differ between HomeSide installations.
# They must be discovered dynamically from the API response.
# Do NOT hardcode index assumptions.


class HomeSideControl:
    """
    Controls a HomeSide house's heat curve.

    Usage:
        control = HomeSideControl(api, profile, logger)

        # Read what's currently set
        baseline = control.read_baseline()

        # Take control with a desired curve
        desired = {1: 38, 2: 36, 3: 35, 4: 33, 5: 31, 6: 30, 7: 28, 8: 25, 9: 22, 10: 20}
        control.enter_control(desired, reason="ML reduction")

        # Hand back control
        control.exit_control()
    """

    def __init__(self, api, profile, logger=None, seq_logger=None):
        """
        Args:
            api: HomeSideAPI instance (authenticated)
            profile: CustomerProfile instance
            logger: Python logger
            seq_logger: Optional Seq logger for structured logging
        """
        self.api = api
        self.profile = profile
        self.logger = logger or logging.getLogger(__name__)
        self.seq_logger = seq_logger
        # Discovered index mappings (populated by read_baseline or _discover_indices)
        self._y_advise_indices: Dict[int, int] = {}  # point_num -> Cwl.Advise.A index for Y values
        self._yref_advise_indices: Dict[int, int] = {}  # point_num -> Cwl.Advise.A index for Yref

    def _parse_variables(self, raw_data):
        """Parse get_heating_data() response into lookups by short name and path."""
        short_lookup = {}
        path_lookup = {}
        for var in raw_data.get('variables', []):
            full_name = var.get('variable', '')
            path = var.get('path', '')
            value = var.get('value')
            short = full_name.split('.')[-1] if '.' in full_name else full_name
            if value is not None:
                short_lookup[short] = value
            if path and value is not None:
                path_lookup[path] = value
        return short_lookup, path_lookup

    def read_baseline(self) -> Optional[Dict[str, Any]]:
        """
        Read current CurveAdaptation_Y values and adaption settings from HomeSide API.
        Discovers Cwl.Advise.A indices dynamically by variable name.

        Returns:
            Dict with 'curve' (dict of point index str -> supply temp),
            'yref' (dict of point index str -> value, for reference),
            'adaption' (bool), 'adapt_time', 'adapt_delay',
            or None on failure.
        """
        try:
            raw_data = self.api.get_heating_data()
            if not raw_data or 'variables' not in raw_data:
                self.logger.error("Failed to read heating data for baseline")
                return None

            short_lookup, path_lookup = self._parse_variables(raw_data)

            # Discover CurveAdaptation_Y and Yref indices dynamically
            curve = {}
            yref = {}
            for var in raw_data.get('variables', []):
                short_name = var.get('variable', '').split('.')[-1]
                path = var.get('path', '')
                value = var.get('value')
                if value is None:
                    continue

                # CurveAdaptation_Y_1-10 (the active adapted curve)
                if 'CurveAdaptation_Y_' in short_name:
                    try:
                        point_num = int(short_name.split('CurveAdaptation_Y_')[1])
                        if 1 <= point_num <= 10:
                            curve[str(point_num)] = float(value)
                            if path.startswith('Cwl.Advise.A['):
                                advise_idx = int(path.replace('Cwl.Advise.A[', '').replace(']', ''))
                                self._y_advise_indices[point_num] = advise_idx
                    except (ValueError, TypeError):
                        pass

                # Yref1-10 (underlying user curve, for reference)
                elif 'Yref' in short_name and 'GT_TILL' in short_name:
                    try:
                        num_str = short_name.split('Yref')[1]
                        point_num = int(num_str)
                        if 1 <= point_num <= 10:
                            yref[str(point_num)] = float(value)
                            if path.startswith('Cwl.Advise.A['):
                                advise_idx = int(path.replace('Cwl.Advise.A[', '').replace(']', ''))
                                self._yref_advise_indices[point_num] = advise_idx
                    except (ValueError, TypeError):
                        pass

            if len(curve) != 10:
                self.logger.warning(f"Incomplete CurveAdaptation_Y data: got {len(curve)}/10 points")
                if not curve:
                    return None

            # Extract adaption settings
            adaption = short_lookup.get('KU_VS1_GT_TILL_1_Adaption')

            baseline = {
                'curve': curve,
                'yref': yref,
                'adaption': bool(adaption) if adaption is not None else None,
                'adapt_time': short_lookup.get('KU_VS1_GT_TILL_1_AdaptTime'),
                'adapt_delay': short_lookup.get('KU_VS1_GT_TILL_1_AdaptDelay'),
            }

            self.logger.info(
                f"Read baseline for {self.profile.customer_id}: "
                f"adaption={baseline['adaption']}, "
                f"curve points={len(curve)}, yref points={len(yref)}"
            )
            return baseline

        except Exception as e:
            self.logger.error(f"Error reading baseline: {e}")
            return None

    def read_active_curve(self) -> Optional[Dict[str, float]]:
        """
        Read the currently active adapted curve (CurveAdaptation_Y_1-10).

        Returns what the system is actually using right now (may differ from Yref
        when adaption is enabled).
        """
        try:
            raw_data = self.api.get_heating_data()
            if not raw_data or 'variables' not in raw_data:
                return None

            var_lookup = {}
            for var in raw_data['variables']:
                path = var.get('path', '')
                short = path.split('.')[-1] if '.' in path else path
                var_lookup[short] = var.get('value')

            curve = {}
            for i in range(1, 11):
                key = f'KU_VS1_GT_TILL_1_CurveAdaptation_Y_{i}'
                val = var_lookup.get(key)
                if val is not None:
                    curve[str(i)] = float(val)

            return curve if curve else None

        except Exception as e:
            self.logger.error(f"Error reading active curve: {e}")
            return None

    def save_baseline(self, baseline: Dict[str, Any]) -> None:
        """Store baseline in profile JSON."""
        self.profile.heat_curve_control.baseline = baseline
        self.profile.save()
        self.logger.info(f"Saved baseline to profile for {self.profile.customer_id}")

    def enter_control(self, desired_curve: Dict[int, float], reason: str = "") -> bool:
        """
        Take control of the heat curve.

        1. Read and store current baseline (Cwl.Advise values)
        2. Write desired curve values via Cwl.Advise.A[64-73]

        Args:
            desired_curve: Dict mapping point index (1-10) to desired supply temp.
                           Only provided points are written; others left unchanged.
            reason: Why we're taking control (for logging)

        Returns:
            True if control was entered successfully
        """
        ctrl = self.profile.heat_curve_control

        if ctrl.in_control:
            self.logger.warning(
                f"Already in control mode since {ctrl.entered_at} "
                f"(reason: {ctrl.reason}). Call exit_control() first."
            )
            return False

        # 1. Read and store baseline
        baseline = self.read_baseline()
        if not baseline:
            self.logger.error("Cannot enter control: failed to read baseline")
            return False

        self.save_baseline(baseline)

        # 2. Write desired curve values via discovered Cwl.Advise indices
        if not self._y_advise_indices:
            self.logger.error("No Cwl.Advise index mapping discovered — read_baseline() must succeed first")
            return False

        success_count = 0
        fail_count = 0
        for point_idx, supply_temp in desired_curve.items():
            point_idx = int(point_idx)
            advise_idx = self._y_advise_indices.get(point_idx)
            if advise_idx is None:
                self.logger.warning(f"No Cwl.Advise index for point {point_idx}, skipping")
                continue

            path = f"Cwl.Advise.A[{advise_idx}]"

            if self.api.write_value(path, supply_temp):
                outdoor = CURVE_OUTDOOR_TEMPS[point_idx]
                self.logger.info(f"Wrote {path} = {supply_temp:.1f} (outdoor {outdoor:+d}C)")
                success_count += 1
            else:
                self.logger.error(f"Failed to write {path} = {supply_temp}")
                fail_count += 1

        if success_count == 0:
            self.logger.error("Failed to write any curve points — aborting control entry")
            return False

        # 4. Update profile state
        ctrl.in_control = True
        ctrl.entered_at = datetime.now(timezone.utc).isoformat()
        ctrl.reason = reason
        self.profile.save()

        msg = (
            f"Entered control mode for {self.profile.customer_id}: "
            f"{success_count} points written, {fail_count} failed. "
            f"Reason: {reason}"
        )
        self.logger.info(msg)
        print(f">> Control ON: {self.profile.friendly_name} — {success_count} curve points set ({reason})")

        if self.seq_logger:
            self.seq_logger.log(
                "Heat curve control entered for {HouseId}: {Reason}",
                level='Information',
                properties={
                    'EventType': 'HeatCurveControlEnter',
                    'HouseId': self.profile.customer_id,
                    'Reason': reason,
                    'PointsWritten': success_count,
                    'PointsFailed': fail_count,
                }
            )

        return True

    def exit_control(self, reason: str = "manual") -> bool:
        """
        Exit control mode and restore baseline curve + adaption settings.

        Args:
            reason: Why we're exiting (for logging)

        Returns:
            True if baseline was restored successfully
        """
        ctrl = self.profile.heat_curve_control

        if not ctrl.in_control:
            self.logger.info("Not in control mode, nothing to restore")
            return True

        baseline = ctrl.baseline
        if not baseline or not baseline.get('curve'):
            self.logger.error("No baseline curve stored in profile — cannot restore!")
            return False

        # 1. Restore CurveAdaptation_Y curve values
        # Re-discover indices if needed (e.g. after container restart)
        if not self._y_advise_indices:
            self.read_baseline()
        if not self._y_advise_indices:
            self.logger.error("Cannot discover Cwl.Advise indices — cannot restore")
            return False

        curve = baseline['curve']
        success_count = 0
        fail_count = 0

        for idx_str, supply_temp in curve.items():
            point_idx = int(idx_str)
            advise_idx = self._y_advise_indices.get(point_idx)
            if advise_idx is None:
                self.logger.warning(f"No Cwl.Advise index for point {point_idx}")
                continue

            path = f"Cwl.Advise.A[{advise_idx}]"

            if self.api.write_value(path, supply_temp):
                success_count += 1
            else:
                self.logger.error(f"Failed to restore {path} = {supply_temp}")
                fail_count += 1

        # 3. Update profile state
        ctrl.in_control = False
        ctrl.entered_at = None
        ctrl.reason = None
        self.profile.save()

        msg = (
            f"Exited control mode for {self.profile.customer_id}: "
            f"{success_count} points restored, {fail_count} failed. "
            f"Reason: {reason}"
        )
        self.logger.info(msg)
        print(f"<< Control OFF: {self.profile.friendly_name} — baseline restored ({reason})")

        if self.seq_logger:
            self.seq_logger.log(
                "Heat curve control exited for {HouseId}: {Reason}",
                level='Information',
                properties={
                    'EventType': 'HeatCurveControlExit',
                    'HouseId': self.profile.customer_id,
                    'Reason': reason,
                    'PointsRestored': success_count,
                    'PointsFailed': fail_count,
                }
            )

        return fail_count == 0

    def _interpolate_yref(self, outdoor_temp: float) -> Optional[float]:
        """
        Interpolate supply temp from stored Yref baseline at a given outdoor temp.

        Uses linear interpolation between the 10 curve points (-30 to +15°C).
        """
        ctrl = self.profile.heat_curve_control
        yref = ctrl.baseline.get('yref', {})
        if not yref:
            return None

        # Build sorted (outdoor_temp, supply_temp) points
        points = []
        for point_str, supply_temp in yref.items():
            point_num = int(point_str)
            if point_num in CURVE_OUTDOOR_TEMPS:
                points.append((CURVE_OUTDOOR_TEMPS[point_num], float(supply_temp)))

        if not points:
            return None

        points.sort(key=lambda x: x[0])

        # Clamp to range
        if outdoor_temp <= points[0][0]:
            return points[0][1]
        if outdoor_temp >= points[-1][0]:
            return points[-1][1]

        # Linear interpolation
        for i in range(len(points) - 1):
            x1, y1 = points[i]
            x2, y2 = points[i + 1]
            if x1 <= outdoor_temp <= x2:
                if x2 == x1:
                    return y1
                ratio = (outdoor_temp - x1) / (x2 - x1)
                return y1 + ratio * (y2 - y1)

        return None

    def compute_ml_curve(self, weather_model, conditions) -> Optional[tuple]:
        """
        Compute ML-adjusted curve as a parallel shift of the baseline Yref.

        Uses the effective temperature model to determine how much warmer/colder
        it "feels" than actual outdoor temp, then shifts the entire Yref curve
        up or down by a uniform supply temp delta.

        Args:
            weather_model: SimpleWeatherModel instance
            conditions: WeatherConditions for current weather

        Returns:
            Tuple of (curve_dict {point_num: supply_temp}, parallel_shift) or None
        """
        ctrl = self.profile.heat_curve_control
        if not ctrl.baseline or not ctrl.baseline.get('yref'):
            self.logger.error("No baseline Yref stored — cannot compute ML curve")
            return None

        # Compute effective temp offset from current conditions
        eff_result = weather_model.effective_temperature(conditions)
        outdoor_offset = eff_result.effective_temp - conditions.temperature

        # Cap offset to ±5°C for safety
        MAX_OFFSET = 5.0
        outdoor_offset = max(-MAX_OFFSET, min(MAX_OFFSET, outdoor_offset))

        # Compute parallel shift: use curve slope at 0°C reference point
        # to convert outdoor offset to supply temp delta
        reference_supply = self._interpolate_yref(0.0)
        shifted_supply = self._interpolate_yref(0.0 + outdoor_offset)

        if reference_supply is None or shifted_supply is None:
            self.logger.error("Cannot compute parallel shift from Yref")
            return None

        parallel_shift = round(shifted_supply - reference_supply, 1)

        # Apply uniform shift to all Yref points
        yref = ctrl.baseline['yref']
        ml_curve = {}
        for point_num in range(1, 11):
            baseline_supply = float(yref.get(str(point_num), 0))
            ml_curve[point_num] = round(baseline_supply + parallel_shift, 1)

        return ml_curve, parallel_shift

    def enter_ml_control(self, weather_model, conditions) -> bool:
        """
        Enter ML curve control mode.

        1. Read and store baseline (both Yref and CurveAdaptation_Y)
        2. Compute initial ML curve from weather conditions
        3. Write ML-adjusted values to Yref (the manual reference curve)

        By writing to Yref instead of CurveAdaptation_Y, HomeSide's own
        adaptation works FROM our ML-adjusted baseline rather than fighting it.

        Returns:
            True if ML control was entered successfully
        """
        ctrl = self.profile.heat_curve_control

        if ctrl.in_control:
            self.logger.warning(
                f"Already in control mode since {ctrl.entered_at} "
                f"(reason: {ctrl.reason}). Call exit_ml_control() first."
            )
            return False

        # 1. Read and store baseline
        baseline = self.read_baseline()
        if not baseline or not baseline.get('yref'):
            self.logger.error("Cannot enter ML control: failed to read baseline Yref")
            return False

        self.save_baseline(baseline)

        # 2. Compute initial ML curve
        result = self.compute_ml_curve(weather_model, conditions)
        if result is None:
            self.logger.error("Cannot enter ML control: failed to compute ML curve")
            return False

        ml_curve, parallel_shift = result

        # 3. Write to Yref indices
        if not self._yref_advise_indices:
            self.logger.error("No Yref Cwl.Advise index mapping discovered")
            return False

        success_count = 0
        for point_num, supply_temp in ml_curve.items():
            advise_idx = self._yref_advise_indices.get(point_num)
            if advise_idx is None:
                continue
            path = f"Cwl.Advise.A[{advise_idx}]"
            if self.api.write_value(path, supply_temp):
                success_count += 1
            else:
                self.logger.error(f"Failed to write Yref {path} = {supply_temp}")

        if success_count == 0:
            self.logger.error("Failed to write any Yref points — aborting ML control entry")
            return False

        # 4. Update profile state
        ctrl.in_control = True
        ctrl.entered_at = datetime.now(timezone.utc).isoformat()
        ctrl.reason = "ML curve control"
        ctrl.ml_last_offset = round(parallel_shift, 2)
        self.profile.save()

        msg = (
            f"Entered ML control for {self.profile.customer_id}: "
            f"shift={parallel_shift:+.1f}°C, {success_count}/10 Yref points written"
        )
        self.logger.info(msg)
        print(f">> ML Control ON: {self.profile.friendly_name} — {success_count} Yref points set (shift: {parallel_shift:+.1f}°C)")

        if self.seq_logger:
            self.seq_logger.log(
                "ML curve control entered for {HouseId}: shift {ParallelShift}°C supply, {PointsWritten}/10 points",
                level='Information',
                properties={
                    'EventType': 'MLCurveControlEnter',
                    'HouseId': self.profile.customer_id,
                    'ParallelShift': round(parallel_shift, 2),
                    'PointsWritten': success_count,
                    'MLCurve': {str(k): v for k, v in ml_curve.items()},
                }
            )

        return True

    def exit_ml_control(self, reason: str = "manual") -> bool:
        """
        Exit ML control mode and restore original Yref values.

        Writes the stored baseline Yref back to the Yref Cwl.Advise indices,
        returning HomeSide to its original manual curve.

        Args:
            reason: Why we're exiting (for logging)

        Returns:
            True if original Yref was restored successfully
        """
        ctrl = self.profile.heat_curve_control

        if not ctrl.in_control:
            self.logger.info("Not in ML control mode, nothing to restore")
            return True

        baseline = ctrl.baseline
        if not baseline or not baseline.get('yref'):
            self.logger.error("No baseline Yref stored in profile — cannot restore!")
            return False

        # Re-discover indices if needed (e.g. after container restart)
        if not self._yref_advise_indices:
            self.read_baseline()
        if not self._yref_advise_indices:
            self.logger.error("Cannot discover Yref Cwl.Advise indices — cannot restore")
            return False

        # Restore original Yref values
        yref = baseline['yref']
        success_count = 0
        fail_count = 0

        for idx_str, supply_temp in yref.items():
            point_idx = int(idx_str)
            advise_idx = self._yref_advise_indices.get(point_idx)
            if advise_idx is None:
                self.logger.warning(f"No Yref Cwl.Advise index for point {point_idx}")
                continue

            path = f"Cwl.Advise.A[{advise_idx}]"
            if self.api.write_value(path, supply_temp):
                success_count += 1
            else:
                self.logger.error(f"Failed to restore Yref {path} = {supply_temp}")
                fail_count += 1

        # Update profile state
        ctrl.in_control = False
        ctrl.entered_at = None
        ctrl.reason = None
        ctrl.ml_last_offset = None
        self.profile.save()

        msg = (
            f"Exited ML control for {self.profile.customer_id}: "
            f"{success_count} Yref points restored, {fail_count} failed. "
            f"Reason: {reason}"
        )
        self.logger.info(msg)
        print(f"<< ML Control OFF: {self.profile.friendly_name} — original Yref restored ({reason})")

        if self.seq_logger:
            self.seq_logger.log(
                "ML curve control exited for {HouseId}: {Reason}",
                level='Information',
                properties={
                    'EventType': 'MLCurveControlExit',
                    'HouseId': self.profile.customer_id,
                    'Reason': reason,
                    'PointsRestored': success_count,
                    'PointsFailed': fail_count,
                }
            )

        return fail_count == 0

    def update_ml_curve(self, weather_model, conditions) -> bool:
        """
        Compute and write ML-adjusted curve to HomeSide Yref.

        Writes to Yref indices so HomeSide's own adaptation works from our
        ML-adjusted baseline. Skips write if parallel shift hasn't changed
        significantly (< 0.3°C) to avoid churn.

        Args:
            weather_model: SimpleWeatherModel instance
            conditions: WeatherConditions for current weather

        Returns:
            True if curve was written (or skipped due to small change)
        """
        result = self.compute_ml_curve(weather_model, conditions)
        if result is None:
            return False

        ml_curve, parallel_shift = result
        ctrl = self.profile.heat_curve_control

        # Check if shift changed enough to justify a write
        if ctrl.ml_last_offset is not None:
            if abs(parallel_shift - ctrl.ml_last_offset) < 0.3:
                self.logger.debug(
                    f"ML curve shift {parallel_shift:+.1f}°C unchanged from last "
                    f"({ctrl.ml_last_offset:+.1f}°C), skipping write"
                )
                return True

        # Ensure we have Yref Cwl.Advise indices
        if not self._yref_advise_indices:
            self.read_baseline()
        if not self._yref_advise_indices:
            self.logger.error("Cannot discover Yref Cwl.Advise indices for ML curve write")
            return False

        # Write all 10 Yref points
        success_count = 0
        for point_num, supply_temp in ml_curve.items():
            advise_idx = self._yref_advise_indices.get(point_num)
            if advise_idx is None:
                continue
            path = f"Cwl.Advise.A[{advise_idx}]"
            if self.api.write_value(path, supply_temp):
                success_count += 1
            else:
                self.logger.error(f"Failed to write Yref {path} = {supply_temp}")

        if success_count == 0:
            self.logger.error("Failed to write any Yref points")
            return False

        # Log baseline for reference
        yref = ctrl.baseline.get('yref', {})

        # Update profile with new shift
        previous_shift = ctrl.ml_last_offset
        ctrl.ml_last_offset = round(parallel_shift, 2)
        self.profile.save()

        msg = (
            f"ML curve updated for {self.profile.customer_id}: "
            f"shift={parallel_shift:+.1f}°C supply, {success_count}/10 Yref points written"
        )
        self.logger.info(msg)

        if self.seq_logger:
            self.seq_logger.log(
                "ML curve update for {HouseId}: shift {ParallelShift:+.1f}°C supply ({PreviousShift} -> {ParallelShift}), {PointsWritten}/10 points",
                level='Information',
                properties={
                    'EventType': 'MLCurveUpdate',
                    'HouseId': self.profile.customer_id,
                    'ParallelShift': round(parallel_shift, 2),
                    'PreviousShift': previous_shift,
                    'PointsWritten': success_count,
                    'MLCurve': {str(k): v for k, v in ml_curve.items()},
                    'BaselineYref': {str(k): float(v) for k, v in yref.items()},
                }
            )

        return True

    def get_status(self) -> Dict[str, Any]:
        """Get current control status."""
        ctrl = self.profile.heat_curve_control
        status = {
            'in_control': ctrl.in_control,
            'entered_at': ctrl.entered_at,
            'reason': ctrl.reason,
            'has_baseline': bool(ctrl.baseline and ctrl.baseline.get('yref')),
            'ml_enabled': ctrl.ml_enabled,
            'ml_last_offset': ctrl.ml_last_offset,
        }
        if ctrl.baseline:
            status['baseline_adaption'] = ctrl.baseline.get('adaption')
            status['baseline_points'] = len(ctrl.baseline.get('curve', {}))
        return status

    # NOTE: Writing to KU_VS1_GT_TILL_1_Adaption causes API timeouts.
    # Only Cwl.Advise.A[*] paths work for writes. The adaption toggle
    # may need a different mechanism (e.g. HomeSide web UI) or a yet-
    # undiscovered write path. For now, we control only the curve values.


def format_curve(curve: Dict[str, float]) -> str:
    """Format a curve dict (point index -> supply temp) as a readable string."""
    lines = []
    for i in range(1, 11):
        key = str(i)
        if key in curve:
            outdoor = CURVE_OUTDOOR_TEMPS[i]
            lines.append(f"  {outdoor:+3d}C -> {curve[key]:.1f}C")
    return '\n'.join(lines)

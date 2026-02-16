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

# Cwl.Advise.A write indices: A[64]=point 1 (-30°C) ... A[73]=point 10 (+15°C)
POINT_TO_ADVISE_INDEX = {i: 63 + i for i in range(1, 11)}
ADVISE_INDEX_TO_POINT = {v: k for k, v in POINT_TO_ADVISE_INDEX.items()}


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
        Read current Cwl.Advise curve values and adaption settings from HomeSide API.

        Cwl.Advise.A[64-73] is the writable override layer. Yref is the underlying
        user curve (read-only via API). We store the Advise values since that's what
        we can write back.

        Returns:
            Dict with 'curve' (dict of point index -> supply temp),
            'yref' (dict of index -> value, for reference),
            'adaption' (bool), 'adapt_time', 'adapt_delay',
            or None on failure.
        """
        try:
            raw_data = self.api.get_heating_data()
            if not raw_data or 'variables' not in raw_data:
                self.logger.error("Failed to read heating data for baseline")
                return None

            short_lookup, path_lookup = self._parse_variables(raw_data)

            # Extract Cwl.Advise.A[64-73] values (the writable override layer)
            curve = {}
            for point_idx, advise_idx in POINT_TO_ADVISE_INDEX.items():
                path = f'Cwl.Advise.A[{advise_idx}]'
                val = path_lookup.get(path)
                if val is not None:
                    curve[str(point_idx)] = float(val)

            if len(curve) != 10:
                self.logger.warning(f"Incomplete Cwl.Advise data: got {len(curve)}/10 points")
                if not curve:
                    return None

            # Also read Yref (underlying user curve) for reference
            yref = {}
            for i in range(1, 11):
                val = short_lookup.get(f'KU_VS1_GT_TILL_1_Yref{i}')
                if val is not None:
                    yref[str(i)] = float(val)

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

        # 2. Write desired curve values via Cwl.Advise
        success_count = 0
        fail_count = 0
        for point_idx, supply_temp in desired_curve.items():
            point_idx = int(point_idx)
            if point_idx not in POINT_TO_ADVISE_INDEX:
                self.logger.warning(f"Invalid point index {point_idx}, skipping")
                continue

            advise_idx = POINT_TO_ADVISE_INDEX[point_idx]
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

        # 1. Restore Cwl.Advise curve values
        curve = baseline['curve']
        success_count = 0
        fail_count = 0

        for idx_str, supply_temp in curve.items():
            point_idx = int(idx_str)
            if point_idx not in POINT_TO_ADVISE_INDEX:
                continue

            advise_idx = POINT_TO_ADVISE_INDEX[point_idx]
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

    def get_status(self) -> Dict[str, Any]:
        """Get current control status."""
        ctrl = self.profile.heat_curve_control
        status = {
            'in_control': ctrl.in_control,
            'entered_at': ctrl.entered_at,
            'reason': ctrl.reason,
            'has_baseline': bool(ctrl.baseline and ctrl.baseline.get('yref')),
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

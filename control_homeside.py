#!/usr/bin/env python3
"""
HomeSide Heat Curve Control

Takes control of a HomeSide house's heat curve by:
1. Reading and storing the current baseline (Yref curve + adaption settings)
2. Disabling adaption and writing a desired curve
3. Restoring the original baseline on exit

Separate from heat_curve_controller.py (which handles predictions/ML).
Each building system type gets its own control script.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import logging


# Yref index (1-10) maps to outdoor temperature
YREF_OUTDOOR_TEMPS = {
    1: -30, 2: -25, 3: -20, 4: -15, 5: -10,
    6: -5,  7: 0,   8: 5,   9: 10,  10: 15,
}

# Cwl.Advise.A write index maps to Yref index
# A[64] = Yref1 (-30), A[65] = Yref2 (-25), ... A[73] = Yref10 (+15)
YREF_TO_WRITE_INDEX = {i: 63 + i for i in range(1, 11)}
WRITE_INDEX_TO_YREF = {v: k for k, v in YREF_TO_WRITE_INDEX.items()}

# Variables to read from HomeSide for baseline
ADAPTION_VARS = [
    'KU_VS1_GT_TILL_1_Adaption',
    'KU_VS1_GT_TILL_1_AdaptTime',
    'KU_VS1_GT_TILL_1_AdaptDelay',
]

YREF_VARS = [f'KU_VS1_GT_TILL_1_Yref{i}' for i in range(1, 11)]

CURVE_ADAPTATION_VARS = [f'KU_VS1_GT_TILL_1_CurveAdaptation_Y_{i}' for i in range(1, 11)]


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

    def read_baseline(self) -> Optional[Dict[str, Any]]:
        """
        Read current Yref curve and adaption settings from HomeSide API.

        Returns:
            Dict with 'yref' (dict of index->value), 'adaption' (bool),
            'adapt_time', 'adapt_delay', or None on failure.
        """
        try:
            raw_data = self.api.get_heating_data()
            if not raw_data or 'variables' not in raw_data:
                self.logger.error("Failed to read heating data for baseline")
                return None

            # Build lookup by short_name
            var_lookup = {}
            for var in raw_data['variables']:
                path = var.get('path', '')
                short = path.split('.')[-1] if '.' in path else path
                var_lookup[short] = var.get('value')

            # Extract Yref values
            yref = {}
            for i in range(1, 11):
                key = f'KU_VS1_GT_TILL_1_Yref{i}'
                val = var_lookup.get(key)
                if val is not None:
                    yref[str(i)] = float(val)

            if len(yref) != 10:
                self.logger.warning(f"Incomplete Yref data: got {len(yref)}/10 points")
                if not yref:
                    return None

            # Extract adaption settings
            adaption = var_lookup.get('KU_VS1_GT_TILL_1_Adaption')
            adapt_time = var_lookup.get('KU_VS1_GT_TILL_1_AdaptTime')
            adapt_delay = var_lookup.get('KU_VS1_GT_TILL_1_AdaptDelay')

            baseline = {
                'yref': yref,
                'adaption': bool(adaption) if adaption is not None else None,
                'adapt_time': adapt_time,
                'adapt_delay': adapt_delay,
            }

            self.logger.info(
                f"Read baseline for {self.profile.customer_id}: "
                f"adaption={baseline['adaption']}, "
                f"yref points={len(yref)}"
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

        1. Read and store current baseline
        2. Disable adaption
        3. Write desired curve values

        Args:
            desired_curve: Dict mapping Yref index (1-10) to desired supply temp.
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

        # 2. Disable adaption (so HomeSide doesn't override our curve)
        adaption_written = self._write_adaption(False)
        if not adaption_written:
            self.logger.warning("Could not disable adaption — continuing with curve write anyway")

        # 3. Write desired curve values
        success_count = 0
        fail_count = 0
        for yref_idx, supply_temp in desired_curve.items():
            yref_idx = int(yref_idx)
            if yref_idx not in YREF_TO_WRITE_INDEX:
                self.logger.warning(f"Invalid Yref index {yref_idx}, skipping")
                continue

            write_idx = YREF_TO_WRITE_INDEX[yref_idx]
            path = f"Cwl.Advise.A[{write_idx}]"

            if self.api.write_value(path, supply_temp):
                outdoor = YREF_OUTDOOR_TEMPS[yref_idx]
                self.logger.info(f"Wrote {path} = {supply_temp:.1f} (outdoor {outdoor:+d}C)")
                success_count += 1
            else:
                self.logger.error(f"Failed to write {path} = {supply_temp}")
                fail_count += 1

        if success_count == 0:
            self.logger.error("Failed to write any curve points — aborting control entry")
            # Try to restore adaption
            if baseline.get('adaption'):
                self._write_adaption(True)
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
        if not baseline or not baseline.get('yref'):
            self.logger.error("No baseline stored in profile — cannot restore!")
            return False

        # 1. Restore Yref curve values
        yref = baseline['yref']
        success_count = 0
        fail_count = 0

        for idx_str, supply_temp in yref.items():
            yref_idx = int(idx_str)
            if yref_idx not in YREF_TO_WRITE_INDEX:
                continue

            write_idx = YREF_TO_WRITE_INDEX[yref_idx]
            path = f"Cwl.Advise.A[{write_idx}]"

            if self.api.write_value(path, supply_temp):
                success_count += 1
            else:
                self.logger.error(f"Failed to restore {path} = {supply_temp}")
                fail_count += 1

        # 2. Restore adaption setting
        original_adaption = baseline.get('adaption')
        if original_adaption is not None:
            self._write_adaption(original_adaption)

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
            status['baseline_points'] = len(ctrl.baseline.get('yref', {}))
        return status

    def _write_adaption(self, enabled: bool) -> bool:
        """
        Write the adaption flag to HomeSide.

        The write path for adaption needs discovery. We try the variable name
        directly first, which works for many HomeSide variables.
        """
        # Try the direct variable name as write path
        path = 'KU_VS1_GT_TILL_1_Adaption'
        value = 'true' if enabled else 'false'

        result = self.api.write_value(path, value)
        if result:
            self.logger.info(f"Set adaption = {enabled}")
        else:
            self.logger.warning(
                f"Failed to write adaption via '{path}'. "
                f"The write path may need discovery — run discover_adaption_write_path()."
            )
        return result

    def discover_adaption_write_path(self) -> Optional[str]:
        """
        Discover the correct write path for the adaption toggle.

        Reads current value, tries writing the opposite via candidate paths,
        reads again to confirm, then restores the original.

        Returns the working write path, or None if none found.
        """
        # Read current adaption state
        baseline = self.read_baseline()
        if baseline is None or baseline.get('adaption') is None:
            self.logger.error("Cannot discover: failed to read current adaption state")
            return None

        original = baseline['adaption']
        opposite = not original
        opposite_str = 'true' if opposite else 'false'
        original_str = 'true' if original else 'false'

        # Candidate write paths to try
        candidates = [
            'KU_VS1_GT_TILL_1_Adaption',
            f'{self.profile.customer_id}.KU_VS1_GT_TILL_1_Adaption',
        ]

        for path in candidates:
            self.logger.info(f"Testing write path: {path} = {opposite_str}")

            if not self.api.write_value(path, opposite_str):
                self.logger.info(f"  Write failed for {path}")
                continue

            # Read back to verify
            import time
            time.sleep(2)
            check = self.read_baseline()
            if check and check.get('adaption') == opposite:
                self.logger.info(f"  Confirmed: {path} works!")
                # Restore original
                self.api.write_value(path, original_str)
                time.sleep(1)
                return path
            else:
                self.logger.info(f"  Write appeared to succeed but value didn't change")

        self.logger.warning("No working write path found for adaption toggle")
        return None


def format_curve(yref: Dict[str, float]) -> str:
    """Format a Yref curve dict as a readable string."""
    lines = []
    for i in range(1, 11):
        key = str(i)
        if key in yref:
            outdoor = YREF_OUTDOOR_TEMPS[i]
            lines.append(f"  {outdoor:+3d}C -> {yref[key]:.1f}C")
    return '\n'.join(lines)

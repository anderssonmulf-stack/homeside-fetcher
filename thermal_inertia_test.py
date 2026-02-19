#!/usr/bin/env python3
"""
Thermal Inertia Test

Measures a building's thermal time constant (τ) by:
1. Heating to setpoint + 0.5°C
2. Turning off heating (minimum supply)
3. Measuring how long it takes for indoor temp to drop 1.0°C
4. Calculating τ from Newton's law of cooling

The test runs at night (23:00–07:00) under stable conditions and requires
explicit user approval via email confirmation link.
"""

import math
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from dataclasses import dataclass, field


# Test parameters
TEST_OVERSHOOT = 0.5      # Heat to setpoint + this value before turning off
TEST_DROP = 1.0           # Measure time for this temperature drop (hardcoded)
TEST_TIMEOUT_HOURS = 8.0  # Max test duration
MIN_SUPPLY_TEMP = 15.0    # Minimum supply temp during cooldown phase

# Qualifying conditions for running the test
TEST_WINDOW_START = 23    # Earliest hour to start (23:00)
TEST_WINDOW_END = 7       # Latest hour to still be running (07:00)
MAX_WIND_SPEED = 3.0      # m/s
MIN_OUTDOOR_DELTA = 20.0  # outdoor must be at least setpoint - 20°C
MAX_FORECAST_SWING = 2.0  # Max outdoor temp change during test window

# How often to check if a house needs calibration
CALIBRATION_STALE_MONTHS = 10


@dataclass
class TestState:
    """Tracks state of an active thermal inertia test."""
    phase: str = "idle"  # idle, heating, cooldown, complete, failed
    started_at: Optional[str] = None
    phase_started_at: Optional[str] = None
    initial_indoor_temp: Optional[float] = None
    peak_indoor_temp: Optional[float] = None
    cooldown_start_temp: Optional[float] = None
    target_drop_temp: Optional[float] = None
    outdoor_temp_at_start: Optional[float] = None
    setpoint: Optional[float] = None
    readings: List[Dict] = field(default_factory=list)
    result_tau: Optional[float] = None
    failure_reason: Optional[str] = None


class ThermalInertiaTest:
    """
    Manages thermal inertia testing for a house.

    Usage:
        test = ThermalInertiaTest(profile, control, logger, seq_logger)

        # Check if test is needed and conditions are right
        if test.needs_calibration() and test.check_conditions(weather_data, forecast):
            # Send email for approval (done by caller)
            pass

        # After approval, run test phases in the poll loop:
        test.start_test(current_indoor, setpoint, outdoor_temp)
        # ... each poll cycle:
        action = test.poll(current_indoor, outdoor_temp)
        # action is "heat", "cooldown", "restore", or None
    """

    def __init__(self, profile, control, logger=None, seq_logger=None):
        """
        Args:
            profile: CustomerProfile instance
            control: HomeSideControl instance (for curve writes)
            logger: Python logger
            seq_logger: Optional Seq logger
        """
        self.profile = profile
        self.control = control
        self.logger = logger or logging.getLogger(__name__)
        self.seq_logger = seq_logger
        self.state = TestState()

    def needs_calibration(self) -> bool:
        """Check if this house needs a thermal inertia measurement."""
        learned = self.profile.learned

        # Never measured
        if learned.thermal_time_constant is None:
            return True

        # Measured but stale
        if learned.thermal_time_constant_measured_at:
            try:
                measured = datetime.fromisoformat(
                    learned.thermal_time_constant_measured_at.replace('Z', '+00:00')
                )
                age_months = (datetime.now(timezone.utc) - measured).days / 30
                if age_months > CALIBRATION_STALE_MONTHS:
                    return True
            except (ValueError, TypeError):
                return True

        return False

    def check_conditions(self, weather_obs: Dict, forecast_data: Dict = None,
                         setpoint: float = 22.0) -> bool:
        """
        Check if current conditions are suitable for running the test.

        Args:
            weather_obs: Current weather observation dict
            forecast_data: Weather forecast for the night
            setpoint: Current indoor setpoint

        Returns:
            True if conditions are suitable
        """
        now = datetime.now(timezone.utc)
        hour = now.hour

        # Must be within test window (23:00-07:00 local, approximate with UTC+1)
        local_hour = (hour + 1) % 24  # Approximate CET
        if not (local_hour >= TEST_WINDOW_START or local_hour < TEST_WINDOW_END - 2):
            # Need at least 2 hours before window closes
            return False

        # Wind speed check
        wind = weather_obs.get('wind_speed')
        if wind is not None and wind > MAX_WIND_SPEED:
            return False

        # Outdoor temp check: must be cold enough for meaningful heat loss
        outdoor = weather_obs.get('temperature')
        if outdoor is None:
            return False
        max_outdoor = setpoint - MIN_OUTDOOR_DELTA
        if outdoor > max_outdoor:
            return False

        # Forecast stability check
        if forecast_data:
            temps = [p.get('temperature') for p in forecast_data.get('points', [])
                     if p.get('temperature') is not None]
            if temps and len(temps) >= 2:
                swing = max(temps) - min(temps)
                if swing > MAX_FORECAST_SWING:
                    return False

        return True

    def start_test(self, current_indoor: float, setpoint: float,
                   outdoor_temp: float) -> str:
        """
        Start the thermal inertia test.

        Phase 1: Heat to setpoint + TEST_OVERSHOOT

        Args:
            current_indoor: Current indoor temperature
            setpoint: Current setpoint
            outdoor_temp: Current outdoor temperature

        Returns:
            Initial phase: "heating" or "cooldown" if already warm enough
        """
        self.state = TestState(
            phase="heating",
            started_at=datetime.now(timezone.utc).isoformat(),
            phase_started_at=datetime.now(timezone.utc).isoformat(),
            initial_indoor_temp=current_indoor,
            setpoint=setpoint,
            outdoor_temp_at_start=outdoor_temp,
        )

        target_heat = setpoint + TEST_OVERSHOOT
        self.logger.info(
            f"Thermal inertia test started for {self.profile.customer_id}: "
            f"indoor={current_indoor:.1f}°C, target={target_heat:.1f}°C, "
            f"outdoor={outdoor_temp:.1f}°C"
        )

        if self.seq_logger:
            self.seq_logger.log(
                "Thermal inertia test started for {HouseId}",
                level='Information',
                properties={
                    'EventType': 'ThermalTestStart',
                    'HouseId': self.profile.customer_id,
                    'IndoorTemp': round(current_indoor, 1),
                    'SetPoint': setpoint,
                    'OutdoorTemp': round(outdoor_temp, 1),
                    'TargetHeatTemp': round(target_heat, 1),
                }
            )

        # If already warm enough, skip straight to cooldown
        if current_indoor >= target_heat:
            return self._enter_cooldown(current_indoor, outdoor_temp)

        return "heating"

    def _enter_cooldown(self, current_indoor: float, outdoor_temp: float) -> str:
        """Transition to cooldown phase."""
        self.state.phase = "cooldown"
        self.state.phase_started_at = datetime.now(timezone.utc).isoformat()
        self.state.peak_indoor_temp = current_indoor
        self.state.cooldown_start_temp = current_indoor
        self.state.target_drop_temp = current_indoor - TEST_DROP
        self.state.readings = []

        self.logger.info(
            f"Thermal test cooldown started: peak={current_indoor:.1f}°C, "
            f"target={self.state.target_drop_temp:.1f}°C"
        )

        return "cooldown"

    def poll(self, current_indoor: float, outdoor_temp: float) -> Optional[str]:
        """
        Called each poll cycle during an active test.

        Args:
            current_indoor: Current indoor temperature
            outdoor_temp: Current outdoor temperature

        Returns:
            Action for the caller:
            - "heat": boost supply temp to heat up
            - "cooldown": reduce supply to minimum
            - "restore": test complete or failed, restore normal operation
            - None: no test active
        """
        if self.state.phase == "idle":
            return None

        now = datetime.now(timezone.utc)

        # Check timeout
        if self.state.started_at:
            started = datetime.fromisoformat(self.state.started_at.replace('Z', '+00:00'))
            if (now - started).total_seconds() > TEST_TIMEOUT_HOURS * 3600:
                return self._finish_test("timeout", outdoor_temp)

        # Check time window (stop if past 07:00 local)
        local_hour = (now.hour + 1) % 24  # Approximate CET
        if self.state.phase != "heating" and TEST_WINDOW_END <= local_hour < TEST_WINDOW_START:
            return self._finish_test("window_closed", outdoor_temp)

        if self.state.phase == "heating":
            target_heat = self.state.setpoint + TEST_OVERSHOOT
            if current_indoor >= target_heat:
                self._enter_cooldown(current_indoor, outdoor_temp)
                return "cooldown"
            return "heat"

        elif self.state.phase == "cooldown":
            # Record reading
            self.state.readings.append({
                'timestamp': now.isoformat(),
                'indoor_temp': round(current_indoor, 2),
                'outdoor_temp': round(outdoor_temp, 2),
            })

            # Check if we've dropped enough
            if current_indoor <= self.state.target_drop_temp:
                return self._finish_test("success", outdoor_temp)

            return "cooldown"

        return None

    def _finish_test(self, reason: str, outdoor_temp: float) -> str:
        """
        Finish the test and calculate τ if possible.

        Returns "restore" to signal the caller to restore normal operation.
        """
        readings = self.state.readings
        cooldown_start = self.state.cooldown_start_temp
        outdoor_at_start = self.state.outdoor_temp_at_start

        if reason == "success" and readings and cooldown_start is not None:
            # Calculate τ from Newton's law of cooling
            # T(t) = T_outdoor + (T_start - T_outdoor) * e^(-t/τ)
            # τ = -t / ln((T_end - T_outdoor) / (T_start - T_outdoor))
            first_reading = datetime.fromisoformat(readings[0]['timestamp'].replace('Z', '+00:00'))
            last_reading = datetime.fromisoformat(readings[-1]['timestamp'].replace('Z', '+00:00'))
            elapsed_hours = (last_reading - first_reading).total_seconds() / 3600

            t_start = cooldown_start
            t_end = readings[-1]['indoor_temp']
            # Use average outdoor temp during cooldown for accuracy
            avg_outdoor = sum(r['outdoor_temp'] for r in readings) / len(readings)

            numerator = t_end - avg_outdoor
            denominator = t_start - avg_outdoor

            if denominator > 0 and numerator > 0 and numerator < denominator:
                tau = -elapsed_hours / math.log(numerator / denominator)
                self.state.result_tau = round(tau, 1)
                self.state.phase = "complete"

                # Store in profile
                self.profile.learned.thermal_time_constant = round(tau, 1)
                self.profile.learned.thermal_time_constant_measured_at = \
                    datetime.now(timezone.utc).isoformat()
                self.profile.learned.thermal_time_constant_source = "measured"
                self.profile.learned.thermal_time_constant_copied_from = None
                self.profile.save()

                self.logger.info(
                    f"Thermal inertia test complete for {self.profile.customer_id}: "
                    f"τ = {tau:.1f} hours (drop: {t_start:.1f} -> {t_end:.1f}°C "
                    f"in {elapsed_hours:.1f}h, avg outdoor: {avg_outdoor:.1f}°C)"
                )

                if self.seq_logger:
                    self.seq_logger.log(
                        "Thermal inertia test complete for {HouseId}: τ = {Tau} hours",
                        level='Information',
                        properties={
                            'EventType': 'ThermalTestComplete',
                            'HouseId': self.profile.customer_id,
                            'Tau': round(tau, 1),
                            'StartTemp': round(t_start, 1),
                            'EndTemp': round(t_end, 1),
                            'ElapsedHours': round(elapsed_hours, 1),
                            'AvgOutdoorTemp': round(avg_outdoor, 1),
                            'ReadingCount': len(readings),
                        }
                    )
            else:
                reason = "invalid_data"

        if reason != "success":
            self.state.phase = "failed"
            self.state.failure_reason = reason

            # If we timed out but have partial data, try to calculate from what we have
            if reason == "timeout" and readings and len(readings) >= 6 and cooldown_start:
                first_reading = datetime.fromisoformat(readings[0]['timestamp'].replace('Z', '+00:00'))
                last_reading = datetime.fromisoformat(readings[-1]['timestamp'].replace('Z', '+00:00'))
                elapsed_hours = (last_reading - first_reading).total_seconds() / 3600
                t_end = readings[-1]['indoor_temp']
                avg_outdoor = sum(r['outdoor_temp'] for r in readings) / len(readings)
                actual_drop = cooldown_start - t_end

                if actual_drop >= 0.3:  # At least 0.3°C drop for partial calculation
                    numerator = t_end - avg_outdoor
                    denominator = cooldown_start - avg_outdoor
                    if denominator > 0 and numerator > 0 and numerator < denominator:
                        tau = -elapsed_hours / math.log(numerator / denominator)
                        self.state.result_tau = round(tau, 1)
                        self.logger.info(
                            f"Thermal test timed out but got partial τ = {tau:.1f}h "
                            f"(drop: {actual_drop:.1f}°C in {elapsed_hours:.1f}h)"
                        )
                        # Store partial result with lower confidence
                        self.profile.learned.thermal_time_constant = round(tau, 1)
                        self.profile.learned.thermal_time_constant_measured_at = \
                            datetime.now(timezone.utc).isoformat()
                        self.profile.learned.thermal_time_constant_source = "measured_partial"
                        self.profile.save()

            self.logger.warning(
                f"Thermal inertia test ended for {self.profile.customer_id}: {reason}"
            )

            if self.seq_logger:
                self.seq_logger.log(
                    "Thermal inertia test ended for {HouseId}: {Reason}",
                    level='Warning',
                    properties={
                        'EventType': 'ThermalTestEnd',
                        'HouseId': self.profile.customer_id,
                        'Reason': reason,
                        'ReadingCount': len(readings),
                        'PartialTau': self.state.result_tau,
                    }
                )

        return "restore"

    def get_supply_for_phase(self) -> Optional[float]:
        """
        Get the supply temp to write for the current test phase.

        Returns:
            Supply temp to set, or None if no test active.
            During heating: setpoint + 10°C (boost)
            During cooldown: MIN_SUPPLY_TEMP (15°C)
        """
        if self.state.phase == "heating":
            # Boost: set supply high to heat up quickly
            return (self.state.setpoint or 22.0) + 10.0
        elif self.state.phase == "cooldown":
            return MIN_SUPPLY_TEMP
        return None

    @property
    def is_active(self) -> bool:
        return self.state.phase in ("heating", "cooldown")

    def abort(self, reason: str = "manual") -> None:
        """Abort an active test."""
        if self.is_active:
            self.state.phase = "failed"
            self.state.failure_reason = f"aborted: {reason}"
            self.logger.info(f"Thermal inertia test aborted: {reason}")


def copy_thermal_constant(source_profile, target_profile, logger=None) -> bool:
    """
    Copy thermal time constant from one profile to another.

    For identical buildings (same construction) that don't need separate tests.

    Args:
        source_profile: CustomerProfile with measured τ
        target_profile: CustomerProfile to copy to

    Returns:
        True if copied successfully
    """
    logger = logger or logging.getLogger(__name__)
    source_tau = source_profile.learned.thermal_time_constant

    if source_tau is None:
        logger.warning(
            f"Cannot copy τ: source {source_profile.customer_id} has no measurement"
        )
        return False

    target_profile.learned.thermal_time_constant = source_tau
    target_profile.learned.thermal_time_constant_measured_at = \
        datetime.now(timezone.utc).isoformat()
    target_profile.learned.thermal_time_constant_source = "copied"
    target_profile.learned.thermal_time_constant_copied_from = \
        source_profile.customer_id
    target_profile.save()

    logger.info(
        f"Copied τ = {source_tau}h from {source_profile.customer_id} "
        f"to {target_profile.customer_id}"
    )
    return True

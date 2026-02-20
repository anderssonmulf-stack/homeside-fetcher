#!/usr/bin/env python3
"""
Thermal Inertia Test

Measures a building's thermal time constant (τ) by:
1. If house is below setpoint: preheat to setpoint + 0.5°C first
2. Lowering Yref to minimum supply temperature
3. Waiting for heating to actually stop (secondary-side delta T)
4. Recording starting temperature when heating confirmed off
5. Measuring how long it takes for indoor temp to drop 1.0°C
6. Calculating τ from Newton's law of cooling

If the house is already at setpoint (e.g., sunny day), preheating is
skipped and the test goes straight to lowering Yref.

DHW events cause primary-side power spikes, so power meter is NOT used
for settling detection. Only secondary-side supply-return delta T is
reliable for confirming space heating is off.

The test runs at night under stable conditions and requires explicit
user approval via email confirmation link.
"""

import math
import os
import secrets
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from dataclasses import dataclass, field


# Test parameters
TEST_OVERSHOOT = 0.5      # Heat to setpoint + this buffer before turning off
TEST_DROP = 1.0           # Measure time for this temperature drop
TEST_TIMEOUT_HOURS = 12.0 # Max total test duration
MIN_SUPPLY_TEMP = 22.0    # Minimum supply temp during cooldown phase (near room temp)

# Test schedule (Swedish local hours, approximate CET)
TEST_START_HOUR = 19      # Start test (lower Yref to minimum)
TEST_END_HOUR = 7         # End of test window

# Settling: confirm heating is actually off before measuring
SETTLING_TIMEOUT_MINUTES = 60  # Max time to wait for heating to settle
# Delta T thresholds per distribution type (supply - return below this = no heating)
# NOTE: Power meter NOT used — DHW events cause primary-side spikes even when
# space heating is off, making power an unreliable indicator.
DELTA_T_THRESHOLDS = {
    'floor': 1.0,         # Floor heating: small delta is normal, need < 1°C
    'radiator': 3.0,      # Radiators: larger normal delta, < 3°C = off
    'ventilation': 2.0,   # Ventilation coils
}
MAX_WIND_SPEED = 3.0      # m/s
MIN_OUTDOOR_DELTA = 20.0  # outdoor must be at least setpoint - 20°C
MAX_FORECAST_SWING = 3.0  # Max outdoor temp change during test window

# How often to check if a house needs calibration
CALIBRATION_STALE_MONTHS = 10


@dataclass
class TestState:
    """Tracks state of an active thermal inertia test."""
    phase: str = "idle"  # idle, heating, settling, cooldown, complete, failed
    started_at: Optional[str] = None
    phase_started_at: Optional[str] = None
    settling_started_at: Optional[str] = None  # When Yref was set to minimum
    initial_indoor_temp: Optional[float] = None
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

    def check_tonight_forecast(self, overnight_forecast: List[Dict],
                              setpoint: float = 22.0) -> bool:
        """
        Check if tonight's forecast is suitable for running the test.

        Called in the morning (~07:00) to evaluate the COMING night's
        conditions from forecast data.

        Args:
            overnight_forecast: Forecast points for tonight (23:00-07:00),
                                each with 'temperature' and optionally 'wind_speed'
            setpoint: Current indoor setpoint

        Returns:
            True if tonight looks suitable
        """
        if not overnight_forecast or len(overnight_forecast) < 2:
            return False

        temps = [p['temperature'] for p in overnight_forecast
                 if p.get('temperature') is not None]
        if not temps:
            return False

        # Temperature check: must be cold enough all night
        max_outdoor = setpoint - MIN_OUTDOOR_DELTA
        if max(temps) > max_outdoor:
            return False

        # Stability check: no big swings overnight
        swing = max(temps) - min(temps)
        if swing > MAX_FORECAST_SWING:
            return False

        # Wind check: use forecast wind if available
        winds = [p['wind_speed'] for p in overnight_forecast
                 if p.get('wind_speed') is not None]
        if winds and max(winds) > MAX_WIND_SPEED:
            return False

        return True

    def start_test(self, current_indoor: float, setpoint: float,
                   outdoor_temp: float) -> str:
        """
        Start the thermal inertia test.

        If the house is already at/above setpoint, skip straight to settling
        (lower Yref to minimum). Otherwise, preheat to setpoint + buffer first.

        Args:
            current_indoor: Current indoor temperature
            setpoint: Current setpoint
            outdoor_temp: Current outdoor temperature

        Returns:
            "heat" if preheating needed, "cooldown" if skipping to settling
        """
        now = datetime.now(timezone.utc)
        target_heat = setpoint + TEST_OVERSHOOT

        if current_indoor >= setpoint:
            # Already warm enough — skip straight to settling
            self.state = TestState(
                phase="settling",
                started_at=now.isoformat(),
                phase_started_at=now.isoformat(),
                settling_started_at=now.isoformat(),
                initial_indoor_temp=current_indoor,
                setpoint=setpoint,
                outdoor_temp_at_start=outdoor_temp,
            )
            self.logger.info(
                f"Thermal inertia test started for {self.profile.customer_id}: "
                f"indoor={current_indoor:.1f}°C >= setpoint {setpoint:.1f}°C "
                f"— skipping preheat, lowering Yref"
            )
            initial_action = "cooldown"
        else:
            # Need to preheat first
            self.state = TestState(
                phase="heating",
                started_at=now.isoformat(),
                phase_started_at=now.isoformat(),
                initial_indoor_temp=current_indoor,
                setpoint=setpoint,
                outdoor_temp_at_start=outdoor_temp,
            )
            self.logger.info(
                f"Thermal inertia test started for {self.profile.customer_id}: "
                f"indoor={current_indoor:.1f}°C < setpoint {setpoint:.1f}°C "
                f"— preheating to {target_heat:.1f}°C first"
            )
            initial_action = "heat"

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
                    'Phase': self.state.phase,
                }
            )

        return initial_action

    def _enter_cooldown(self, current_indoor: float, outdoor_temp: float) -> str:
        """Transition to cooldown phase — T_start recorded here."""
        self.state.phase = "cooldown"
        self.state.phase_started_at = datetime.now(timezone.utc).isoformat()
        self.state.cooldown_start_temp = current_indoor
        self.state.target_drop_temp = current_indoor - TEST_DROP
        self.state.readings = []

        self.logger.info(
            f"Thermal test cooldown started: T_start={current_indoor:.1f}°C, "
            f"target={self.state.target_drop_temp:.1f}°C (drop {TEST_DROP}°C)"
        )

        return "cooldown"

    def poll(self, current_indoor: float, outdoor_temp: float,
             supply_temp: Optional[float] = None,
             return_temp: Optional[float] = None,
             dh_power: Optional[float] = None) -> Optional[str]:
        """
        Called each poll cycle during an active test.

        Args:
            current_indoor: Current indoor temperature
            outdoor_temp: Current outdoor temperature
            supply_temp: Actual supply temperature (secondary side)
            return_temp: Actual return temperature (secondary side)
            dh_power: District heating power in kW (if available from meter)

        Returns:
            Action for the caller:
            - "heat": boost supply temp (preheating if house is cold)
            - "cooldown": reduce supply to minimum (settling + measuring)
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

        local_hour = (now.hour + 1) % 24  # Approximate CET

        # Check time window (stop if past 07:00 local)
        if TEST_END_HOUR <= local_hour < TEST_START_HOUR:
            return self._finish_test("window_closed", outdoor_temp)

        if self.state.phase == "heating":
            target_heat = self.state.setpoint + TEST_OVERSHOOT
            if current_indoor >= target_heat:
                # Target reached — go straight to settling (lower Yref)
                now_iso = now.isoformat()
                self.state.phase = "settling"
                self.state.phase_started_at = now_iso
                self.state.settling_started_at = now_iso
                self.logger.info(
                    f"Thermal test: preheat done ({current_indoor:.1f}°C >= "
                    f"{target_heat:.1f}°C), lowering Yref"
                )
                return "cooldown"
            return "heat"

        elif self.state.phase == "settling":
            # Wait for heating to actually stop before starting measurement.
            # Only use secondary-side delta T — primary power is unreliable
            # because DHW events cause spikes even when space heating is off.
            heating_off = self._is_heating_off(supply_temp, return_temp)

            if heating_off:
                self.logger.info(
                    f"Thermal test: heating confirmed off — starting measurement "
                    f"(indoor={current_indoor:.1f}°C, "
                    f"delta_t={round(supply_temp - return_temp, 1) if supply_temp and return_temp else '?'})"
                )
                self._enter_cooldown(current_indoor, outdoor_temp)
                return "cooldown"

            # Check settling timeout
            if self.state.settling_started_at:
                settle_start = datetime.fromisoformat(
                    self.state.settling_started_at.replace('Z', '+00:00')
                )
                settle_minutes = (now - settle_start).total_seconds() / 60
                if settle_minutes > SETTLING_TIMEOUT_MINUTES:
                    self.logger.warning(
                        f"Thermal test: heating didn't stop within {SETTLING_TIMEOUT_MINUTES} min, "
                        f"starting measurement anyway "
                        f"(delta_t={round(supply_temp - return_temp, 1) if supply_temp and return_temp else '?'})"
                    )
                    self._enter_cooldown(current_indoor, outdoor_temp)
                    return "cooldown"

            return "cooldown"  # Keep writing MIN_SUPPLY_TEMP

        elif self.state.phase == "cooldown":
            # Record reading (include supply/return/power for energy compensation)
            self.state.readings.append({
                'timestamp': now.isoformat(),
                'indoor_temp': round(current_indoor, 2),
                'outdoor_temp': round(outdoor_temp, 2),
                'supply_temp': round(supply_temp, 2) if supply_temp is not None else None,
                'return_temp': round(return_temp, 2) if return_temp is not None else None,
                'dh_power': round(dh_power, 3) if dh_power is not None else None,
            })

            # Check if we've dropped enough
            if current_indoor <= self.state.target_drop_temp:
                return self._finish_test("success", outdoor_temp)

            return "cooldown"

        return None

    def _finish_test(self, reason: str, outdoor_temp: float) -> str:
        """
        Finish the test and calculate τ if possible.

        Uses energy-compensated calculation when k-value is available:
        residual heating during cooldown inflates the raw τ, so we correct
        using the energy balance equation.

        Returns "restore" to signal the caller to restore normal operation.
        """
        readings = self.state.readings
        cooldown_start = self.state.cooldown_start_temp

        can_calculate = (
            reason in ("success", "timeout")
            and readings
            and len(readings) >= 6
            and cooldown_start is not None
        )

        if can_calculate:
            first_reading = datetime.fromisoformat(readings[0]['timestamp'].replace('Z', '+00:00'))
            last_reading = datetime.fromisoformat(readings[-1]['timestamp'].replace('Z', '+00:00'))
            elapsed_hours = (last_reading - first_reading).total_seconds() / 3600
            t_start = cooldown_start
            t_end = readings[-1]['indoor_temp']
            actual_drop = t_start - t_end
            avg_outdoor = sum(r['outdoor_temp'] for r in readings) / len(readings)

            # Need at least 0.3°C drop for any calculation
            if actual_drop < 0.3:
                can_calculate = False

        if can_calculate:
            numerator = t_end - avg_outdoor
            denominator = t_start - avg_outdoor

            if denominator <= 0 or numerator <= 0 or numerator >= denominator:
                can_calculate = False

        if can_calculate:
            # Raw τ (Newton's cooling, no energy compensation)
            tau_raw = -elapsed_hours / math.log(numerator / denominator)

            # Try compensated τ using energy balance
            tau = self._compute_compensated_tau(
                readings, t_start, t_end, avg_outdoor, elapsed_hours, tau_raw
            )

            source = "measured" if reason == "success" else "measured_partial"
            tau_int = int(tau)  # Round down to nearest hour
            self.state.result_tau = tau_int
            self.state.phase = "complete" if reason == "success" else "failed"
            if reason != "success":
                self.state.failure_reason = reason

            # Store in profile
            self.profile.learned.thermal_time_constant = tau_int
            self.profile.learned.thermal_time_constant_measured_at = \
                datetime.now(timezone.utc).isoformat()
            self.profile.learned.thermal_time_constant_source = source
            self.profile.learned.thermal_time_constant_copied_from = None
            self.profile.save()

            self.logger.info(
                f"Thermal inertia test {'complete' if reason == 'success' else 'partial'} "
                f"for {self.profile.customer_id}: "
                f"τ = {tau_int}h (raw={tau_raw:.1f}h, compensated={tau:.1f}h, "
                f"drop: {t_start:.1f} -> {t_end:.1f}°C "
                f"in {elapsed_hours:.1f}h, avg outdoor: {avg_outdoor:.1f}°C)"
            )

            if self.seq_logger:
                self.seq_logger.log(
                    "Thermal inertia test result for {HouseId}: τ = {Tau} hours",
                    level='Information',
                    properties={
                        'EventType': 'ThermalTestComplete',
                        'HouseId': self.profile.customer_id,
                        'Tau': tau_int,
                        'TauRaw': round(tau_raw, 1),
                        'TauCompensated': round(tau, 1),
                        'StartTemp': round(t_start, 1),
                        'EndTemp': round(t_end, 1),
                        'ElapsedHours': round(elapsed_hours, 1),
                        'AvgOutdoorTemp': round(avg_outdoor, 1),
                        'ReadingCount': len(readings),
                        'Reason': reason,
                    }
                )
        else:
            # No usable data
            self.state.phase = "failed"
            self.state.failure_reason = reason if reason != "success" else "invalid_data"

            self.logger.warning(
                f"Thermal inertia test ended for {self.profile.customer_id}: "
                f"{self.state.failure_reason}"
            )

            if self.seq_logger:
                self.seq_logger.log(
                    "Thermal inertia test ended for {HouseId}: {Reason}",
                    level='Warning',
                    properties={
                        'EventType': 'ThermalTestEnd',
                        'HouseId': self.profile.customer_id,
                        'Reason': self.state.failure_reason,
                        'ReadingCount': len(readings),
                    }
                )

        return "restore"

    def _compute_compensated_tau(self, readings: List[Dict],
                                 t_start: float, t_end: float,
                                 avg_outdoor: float, elapsed_hours: float,
                                 tau_raw: float) -> float:
        """
        Compute energy-compensated τ using the energy balance approach.

        Since heating can't be fully stopped on HomeSide systems, the raw τ
        from Newton's cooling is inflated. This method corrects for residual
        heating energy using:

            C * (T_end - T_start) = E_input - k * ∫(T_in - T_out) dt
            τ = C / k

        Energy input estimation (in priority order):
        1. dh_power from real-time meter (if available) — most accurate
        2. supply-return delta_T calibrated against k-value — fallback

        DHW events (supply > threshold) are filtered out so only heating
        energy is counted.

        Falls back to raw τ if k-value is unavailable or calculation fails.
        """
        # Get DHW threshold from profile, default 45°C
        dhw_threshold = getattr(
            self.profile.energy_separation, 'dhw_temp_threshold', 45.0
        )

        # Get k-value from energy separation config
        k = getattr(self.profile.energy_separation, 'heat_loss_k', None)
        if not k or k <= 0:
            self.logger.info(
                f"Thermal test: no k-value available, using raw τ={tau_raw:.1f}h"
            )
            return tau_raw

        # Filter out DHW events: exclude readings where supply > threshold
        # This is the energy separation step — DHW power spikes don't
        # contribute to space heating
        clean = [r for r in readings
                 if r.get('supply_temp') is not None
                 and r['supply_temp'] < dhw_threshold]

        if len(clean) < 6:
            self.logger.info(
                f"Thermal test: too few clean readings ({len(clean)}), "
                f"using raw τ={tau_raw:.1f}h"
            )
            return tau_raw

        # Estimate total heating energy input during cooldown
        total_energy_input, energy_source = self._estimate_heating_energy(
            clean, k, dhw_threshold
        )

        if total_energy_input is None:
            self.logger.info(
                f"Thermal test: could not estimate energy input, "
                f"using raw τ={tau_raw:.1f}h"
            )
            return tau_raw

        # Integrate (T_in - T_out) over the test period (DHW-filtered)
        integral_temp_diff = 0.0
        for i in range(1, len(clean)):
            dt_h = self._reading_dt_hours(clean[i-1], clean[i])
            if dt_h is None or dt_h > 0.5:
                continue  # Skip gaps
            avg_diff = ((clean[i]['indoor_temp'] - clean[i]['outdoor_temp']) +
                        (clean[i-1]['indoor_temp'] - clean[i-1]['outdoor_temp'])) / 2
            integral_temp_diff += avg_diff * dt_h

        heat_loss_total = k * integral_temp_diff
        delta_t_indoor = t_end - t_start  # Negative (cooling)

        if abs(delta_t_indoor) < 0.1:
            self.logger.info(
                f"Thermal test: indoor temp change too small ({delta_t_indoor:.1f}°C), "
                f"using raw τ={tau_raw:.1f}h"
            )
            return tau_raw

        # C * delta_T = E_input - heat_loss  →  C = (E_input - heat_loss) / delta_T
        C = (total_energy_input - heat_loss_total) / delta_t_indoor
        tau_compensated = C / k

        self.logger.info(
            f"Thermal test energy compensation ({energy_source}): "
            f"E_input={total_energy_input:.2f}kWh, "
            f"heat_loss={heat_loss_total:.2f}kWh, "
            f"C={C:.1f}kWh/°C, "
            f"τ_raw={tau_raw:.1f}h, τ_comp={tau_compensated:.1f}h"
        )

        # Sanity check: compensated τ should be positive and less than raw
        if tau_compensated <= 0:
            self.logger.warning(
                f"Thermal test: compensated τ={tau_compensated:.1f}h is invalid, "
                f"using raw τ={tau_raw:.1f}h"
            )
            return tau_raw

        if tau_compensated > tau_raw:
            self.logger.warning(
                f"Thermal test: compensated τ={tau_compensated:.1f}h > raw τ={tau_raw:.1f}h, "
                f"energy estimate unreliable, using raw τ"
            )
            return tau_raw

        return tau_compensated

    def _estimate_heating_energy(self, clean_readings: List[Dict],
                                 k: float, dhw_threshold: float
                                 ) -> tuple:
        """
        Estimate total heating energy input during cooldown.

        Prefers real-time dh_power from the district heating meter (already
        DHW-filtered by caller). Falls back to supply-return delta_T
        estimation if dh_power is not available.

        Returns:
            (total_energy_kwh, source_str) or (None, None) on failure
        """
        # Check if we have dh_power readings
        readings_with_power = [r for r in clean_readings
                               if r.get('dh_power') is not None]

        if len(readings_with_power) >= 6:
            return self._energy_from_dh_power(readings_with_power)

        # Fallback: estimate from supply-return delta_T
        readings_with_temps = [r for r in clean_readings
                               if r.get('return_temp') is not None]

        if len(readings_with_temps) >= 6:
            return self._energy_from_delta_t(readings_with_temps, k)

        return None, None

    def _energy_from_dh_power(self, readings: List[Dict]) -> tuple:
        """
        Integrate real-time dh_power readings (already DHW-filtered).

        The dh_power field is total district heating power in kW. Since
        DHW events have already been filtered out (supply > threshold),
        the remaining readings represent heating-only power.
        """
        total_energy = 0.0
        for i in range(1, len(readings)):
            dt_h = self._reading_dt_hours(readings[i-1], readings[i])
            if dt_h is None or dt_h > 0.5:
                continue  # Skip gaps > 30 min
            avg_power = (readings[i-1]['dh_power'] + readings[i]['dh_power']) / 2
            total_energy += avg_power * dt_h

        self.logger.info(
            f"Thermal test: heating energy from dh_power = {total_energy:.2f} kWh "
            f"({len(readings)} readings, DHW events excluded)"
        )
        return total_energy, "dh_power"

    def _energy_from_delta_t(self, readings: List[Dict], k: float) -> tuple:
        """
        Estimate energy from supply-return delta_T, calibrated against k-value.

        At steady state: Q = k * (T_in - T_out) = flow * cp * delta_T
        So: kWh_per_degC_deltaT = k * (T_in - T_out) / delta_T_ref
        """
        # Use first few readings as calibration reference
        ref = readings[:min(6, len(readings))]
        ref_delta_t = sum(r['supply_temp'] - r['return_temp'] for r in ref) / len(ref)
        ref_indoor = sum(r['indoor_temp'] for r in ref) / len(ref)
        ref_outdoor = sum(r['outdoor_temp'] for r in ref) / len(ref)

        if ref_delta_t < 0.1:
            return None, None

        kwh_per_degc = k * (ref_indoor - ref_outdoor) / ref_delta_t

        total_energy = 0.0
        for i in range(1, len(readings)):
            dt_h = self._reading_dt_hours(readings[i-1], readings[i])
            if dt_h is None or dt_h > 0.5:
                continue
            avg_delta = ((readings[i]['supply_temp'] - readings[i]['return_temp']) +
                         (readings[i-1]['supply_temp'] - readings[i-1]['return_temp'])) / 2
            total_energy += kwh_per_degc * avg_delta * dt_h

        self.logger.info(
            f"Thermal test: heating energy from delta_T = {total_energy:.2f} kWh "
            f"(calibration: {kwh_per_degc:.2f} kWh/°C, ref_ΔT={ref_delta_t:.1f}°C)"
        )
        return total_energy, "delta_T"

    @staticmethod
    def _reading_dt_hours(r1: Dict, r2: Dict) -> Optional[float]:
        """Time difference between two readings in hours."""
        try:
            t1 = datetime.fromisoformat(r1['timestamp'].replace('Z', '+00:00'))
            t2 = datetime.fromisoformat(r2['timestamp'].replace('Z', '+00:00'))
            return (t2 - t1).total_seconds() / 3600
        except (KeyError, ValueError):
            return None

    def _is_heating_off(self, supply_temp: Optional[float],
                        return_temp: Optional[float]) -> bool:
        """
        Check if space heating has actually stopped using secondary-side delta T.

        NOTE: Primary-side power meter is NOT used here because DHW (domestic
        hot water) events cause power spikes even when space heating is off.
        Secondary-side supply-return delta T is the reliable indicator since
        DHW uses a separate circuit.

        Thresholds depend on distribution type:
        - Floor heating: < 1°C (normally small delta)
        - Radiators: < 3°C (normally large delta)
        - Ventilation: < 2°C
        """
        if supply_temp is not None and return_temp is not None:
            delta_t = abs(supply_temp - return_temp)
            dist_type = self.profile.heating_system.distribution_type.split(',')[0].strip()
            threshold = DELTA_T_THRESHOLDS.get(dist_type, 2.0)
            return delta_t < threshold

        # No data to verify — assume off after settling timeout
        return False

    def get_supply_for_phase(self) -> Optional[float]:
        """
        Get the supply temp to write for the current test phase.

        Returns:
            During heating: setpoint + 10°C (boost)
            During settling/cooldown: MIN_SUPPLY_TEMP (15°C)
        """
        if self.state.phase == "heating":
            return (self.state.setpoint or 22.0) + 10.0
        elif self.state.phase in ("settling", "cooldown"):
            return MIN_SUPPLY_TEMP
        return None

    @property
    def is_active(self) -> bool:
        return self.state.phase in ("heating", "settling", "cooldown")

    def abort(self, reason: str = "manual") -> None:
        """Abort an active test."""
        if self.is_active:
            self.state.phase = "failed"
            self.state.failure_reason = f"aborted: {reason}"
            self.logger.info(f"Thermal inertia test aborted: {reason}")


def request_thermal_test(profile, overnight_forecast: List[Dict],
                         setpoint: float = 22.0, logger=None,
                         seq_logger=None) -> bool:
    """
    Check if a thermal test should be requested and send email for approval.

    Called in the morning (~07:00 Swedish) by the fetcher. Evaluates tonight's
    forecast. If conditions look good and the house needs calibration, stores
    a pending request in the profile and sends an approval email.

    The user has all day to approve. If approved, the test starts at 23:00.

    Args:
        profile: CustomerProfile instance
        overnight_forecast: Forecast points for tonight (23:00-07:00),
                           each with 'temperature' and optionally 'wind_speed'
        setpoint: Current indoor setpoint
        logger: Python logger
        seq_logger: Optional Seq logger

    Returns:
        True if a request was sent
    """
    logger = logger or logging.getLogger(__name__)

    # Skip if there's already a pending or active request
    if profile.thermal_test.status in ("pending_approval", "approved", "in_progress"):
        return False

    # Check if calibration is needed
    test = ThermalInertiaTest(profile, control=None, logger=logger)
    if not test.needs_calibration():
        return False

    # Check tonight's forecast
    if not test.check_tonight_forecast(overnight_forecast, setpoint):
        logger.debug(f"Thermal test forecast not suitable for {profile.customer_id}")
        return False

    # Forecast looks good — create approval request
    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)

    # Extract forecast summary for email
    forecast_temps = [p['temperature'] for p in overnight_forecast
                      if p.get('temperature') is not None]
    forecast_winds = [p['wind_speed'] for p in overnight_forecast
                      if p.get('wind_speed') is not None]

    profile.thermal_test.status = "pending_approval"
    profile.thermal_test.token = token
    profile.thermal_test.requested_at = now.isoformat()
    profile.thermal_test.expires_at = (now + timedelta(hours=16)).isoformat()  # Until 23:00
    profile.thermal_test.conditions = {
        'forecast_temp_min': round(min(forecast_temps), 1) if forecast_temps else None,
        'forecast_temp_max': round(max(forecast_temps), 1) if forecast_temps else None,
        'forecast_wind_max': round(max(forecast_winds), 1) if forecast_winds else None,
        'setpoint': setpoint,
    }
    profile.save()

    # Send email
    email_sent = _send_thermal_test_email(
        profile, token, profile.thermal_test.conditions, setpoint, logger
    )

    if seq_logger:
        seq_logger.log(
            "Thermal test requested for {HouseId} (email_sent={EmailSent})",
            level='Information',
            properties={
                'EventType': 'ThermalTestRequested',
                'HouseId': profile.customer_id,
                'ForecastConditions': profile.thermal_test.conditions,
                'EmailSent': email_sent,
            }
        )

    cond = profile.thermal_test.conditions
    logger.info(
        f"Thermal test requested for {profile.customer_id}: "
        f"forecast {cond.get('forecast_temp_min')} to {cond.get('forecast_temp_max')}°C, "
        f"wind max {cond.get('forecast_wind_max')} m/s, "
        f"email_sent={email_sent}"
    )

    return True


def _send_thermal_test_email(profile, token: str, conditions: Dict,
                             setpoint: float, logger) -> bool:
    """Send thermal test approval email to admins via SMTP."""
    smtp_server = os.environ.get('SMTP_SERVER', 'send.one.com')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_password = os.environ.get('SMTP_PASSWORD', '')
    from_email = os.environ.get('FROM_EMAIL', smtp_user)
    from_name = os.environ.get('FROM_NAME', 'BVPro')
    base_url = os.environ.get('BASE_URL', '')
    admin_emails = os.environ.get('ADMIN_EMAILS', '').split(',')

    if not smtp_user or not smtp_password or not base_url:
        logger.warning("SMTP or BASE_URL not configured, cannot send thermal test email")
        return False

    approve_url = f"{base_url}/thermal-test/{profile.customer_id}/{token}/approve"
    decline_url = f"{base_url}/thermal-test/{profile.customer_id}/{token}/decline"

    temp_min = conditions.get('forecast_temp_min', '?')
    temp_max = conditions.get('forecast_temp_max', '?')
    wind_max = conditions.get('forecast_wind_max', '?')
    house_name = profile.friendly_name or profile.customer_id

    subject = f"[BVPro] Thermal calibration tonight? {house_name}"

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #2c3e50;">Thermal Calibration Request</h2>

        <p>Tonight's forecast looks suitable for measuring the thermal time constant
           of <strong>{house_name}</strong>.</p>

        <p>The test will:</p>
        <ol>
            <li>19:00 — If below setpoint, preheat first. Otherwise reduce heating immediately.</li>
            <li>Wait for heating to fully stop (verified by supply-return delta T)</li>
            <li>Measure how quickly the house cools down (1°C drop)</li>
            <li>Restore normal heating automatically when done</li>
        </ol>

        <table style="border-collapse: collapse; margin: 20px 0;">
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">House:</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{house_name}</td>
            </tr>
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Forecast overnight:</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{temp_min} to {temp_max}°C</td>
            </tr>
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Max wind:</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{wind_max} m/s</td>
            </tr>
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Setpoint:</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{setpoint}°C</td>
            </tr>
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Max indoor swing:</td>
                <td style="padding: 8px; border: 1px solid #ddd;">~1°C (barely noticeable)</td>
            </tr>
        </table>

        <p>If approved, the test runs tonight 23:00-07:00 and will not affect comfort noticeably.</p>

        <div style="margin: 30px 0;">
            <a href="{approve_url}"
               style="display: inline-block; padding: 14px 28px; background-color: #27ae60;
                      color: white; text-decoration: none; border-radius: 4px;
                      margin-right: 10px; font-weight: bold;">
                Approve Test
            </a>
            <a href="{decline_url}"
               style="display: inline-block; padding: 14px 28px; background-color: #e74c3c;
                      color: white; text-decoration: none; border-radius: 4px;
                      font-weight: bold;">
                Decline
            </a>
        </div>

        <p style="color: #7f8c8d; font-size: 12px;">
            Approve before 23:00 tonight for the test to run. The link expires at 23:00.
        </p>

        <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
            This is an automated message from BVPro.
        </p>
    </body>
    </html>
    """

    text_body = f"""
Thermal Calibration Request

Tonight's forecast looks suitable for measuring the thermal time constant
of {house_name}.

Forecast overnight: {temp_min} to {temp_max}°C
Max wind: {wind_max} m/s
Setpoint: {setpoint}°C

Approve: {approve_url}
Decline: {decline_url}

This link expires at 23:00 tonight.
    """

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{from_name} <{from_email}>"

        msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))

        recipients = [e.strip() for e in admin_emails if e.strip()]
        if not recipients:
            logger.warning("No ADMIN_EMAILS configured for thermal test email")
            return False

        msg['To'] = ', '.join(recipients)

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)

        logger.info(f"Thermal test email sent to {recipients}")
        return True

    except Exception as e:
        logger.error(f"Failed to send thermal test email: {e}")
        return False


def check_thermal_test_approval(profile) -> Optional[str]:
    """
    Check if a pending thermal test has been approved, declined, or expired.

    Returns:
        "approved", "declined", "expired", or None if no pending request
    """
    if profile.thermal_test.status != "pending_approval":
        return None

    # Check expiry
    if profile.thermal_test.expires_at:
        try:
            expires = datetime.fromisoformat(
                profile.thermal_test.expires_at.replace('Z', '+00:00')
            )
            if datetime.now(timezone.utc) > expires:
                profile.thermal_test.status = "none"
                profile.thermal_test.token = None
                profile.save()
                return "expired"
        except (ValueError, TypeError):
            pass

    return None  # Still pending


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

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
MIN_SUPPLY_TEMP = 15.0    # Minimum supply temp during cooldown phase

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
             return_temp: Optional[float] = None) -> Optional[str]:
        """
        Called each poll cycle during an active test.

        Args:
            current_indoor: Current indoor temperature
            outdoor_temp: Current outdoor temperature
            supply_temp: Actual supply temperature (secondary side)
            return_temp: Actual return temperature (secondary side)

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

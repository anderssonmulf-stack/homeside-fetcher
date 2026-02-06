#!/usr/bin/env python3
"""
Weather Sensitivity Learner (ML2)

Learns per-house solar (and wind) coefficients by detecting "solar heating events" -
periods when heating demand drops unexpectedly on sunny days despite cold outdoor temps.

Key Detection Signal - Supply-Return Temperature Delta:
- Normal heating: 2-3°C delta
- No heating: <0.5°C delta

When delta drops while outdoor temp is cold + clear skies + sun above horizon = solar event

Naming convention: All new variables use _ml2 suffix to distinguish from original model.
"""

import math
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from collections import deque

try:
    from astral import LocationInfo
    from astral.sun import elevation
    ASTRAL_AVAILABLE = True
except ImportError:
    ASTRAL_AVAILABLE = False


@dataclass
class SolarEvent:
    """
    Detected solar heating event.

    Occurs when heating demand drops due to solar gain despite cold outdoor temps.
    """
    timestamp: datetime                 # Event start time
    end_timestamp: datetime             # Event end time
    duration_minutes: float             # Event duration
    avg_supply_return_delta: float      # Average supply-return delta during event (°C)
    avg_outdoor_temp: float             # Average outdoor temp during event (°C)
    avg_indoor_temp: float              # Average indoor temp during event (°C)
    avg_cloud_cover: float              # Average cloud cover (0-8 octas)
    avg_sun_elevation: float            # Average sun elevation (degrees)
    avg_wind_speed: float               # Average wind speed (m/s)
    implied_solar_coefficient_ml2: float  # Back-calculated solar coefficient

    # Additional context
    observations_count: int = 0         # Number of 15-min observations in event
    peak_sun_elevation: float = 0.0     # Peak sun elevation during event

    def to_dict(self) -> dict:
        """Convert to dictionary for InfluxDB storage."""
        return {
            'timestamp': self.timestamp.isoformat(),
            'end_timestamp': self.end_timestamp.isoformat(),
            'duration_minutes': round(self.duration_minutes, 1),
            'avg_supply_return_delta': round(self.avg_supply_return_delta, 2),
            'avg_outdoor_temp': round(self.avg_outdoor_temp, 1),
            'avg_indoor_temp': round(self.avg_indoor_temp, 1),
            'avg_cloud_cover': round(self.avg_cloud_cover, 1),
            'avg_sun_elevation': round(self.avg_sun_elevation, 1),
            'avg_wind_speed': round(self.avg_wind_speed, 1),
            'implied_solar_coefficient_ml2': round(self.implied_solar_coefficient_ml2, 1),
            'observations_count': self.observations_count,
            'peak_sun_elevation': round(self.peak_sun_elevation, 1),
        }


@dataclass
class Observation:
    """Single observation for solar event detection."""
    timestamp: datetime
    supply_temp: float
    return_temp: float
    room_temp: float
    outdoor_temp: float
    cloud_cover: float          # 0-8 octas
    wind_speed: float           # m/s
    sun_elevation: float        # degrees (calculated)

    @property
    def supply_return_delta(self) -> float:
        """Supply-return temperature delta (heating intensity indicator)."""
        return self.supply_temp - self.return_temp


@dataclass
class LearnedWeatherCoefficients:
    """
    Weather coefficients learned from solar event detection.

    Uses _ml2 suffix to distinguish from original model coefficients.
    """
    solar_coefficient_ml2: float = 6.0      # Default (will be learned to 30-50)
    wind_coefficient_ml2: float = 0.15      # Fixed low value for FTX houses
    solar_confidence_ml2: float = 0.0       # 0-1 confidence
    total_solar_events: int = 0
    events_since_last_update: int = 0
    next_update_at_events: int = 3          # First update after 3 events
    updated_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'solar_coefficient_ml2': self.solar_coefficient_ml2,
            'wind_coefficient_ml2': self.wind_coefficient_ml2,
            'solar_confidence_ml2': self.solar_confidence_ml2,
            'total_solar_events': self.total_solar_events,
            'events_since_last_update': self.events_since_last_update,
            'next_update_at_events': self.next_update_at_events,
            'updated_at': self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'LearnedWeatherCoefficients':
        return cls(
            solar_coefficient_ml2=data.get('solar_coefficient_ml2', 6.0),
            wind_coefficient_ml2=data.get('wind_coefficient_ml2', 0.15),
            solar_confidence_ml2=data.get('solar_confidence_ml2', 0.0),
            total_solar_events=data.get('total_solar_events', 0),
            events_since_last_update=data.get('events_since_last_update', 0),
            next_update_at_events=data.get('next_update_at_events', 3),
            updated_at=data.get('updated_at'),
        )


@dataclass
class ThermalResponseTiming:
    """
    Learned thermal response timing for predictive control.

    How quickly the building responds to effective_temp changes.
    """
    heat_up_lag_minutes_ml2: float = 60.0     # Time for indoor to respond to rising eff_temp
    cool_down_lag_minutes_ml2: float = 90.0   # Time for indoor to respond to falling eff_temp
    confidence_ml2: float = 0.0
    total_transitions: int = 0
    updated_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'heat_up_lag_minutes_ml2': self.heat_up_lag_minutes_ml2,
            'cool_down_lag_minutes_ml2': self.cool_down_lag_minutes_ml2,
            'confidence_ml2': self.confidence_ml2,
            'total_transitions': self.total_transitions,
            'updated_at': self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ThermalResponseTiming':
        return cls(
            heat_up_lag_minutes_ml2=data.get('heat_up_lag_minutes_ml2', 60.0),
            cool_down_lag_minutes_ml2=data.get('cool_down_lag_minutes_ml2', 90.0),
            confidence_ml2=data.get('confidence_ml2', 0.0),
            total_transitions=data.get('total_transitions', 0),
            updated_at=data.get('updated_at'),
        )


class WeatherSensitivityLearner:
    """
    Learns building weather sensitivity from observed solar heating events.

    Detects when heating stops due to solar gain (supply-return delta drops)
    and back-calculates the implied solar coefficient.

    Detection methods:
    1. Traditional: Low delta + clear sky (cloud < 3 octas) + sun up
    2. Sensor-based: Low delta + outdoor sensor shows solar heating
       (sudden temp rise or reading significantly above expected)

    The sensor-based method is more reliable for local conditions since
    cloud forecasts can be wrong for the specific location.
    """

    # Detection thresholds
    SUPPLY_RETURN_DELTA_THRESHOLD = 0.5     # °C - below this = minimal heating
    OUTDOOR_INDOOR_DIFF_THRESHOLD = 5.0     # °C - must be cold outside (using baseline, not sensor reading)
    CLOUD_COVER_THRESHOLD = 3               # octas - must be mostly clear (traditional method)
    SUN_ELEVATION_THRESHOLD = 10.0          # degrees - sun must be well up
    MIN_EVENT_DURATION_MINUTES = 30         # Must be sustained

    # Sensor-based solar detection thresholds
    OUTDOOR_TEMP_RISE_THRESHOLD = 3.0       # °C rise in 30min indicates sun on sensor
    OUTDOOR_TEMP_ANOMALY_THRESHOLD = 4.0    # °C above expected = sun exposure

    # Learning schedule
    FIRST_UPDATE_EVENTS = 3
    SECOND_UPDATE_EVENTS = 6
    REGULAR_UPDATE_EVENTS = 12

    # Blending weights
    NEW_COEFFICIENT_WEIGHT = 0.7
    OLD_COEFFICIENT_WEIGHT = 0.3

    def __init__(
        self,
        heat_loss_k: float,
        latitude: float,
        longitude: float,
        coefficients: Optional[LearnedWeatherCoefficients] = None,
        timing: Optional[ThermalResponseTiming] = None,
        logger: Optional[logging.Logger] = None,
        buffer_hours: int = 24
    ):
        """
        Initialize the weather sensitivity learner.

        Args:
            heat_loss_k: Building heat loss coefficient (kW/°C)
            latitude: Location latitude for sun calculations
            longitude: Location longitude for sun calculations
            coefficients: Previously learned coefficients (or None for fresh start)
            timing: Previously learned timing (or None for fresh start)
            logger: Logger instance
            buffer_hours: Hours of observations to keep in buffer
        """
        self.heat_loss_k = heat_loss_k
        self.latitude = latitude
        self.longitude = longitude
        self.coefficients = coefficients or LearnedWeatherCoefficients()
        self.timing = timing or ThermalResponseTiming()
        self.logger = logger or logging.getLogger(__name__)

        # Observation buffer (circular, keeps last N hours)
        self.buffer_size = buffer_hours * 4  # 15-min intervals
        self.observation_buffer: deque = deque(maxlen=self.buffer_size)

        # Event detection state
        self.current_event_start: Optional[datetime] = None
        self.current_event_observations: List[Observation] = []

        # Completed events waiting for coefficient update
        self.detected_events: List[SolarEvent] = []

        # Location for sun calculations
        if ASTRAL_AVAILABLE:
            self.location = LocationInfo(latitude=latitude, longitude=longitude)
        else:
            self.location = None
            self.logger.warning("astral not available, sun elevation will be estimated")

        # Outdoor temp baseline tracking (for sensor-based solar detection)
        # Uses nighttime/early morning temps as baseline
        self.outdoor_temp_baseline: Optional[float] = None
        self.baseline_samples: List[float] = []  # Temps from before sunrise

        # Thermal lag learning state
        self.effective_temp_history: List[tuple] = []  # (timestamp, effective_temp, indoor_temp)
        self.pending_transitions: List[dict] = []  # Transitions waiting for indoor response
        self.detected_lags: List[dict] = []  # Completed lag measurements

        # Predictive solar detection state
        self.early_warning_active: bool = False
        self.early_warning_start: Optional[datetime] = None

    def process_observation(
        self,
        timestamp: datetime,
        supply_temp: float,
        return_temp: float,
        room_temp: float,
        outdoor_temp: float,
        cloud_cover: float,
        wind_speed: float
    ) -> dict:
        """
        Process a new observation for all ML2 learning systems.

        This is the main entry point for the fetcher loop. It handles:
        1. Solar event detection (coefficient learning)
        2. Thermal lag tracking (timing learning)
        3. Early solar warning (predictive control)

        Args:
            timestamp: Observation timestamp
            supply_temp: Heating supply temperature (°C)
            return_temp: Heating return temperature (°C)
            room_temp: Indoor temperature (°C)
            outdoor_temp: Outdoor temperature (°C)
            cloud_cover: Cloud cover (0-8 octas)
            wind_speed: Wind speed (m/s)

        Returns:
            Dict with results from all systems:
            {
                'solar_event': SolarEvent or None,
                'early_warning': dict or None,
                'thermal_lag': dict or None,
                'heating_adjustment': dict or None
            }
        """
        result = {
            'solar_event': None,
            'early_warning': None,
            'thermal_lag': None,
            'heating_adjustment': None,
        }

        # 1. Solar event detection
        result['solar_event'] = self.add_observation(
            timestamp, supply_temp, return_temp, room_temp,
            outdoor_temp, cloud_cover, wind_speed
        )

        # 2. Check for early solar warning
        if self.observation_buffer:
            obs = list(self.observation_buffer)[-1]
            result['early_warning'] = self.detect_solar_event_early(obs)

        # 3. Track thermal lag
        result['thermal_lag'] = self.track_thermal_lag(
            timestamp, outdoor_temp, room_temp, wind_speed, cloud_cover
        )

        # 4. Get heating adjustment recommendation
        result['heating_adjustment'] = self.get_predictive_heating_adjustment()

        return result

    def add_observation(
        self,
        timestamp: datetime,
        supply_temp: float,
        return_temp: float,
        room_temp: float,
        outdoor_temp: float,
        cloud_cover: float,
        wind_speed: float
    ) -> Optional[SolarEvent]:
        """
        Add a new observation and check for solar event completion.

        Called every 15 minutes from the main fetcher loop.
        For comprehensive processing, use process_observation() instead.

        Args:
            timestamp: Observation timestamp
            supply_temp: Heating supply temperature (°C)
            return_temp: Heating return temperature (°C)
            room_temp: Indoor temperature (°C)
            outdoor_temp: Outdoor temperature (°C)
            cloud_cover: Cloud cover (0-8 octas)
            wind_speed: Wind speed (m/s)

        Returns:
            Completed SolarEvent if one just ended, None otherwise
        """
        # Calculate sun elevation
        sun_elev = self._calculate_sun_elevation(timestamp)

        obs = Observation(
            timestamp=timestamp,
            supply_temp=supply_temp,
            return_temp=return_temp,
            room_temp=room_temp,
            outdoor_temp=outdoor_temp,
            cloud_cover=cloud_cover,
            wind_speed=wind_speed,
            sun_elevation=sun_elev
        )

        # Add to buffer
        self.observation_buffer.append(obs)

        # Update outdoor temp baseline (use pre-sunrise temps)
        self._update_outdoor_baseline(obs)

        # Check if this observation meets solar event criteria
        is_solar_condition = self._is_solar_condition(obs)

        if is_solar_condition:
            # Start or continue event
            if self.current_event_start is None:
                self.current_event_start = timestamp
                self.current_event_observations = [obs]
            else:
                self.current_event_observations.append(obs)
        else:
            # Event ended - check if it was long enough
            completed_event = self._finalize_event()
            if completed_event:
                self.detected_events.append(completed_event)
                self.coefficients.events_since_last_update += 1
                self.coefficients.total_solar_events += 1
                return completed_event

        return None

    def _update_outdoor_baseline(self, obs: Observation) -> None:
        """
        Update the outdoor temperature baseline using pre-sunrise readings.

        The baseline represents the "true" outdoor temp without sun exposure.
        We use readings from before sunrise (sun_elev < 0) or early morning.
        """
        # Only use readings when sun is below horizon or very low
        if obs.sun_elevation < 5.0:
            self.baseline_samples.append(obs.outdoor_temp)
            # Keep last 8 samples (2 hours of pre-sunrise data)
            if len(self.baseline_samples) > 8:
                self.baseline_samples = self.baseline_samples[-8:]
            # Update baseline as median of recent samples
            if len(self.baseline_samples) >= 2:
                sorted_samples = sorted(self.baseline_samples)
                self.outdoor_temp_baseline = sorted_samples[len(sorted_samples) // 2]

    def _detect_sensor_solar_exposure(self, obs: Observation) -> bool:
        """
        Detect if the outdoor sensor is exposed to direct sunlight.

        Uses two methods:
        1. Temp anomaly: Current reading significantly above baseline
        2. Recent rise: Sudden temp increase (sun just hit sensor)

        Returns True if sensor appears to be sun-exposed.
        """
        # Method 1: Check for anomaly vs baseline
        if self.outdoor_temp_baseline is not None:
            anomaly = obs.outdoor_temp - self.outdoor_temp_baseline
            if anomaly >= self.OUTDOOR_TEMP_ANOMALY_THRESHOLD:
                return True

        # Method 2: Check for recent rapid rise
        if len(self.observation_buffer) >= 2:
            # Look at temp change over last 30 min (2 observations)
            recent_temps = [o.outdoor_temp for o in list(self.observation_buffer)[-3:]]
            if len(recent_temps) >= 2:
                temp_rise = obs.outdoor_temp - min(recent_temps[:-1])
                # Only count rises (not drops - sun moving away is normal)
                if temp_rise >= self.OUTDOOR_TEMP_RISE_THRESHOLD:
                    return True

        return False

    def _is_solar_condition(self, obs: Observation) -> bool:
        """
        Check if observation meets solar heating event criteria.

        Required conditions:
        1. Supply-return delta < 0.5°C (minimal heating)
        2. Would normally need heating (baseline outdoor temp is cold)
        3. Sun above horizon (elevation > 10°)

        Plus ONE of:
        A. Cloud cover < 3 octas (traditional method)
        B. Outdoor sensor shows solar exposure (sensor-based method)

        The sensor-based method is more reliable for local conditions.
        """
        # 1. Minimal heating (supply-return delta low)
        if obs.supply_return_delta >= self.SUPPLY_RETURN_DELTA_THRESHOLD:
            return False

        # 2. Would normally need heating
        # Use baseline temp if available (more accurate than sun-heated sensor reading)
        baseline_temp = self.outdoor_temp_baseline if self.outdoor_temp_baseline is not None else obs.outdoor_temp
        temp_diff = obs.room_temp - baseline_temp
        if temp_diff < self.OUTDOOR_INDOOR_DIFF_THRESHOLD:
            return False

        # 3. Sun above horizon
        if obs.sun_elevation <= self.SUN_ELEVATION_THRESHOLD:
            return False

        # 4. Either clear sky OR sensor shows solar exposure
        clear_sky = obs.cloud_cover < self.CLOUD_COVER_THRESHOLD
        sensor_exposed = self._detect_sensor_solar_exposure(obs)

        if not (clear_sky or sensor_exposed):
            return False

        return True

    def _finalize_event(self) -> Optional[SolarEvent]:
        """
        Finalize the current event if it meets duration requirements.

        Returns:
            SolarEvent if valid, None otherwise
        """
        if self.current_event_start is None or not self.current_event_observations:
            self.current_event_start = None
            self.current_event_observations = []
            return None

        observations = self.current_event_observations

        # Calculate duration
        duration = (observations[-1].timestamp - observations[0].timestamp).total_seconds() / 60

        # Check minimum duration
        if duration < self.MIN_EVENT_DURATION_MINUTES:
            self.current_event_start = None
            self.current_event_observations = []
            return None

        # Calculate averages
        avg_delta = sum(o.supply_return_delta for o in observations) / len(observations)
        avg_outdoor_sensor = sum(o.outdoor_temp for o in observations) / len(observations)
        avg_indoor = sum(o.room_temp for o in observations) / len(observations)
        avg_cloud = sum(o.cloud_cover for o in observations) / len(observations)
        avg_sun = sum(o.sun_elevation for o in observations) / len(observations)
        avg_wind = sum(o.wind_speed for o in observations) / len(observations)
        peak_sun = max(o.sun_elevation for o in observations)

        # Use baseline outdoor temp for coefficient calculation (not sun-heated sensor)
        # This gives a more accurate picture of actual outdoor conditions
        avg_outdoor = self.outdoor_temp_baseline if self.outdoor_temp_baseline is not None else avg_outdoor_sensor

        # Check if this event was detected via sensor (outdoor temp anomaly)
        # If so, the SMHI cloud cover is wrong - assume mostly clear (1.5 octas)
        sensor_detected = False
        if self.outdoor_temp_baseline is not None:
            sensor_anomaly = avg_outdoor_sensor - self.outdoor_temp_baseline
            sensor_detected = sensor_anomaly >= self.OUTDOOR_TEMP_ANOMALY_THRESHOLD

        # Use effective cloud cover for coefficient calculation
        # If sensor shows sun exposure but SMHI says cloudy, trust the sensor
        effective_cloud = 1.5 if sensor_detected and avg_cloud >= self.CLOUD_COVER_THRESHOLD else avg_cloud

        # Calculate implied solar coefficient
        implied_coeff = self._calculate_implied_solar_coefficient(
            avg_indoor, avg_outdoor, effective_cloud, avg_sun, avg_wind
        )

        event = SolarEvent(
            timestamp=observations[0].timestamp,
            end_timestamp=observations[-1].timestamp,
            duration_minutes=duration,
            avg_supply_return_delta=avg_delta,
            avg_outdoor_temp=avg_outdoor,
            avg_indoor_temp=avg_indoor,
            avg_cloud_cover=avg_cloud,
            avg_sun_elevation=avg_sun,
            avg_wind_speed=avg_wind,
            implied_solar_coefficient_ml2=implied_coeff,
            observations_count=len(observations),
            peak_sun_elevation=peak_sun
        )

        # Reset state
        self.current_event_start = None
        self.current_event_observations = []

        self.logger.info(
            f"Solar event detected: {duration:.0f}min, "
            f"sun={avg_sun:.1f}°, cloud={avg_cloud:.1f}, "
            f"implied_coeff={implied_coeff:.1f}"
        )

        return event

    def _calculate_implied_solar_coefficient(
        self,
        indoor_temp: float,
        outdoor_temp: float,
        cloud_cover: float,
        sun_elevation: float,
        wind_speed: float
    ) -> float:
        """
        Calculate implied solar coefficient using sensor anomaly method.

        The outdoor sensor anomaly (reading - baseline) directly measures
        how much the sun is "warming" that location. This is more reliable
        than physics-based calculation at high latitudes where low sun
        angles make the calculation unstable.

        Method:
        1. Sensor anomaly = current outdoor reading - baseline
        2. This represents the effective solar heating at current intensity
        3. Coefficient = anomaly / intensity (what it would be at full intensity)

        Falls back to physics-based calculation if no sensor anomaly available.
        """
        # Calculate solar intensity from sun elevation
        if sun_elevation <= 0:
            return 0.0
        intensity = math.sin(math.radians(sun_elevation))

        # Cloud transmission factor
        cloud_fraction = cloud_cover / 8.0
        cloud_transmission = 1.0 - (cloud_fraction * 0.9)

        # Combined intensity
        combined_intensity = intensity * cloud_transmission
        if combined_intensity < 0.1:
            return 0.0  # Too low to measure reliably

        # PRIMARY METHOD: Use sensor anomaly if we have baseline
        # The outdoor temp sensor anomaly directly measures solar heating effect
        if self.outdoor_temp_baseline is not None:
            # Get the sensor anomaly from the most recent observation
            if self.observation_buffer:
                recent_obs = list(self.observation_buffer)[-1]
                sensor_anomaly = recent_obs.outdoor_temp - self.outdoor_temp_baseline

                if sensor_anomaly > 1.0:  # Only if there's measurable anomaly
                    # The anomaly at current intensity tells us the coefficient
                    implied_coeff = sensor_anomaly / combined_intensity
                    # Cap at reasonable range (30-60 typical, up to 80 for excellent solar)
                    return max(15.0, min(80.0, implied_coeff))

        # FALLBACK: Physics-based calculation (less reliable at high latitudes)
        wind_effect = self.coefficients.wind_coefficient_ml2 * math.sqrt(max(0, wind_speed))
        solar_effect = indoor_temp - outdoor_temp + wind_effect
        implied_coeff = solar_effect / combined_intensity

        # More conservative cap for fallback method
        return max(15.0, min(60.0, implied_coeff))

    def _calculate_sun_elevation(self, timestamp: datetime) -> float:
        """Calculate sun elevation angle for the given timestamp."""
        if ASTRAL_AVAILABLE and self.location:
            try:
                return elevation(self.location.observer, timestamp)
            except Exception:
                pass

        # Fallback: simple approximation
        # This won't be accurate but gives a rough estimate
        hour = timestamp.hour + timestamp.minute / 60.0
        # Peak around noon (12-13), negative at night
        return 45.0 * math.sin(math.pi * (hour - 6) / 12) if 6 < hour < 18 else 0.0

    def should_update_coefficients(self) -> bool:
        """
        Check if we have enough new events to update coefficients.

        Learning schedule:
        - First update: after 3 events
        - Second update: after 6 events
        - Then: every 12 events
        """
        return self.coefficients.events_since_last_update >= self.coefficients.next_update_at_events

    def update_coefficients(self) -> LearnedWeatherCoefficients:
        """
        Update solar coefficient based on detected events.

        Uses weighted blending: 70% new, 30% old.

        Returns:
            Updated coefficients
        """
        if not self.detected_events:
            return self.coefficients

        # Get recent events (since last update)
        recent_events = self.detected_events[-self.coefficients.events_since_last_update:]

        if not recent_events:
            return self.coefficients

        # Calculate new coefficient as median of implied coefficients
        implied_coeffs = [e.implied_solar_coefficient_ml2 for e in recent_events]
        implied_coeffs.sort()

        if len(implied_coeffs) >= 3:
            # Use median for robustness
            mid = len(implied_coeffs) // 2
            new_coeff = implied_coeffs[mid]
        else:
            # Use mean for small samples
            new_coeff = sum(implied_coeffs) / len(implied_coeffs)

        # Blend with existing coefficient
        old_coeff = self.coefficients.solar_coefficient_ml2
        blended_coeff = (
            self.NEW_COEFFICIENT_WEIGHT * new_coeff +
            self.OLD_COEFFICIENT_WEIGHT * old_coeff
        )

        # Update confidence based on event count and coefficient stability
        stability = 1.0 - min(1.0, abs(new_coeff - old_coeff) / 20.0)
        event_confidence = min(1.0, self.coefficients.total_solar_events / 20.0)
        new_confidence = 0.5 * stability + 0.5 * event_confidence

        # Update coefficients
        self.coefficients.solar_coefficient_ml2 = round(blended_coeff, 1)
        self.coefficients.solar_confidence_ml2 = round(new_confidence, 2)
        self.coefficients.events_since_last_update = 0
        self.coefficients.updated_at = datetime.now(timezone.utc).isoformat()

        # Update learning schedule
        current_threshold = self.coefficients.next_update_at_events
        if current_threshold == self.FIRST_UPDATE_EVENTS:
            self.coefficients.next_update_at_events = self.SECOND_UPDATE_EVENTS
        elif current_threshold == self.SECOND_UPDATE_EVENTS:
            self.coefficients.next_update_at_events = self.REGULAR_UPDATE_EVENTS
        # else: stay at REGULAR_UPDATE_EVENTS

        self.logger.info(
            f"Updated solar coefficient: {old_coeff:.1f} -> {blended_coeff:.1f} "
            f"(from {len(recent_events)} events, confidence={new_confidence:.0%})"
        )

        return self.coefficients

    def process_historical_observation(
        self,
        timestamp: datetime,
        supply_temp: float,
        return_temp: float,
        room_temp: float,
        outdoor_temp: float,
        cloud_cover: float,
        wind_speed: float
    ) -> Optional[SolarEvent]:
        """
        Process a historical observation (for backfill).

        Same as add_observation but doesn't affect real-time state tracking.
        Used by backfill_weather_learning.py to process historical data.
        """
        return self.add_observation(
            timestamp, supply_temp, return_temp, room_temp,
            outdoor_temp, cloud_cover, wind_speed
        )

    def get_state(self) -> dict:
        """Get current learner state for persistence/debugging."""
        return {
            'coefficients': self.coefficients.to_dict(),
            'timing': self.timing.to_dict(),
            'buffer_size': len(self.observation_buffer),
            'detected_events_count': len(self.detected_events),
            'current_event_active': self.current_event_start is not None,
            'current_event_observations': len(self.current_event_observations) if self.current_event_observations else 0,
            'early_warning_active': self.early_warning_active,
        }

    # =========================================================================
    # Predictive Solar Detection
    # =========================================================================

    def detect_solar_event_early(self, obs: Observation) -> Optional[dict]:
        """
        Detect an impending solar event based on outdoor sensor behavior.

        When the outdoor temp sensor shows rapid rise (sun hitting it),
        this provides an early warning that solar heating is beginning.
        This allows proactive heating reduction ~30-60 min before the
        indoor temp would naturally rise.

        Args:
            obs: Current observation

        Returns:
            Early warning dict if detected, None otherwise:
            {
                'type': 'solar_early_warning',
                'start_time': datetime,
                'outdoor_rise': float,  # °C rise from baseline
                'estimated_lead_time_minutes': float,  # Time before indoor effect
                'confidence': float  # 0-1
            }
        """
        # Only look for early warnings during potential solar hours
        if obs.sun_elevation < 5.0:
            self._clear_early_warning()
            return None

        # Need a baseline to detect anomaly
        if self.outdoor_temp_baseline is None:
            return None

        outdoor_anomaly = obs.outdoor_temp - self.outdoor_temp_baseline

        # Check for rapid outdoor temp rise (sun hitting sensor)
        rapid_rise = False
        if len(self.observation_buffer) >= 2:
            prev_obs = list(self.observation_buffer)[-2]
            temp_change_30min = obs.outdoor_temp - prev_obs.outdoor_temp
            rapid_rise = temp_change_30min >= 2.0  # 2°C rise in 15 min

        # Detect early warning conditions
        if outdoor_anomaly >= 3.0 or rapid_rise:
            if not self.early_warning_active:
                self.early_warning_active = True
                self.early_warning_start = obs.timestamp

                # Estimate lead time based on learned thermal lag
                lead_time = self.timing.heat_up_lag_minutes_ml2

                self.logger.info(
                    f"Solar early warning: outdoor +{outdoor_anomaly:.1f}°C from baseline, "
                    f"estimated {lead_time:.0f}min before indoor effect"
                )

                return {
                    'type': 'solar_early_warning',
                    'start_time': obs.timestamp,
                    'outdoor_rise': outdoor_anomaly,
                    'estimated_lead_time_minutes': lead_time,
                    'confidence': min(1.0, outdoor_anomaly / 5.0)  # More rise = more confident
                }

        # Clear warning if anomaly subsides
        elif self.early_warning_active and outdoor_anomaly < 2.0:
            self._clear_early_warning()

        return None

    def _clear_early_warning(self):
        """Clear the early warning state."""
        self.early_warning_active = False
        self.early_warning_start = None

    # =========================================================================
    # Thermal Lag Learning
    # =========================================================================

    def track_thermal_lag(
        self,
        timestamp: datetime,
        outdoor_temp: float,
        indoor_temp: float,
        wind_speed: float,
        cloud_cover: float
    ) -> Optional[dict]:
        """
        Track thermal response lag by detecting when indoor temp responds
        to changes in effective outdoor temperature.

        This learns how long it takes for the building to respond to
        changes in weather conditions, enabling predictive heating control.

        Args:
            timestamp: Observation time
            outdoor_temp: Outdoor temperature (°C)
            indoor_temp: Indoor temperature (°C)
            wind_speed: Wind speed (m/s)
            cloud_cover: Cloud cover (0-8 octas)

        Returns:
            Completed lag measurement if one was detected, None otherwise:
            {
                'type': 'rising' or 'falling',
                'lag_minutes': float,
                'effective_temp_change': float,
                'indoor_temp_change': float,
                'confidence': float
            }
        """
        # Calculate effective temperature (simplified)
        effective_temp = self._calculate_effective_temp(
            timestamp, outdoor_temp, wind_speed, cloud_cover
        )

        # Add to history
        self.effective_temp_history.append((timestamp, effective_temp, indoor_temp))

        # Keep 4 hours of history (16 samples at 15-min intervals)
        if len(self.effective_temp_history) > 16:
            self.effective_temp_history = self.effective_temp_history[-16:]

        # Need at least 2 hours of history to detect transitions
        if len(self.effective_temp_history) < 8:
            return None

        # Check for significant effective_temp change in last 2 hours
        history_2h_ago = self.effective_temp_history[-8]
        eff_temp_change = effective_temp - history_2h_ago[1]

        # Detect significant transition (>3°C in 2 hours)
        if abs(eff_temp_change) >= 3.0:
            transition_type = 'rising' if eff_temp_change > 0 else 'falling'

            # Check if we already have this transition pending
            existing = [t for t in self.pending_transitions
                        if t['type'] == transition_type
                        and (timestamp - t['start_time']).total_seconds() < 7200]

            if not existing:
                # New transition detected - start watching for indoor response
                self.pending_transitions.append({
                    'type': transition_type,
                    'start_time': timestamp,
                    'eff_temp_at_start': history_2h_ago[1],
                    'indoor_at_start': history_2h_ago[2],
                    'eff_temp_change': eff_temp_change,
                })
                self.logger.debug(
                    f"Thermal transition detected: {transition_type}, "
                    f"effective_temp changed {eff_temp_change:+.1f}°C"
                )

        # Check pending transitions for indoor response
        completed = None
        remaining_transitions = []

        for transition in self.pending_transitions:
            age_minutes = (timestamp - transition['start_time']).total_seconds() / 60

            # Timeout after 4 hours
            if age_minutes > 240:
                continue

            # Check if indoor temp has responded
            indoor_change = indoor_temp - transition['indoor_at_start']
            expected_direction = 1 if transition['type'] == 'rising' else -1

            # Indoor responding in the expected direction (>0.5°C change)
            if indoor_change * expected_direction >= 0.5:
                # Found indoor response - calculate lag
                lag_minutes = age_minutes

                lag_data = {
                    'type': transition['type'],
                    'lag_minutes': lag_minutes,
                    'effective_temp_change': transition['eff_temp_change'],
                    'indoor_temp_change': indoor_change,
                    'confidence': min(1.0, abs(indoor_change) / 1.0)  # More change = more confident
                }

                self.detected_lags.append(lag_data)
                self.timing.total_transitions += 1

                self.logger.info(
                    f"Thermal lag measured: {transition['type']} response "
                    f"took {lag_minutes:.0f}min (indoor {indoor_change:+.1f}°C)"
                )

                # Update timing model
                self._update_thermal_timing(lag_data)
                completed = lag_data
            else:
                remaining_transitions.append(transition)

        self.pending_transitions = remaining_transitions
        return completed

    def _calculate_effective_temp(
        self,
        timestamp: datetime,
        outdoor_temp: float,
        wind_speed: float,
        cloud_cover: float
    ) -> float:
        """
        Calculate effective outdoor temperature considering solar and wind effects.

        This is a simplified calculation for thermal lag tracking.
        """
        # Get sun elevation
        sun_elev = self._calculate_sun_elevation(timestamp)

        # Wind effect (makes it feel colder)
        wind_effect = -self.coefficients.wind_coefficient_ml2 * math.sqrt(max(0, wind_speed))

        # Solar effect (makes it feel warmer)
        solar_effect = 0.0
        if sun_elev > 0:
            intensity = math.sin(math.radians(sun_elev))
            cloud_transmission = 1.0 - (cloud_cover / 8.0 * 0.9)
            solar_effect = self.coefficients.solar_coefficient_ml2 * intensity * cloud_transmission / 10.0  # Scaled

        return outdoor_temp + wind_effect + solar_effect

    def _update_thermal_timing(self, lag_data: dict) -> None:
        """
        Update thermal response timing based on new lag measurement.

        Uses weighted blending similar to coefficient updates.
        """
        lag_minutes = lag_data['lag_minutes']
        lag_type = lag_data['type']
        confidence = lag_data['confidence']

        # Update the appropriate lag
        if lag_type == 'rising':
            old_lag = self.timing.heat_up_lag_minutes_ml2
            # Blend with weight based on confidence
            weight = 0.3 * confidence  # 30% weight for new, scaled by confidence
            new_lag = (1 - weight) * old_lag + weight * lag_minutes
            self.timing.heat_up_lag_minutes_ml2 = round(new_lag, 1)
        else:
            old_lag = self.timing.cool_down_lag_minutes_ml2
            weight = 0.3 * confidence
            new_lag = (1 - weight) * old_lag + weight * lag_minutes
            self.timing.cool_down_lag_minutes_ml2 = round(new_lag, 1)

        # Update confidence based on sample count
        self.timing.confidence_ml2 = min(1.0, self.timing.total_transitions / 10.0)
        self.timing.updated_at = datetime.now(timezone.utc).isoformat()

    def should_update_timing(self) -> bool:
        """Check if timing should be persisted (after significant updates)."""
        return self.timing.total_transitions > 0 and self.timing.total_transitions % 5 == 0

    def get_predictive_heating_adjustment(self, forecast_hours: int = 2) -> Optional[dict]:
        """
        Get recommended heating adjustment based on predicted solar conditions.

        This uses the early warning system and thermal lag learning to recommend
        heating changes before they would normally be needed.

        Args:
            forecast_hours: Hours to look ahead

        Returns:
            Recommended adjustment or None:
            {
                'action': 'reduce' or 'increase' or 'maintain',
                'reason': str,
                'lead_time_minutes': float,
                'confidence': float
            }
        """
        if self.early_warning_active and self.early_warning_start:
            # Solar early warning is active - recommend reducing heat
            minutes_active = 0
            if self.observation_buffer:
                latest = list(self.observation_buffer)[-1]
                minutes_active = (latest.timestamp - self.early_warning_start).total_seconds() / 60

            # Only recommend if warning is fresh (< 30 min old)
            if minutes_active < 30:
                return {
                    'action': 'reduce',
                    'reason': 'Solar heating detected on outdoor sensor',
                    'lead_time_minutes': max(0, self.timing.heat_up_lag_minutes_ml2 - minutes_active),
                    'confidence': min(1.0, self.timing.confidence_ml2 + 0.3)  # Boost confidence for active warning
                }

        return None

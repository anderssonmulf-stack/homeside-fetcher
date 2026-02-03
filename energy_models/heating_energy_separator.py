"""
Heating Energy Separator - HomeSide On-Demand DHW Method

Separates district heating energy into:
- Space heating energy (radiators, floor heating)
- Domestic hot water (DHW) energy (tap water)

This method is specific to HomeSide setups where:
- Hot water is heated on-demand from district heating (no storage tank)
- hot_water_temp rises when tap water is being used
- Energy meter resolution is 1 kWh (coarse)

The algorithm:
1. Detect DHW events by identifying hot_water_temp peaks
2. Estimate energy used during DHW events based on:
   - Temperature rise duration
   - Flow characteristics
   - Typical DHW energy per event
3. Distribute the 1 kWh resolution over time
4. Remaining energy after DHW = space heating

Configuration in customer profile:
{
    "energy_separation": {
        "method": "homeside_ondemand_dhw",
        "enabled": true,
        "dhw_temp_threshold": 45.0,      # Min temp to consider DHW active
        "dhw_temp_rise_threshold": 2.0,  # Min temp rise to detect event
        "dhw_baseline_temp": 25.0,       # Baseline temp when not in use
        "avg_dhw_power_kw": 25.0         # Typical instantaneous DHW power
    }
}
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
from zoneinfo import ZoneInfo
import statistics

# Swedish timezone for proper day boundaries
SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


@dataclass
class DHWEvent:
    """Represents a detected domestic hot water usage event."""
    start_time: datetime
    end_time: datetime
    peak_temp: float
    temp_rise: float  # Rise from baseline
    duration_minutes: float
    estimated_energy_kwh: float

    @property
    def duration(self) -> timedelta:
        return self.end_time - self.start_time


@dataclass
class EnergySeparationResult:
    """Result of energy separation for a time period."""
    timestamp: datetime
    total_energy_kwh: float
    heating_energy_kwh: float
    dhw_energy_kwh: float
    dhw_events: List[DHWEvent] = field(default_factory=list)
    confidence: float = 0.0  # 0-1, how confident we are in the separation
    method: str = "homeside_ondemand_dhw"


class HomeSideOnDemandDHWSeparator:
    """
    Separates heating energy from DHW energy for HomeSide on-demand systems.

    In this setup:
    - District heating provides both space heating and DHW
    - DHW is heated instantaneously (no storage tank)
    - When hot water tap is opened, hot_water_temp rises
    - When tap is closed, hot_water_temp falls back to baseline
    """

    # Default configuration
    DEFAULT_CONFIG = {
        'dhw_temp_threshold': 45.0,       # Min temp to consider DHW active
        'dhw_temp_rise_threshold': 2.0,   # Min rise from baseline to detect event
        'dhw_baseline_temp': 25.0,        # Expected temp when DHW not in use
        'avg_dhw_power_kw': 25.0,         # Typical instantaneous power during DHW
        'min_event_duration_sec': 30,     # Minimum event duration to count
        'max_event_gap_sec': 120,         # Max gap to merge events
        'cold_water_temp': 8.0,           # Assumed cold water inlet temp
        'hot_water_target_temp': 55.0,    # Target hot water delivery temp
    }

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize separator with configuration.

        Args:
            config: Configuration dict, missing keys use defaults
        """
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}

    def detect_dhw_events(
        self,
        hot_water_temps: List[Dict],  # [{'timestamp': datetime, 'value': float}, ...]
        min_samples: int = 3
    ) -> List[DHWEvent]:
        """
        Detect DHW usage events from hot water temperature data.

        Args:
            hot_water_temps: List of temperature readings with timestamps
            min_samples: Minimum samples needed for valid detection

        Returns:
            List of detected DHW events
        """
        if len(hot_water_temps) < min_samples:
            return []

        # Sort by timestamp
        temps = sorted(hot_water_temps, key=lambda x: x['timestamp'])

        # Calculate baseline (typical low temperature)
        values = [t['value'] for t in temps if t['value'] is not None]
        if not values:
            return []

        # Use lower quartile as baseline estimate
        sorted_values = sorted(values)
        baseline_idx = len(sorted_values) // 4
        baseline = sorted_values[baseline_idx] if baseline_idx > 0 else sorted_values[0]
        baseline = max(baseline, self.config['dhw_baseline_temp'])

        events = []
        in_event = False
        event_start = None
        event_temps = []

        threshold = self.config['dhw_temp_threshold']
        rise_threshold = self.config['dhw_temp_rise_threshold']

        for reading in temps:
            temp = reading['value']
            ts = reading['timestamp']

            if temp is None:
                continue

            # Check if we're in a DHW event (temp above threshold AND risen from baseline)
            is_dhw_active = (temp >= threshold and temp - baseline >= rise_threshold)

            if is_dhw_active and not in_event:
                # Start of new event
                in_event = True
                event_start = ts
                event_temps = [(ts, temp)]

            elif is_dhw_active and in_event:
                # Continue event
                event_temps.append((ts, temp))

            elif not is_dhw_active and in_event:
                # End of event
                in_event = False

                if event_temps:
                    event = self._create_event(event_start, ts, event_temps, baseline)
                    if event and event.duration_minutes >= self.config['min_event_duration_sec'] / 60:
                        events.append(event)

                event_temps = []

        # Handle event still in progress at end of data
        if in_event and event_temps:
            last_ts = temps[-1]['timestamp']
            event = self._create_event(event_start, last_ts, event_temps, baseline)
            if event:
                events.append(event)

        # Merge events that are close together
        events = self._merge_close_events(events)

        return events

    def _create_event(
        self,
        start: datetime,
        end: datetime,
        temps: List[Tuple[datetime, float]],
        baseline: float
    ) -> Optional[DHWEvent]:
        """Create a DHW event from temperature readings."""
        if not temps:
            return None

        peak_temp = max(t[1] for t in temps)
        temp_rise = peak_temp - baseline
        duration = (end - start).total_seconds() / 60  # minutes

        # Estimate energy based on duration and typical power
        # E = P * t where P is average power during DHW event
        avg_power = self.config['avg_dhw_power_kw']
        estimated_energy = avg_power * (duration / 60)  # kWh

        # Adjust based on temperature rise (higher rise = more energy)
        # Typical DHW needs ~40°C rise (8°C to 55°C)
        expected_rise = self.config['hot_water_target_temp'] - self.config['cold_water_temp']
        rise_factor = min(temp_rise / expected_rise, 1.5) if expected_rise > 0 else 1.0
        estimated_energy *= rise_factor

        return DHWEvent(
            start_time=start,
            end_time=end,
            peak_temp=peak_temp,
            temp_rise=temp_rise,
            duration_minutes=duration,
            estimated_energy_kwh=estimated_energy
        )

    def _merge_close_events(self, events: List[DHWEvent]) -> List[DHWEvent]:
        """Merge events that are close together in time."""
        if len(events) <= 1:
            return events

        max_gap = timedelta(seconds=self.config['max_event_gap_sec'])
        merged = []
        current = events[0]

        for next_event in events[1:]:
            gap = next_event.start_time - current.end_time

            if gap <= max_gap:
                # Merge events
                current = DHWEvent(
                    start_time=current.start_time,
                    end_time=next_event.end_time,
                    peak_temp=max(current.peak_temp, next_event.peak_temp),
                    temp_rise=max(current.temp_rise, next_event.temp_rise),
                    duration_minutes=(next_event.end_time - current.start_time).total_seconds() / 60,
                    estimated_energy_kwh=current.estimated_energy_kwh + next_event.estimated_energy_kwh
                )
            else:
                merged.append(current)
                current = next_event

        merged.append(current)
        return merged

    def separate_energy(
        self,
        energy_data: List[Dict],      # [{'timestamp': datetime, 'consumption': float}, ...]
        hot_water_temps: List[Dict],  # [{'timestamp': datetime, 'value': float}, ...]
        period_hours: int = 24
    ) -> List[EnergySeparationResult]:
        """
        Separate total energy into heating and DHW components.

        Args:
            energy_data: Energy consumption readings (hourly, 1 kWh resolution)
            hot_water_temps: Hot water temperature readings
            period_hours: Period to group results (default 24h for daily)

        Returns:
            List of separation results per period
        """
        if not energy_data or not hot_water_temps:
            return []

        # Detect DHW events
        dhw_events = self.detect_dhw_events(hot_water_temps)

        # Sort energy data by timestamp
        energy = sorted(energy_data, key=lambda x: x['timestamp'])

        # Build hourly consumption lookup for capping DHW estimates
        # Key: hour start timestamp, Value: consumption in kWh
        hourly_consumption = {}
        for reading in energy:
            ts = reading['timestamp']
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            hourly_consumption[ts] = reading.get('consumption', 0) or 0

        # Group by period using Swedish day boundaries
        # Energy data timestamps are UTC, but we want to group by Swedish calendar day
        results = []
        first_ts = energy[0]['timestamp']

        # Ensure timezone awareness
        if first_ts.tzinfo is None:
            first_ts = first_ts.replace(tzinfo=timezone.utc)

        # Convert to Swedish time, get start of that day, convert back to UTC
        swedish_time = first_ts.astimezone(SWEDISH_TZ)
        swedish_midnight = swedish_time.replace(hour=0, minute=0, second=0, microsecond=0)
        period_start = swedish_midnight.astimezone(timezone.utc)

        period_delta = timedelta(hours=period_hours)

        current_period_energy = 0.0
        current_period_events = []

        # Maximum fraction of hourly consumption that can be DHW
        # Heating is always running, so DHW can't be 100% of any hour
        max_hourly_dhw_fraction = 0.80

        for reading in energy:
            ts = reading['timestamp']
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            consumption = reading.get('consumption', 0) or 0

            # Check if we've moved to a new period
            while ts >= period_start + period_delta:
                # Finalize current period
                if current_period_energy > 0:
                    result = self._calculate_period_result(
                        period_start,
                        current_period_energy,
                        current_period_events
                    )
                    results.append(result)

                period_start += period_delta
                current_period_energy = 0.0
                current_period_events = []

            current_period_energy += consumption

            # Find DHW events in this hour and cap by actual consumption
            hour_start = ts
            hour_end = ts + timedelta(hours=1)
            hour_dhw_budget = consumption * max_hourly_dhw_fraction
            hour_dhw_used = 0.0

            for event in dhw_events:
                if event.start_time >= hour_start and event.start_time < hour_end:
                    # Cap event energy by remaining hourly budget
                    remaining_budget = hour_dhw_budget - hour_dhw_used
                    if remaining_budget > 0:
                        capped_energy = min(event.estimated_energy_kwh, remaining_budget)
                        # Create a modified event with capped energy
                        capped_event = DHWEvent(
                            start_time=event.start_time,
                            end_time=event.end_time,
                            peak_temp=event.peak_temp,
                            temp_rise=event.temp_rise,
                            duration_minutes=event.duration_minutes,
                            estimated_energy_kwh=capped_energy
                        )
                        current_period_events.append(capped_event)
                        hour_dhw_used += capped_energy

        # Finalize last period
        if current_period_energy > 0:
            result = self._calculate_period_result(
                period_start,
                current_period_energy,
                current_period_events
            )
            results.append(result)

        return results

    def _calculate_period_result(
        self,
        period_start: datetime,
        total_energy: float,
        dhw_events: List[DHWEvent]
    ) -> EnergySeparationResult:
        """Calculate the energy separation for a period."""

        # Sum up estimated DHW energy
        estimated_dhw = sum(e.estimated_energy_kwh for e in dhw_events)

        # Sanity check: DHW can't exceed total energy
        # Typical household: DHW is 20-40% of total heating energy
        max_dhw_fraction = 0.6  # DHW shouldn't exceed 60% of total
        if estimated_dhw > total_energy * max_dhw_fraction:
            # Scale down DHW estimate
            estimated_dhw = total_energy * max_dhw_fraction

        # If no events detected but we have energy, assume it's all heating
        if not dhw_events:
            estimated_dhw = 0.0

        heating_energy = total_energy - estimated_dhw

        # Calculate confidence based on data quality
        # More events detected with consistent patterns = higher confidence
        confidence = 0.5  # Base confidence
        if dhw_events:
            # Adjust based on number of events (more data = more confidence)
            event_factor = min(len(dhw_events) / 10, 0.3)  # Up to +0.3
            confidence += event_factor

            # Consistent event durations = higher confidence
            if len(dhw_events) >= 3:
                durations = [e.duration_minutes for e in dhw_events]
                cv = statistics.stdev(durations) / statistics.mean(durations) if statistics.mean(durations) > 0 else 1
                consistency_factor = max(0, 0.2 - cv * 0.1)  # Up to +0.2
                confidence += consistency_factor

        confidence = min(confidence, 1.0)

        return EnergySeparationResult(
            timestamp=period_start,
            total_energy_kwh=total_energy,
            heating_energy_kwh=round(heating_energy, 2),
            dhw_energy_kwh=round(estimated_dhw, 2),
            dhw_events=dhw_events,
            confidence=round(confidence, 2),
            method="homeside_ondemand_dhw"
        )


def get_energy_separator(method: str, config: Optional[Dict] = None):
    """
    Factory function to get the appropriate energy separator.

    Args:
        method: Separation method name
        config: Method-specific configuration

    Returns:
        Energy separator instance

    Raises:
        ValueError: If method is unknown
    """
    separators = {
        'homeside_ondemand_dhw': HomeSideOnDemandDHWSeparator,
    }

    if method not in separators:
        raise ValueError(f"Unknown energy separation method: {method}. Available: {list(separators.keys())}")

    return separators[method](config)

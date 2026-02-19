"""
Customer Profile Manager

Handles loading, saving, and validating customer-specific settings.
Each customer has a JSON file in the profiles/ directory containing:
- Building characteristics
- Comfort preferences
- Heating system settings
- Learned parameters (auto-updated by the forecaster)

All customer-specific variables are centralized here for maintainability
and future GUI integration.
"""

import json
import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, field, asdict


@dataclass
class BuildingConfig:
    """Building characteristics affecting thermal behavior."""
    description: str = ""
    thermal_response: str = "medium"  # slow, medium, fast


@dataclass
class ComfortConfig:
    """User comfort preferences."""
    target_indoor_temp: float = 22.0
    acceptable_deviation: float = 1.0  # +/- from target


@dataclass
class HeatingSystemConfig:
    """Heating system characteristics."""
    response_time_minutes: int = 30
    max_supply_temp: float = 55.0
    # Distribution type(s): "floor", "radiator", "ventilation", or comma-separated combo
    distribution_type: str = "floor"
    # Whether primary-side power is available (dh_power field from HomeSide)
    has_power_meter: bool = False


@dataclass
class LearnedWeatherCoefficients:
    """
    Weather coefficients learned from solar event detection (ML2 model).

    Uses _ml2 suffix to distinguish from original model coefficients.
    Solar coefficient is learned from detected solar heating events.
    Wind coefficient is fixed low for modern FTX houses.
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


@dataclass
class ThermalResponseTiming:
    """
    Learned thermal response timing for predictive control (ML2 model).

    How quickly the building responds to effective_temp changes.
    Used for predictive heating control.
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


@dataclass
class LearnedParameters:
    """
    Parameters learned from historical data.
    These are auto-updated by the forecaster.
    """
    thermal_coefficient: Optional[float] = None
    thermal_coefficient_confidence: float = 0.0
    hourly_bias: Dict[str, float] = field(default_factory=dict)
    samples_since_last_update: int = 0
    total_samples: int = 0
    next_update_at_samples: int = 24  # First update after 24 samples
    updated_at: Optional[str] = None
    # ML2 weather sensitivity coefficients (learned from solar events)
    weather_coefficients: LearnedWeatherCoefficients = field(default_factory=LearnedWeatherCoefficients)
    thermal_timing: ThermalResponseTiming = field(default_factory=ThermalResponseTiming)
    # Thermal time constant (hours) — how fast the building responds to heating changes
    thermal_time_constant: Optional[float] = None
    thermal_time_constant_measured_at: Optional[str] = None
    thermal_time_constant_source: Optional[str] = None  # "measured" or "copied"
    thermal_time_constant_copied_from: Optional[str] = None  # customer_id if copied


@dataclass
class EnergySeparationConfig:
    """
    Configuration for separating district heating energy into components.

    Different methods are available for different heating system setups:
    - k_calibration: Uses calibrated heat loss coefficient (recommended)
    - homeside_ondemand_dhw: For HomeSide systems with on-demand DHW heating
    """
    enabled: bool = False
    method: str = "k_calibration"  # Separation method to use

    # K-calibration method settings (from heating_energy_calibrator.py)
    heat_loss_k: Optional[float] = None    # Calibrated heat loss coefficient (kW/°C)
    k_percentile: int = 15                 # Percentile used for calibration
    calibration_date: Optional[str] = None # Date of last calibration
    calibration_days: int = 0              # Number of days used in calibration
    dhw_percentage: Optional[float] = None # Estimated DHW percentage from calibration

    # HomeSide on-demand DHW method settings (legacy)
    dhw_temp_threshold: float = 45.0       # Min temp to consider DHW active
    dhw_temp_rise_threshold: float = 2.0   # Min rise from baseline to detect event
    dhw_baseline_temp: float = 25.0        # Expected temp when DHW not in use
    avg_dhw_power_kw: float = 25.0         # Typical instantaneous power during DHW
    cold_water_temp: float = 8.0           # Assumed cold water inlet temp
    hot_water_target_temp: float = 55.0    # Target hot water delivery temp


@dataclass
class ThermalTestRequest:
    """Pending thermal inertia test request stored in profile for cross-process communication."""
    status: str = "none"  # none, pending_approval, approved, declined, in_progress, completed, failed
    token: Optional[str] = None
    requested_at: Optional[str] = None
    expires_at: Optional[str] = None
    conditions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HeatCurveControlConfig:
    """
    State for heat curve control via control_homeside.py.

    Stores baseline values (Yref curve + adaption settings) captured before
    entering control mode, so they can be restored on exit.
    """
    in_control: bool = False
    baseline: Dict[str, Any] = field(default_factory=dict)
    entered_at: Optional[str] = None
    reason: Optional[str] = None
    # Curve control mode: "intelligent" (ML), "adaptive" (HomeSide), "manual" (static)
    curve_control_mode: str = "adaptive"
    # ML curve control: writes weather-adjusted curve to HomeSide
    ml_enabled: bool = False  # Kept for backward compat; derived from curve_control_mode
    ml_update_interval_minutes: int = 30
    ml_last_offset: Optional[float] = None
    ml_reactive_threshold: float = 1.0

    def __post_init__(self):
        # Backward compat: if ml_enabled was set in JSON but no curve_control_mode,
        # upgrade to "intelligent" so existing profiles keep working
        if self.ml_enabled and self.curve_control_mode == "adaptive":
            self.curve_control_mode = "intelligent"


@dataclass
class CustomerProfile:
    """
    Complete customer profile containing all settings and learned parameters.

    Usage:
        profile = CustomerProfile.load("HEM_FJV_Villa_149")
        print(profile.comfort.target_indoor_temp)
        profile.learned.thermal_coefficient = 0.00012
        profile.save()
    """
    schema_version: int = 1
    customer_id: str = ""
    friendly_name: str = ""
    meter_ids: list = field(default_factory=list)  # Energy meter IDs mapped to this house
    building: BuildingConfig = field(default_factory=BuildingConfig)
    comfort: ComfortConfig = field(default_factory=ComfortConfig)
    heating_system: HeatingSystemConfig = field(default_factory=HeatingSystemConfig)
    learned: LearnedParameters = field(default_factory=LearnedParameters)
    energy_separation: EnergySeparationConfig = field(default_factory=EnergySeparationConfig)
    heat_curve_control: HeatCurveControlConfig = field(default_factory=HeatCurveControlConfig)
    thermal_test: ThermalTestRequest = field(default_factory=ThermalTestRequest)
    variable_overrides: Dict[str, str] = field(default_factory=dict)

    _profiles_dir: str = field(default="profiles", repr=False)
    _logger: logging.Logger = field(default=None, repr=False)

    def __post_init__(self):
        if self._logger is None:
            self._logger = logging.getLogger(__name__)

    @classmethod
    def load(cls, customer_id: str, profiles_dir: str = "profiles") -> "CustomerProfile":
        """
        Load a customer profile from JSON file.

        Args:
            customer_id: The customer identifier (filename without .json)
            profiles_dir: Directory containing profile files

        Returns:
            CustomerProfile instance

        Raises:
            FileNotFoundError: If profile doesn't exist
            json.JSONDecodeError: If profile is invalid JSON
        """
        logger = logging.getLogger(__name__)
        filepath = os.path.join(profiles_dir, f"{customer_id}.json")

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Customer profile not found: {filepath}")

        with open(filepath, 'r') as f:
            data = json.load(f)

        profile = cls._from_dict(data)
        profile._profiles_dir = profiles_dir
        profile._logger = logger

        logger.info(f"Loaded customer profile: {profile.friendly_name} ({customer_id})")
        return profile

    @classmethod
    def load_by_path(cls, filepath: str) -> "CustomerProfile":
        """Load a customer profile from a specific file path."""
        logger = logging.getLogger(__name__)

        with open(filepath, 'r') as f:
            data = json.load(f)

        profile = cls._from_dict(data)
        profile._profiles_dir = os.path.dirname(filepath)
        profile._logger = logger

        return profile

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "CustomerProfile":
        """Create a CustomerProfile from a dictionary."""
        # Parse learned parameters with nested dataclasses
        learned_data = data.get("learned", {})
        weather_coeff_data = learned_data.pop("weather_coefficients", {})
        thermal_timing_data = learned_data.pop("thermal_timing", {})

        learned = LearnedParameters(
            **{k: v for k, v in learned_data.items() if k not in ['weather_coefficients', 'thermal_timing']},
            weather_coefficients=LearnedWeatherCoefficients(**weather_coeff_data) if weather_coeff_data else LearnedWeatherCoefficients(),
            thermal_timing=ThermalResponseTiming(**thermal_timing_data) if thermal_timing_data else ThermalResponseTiming()
        )

        customer_id = data.get("customer_id", "")
        # Meter IDs only from env vars (HOUSE_<id>_METER_IDS) — never from profile JSON
        meter_ids = get_meter_ids_from_env(customer_id)

        return cls(
            schema_version=data.get("schema_version", 1),
            customer_id=customer_id,
            friendly_name=data.get("friendly_name", ""),
            meter_ids=meter_ids,
            building=BuildingConfig(**data.get("building", {})),
            comfort=ComfortConfig(**data.get("comfort", {})),
            heating_system=HeatingSystemConfig(**data.get("heating_system", {})),
            learned=learned,
            energy_separation=EnergySeparationConfig(**data.get("energy_separation", {})),
            heat_curve_control=HeatCurveControlConfig(**data.get("heat_curve_control", {})),
            thermal_test=ThermalTestRequest(**data.get("thermal_test", {})),
            variable_overrides=data.get("variable_overrides", {})
        )

    def save(self) -> None:
        """Save the profile back to JSON file."""
        filepath = os.path.join(self._profiles_dir, f"{self.customer_id}.json")

        data = {
            "schema_version": self.schema_version,
            "customer_id": self.customer_id,
            "friendly_name": self.friendly_name,
            "meter_ids": self.meter_ids,
            "building": asdict(self.building),
            "comfort": asdict(self.comfort),
            "heating_system": asdict(self.heating_system),
            "learned": asdict(self.learned),
            "energy_separation": asdict(self.energy_separation),
            "heat_curve_control": asdict(self.heat_curve_control),
            "thermal_test": asdict(self.thermal_test),
            "variable_overrides": self.variable_overrides
        }

        # Omit thermal_test from JSON if status is "none" (keep profiles clean)
        if data["thermal_test"]["status"] == "none":
            del data["thermal_test"]

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        self._logger.info(f"Saved customer profile: {self.customer_id}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to dictionary (for GUI/API)."""
        result = {
            "schema_version": self.schema_version,
            "customer_id": self.customer_id,
            "friendly_name": self.friendly_name,
            "meter_ids": self.meter_ids,
            "building": asdict(self.building),
            "comfort": asdict(self.comfort),
            "heating_system": asdict(self.heating_system),
            "learned": asdict(self.learned),
            "energy_separation": asdict(self.energy_separation),
            "heat_curve_control": asdict(self.heat_curve_control),
            "thermal_test": asdict(self.thermal_test),
            "variable_overrides": self.variable_overrides
        }

        if result["thermal_test"]["status"] == "none":
            del result["thermal_test"]

        return result

    def update_learned_params(
        self,
        thermal_coefficient: Optional[float] = None,
        confidence: Optional[float] = None
    ) -> None:
        """
        Update learned parameters from thermal analyzer.
        Called during data collection.
        """
        if thermal_coefficient is not None:
            self.learned.thermal_coefficient = thermal_coefficient
        if confidence is not None:
            self.learned.thermal_coefficient_confidence = confidence
        self.learned.updated_at = datetime.utcnow().isoformat() + "Z"

    def record_sample(self) -> bool:
        """
        Record that a new sample was collected.

        Returns:
            True if it's time to update hourly_bias, False otherwise
        """
        self.learned.samples_since_last_update += 1
        self.learned.total_samples += 1

        should_update = (
            self.learned.samples_since_last_update >=
            self.learned.next_update_at_samples
        )

        if should_update:
            # Schedule next update: 24 -> 48 -> 96 (then stay at 96)
            current = self.learned.next_update_at_samples
            if current == 24:
                self.learned.next_update_at_samples = 48
            elif current == 48:
                self.learned.next_update_at_samples = 96
            # else: stay at 96 (daily updates)

            self.learned.samples_since_last_update = 0

        return should_update

    def get_status(self) -> Dict[str, Any]:
        """
        Get profile status for GUI display.

        Returns human-readable status of the learning system.
        """
        learned = self.learned

        if learned.thermal_coefficient is None:
            learning_status = "Waiting for initial data"
        elif learned.thermal_coefficient_confidence < 0.5:
            learning_status = "Learning (low confidence)"
        elif learned.thermal_coefficient_confidence < 0.8:
            learning_status = "Learning (moderate confidence)"
        else:
            learning_status = "Stable (high confidence)"

        hourly_coverage = len([b for b in learned.hourly_bias.values() if b != 0])

        # ML2 weather learning status
        weather = learned.weather_coefficients
        if weather.total_solar_events == 0:
            weather_status = "Waiting for solar events"
        elif weather.solar_confidence_ml2 < 0.3:
            weather_status = f"Learning ({weather.total_solar_events} events)"
        elif weather.solar_confidence_ml2 < 0.6:
            weather_status = f"Calibrating ({weather.solar_confidence_ml2:.0%})"
        else:
            weather_status = f"Calibrated ({weather.solar_confidence_ml2:.0%})"

        return {
            "customer": self.friendly_name,
            "target_temp": self.comfort.target_indoor_temp,
            "learning_status": learning_status,
            "thermal_coefficient": learned.thermal_coefficient,
            "confidence": f"{learned.thermal_coefficient_confidence:.0%}",
            "total_samples": learned.total_samples,
            "hourly_bias_coverage": f"{hourly_coverage}/24 hours",
            "next_update_in": (
                learned.next_update_at_samples -
                learned.samples_since_last_update
            ),
            "last_updated": learned.updated_at,
            # ML2 weather sensitivity
            "weather_status_ml2": weather_status,
            "solar_coefficient_ml2": weather.solar_coefficient_ml2,
            "solar_events_total": weather.total_solar_events,
            "solar_events_until_update": (
                weather.next_update_at_events -
                weather.events_since_last_update
            ),
        }


def find_profile_for_client_id(client_id: str, profiles_dir: str = "profiles") -> Optional[CustomerProfile]:
    """
    Find a profile that matches a HomeSide client ID.

    The client_id from HomeSide is like "38/xxx/HEM_FJV_149/HEM_FJV_Villa_149"
    We extract "HEM_FJV_Villa_149" and look for a matching profile.

    Args:
        client_id: Full HomeSide client ID
        profiles_dir: Directory containing profiles

    Returns:
        CustomerProfile if found, None otherwise
    """
    logger = logging.getLogger(__name__)

    # Extract customer_id from full path (last segment)
    parts = client_id.split("/")
    possible_ids = [parts[-1]] if parts else []

    # Also try second-to-last if available
    if len(parts) >= 2:
        possible_ids.append(parts[-2])

    # Check for matching profile file
    for cust_id in possible_ids:
        filepath = os.path.join(profiles_dir, f"{cust_id}.json")
        if os.path.exists(filepath):
            try:
                return CustomerProfile.load(cust_id, profiles_dir)
            except Exception as e:
                logger.error(f"Failed to load profile {cust_id}: {e}")

    logger.warning(f"No profile found for client_id: {client_id}")
    return None


def get_meter_ids_from_env(customer_id: str) -> list:
    """
    Read meter_ids from environment variable HOUSE_<customer_id>_METER_IDS.

    Returns:
        List of meter ID strings, or empty list if not set.
    """
    env_key = f"HOUSE_{customer_id}_METER_IDS"
    value = os.getenv(env_key, "")
    if value:
        return [m.strip() for m in value.split(",") if m.strip()]
    return []


def get_building_meter_ids(building_id: str, buildings_dir: str = "buildings") -> list:
    """
    Read meter_ids for a building.

    Checks BUILDING_<building_id>_METER_IDS env var first, then falls back
    to the meter_ids field in the building's JSON config.

    Returns:
        List of meter ID strings, or empty list if not configured.
    """
    # Env var takes precedence
    env_key = f"BUILDING_{building_id}_METER_IDS"
    value = os.getenv(env_key, "")
    if value:
        return [m.strip() for m in value.split(",") if m.strip()]

    # Fall back to JSON config
    filepath = os.path.join(buildings_dir, f"{building_id}.json")
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            meter_ids = data.get('meter_ids', [])
            return [str(m).strip() for m in meter_ids if str(m).strip()]
        except Exception:
            pass

    return []


def build_meter_mapping(profiles_dir: str = "profiles", buildings_dir: str = "buildings") -> Dict[str, dict]:
    """
    Build a mapping of meter_id -> entity info from all profiles and buildings.

    Used by the energy importer to look up which house/building a meter belongs to.

    Args:
        profiles_dir: Directory containing house profile JSON files
        buildings_dir: Directory containing building config JSON files

    Returns:
        Dictionary mapping meter_id to {"id": entity_id, "type": "house"|"building",
                                         "friendly_name": name}
    """
    logger = logging.getLogger(__name__)
    mapping = {}

    def _add_meter(meter_id: str, entity_id: str, entity_type: str, friendly_name: str):
        meter_id = str(meter_id).strip()
        if not meter_id:
            return
        if meter_id in mapping:
            logger.warning(
                f"Duplicate meter_id {meter_id}: "
                f"already mapped to {mapping[meter_id]['id']}, "
                f"ignoring mapping to {entity_id}"
            )
        else:
            mapping[meter_id] = {
                "id": entity_id,
                "type": entity_type,
                "friendly_name": friendly_name
            }

    # Scan house profiles
    if os.path.exists(profiles_dir):
        for filename in os.listdir(profiles_dir):
            if not filename.endswith('.json') or '_signals.json' in filename:
                continue

            try:
                filepath = os.path.join(profiles_dir, filename)
                with open(filepath, 'r') as f:
                    data = json.load(f)

                customer_id = data.get('customer_id', '')
                friendly_name = data.get('friendly_name', customer_id)
                meter_ids = get_meter_ids_from_env(customer_id)

                for mid in meter_ids:
                    _add_meter(mid, customer_id, "house", friendly_name)

            except Exception as e:
                logger.error(f"Error loading profile {filename}: {e}")
    else:
        logger.warning(f"Profiles directory not found: {profiles_dir}")

    # Scan building configs
    if os.path.exists(buildings_dir):
        for filename in os.listdir(buildings_dir):
            if not filename.endswith('.json') or '_signals.json' in filename:
                continue

            try:
                filepath = os.path.join(buildings_dir, filename)
                with open(filepath, 'r') as f:
                    data = json.load(f)

                building_id = data.get('building_id', filename.replace('.json', ''))
                friendly_name = data.get('friendly_name', building_id)
                meter_ids = get_building_meter_ids(building_id, buildings_dir)

                for mid in meter_ids:
                    _add_meter(mid, building_id, "building", friendly_name)

            except Exception as e:
                logger.error(f"Error loading building config {filename}: {e}")

    logger.info(f"Built meter mapping: {len(mapping)} meter(s) across profiles and buildings")
    return mapping


def find_customer_by_meter_id(meter_id: str, profiles_dir: str = "profiles") -> Optional[str]:
    """
    Find customer_id for a given meter_id.

    Args:
        meter_id: The energy meter ID to look up
        profiles_dir: Directory containing profile JSON files

    Returns:
        customer_id if found, None otherwise
    """
    mapping = build_meter_mapping(profiles_dir)
    entry = mapping.get(str(meter_id).strip())
    return entry['id'] if entry else None

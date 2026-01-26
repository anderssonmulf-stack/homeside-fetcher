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
    building: BuildingConfig = field(default_factory=BuildingConfig)
    comfort: ComfortConfig = field(default_factory=ComfortConfig)
    heating_system: HeatingSystemConfig = field(default_factory=HeatingSystemConfig)
    learned: LearnedParameters = field(default_factory=LearnedParameters)

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
        return cls(
            schema_version=data.get("schema_version", 1),
            customer_id=data.get("customer_id", ""),
            friendly_name=data.get("friendly_name", ""),
            building=BuildingConfig(**data.get("building", {})),
            comfort=ComfortConfig(**data.get("comfort", {})),
            heating_system=HeatingSystemConfig(**data.get("heating_system", {})),
            learned=LearnedParameters(**data.get("learned", {}))
        )

    def save(self) -> None:
        """Save the profile back to JSON file."""
        filepath = os.path.join(self._profiles_dir, f"{self.customer_id}.json")

        data = {
            "schema_version": self.schema_version,
            "customer_id": self.customer_id,
            "friendly_name": self.friendly_name,
            "building": asdict(self.building),
            "comfort": asdict(self.comfort),
            "heating_system": asdict(self.heating_system),
            "learned": asdict(self.learned)
        }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        self._logger.info(f"Saved customer profile: {self.customer_id}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to dictionary (for GUI/API)."""
        return {
            "schema_version": self.schema_version,
            "customer_id": self.customer_id,
            "friendly_name": self.friendly_name,
            "building": asdict(self.building),
            "comfort": asdict(self.comfort),
            "heating_system": asdict(self.heating_system),
            "learned": asdict(self.learned)
        }

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
            "last_updated": learned.updated_at
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

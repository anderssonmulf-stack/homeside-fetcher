#!/usr/bin/env python3
"""
Seq Logger Module
Centralized structured logging to Seq with automatic client_id tagging.

Usage:
    from seq_logger import SeqLogger

    # Initialize once at startup
    seq = SeqLogger(client_id="38/Account/HEM_FJV_149/...")

    # Log events with automatic client_id
    seq.log("Data collected", level='Information', properties={'temp': 22.5})

    # Log consolidated data collection event
    seq.log_data_collection(iteration=1, heating_data={...}, forecast={...}, ...)
"""

import os
import requests
from datetime import datetime, timezone
from typing import Dict, Optional, Any


class SeqLogger:
    """
    Centralized Seq logger with automatic client_id tagging.

    All log events automatically include:
    - client_id: House identifier for filtering multi-site deployments
    - friendly_name: Human-readable site name
    - Application: 'HomeSide'
    - Component: Configurable per-module component name
    """

    # Display name sources
    DISPLAY_FRIENDLY_NAME = 'friendly_name'
    DISPLAY_CLIENT_ID = 'client_id'
    DISPLAY_USERNAME = 'username'

    def __init__(
        self,
        client_id: str = None,
        friendly_name: str = None,
        username: str = None,
        seq_url: str = None,
        seq_api_key: str = None,
        component: str = 'Fetcher',
        display_name_source: str = 'friendly_name'
    ):
        """
        Initialize Seq logger.

        Args:
            client_id: House/client identifier (for multi-site filtering)
            friendly_name: Human-readable site name (e.g., "Daggis8")
            username: HomeSide username
            seq_url: Seq server URL (defaults to SEQ_URL env var)
            seq_api_key: Seq API key (defaults to SEQ_API_KEY env var)
            component: Component name for log categorization
            display_name_source: Which name to show in log messages:
                'friendly_name' (default), 'client_id', or 'username'
        """
        self.client_id = client_id or 'unknown'
        self.friendly_name = friendly_name
        self.username = username
        self.seq_url = seq_url or os.getenv('SEQ_URL')
        self.seq_api_key = seq_api_key or os.getenv('SEQ_API_KEY')
        self.component = component
        self.display_name_source = display_name_source

        # Extract short client_id for display (last part of path)
        if self.client_id and '/' in self.client_id:
            self.client_id_short = self.client_id.split('/')[-1]
        else:
            self.client_id_short = self.client_id

        # Set display name based on preference
        self._update_display_name()

    def _update_display_name(self):
        """Update the display name based on current settings."""
        if self.display_name_source == self.DISPLAY_FRIENDLY_NAME and self.friendly_name:
            self.display_name = self.friendly_name
        elif self.display_name_source == self.DISPLAY_USERNAME and self.username:
            self.display_name = self.username
        elif self.display_name_source == self.DISPLAY_CLIENT_ID:
            self.display_name = self.client_id_short
        else:
            # Fallback: use first available
            self.display_name = self.friendly_name or self.client_id_short or self.username or 'unknown'

    @property
    def enabled(self) -> bool:
        """Check if Seq logging is configured."""
        return bool(self.seq_url)

    def set_client_id(self, client_id: str):
        """Update client_id (e.g., after auto-discovery)."""
        self.client_id = client_id
        if client_id and '/' in client_id:
            self.client_id_short = client_id.split('/')[-1]
        else:
            self.client_id_short = client_id
        self._update_display_name()

    def set_friendly_name(self, friendly_name: str):
        """Update friendly name."""
        self.friendly_name = friendly_name
        self._update_display_name()

    def set_username(self, username: str):
        """Update username."""
        self.username = username
        self._update_display_name()

    def set_display_source(self, source: str):
        """Change which identifier is shown in log messages."""
        self.display_name_source = source
        self._update_display_name()

    def log(
        self,
        message: str,
        level: str = 'Information',
        properties: Dict[str, Any] = None
    ) -> bool:
        """
        Send a log event to Seq.

        Args:
            message: Log message template
            level: Log level (Debug, Information, Warning, Error)
            properties: Additional structured properties

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.seq_url:
            return False

        # Build properties with defaults
        props = {
            'Application': 'HomeSide',
            'Component': self.component,
            'ClientId': self.client_id,
            'ClientIdShort': self.client_id_short,
            'FriendlyName': self.friendly_name or '',
            'Username': self.username or '',
            'DisplayName': self.display_name,
        }

        # Merge custom properties
        if properties:
            props.update(properties)

        # Build Seq event
        event = {
            'Timestamp': datetime.now(timezone.utc).isoformat(),
            'Level': level,
            'MessageTemplate': message,
            'Properties': props
        }

        payload = {'Events': [event]}

        try:
            # Build Seq raw events URL
            url = self.seq_url.rstrip('/')
            if '/api' in url:
                url = url.replace('/api', '')
            url = f"{url}/api/events/raw"

            headers = {'Content-Type': 'application/json'}
            if self.seq_api_key:
                headers['X-Seq-ApiKey'] = self.seq_api_key

            response = requests.post(url, json=payload, headers=headers, timeout=5)
            response.raise_for_status()
            return True

        except Exception:
            # Silently fail - don't break execution if Seq is unavailable
            return False

    def log_data_collection(
        self,
        iteration: int,
        heating_data: Dict,
        forecast: Dict = None,
        recommendation: Dict = None,
        thermal: Dict = None,
        curve_recommendation: Dict = None,
        heat_curve_enabled: bool = False,
        curve_adjustment_active: bool = False,
        session_info: Dict = None
    ) -> bool:
        """
        Log a consolidated data collection event.

        Combines heating data, weather forecast, recommendations, and thermal
        analysis into a single structured log event.

        Args:
            iteration: Collection iteration number
            heating_data: Extracted heating system values
            forecast: Weather forecast trend data
            recommendation: Heating recommendation
            thermal: Thermal coefficient data
            curve_recommendation: Heat curve controller recommendation
            heat_curve_enabled: Whether heat curve control is active
            curve_adjustment_active: Whether a curve adjustment is in progress
            session_info: Session token info (last8, source, updated_at)
        """
        props = {
            'EventType': 'DataCollected',
            'Iteration': iteration,
        }

        # Session info
        if session_info:
            props['SessionTokenLast8'] = session_info.get('last8', 'N/A')
            props['SessionTokenSource'] = session_info.get('source', 'unknown')
            props['SessionTokenUpdatedAt'] = session_info.get('updated_at')

        # Heating data (convert snake_case to PascalCase)
        if heating_data:
            props['VariableCount'] = len(heating_data) - 1  # Exclude timestamp
            for key, value in heating_data.items():
                if key != 'timestamp':
                    pascal_key = ''.join(word.capitalize() for word in key.split('_'))
                    if isinstance(value, (int, float)):
                        props[pascal_key] = round(float(value), 2)
                    else:
                        props[pascal_key] = value

        # Weather forecast
        if forecast:
            props['ForecastTrend'] = forecast.get('trend')
            props['ForecastTrendSymbol'] = forecast.get('trend_symbol')
            props['ForecastChange'] = round(float(forecast.get('change', 0)), 2)
            props['ForecastCurrentTemp'] = round(float(forecast.get('current_temp', 0)), 2)
            props['ForecastAvgTemp'] = round(float(forecast.get('avg_temp', 0)), 2)
            props['ForecastCloudCondition'] = forecast.get('cloud_condition')
            if forecast.get('avg_cloud_cover') is not None:
                props['ForecastCloudCover'] = round(float(forecast['avg_cloud_cover']), 2)

        # Heating recommendation
        if recommendation:
            props['HeatingAction'] = 'reduce' if recommendation.get('reduce_heating') else 'maintain'
            props['HeatingConfidence'] = round(float(recommendation.get('confidence', 0)), 2)
            props['HeatingReason'] = recommendation.get('reason')
            props['SolarFactor'] = recommendation.get('solar_factor', 'unknown')

        # Thermal coefficient
        if thermal:
            props['ThermalCoefficient'] = round(float(thermal.get('coefficient', 0)), 4)
            props['ThermalConfidence'] = round(float(thermal.get('confidence', 0)), 2)
            props['ThermalDataPoints'] = thermal.get('data_points', 0)

        # Heat curve recommendation
        if curve_recommendation:
            props['CurveReduce'] = curve_recommendation.get('reduce', False)
            props['CurveDelta'] = round(float(curve_recommendation.get('delta', 0)), 2)
            props['CurveDuration'] = round(float(curve_recommendation.get('duration_hours', 0)), 1)
            props['CurveConfidence'] = round(float(curve_recommendation.get('confidence', 0)), 2)
            props['CurveReason'] = curve_recommendation.get('reason')
            props['CurveAffectedPoints'] = ','.join(map(str, curve_recommendation.get('affected_points', [])))
            props['CurveModeActive'] = heat_curve_enabled
            props['CurveAdjustmentActive'] = curve_adjustment_active

        # Build concise message
        msg_parts = [f"#{iteration}"]

        if heating_data and 'room_temperature' in heating_data:
            msg_parts.append(f"🏠{heating_data['room_temperature']:.1f}°C")

        if heating_data and 'outdoor_temperature' in heating_data:
            msg_parts.append(f"🌡️{heating_data['outdoor_temperature']:.1f}°C")

        if forecast:
            msg_parts.append(f"{forecast.get('trend_symbol', '→')}{forecast.get('change', 0):+.1f}°C")

        if recommendation:
            if recommendation.get('reduce_heating'):
                msg_parts.append(f"🔽{recommendation.get('confidence', 0):.0%}")
            else:
                msg_parts.append("➡️")

        if curve_recommendation and curve_recommendation.get('reduce'):
            mode_icon = "📉" if heat_curve_enabled else "📊"
            msg_parts.append(f"{mode_icon}{curve_recommendation.get('delta', 0):+.1f}°C")

        message = f"[{self.display_name}] {' | '.join(msg_parts)}"

        return self.log(message, level='Information', properties=props)

    def log_token_refresh(self, success: bool, method: str = 'API', username: str = None):
        """Log a token refresh event."""
        if success:
            self.log(
                f"[{self.display_name}] Session token refreshed via {method}",
                level='Information',
                properties={
                    'EventType': 'TokenRefreshed',
                    'Method': method,
                    'Username': username
                }
            )
        else:
            self.log(
                f"[{self.display_name}] Failed to refresh session token",
                level='Error',
                properties={
                    'EventType': 'TokenRefreshFailed',
                    'Username': username
                }
            )

    def log_error(self, message: str, error: Exception = None, properties: Dict = None):
        """Log an error event."""
        props = properties or {}
        if error:
            props['ErrorType'] = type(error).__name__
            props['ErrorMessage'] = str(error)

        self.log(f"[{self.display_name}] {message}", level='Error', properties=props)

    def log_warning(self, message: str, properties: Dict = None):
        """Log a warning event."""
        self.log(f"[{self.display_name}] {message}", level='Warning', properties=properties)


# Global instance for simple usage (initialized without client_id)
_default_logger: Optional[SeqLogger] = None


def get_logger() -> SeqLogger:
    """Get the default SeqLogger instance."""
    global _default_logger
    if _default_logger is None:
        _default_logger = SeqLogger()
    return _default_logger


def init_logger(client_id: str = None, **kwargs) -> SeqLogger:
    """Initialize and return the default SeqLogger instance."""
    global _default_logger
    _default_logger = SeqLogger(client_id=client_id, **kwargs)
    return _default_logger

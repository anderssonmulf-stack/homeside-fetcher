"""
Audit Logging for Svenskeb Settings GUI
Logs all changes to Seq for tracking and accountability.
"""

import os
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional


class AuditLogger:
    """Logs audit events to Seq structured logging server"""

    def __init__(self):
        self.seq_url = os.environ.get('SEQ_URL', 'http://seq:5341')
        self.seq_api_key = os.environ.get('SEQ_API_KEY', '')
        self.app_name = 'SvenskebGUI'

    def log(self, event_type: str, user_id: str, properties: Dict = None):
        """
        Log an audit event to Seq

        Args:
            event_type: Type of event (UserLogin, SettingChanged, UserApproved, etc.)
            user_id: Username of the user who performed the action
            properties: Additional properties to log
        """
        properties = properties or {}

        event = {
            '@t': datetime.utcnow().isoformat() + 'Z',
            '@mt': f'[{self.app_name}] {event_type}: {{UserId}} - {{Details}}',
            '@l': 'Information',
            'EventType': event_type,
            'UserId': user_id,
            'Details': self._format_details(event_type, properties),
            'Application': self.app_name,
            **properties
        }

        self._send_to_seq(event)

    def _format_details(self, event_type: str, properties: Dict) -> str:
        """Format a human-readable details string"""
        if event_type == 'UserLogin':
            return f"logged in from {properties.get('ip', 'unknown')}"
        elif event_type == 'LoginFailed':
            return f"failed login attempt from {properties.get('ip', 'unknown')}"
        elif event_type == 'UserLogout':
            return "logged out"
        elif event_type == 'UserRegistered':
            return f"registered with email {properties.get('email', 'unknown')}"
        elif event_type == 'UserApproved':
            return f"approved user {properties.get('approved_user', 'unknown')}"
        elif event_type == 'UserRejected':
            return f"rejected user {properties.get('rejected_user', 'unknown')}"
        elif event_type == 'UserModified':
            return f"modified user {properties.get('modified_user', 'unknown')}"
        elif event_type == 'UserCreatedByAdmin':
            return f"created user {properties.get('created_user', 'unknown')} with role {properties.get('role', 'unknown')}"
        elif event_type == 'SettingChanged':
            setting = properties.get('setting', 'unknown')
            old_val = properties.get('old_value', '?')
            new_val = properties.get('new_value', '?')
            house = properties.get('house_id', 'unknown')
            return f"changed {setting} from {old_val} to {new_val} for {house}"
        elif event_type == 'ActionCompleted':
            action = properties.get('action', 'unknown')
            action_type = properties.get('action_type', 'unknown')
            return f"completed {action_type} action: {action}"
        else:
            return json.dumps(properties)

    def _send_to_seq(self, event: Dict):
        """Send event to Seq server"""
        try:
            headers = {
                'Content-Type': 'application/vnd.serilog.clef'
            }
            if self.seq_api_key:
                headers['X-Seq-ApiKey'] = self.seq_api_key

            # Seq expects CLEF format (one JSON per line)
            response = requests.post(
                f"{self.seq_url}/api/events/raw",
                data=json.dumps(event),
                headers=headers,
                timeout=5
            )

            if response.status_code not in (200, 201):
                print(f"Warning: Failed to log to Seq: {response.status_code}")

        except Exception as e:
            # Don't let logging failures break the app
            print(f"Warning: Could not send to Seq: {e}")

    def get_recent_changes(self, houses: Optional[List[str]] = None, limit: int = 50) -> List[Dict]:
        """
        Query Seq for recent setting changes

        Args:
            houses: List of house IDs to filter by, or None for all
            limit: Maximum number of events to return
        """
        try:
            # Build Seq query
            filter_parts = ["EventType = 'SettingChanged'"]
            if houses:
                house_filter = " or ".join([f"house_id = '{h}'" for h in houses])
                filter_parts.append(f"({house_filter})")

            query = " and ".join(filter_parts)

            headers = {}
            if self.seq_api_key:
                headers['X-Seq-ApiKey'] = self.seq_api_key

            # Query Seq events API
            response = requests.get(
                f"{self.seq_url}/api/events",
                params={
                    'filter': query,
                    'count': limit
                },
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                events = response.json()
                return self._format_events(events)
            else:
                return []

        except Exception as e:
            print(f"Warning: Could not query Seq: {e}")
            return []

    def get_house_changes(self, house_id: str, limit: int = 20) -> List[Dict]:
        """Get changes for a specific house"""
        return self.get_recent_changes(houses=[house_id], limit=limit)

    def _format_events(self, events: List) -> List[Dict]:
        """Format Seq events for display"""
        formatted = []
        for event in events:
            props = event.get('Properties', {})
            formatted.append({
                'timestamp': event.get('Timestamp', ''),
                'user_id': props.get('UserId', 'system'),
                'event_type': props.get('EventType', 'unknown'),
                'details': props.get('Details', ''),
                'house_id': props.get('house_id', ''),
                'setting': props.get('setting', ''),
                'old_value': props.get('old_value'),
                'new_value': props.get('new_value')
            })
        return formatted

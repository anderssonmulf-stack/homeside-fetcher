#!/usr/bin/env python3
"""
Arrigo BMS API Client (Direct)

Client for commercial buildings that connect directly to Arrigo BMS servers,
bypassing the HomeSide portal. Used for buildings like TE236_HEM_Kontor.

Auth flow (simpler than residential):
    Direct Arrigo login → JWT Bearer token → GraphQL API

Residential (via HomeSide) auth flow for comparison:
    HomeSide auth → session token → BMS token → Bearer → Arrigo GraphQL

Usage:
    # Explore - list all signals
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --list-signals

    # Explore - show folder structure
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --list-folders

    # Explore - show alarms
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --list-alarms

    # Fetch historical data (dry run)
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --days 7 --dry-run

    # Test connection
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --test
"""

import os
import sys
import json
import re
import time
import logging
import argparse
import requests
import base64
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple


# ── Auto-categorization rules for signal names ──────────────────────
# Maps signal name patterns to categories.
# Order matters: first match wins.

CATEGORY_RULES = [
    # District heating / primary side
    (r'Pages\(\d+\)\.(TempForw|TempRet|Power|Energy|Flow|Volume)', 'district_heating'),
    (r'VMM\d+', 'energy_metering'),
    (r'KMM\d+', 'cooling_metering'),
    (r'ELM\d+', 'electricity_metering'),
    (r'VM\d+_Utgång', 'water_metering'),

    # Heating system
    (r'VS\d+.*GT_TILL', 'heating'),
    (r'VS\d+.*GT_RETUR', 'heating'),
    (r'VS\d+.*SV', 'heating'),
    (r'PID_VS\d+', 'heating'),

    # Hot water
    (r'VV\d+|VVC\d+', 'hot_water'),

    # Ventilation / AHU
    (r'LB\d+_SA_', 'ventilation'),
    (r'LB\d+_EA_', 'ventilation'),
    (r'LB\d+_RHX_', 'ventilation'),
    (r'LB\d+_Reheat', 'ventilation'),
    (r'LB\d+_Cool', 'ventilation'),
    (r'LB\d+_Outdoor', 'ventilation'),

    # Cooling system
    (r'KB\d+|KM\d+', 'cooling'),
    (r'DAGGP|ABSOLUTFUKT', 'cooling'),

    # Climate / outdoor
    (r'GT_UTE', 'climate'),

    # Pressure
    (r'GP_EXP', 'pressure'),

    # Efficiency / misc
    (r'Effekt_Procent', 'efficiency'),
]


def categorize_signal(signal_name: str) -> str:
    """Auto-categorize a signal based on its name pattern."""
    # Extract the variable part after the device prefix
    # e.g., "HEM_Kontor1.GT_UTE" → "GT_UTE"
    if '.' in signal_name:
        var_part = signal_name.split('.', 1)[1]
    else:
        var_part = signal_name

    for pattern, category in CATEGORY_RULES:
        if re.search(pattern, var_part):
            return category

    return 'other'


class ArrigoAPI:
    """
    Direct Arrigo BMS API client for commercial buildings.

    Connects directly to an Arrigo server without going through the
    HomeSide portal. Authentication is via username/password → JWT.
    Data access is via GraphQL.
    """

    def __init__(self, host: str, username: str, password: str,
                 logger=None, verbose: bool = False):
        """
        Args:
            host: Arrigo server hostname (e.g., "exodrift05.systeminstallation.se")
            username: Arrigo username (e.g., "Ulf Andersson")
            password: Arrigo password
            logger: Optional Python logger
            verbose: Print debug output
        """
        self.host = host
        self.base_url = f"https://{host}"
        self.arrigo_url = f"{self.base_url}/Arrigo"
        self.api_url = f"{self.arrigo_url}/api"
        self.graphql_url = f"{self.api_url}/graphql"

        self.username = username
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        self.verbose = verbose

        # Auth tokens
        self.auth_token = None      # JWT from login
        self.refresh_token = None   # For token renewal
        self.token_expires_at = None
        self.account = None         # e.g., "HEMLocal"

        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        # Building structure (discovered via folder queries)
        self.account_id = None      # Base64 encoded account ID
        self.account_name = None    # e.g., "TE236_HEM_Kontor"
        self.folders = []           # Sub-areas (FVC, TA1, etc.)

        # Signal mapping: signal_id (base64) -> signal info
        self.signal_map = {}
        # Reverse: field_name -> signal_id
        self.field_to_signal = {}

    def log(self, message: str):
        if self.verbose:
            print(f"  [DEBUG] {message}")
        if self.logger:
            self.logger.debug(message)

    # ── Authentication ───────────────────────────────────────────────

    def login(self) -> bool:
        """
        Authenticate directly with Arrigo server.

        POST /Arrigo/api/login → JWT authToken

        Returns:
            True if login successful
        """
        self.logger.info(f"Logging in to {self.host} as '{self.username}'...")

        try:
            payload = {
                "account": "",
                "username": self.username,
                "password": self.password,
                "newPassword": "",
                "remember": True,
                "currentPhase": "LoginIn",
                "configAccount": False,
                "passwordExpirePanel": False,
                "passwordRequirementsNotMet": False,
                "requestPassword": False,
                "passwordLoading": False,
            }

            response = self.session.post(
                f"{self.api_url}/login",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            self.auth_token = data.get('authToken')
            self.refresh_token = data.get('refreshToken')
            self.account = data.get('account')

            expires_in = data.get('expires_in', 10800)  # Default 3 hours
            self.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            if not self.auth_token:
                self.logger.error("No authToken in login response")
                return False

            # Set Bearer auth for all subsequent requests
            self.session.headers.update({
                "Authorization": f"Bearer {self.auth_token}"
            })

            self.logger.info(f"Logged in (account: {self.account}, expires in {expires_in}s)")
            self.log(f"Token: {self.auth_token[:30]}...")

            return True

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Login failed (HTTP {e.response.status_code})")
            self.log(f"Response: {e.response.text[:500]}")
            return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Login failed: {e}")
            return False

    def is_token_valid(self) -> bool:
        """Check if current token is still valid (with 5 min margin)."""
        if not self.auth_token or not self.token_expires_at:
            return False
        return datetime.now(timezone.utc) < (self.token_expires_at - timedelta(minutes=5))

    def ensure_auth(self) -> bool:
        """Ensure we have a valid auth token, refreshing if needed."""
        if self.is_token_valid():
            return True
        self.logger.info("Token expired or missing, re-authenticating...")
        return self.login()

    # ── GraphQL helpers ──────────────────────────────────────────────

    def _graphql(self, query: str, variables: dict = None, timeout: int = 60) -> Optional[dict]:
        """
        Execute a GraphQL query.

        Returns:
            Parsed response data, or None on error
        """
        if not self.ensure_auth():
            return None

        payload = {'query': query}
        if variables:
            payload['variables'] = variables

        try:
            response = self.session.post(
                self.graphql_url,
                json=payload,
                timeout=timeout
            )

            if response.status_code == 401:
                self.logger.info("Token expired, re-authenticating...")
                if self.login():
                    response = self.session.post(
                        self.graphql_url,
                        json=payload,
                        timeout=timeout
                    )
                else:
                    return None

            if response.status_code != 200:
                self.logger.error(f"GraphQL returned {response.status_code}")
                self.log(f"Response: {response.text[:500]}")
                return None

            result = response.json()

            if 'errors' in result:
                self.logger.error(f"GraphQL errors: {result['errors']}")
                return None

            return result.get('data')

        except requests.exceptions.Timeout:
            self.logger.error("GraphQL query timed out")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"GraphQL request failed: {e}")
            return None

    # ── Folder / Building Structure ──────────────────────────────────

    def discover_folders(self) -> bool:
        """
        Discover the building's folder hierarchy.

        The root folder is the Account (e.g., TE236_HEM_Kontor),
        with UserArea children for each subsystem (FVC, TA1-TA5, etc.).

        Returns:
            True if folders discovered
        """
        print("\nDiscovering building structure...")

        query = '''
        query FolderQuery($id: ID) {
            folder(id: $id) {
                name
                id
                title
                icon
                __typename
                folders {
                    totalCount
                    items {
                        name
                        id
                        title
                        icon
                        __typename
                    }
                }
            }
        }
        '''

        data = self._graphql(query, {'id': None})
        if not data or 'folder' not in data:
            print("  ERROR: Could not discover folders")
            return False

        root = data['folder']
        self.account_id = root.get('id')
        self.account_name = root.get('name')

        print(f"  Account: {root.get('title', root.get('name'))} ({root.get('__typename')})")
        self.log(f"Account ID: {self.account_id}")

        children = root.get('folders', {}).get('items', [])
        self.folders = children

        if children:
            print(f"  Sub-areas ({len(children)}):")
            for folder in children:
                print(f"    - {folder.get('name')}: {folder.get('title', '')}")

        return True

    # ── Analog Signals ───────────────────────────────────────────────

    def discover_signals(self, folder_id: str = None) -> bool:
        """
        Discover available analog signals.

        Args:
            folder_id: Optional folder ID to scope the query. None = all signals.

        Returns:
            True if signals discovered
        """
        self.logger.debug("Discovering analog signals...")

        query = '''
        {
            analogs(first: 500) {
                totalCount
                items {
                    id
                    name
                    value
                    unit
                }
            }
        }
        '''

        data = self._graphql(query, timeout=120)
        if not data or 'analogs' not in data:
            self.logger.error("Could not discover analog signals")
            return False

        items = data['analogs'].get('items', [])
        total = data['analogs'].get('totalCount', 0)

        self.signal_map = {}
        self.field_to_signal = {}

        for item in items:
            signal_id = item['id']
            signal_name = item.get('name', '')
            value = item.get('value')
            unit = item.get('unit', '')

            self.signal_map[signal_id] = {
                'name': signal_name,
                'unit': unit,
                'current_value': value,
            }

            # Decode base64 ID for readable reference
            try:
                decoded_id = base64.b64decode(signal_id).decode('utf-8')
            except Exception:
                decoded_id = signal_id

            self.log(f"  Signal: {signal_name} = {value} {unit} (id: {decoded_id})")

        self.logger.debug(f"Discovered {total} analog signals, mapped {len(self.signal_map)}")
        return True

    def discover_digital_signals(self) -> dict:
        """
        Discover available digital (on/off) signals.

        Returns:
            Dict of signal_id -> signal info, or empty dict on error
        """
        self.logger.debug("Discovering digital signals...")

        query = '''
        {
            digitals(first: 500) {
                totalCount
                items {
                    id
                    name
                    state
                    signalText
                    stateTexts
                    signalType
                }
            }
        }
        '''

        data = self._graphql(query, timeout=120)
        if not data or 'digitals' not in data:
            self.logger.error("Could not discover digital signals")
            return {}

        items = data['digitals'].get('items', [])
        total = data['digitals'].get('totalCount', 0)

        self.logger.debug(f"Discovered {total} digital signals ({len(items)} returned)")

        digital_map = {}
        for item in items:
            digital_map[item['id']] = {
                'name': item.get('name', ''),
                'state': item.get('state'),
                'signal_text': item.get('signalText', ''),
                'state_texts': item.get('stateTexts', ''),
                'signal_type': item.get('signalType', ''),
            }

        return digital_map

    # ── Alarms ───────────────────────────────────────────────────────

    def get_alarms(self, status: List[str] = None, first: int = 50) -> List[dict]:
        """
        Fetch alarms, optionally filtered by status.

        Args:
            status: List of statuses to filter: ALARMED, RETURNED, ACKNOWLEDGED, BLOCKED
            first: Max number of alarms to return

        Returns:
            List of alarm dicts
        """
        query = '''
        query AlarmsQuery(
            $status: [AlarmStatus],
            $first: Int,
            $orderBy: [AlarmOrderBy]
        ) {
            alarms(
                first: $first,
                filter: { status: $status },
                orderBy: $orderBy
            ) {
                edges {
                    node {
                        id
                        alarmText
                        status
                        name
                        folder { id, title, name }
                        alarmTime
                        eventTime
                        priority
                        noOfAlarms
                        description
                        actionText
                    }
                }
            }
        }
        '''

        variables = {
            'first': first,
            'orderBy': [{'fieldName': 'eventTime', 'sortDirection': 'descending'}],
        }
        if status:
            variables['status'] = status

        data = self._graphql(query, variables)
        if not data or 'alarms' not in data:
            return []

        alarms = []
        for edge in data['alarms'].get('edges', []):
            node = edge.get('node', {})
            alarms.append(node)

        return alarms

    # ── Historical Data ──────────────────────────────────────────────

    def fetch_analog_history(
        self,
        signal_ids: List[str],
        start_time: datetime,
        end_time: datetime,
        resolution_seconds: int = 3600,
        max_points: int = 50000
    ) -> List[dict]:
        """
        Fetch historical analog data via analogsHistory query.

        Args:
            signal_ids: List of base64-encoded signal IDs
            start_time: Start of time range (UTC)
            end_time: End of time range (UTC)
            resolution_seconds: Data resolution (default: 3600 = 1 hour)
            max_points: Max data points to return

        Returns:
            List of {signalId, time, value, reliability, timeLength} dicts
        """
        query = '''
        query GetHistory($filter: AnalogEventFilter) {
            analogsHistory(first: $first, filter: $filter) {
                totalCount
                items {
                    signalId
                    time
                    value
                    reliability
                    timeLength
                }
            }
        }
        '''.replace('$first', str(max_points))

        variables = {
            'filter': {
                'signalId': signal_ids,
                'ranges': [{
                    'from': start_time.isoformat(),
                    'to': end_time.isoformat(),
                }],
                'timeLength': resolution_seconds,
            }
        }

        self.log(f"Fetching history: {len(signal_ids)} signals, "
                 f"{start_time.date()} to {end_time.date()}, "
                 f"resolution {resolution_seconds}s")

        data = self._graphql(query, variables, timeout=300)
        if not data or 'analogsHistory' not in data:
            return []

        items = data['analogsHistory'].get('items', [])
        total = data['analogsHistory'].get('totalCount', 0)

        self.logger.debug(f"Received {len(items)} history data points (total: {total})")
        return items

    def fetch_digital_history(
        self,
        signal_ids: List[str],
        start_time: datetime,
        end_time: datetime,
        resolution_seconds: int = 3600,
        max_points: int = 50000
    ) -> List[dict]:
        """
        Fetch historical digital data via digitalsHistory query.

        Args:
            signal_ids: List of base64-encoded signal IDs
            start_time: Start of time range (UTC)
            end_time: End of time range (UTC)
            resolution_seconds: Data resolution
            max_points: Max data points to return

        Returns:
            List of history items
        """
        query = '''
        query GetDigitalHistory($filter: DigitalEventFilter) {
            digitalsHistory(first: $first, filter: $filter) {
                totalCount
                items {
                    signalId
                    time
                    value
                    reliability
                    timeLength
                }
            }
        }
        '''.replace('$first', str(max_points))

        variables = {
            'filter': {
                'signalId': signal_ids,
                'ranges': [{
                    'from': start_time.isoformat(),
                    'to': end_time.isoformat(),
                }],
                'timeLength': resolution_seconds,
            }
        }

        data = self._graphql(query, variables, timeout=300)
        if not data or 'digitalsHistory' not in data:
            return []

        return data['digitalsHistory'].get('items', [])

    # ── Schema Introspection ─────────────────────────────────────────

    def introspect_schema(self) -> Optional[dict]:
        """
        Query GraphQL schema to discover available query fields.

        Returns:
            Schema info dict or None
        """
        query = '''
        {
            __type(name: "Root") {
                fields {
                    name
                    description
                    deprecationReason
                }
            }
        }
        '''

        data = self._graphql(query)
        if data and '__type' in data:
            return data['__type']
        return None

    # ── Convenience / High-Level Methods ─────────────────────────────

    def get_all_current_values(self) -> Dict[str, dict]:
        """
        Get current values for all analog signals.

        Returns:
            Dict of signal_name -> {value, unit, signal_id}
        """
        if not self.signal_map:
            self.discover_signals()

        result = {}
        for signal_id, info in self.signal_map.items():
            name = info['name']
            result[name] = {
                'value': info['current_value'],
                'unit': info['unit'],
                'signal_id': signal_id,
            }

        return result

    # ── Building Config Discovery ────────────────────────────────────

    def discover_building(self) -> dict:
        """
        Full discovery: folders, analog signals, digital signals, alarms.

        Returns a complete building inventory dict suitable for saving
        as a building config file.
        """
        print("\n" + "=" * 60)
        print(f"Discovering building: {self.host}")
        print("=" * 60)

        # Folders
        self.discover_folders()

        # Analog signals
        self.discover_signals()

        # Digital signals
        digital_map = self.discover_digital_signals()

        # Alarms (current snapshot)
        alarms = self.get_alarms(first=100)

        # Build config structure
        config = {
            "schema_version": 1,
            "building_id": self.account_name or self.host.split('.')[0],
            "friendly_name": "",
            "building_type": "commercial",
            "connection": {
                "system": "arrigo",
                "host": self.host,
                "account": self.account,
            },
            "discovered_at": datetime.now(timezone.utc).isoformat(),
            "sub_areas": [],
            "analog_signals": {},
            "digital_signals": {},
            "alarm_monitoring": {
                "enabled": False,
                "priorities": ["A", "B"],
                "poll_interval_minutes": 15,
            },
        }

        # Sub-areas
        for folder in self.folders:
            config["sub_areas"].append({
                "id": folder.get('id'),
                "name": folder.get('name'),
                "title": folder.get('title', ''),
                "type": folder.get('__typename', ''),
            })

        # Analog signals - all discovered, none fetched by default
        for signal_id, info in sorted(self.signal_map.items(), key=lambda x: x[1]['name']):
            name = info['name']
            category = categorize_signal(name)

            config["analog_signals"][name] = {
                "signal_id": signal_id,
                "unit": info.get('unit', ''),
                "category": category,
                "field_name": None,
                "fetch": False,
                "discovered_value": info.get('current_value'),
            }

        # Digital signals
        for signal_id, info in sorted(digital_map.items(), key=lambda x: x[1]['name']):
            name = info['name']
            category = categorize_signal(name)

            config["digital_signals"][name] = {
                "signal_id": signal_id,
                "category": category,
                "field_name": None,
                "fetch": False,
                "discovered_state": info.get('state'),
                "signal_text": info.get('signal_text', ''),
                "state_texts": info.get('state_texts', ''),
            }

        # Summary
        categories = {}
        for sig in config["analog_signals"].values():
            cat = sig["category"]
            categories[cat] = categories.get(cat, 0) + 1
        for sig in config["digital_signals"].values():
            cat = sig["category"]
            categories[cat] = categories.get(cat, 0) + 1

        print(f"\n{'=' * 60}")
        print(f"Discovery complete: {config['building_id']}")
        print(f"  Sub-areas: {len(config['sub_areas'])}")
        print(f"  Analog signals: {len(config['analog_signals'])}")
        print(f"  Digital signals: {len(config['digital_signals'])}")
        print(f"  Active alarms: {len(alarms)}")
        print(f"\n  Signals by category:")
        for cat, count in sorted(categories.items()):
            print(f"    {cat:25s} {count}")
        print(f"{'=' * 60}")

        return config

    def test_connection(self) -> bool:
        """
        Test the full connection: login → folder discovery → signal count.

        Returns:
            True if everything works
        """
        print("=" * 60)
        print(f"Testing connection to {self.host}")
        print("=" * 60)

        # Step 1: Login
        if not self.login():
            print("\nFAILED: Could not authenticate")
            return False

        # Step 2: Schema
        print("\nQuerying schema...")
        schema = self.introspect_schema()
        if schema:
            fields = schema.get('fields', [])
            print(f"  Available query fields: {len(fields)}")
            for f in fields:
                print(f"    - {f['name']}: {f.get('description', '')}")

        # Step 3: Folders
        self.discover_folders()

        # Step 4: Signals
        self.discover_signals()

        # Step 5: Summary
        print(f"\n{'=' * 60}")
        print(f"Connection test PASSED")
        print(f"  Server: {self.host}")
        print(f"  Account: {self.account}")
        print(f"  Building: {self.account_name}")
        print(f"  Sub-areas: {len(self.folders)}")
        print(f"  Analog signals: {len(self.signal_map)}")
        print(f"{'=' * 60}")

        return True


# ── Building Config File Helpers ──────────────────────────────────────

BUILDINGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'buildings')


def get_building_config_path(building_id: str) -> str:
    """Get the file path for a building config."""
    return os.path.join(BUILDINGS_DIR, f"{building_id}.json")


def load_building_config(building_id: str) -> Optional[dict]:
    """
    Load a building config from buildings/<building_id>.json.

    Returns:
        Config dict or None if not found
    """
    path = get_building_config_path(building_id)
    if not os.path.exists(path):
        return None

    with open(path, 'r') as f:
        return json.load(f)


def save_building_config(config: dict, path: str = None) -> str:
    """
    Save a building config to disk.

    Args:
        config: Building config dict
        path: Optional override path. Default: buildings/<building_id>.json

    Returns:
        Path where config was saved
    """
    os.makedirs(BUILDINGS_DIR, exist_ok=True)

    if not path:
        building_id = config.get('building_id', 'unknown')
        path = get_building_config_path(building_id)

    with open(path, 'w') as f:
        json.dump(config, f, indent=2, ensure_ascii=False, default=str)

    return path


def get_fetch_signals(config: dict) -> Tuple[dict, dict]:
    """
    Extract the signals marked for fetching from a building config.

    Returns:
        Tuple of (analog_fetch_map, digital_fetch_map)
        Each is dict of signal_name -> {signal_id, field_name, unit, category}
    """
    analog_fetch = {}
    for name, sig in config.get('analog_signals', {}).items():
        if sig.get('fetch'):
            entry = {
                'signal_id': sig['signal_id'],
                'field_name': sig.get('field_name') or name,
                'unit': sig.get('unit', ''),
                'category': sig.get('category', 'other'),
            }
            if 'min_value' in sig:
                entry['min_value'] = sig['min_value']
            analog_fetch[name] = entry

    digital_fetch = {}
    for name, sig in config.get('digital_signals', {}).items():
        if sig.get('fetch'):
            digital_fetch[name] = {
                'signal_id': sig['signal_id'],
                'field_name': sig.get('field_name') or name,
                'unit': sig.get('unit', ''),
                'category': sig.get('category', 'other'),
            }

    return analog_fetch, digital_fetch


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Arrigo BMS API client for commercial buildings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Test connection
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --test

    # List all analog signals
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --list-signals

    # List building folders
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --list-folders

    # List active alarms
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --list-alarms

    # Fetch 7 days of history for all signals
    python3 arrigo_api.py --host exodrift05.systeminstallation.se \\
        --username "Ulf Andersson" --password "xxx" --days 7
        """
    )

    # Connection
    parser.add_argument('--host', required=True,
                        help='Arrigo server hostname (e.g., exodrift05.systeminstallation.se)')
    parser.add_argument('--username', required=True, help='Arrigo username')
    parser.add_argument('--password', required=True, help='Arrigo password')

    # Modes
    parser.add_argument('--discover', action='store_true',
                        help='Discover all signals and generate building config file')
    parser.add_argument('--test', action='store_true', help='Test connection and exit')
    parser.add_argument('--list-signals', action='store_true', help='List all analog signals')
    parser.add_argument('--list-digital', action='store_true', help='List all digital signals')
    parser.add_argument('--list-folders', action='store_true', help='List building folder structure')
    parser.add_argument('--list-alarms', action='store_true', help='List current alarms')

    # History
    parser.add_argument('--days', type=int, help='Fetch N days of analog history')
    parser.add_argument('--resolution', type=int, default=3600,
                        help='History resolution in seconds (default: 3600)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be fetched')

    # Output
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose debug output')
    parser.add_argument('--json', action='store_true', help='Output as JSON')

    args = parser.parse_args()

    client = ArrigoAPI(
        host=args.host,
        username=args.username,
        password=args.password,
        verbose=args.verbose,
    )

    # ── Discover mode ─────────────────────────────────────────────
    if args.discover:
        if not client.login():
            sys.exit(1)

        config = client.discover_building()

        # Save to buildings/ directory
        path = save_building_config(config)
        print(f"\nBuilding config saved to: {path}")
        print(f"\nNext steps:")
        print(f"  1. Open {path}")
        print(f"  2. Set 'friendly_name' for the building")
        print(f"  3. For signals you want to fetch on schedule:")
        print(f"     - Set \"fetch\": true")
        print(f"     - Set \"field_name\" to a descriptive name (e.g., \"outdoor_temperature\")")
        print(f"  4. Configure alarm_monitoring if needed")
        sys.exit(0)

    # ── Test mode ────────────────────────────────────────────────
    if args.test:
        success = client.test_connection()
        sys.exit(0 if success else 1)

    # ── Authenticate ─────────────────────────────────────────────
    if not client.login():
        sys.exit(1)

    # ── List folders ─────────────────────────────────────────────
    if args.list_folders:
        client.discover_folders()
        if args.json:
            print(json.dumps(client.folders, indent=2))
        sys.exit(0)

    # ── List analog signals ──────────────────────────────────────
    if args.list_signals:
        client.discover_signals()
        print(f"\nAnalog signals ({len(client.signal_map)}):")
        for signal_id, info in sorted(client.signal_map.items(), key=lambda x: x[1]['name']):
            v = info['current_value']
            u = info['unit']
            val_str = f"{v:.2f} {u}" if isinstance(v, (int, float)) else f"{v} {u}"
            print(f"  {info['name']:50s} {val_str}")
        sys.exit(0)

    # ── List digital signals ─────────────────────────────────────
    if args.list_digital:
        digital_map = client.discover_digital_signals()
        print(f"\nDigital signals ({len(digital_map)}):")
        for signal_id, info in sorted(digital_map.items(), key=lambda x: x[1]['name']):
            state = info.get('state', '?')
            text = info.get('signal_text', '')
            print(f"  {info['name']:50s} {state:10s} {text}")
        sys.exit(0)

    # ── List alarms ──────────────────────────────────────────────
    if args.list_alarms:
        alarms = client.get_alarms()
        if not alarms:
            print("No alarms found.")
        else:
            print(f"\nAlarms ({len(alarms)}):")
            for a in alarms:
                status = a.get('status', '?')
                priority = a.get('priority', '?')
                text = a.get('alarmText', '')
                event_time = a.get('eventTime', '')
                folder = a.get('folder', {}).get('title', '')
                print(f"  [{priority}] {status:15s} {event_time[:19]}  {text}")
                if folder:
                    print(f"      Folder: {folder}")
        if args.json:
            print(json.dumps(alarms, indent=2, default=str))
        sys.exit(0)

    # ── Fetch history ────────────────────────────────────────────
    if args.days:
        client.discover_signals()

        if not client.signal_map:
            print("No signals found, cannot fetch history.")
            sys.exit(1)

        signal_ids = list(client.signal_map.keys())
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=args.days)

        print(f"\nFetching {args.days} days of history for {len(signal_ids)} signals...")
        print(f"  Range: {start_time.date()} to {end_time.date()}")
        print(f"  Resolution: {args.resolution}s")

        if args.dry_run:
            print("\n[DRY RUN] Would fetch history for:")
            for sid, info in client.signal_map.items():
                print(f"  - {info['name']}")
            sys.exit(0)

        items = client.fetch_analog_history(
            signal_ids=signal_ids,
            start_time=start_time,
            end_time=end_time,
            resolution_seconds=args.resolution,
        )

        if not items:
            print("No historical data returned.")
            sys.exit(1)

        # Group by signal for summary
        by_signal = {}
        for item in items:
            sid = item['signalId']
            name = client.signal_map.get(sid, {}).get('name', sid)
            by_signal.setdefault(name, []).append(item)

        print(f"\nData points per signal:")
        for name, points in sorted(by_signal.items()):
            values = [p['value'] for p in points if p.get('value') is not None]
            if values:
                print(f"  {name:50s} {len(values):5d} pts  "
                      f"min={min(values):.1f}  max={max(values):.1f}  "
                      f"avg={sum(values)/len(values):.1f}")
            else:
                print(f"  {name:50s} {len(points):5d} pts  (no values)")

        if args.json:
            print(json.dumps(items, indent=2, default=str))

        sys.exit(0)

    # No mode selected
    parser.print_help()


if __name__ == "__main__":
    main()

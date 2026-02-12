#!/usr/bin/env python3
"""
Historical Data Import Script

Fetches historical heating data from the Arrigo GraphQL API and imports it
to InfluxDB. This is used when onboarding new customers to bootstrap the
thermal analyzer with historical data.

Usage:
    # Dry run - see what data would be fetched
    python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90 --dry-run

    # Import to InfluxDB
    python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90

    # Import specific house (if user has multiple)
    python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90 --house-index 0
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False
    print("Warning: influxdb_client not available. Install with: pip install influxdb-client")


# Default Arrigo server for Öresundskraft district heating
# Other district heating companies may use different servers
DEFAULT_ARRIGO_HOST = "exodrift10.systeminstallation.se"

# Known district heating companies and their Arrigo servers
DISTRICT_HEATING_SERVERS = {
    "oresundskraft": "exodrift10.systeminstallation.se",
    # Add more as we onboard customers from other regions
    # "example_company": "arrigoXX.systeminstallation.se",
}


# Signal name patterns to match -> our field name
# The Arrigo API returns signal names like "HEM_FJV_Villa_34.GT_UTE"
# We match the suffix after the last dot
SIGNAL_PATTERN_MAPPING = {
    # Outdoor temperature
    "GT_UTE": "outdoor_temperature",
    "GT_UTE_MEDEL_24": "outdoor_temp_24h_avg",

    # Room temperature
    "MEAN_GT_RUM_1_Output": "room_temperature",

    # Heating system
    "AI_VS1_GT_TILL_1_Output": "supply_temp",
    "AI_VS1_GT_RETUR_1_Output": "return_temp",
    "AI_VV1_TAPP_GT41_Output": "hot_water_temp",

    # System pressure
    "AI_GP_EXP1_1_Output": "system_pressure",

    # Setpoints
    "KU_VS1_GT_TILL_1_SetPoint": "target_temp_setpoint",
    "PID_VS1_GT_TILL_1_SetP": "supply_setpoint",

    # District heating data
    "VMM1_TempForw": "dh_supply_temp",
    "VMM1_TempRet": "dh_return_temp",
    "VMM1_Power": "dh_power",
    "VMM1_Flow": "dh_flow",
    "VMM1_Energy": "dh_energy_total",
}

# Core signals needed for thermal analysis
CORE_SIGNALS = ["outdoor_temperature", "room_temperature", "supply_temp", "return_temp"]


class ArrigoHistoricalClient:
    """
    Client for fetching historical data from Arrigo BMS API.

    The Arrigo API provides GraphQL endpoints for querying historical
    time-series data from heating systems.
    """

    def __init__(self, username: str, password: str, arrigo_host: str = None, verbose: bool = False):
        """
        Initialize the Arrigo client.

        Args:
            username: HomeSide username (e.g., "FC2000232581")
            password: HomeSide password
            arrigo_host: Arrigo server hostname (default: exodrift10.systeminstallation.se)
            verbose: Enable verbose logging
        """
        self.username = username.replace(" ", "")  # Remove spaces
        self.password = password
        self.verbose = verbose

        # HomeSide portal for authentication
        self.homeside_url = "https://homeside.systeminstallation.se"

        # Arrigo BMS server
        self.arrigo_host = arrigo_host or DEFAULT_ARRIGO_HOST
        self.arrigo_url = f"https://{self.arrigo_host}/arrigo"
        self.graphql_url = f"{self.arrigo_url}/api/graphql"

        # Session and tokens
        self.session = requests.Session()
        self.session_token = None
        self.bms_token = None
        self.uid = None
        self.extend_uid = None

        # Arrigo session (separate from HomeSide)
        self.arrigo_session = requests.Session()
        self.arrigo_session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        # House info
        self.houses = []
        self.selected_house = None
        self.client_id = None

        # Signal mapping: signal_id (base64) -> field_name
        self.signal_map = {}
        # Reverse mapping: field_name -> signal_id
        self.field_to_signal = {}

    def log(self, message: str):
        """Print message if verbose mode enabled."""
        if self.verbose:
            print(f"  [DEBUG] {message}")

    def authenticate(self) -> bool:
        """
        Authenticate with HomeSide API.

        Returns:
            True if authentication successful
        """
        print(f"Authenticating as {self.username}...")

        try:
            # Step 1: Get session token via account auth
            auth_url = f"{self.homeside_url}/api/v2/authorize/account"
            payload = {
                "user": {
                    "Account": "homeside",
                    "UserName": self.username,
                    "Password": self.password
                },
                "lang": "sv"
            }

            response = self.session.post(auth_url, json=payload, timeout=30)
            response.raise_for_status()

            data = response.json()
            self.session_token = data.get('querykey')

            if not self.session_token:
                print("ERROR: No session token in response")
                return False

            self.session.headers.update({
                'Authorization': self.session_token,
                'Content-Type': 'application/json'
            })

            print(f"  Session token obtained")
            self.log(f"Token: {self.session_token[:20]}...")

            # Step 2: Discover houses
            return self.discover_houses()

        except requests.exceptions.RequestException as e:
            print(f"ERROR: Authentication failed: {e}")
            return False

    def discover_houses(self) -> bool:
        """
        Discover available houses for this user.

        Returns:
            True if at least one house found
        """
        try:
            response = self.session.post(
                f"{self.homeside_url}/api/v2/housefidlist",
                json={},
                timeout=30
            )
            response.raise_for_status()

            self.houses = response.json()

            if not self.houses:
                print("ERROR: No houses found for this user")
                return False

            print(f"  Found {len(self.houses)} house(s):")
            for i, house in enumerate(self.houses):
                name = house.get('name', 'Unknown')
                client_id = house.get('restapiurl', '')
                print(f"    [{i}] {name}")
                self.log(f"        Client ID: {client_id}")

            return True

        except requests.exceptions.RequestException as e:
            print(f"ERROR: Failed to discover houses: {e}")
            return False

    def select_house(self, index: int = 0) -> bool:
        """
        Select a house to work with.

        Args:
            index: Index of house in the list (default: 0)

        Returns:
            True if house selected and BMS token obtained
        """
        if not self.houses:
            print("ERROR: No houses available. Call authenticate() first.")
            return False

        if index >= len(self.houses):
            print(f"ERROR: House index {index} out of range (0-{len(self.houses)-1})")
            return False

        self.selected_house = self.houses[index]
        self.client_id = self.selected_house.get('restapiurl')

        house_name = self.selected_house.get('name', 'Unknown')
        print(f"\nSelected house: {house_name}")

        # Get BMS token for this house
        return self.get_bms_token()

    def get_bms_token(self) -> bool:
        """
        Get BMS token required for Arrigo API access.

        Returns:
            True if token obtained
        """
        if not self.client_id:
            print("ERROR: No house selected")
            return False

        try:
            payload = {"clientid": self.client_id}
            response = self.session.post(
                f"{self.homeside_url}/api/v2/housearrigobmsapi/getarrigobmstoken",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            self.bms_token = data.get('token')
            self.uid = data.get('uid')
            self.extend_uid = data.get('extendUid')

            if not self.bms_token:
                print("ERROR: No BMS token in response")
                return False

            print(f"  BMS token obtained")
            return True

        except requests.exceptions.RequestException as e:
            print(f"ERROR: Failed to get BMS token: {e}")
            return False

    def discover_signals(self) -> bool:
        """
        Discover available analog signals from Arrigo and map them to field names.

        Populates self.signal_map and self.field_to_signal.

        Returns:
            True if signals discovered successfully
        """
        print("\n  Discovering available signals...")

        query = '''
        {
            analogs(first: 200) {
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

        try:
            response = self.arrigo_session.post(
                self.graphql_url,
                json={'query': query},
                timeout=60
            )

            if response.status_code != 200:
                print(f"  ERROR: GraphQL returned {response.status_code}")
                return False

            result = response.json()
            if 'errors' in result:
                print(f"  ERROR: {result['errors']}")
                return False

            items = result.get('data', {}).get('analogs', {}).get('items', [])
            total = result.get('data', {}).get('analogs', {}).get('totalCount', 0)

            print(f"  Found {total} analog signals")

            # Map signals to field names
            for item in items:
                signal_id = item['id']  # Base64 encoded ID
                signal_name = item.get('name', '')

                # Extract the variable name (after the dot)
                # e.g., "HEM_FJV_Villa_34.GT_UTE" -> "GT_UTE"
                if '.' in signal_name:
                    var_name = signal_name.split('.')[-1]
                else:
                    var_name = signal_name

                # Check if this matches any of our patterns
                field_name = SIGNAL_PATTERN_MAPPING.get(var_name)
                if field_name:
                    self.signal_map[signal_id] = {
                        'field_name': field_name,
                        'arrigo_name': signal_name,
                        'unit': item.get('unit', ''),
                        'current_value': item.get('value')
                    }
                    self.field_to_signal[field_name] = signal_id
                    self.log(f"  Mapped: {var_name} -> {field_name} (id: {signal_id})")

            # Check we have core signals
            missing = [s for s in CORE_SIGNALS if s not in self.field_to_signal]
            if missing:
                print(f"  Warning: Missing core signals: {missing}")
            else:
                print(f"  All core signals found")

            print(f"  Mapped {len(self.signal_map)} signals to field names")
            return True

        except Exception as e:
            print(f"  ERROR: Failed to discover signals: {e}")
            return False

    def get_available_signals(self) -> List[str]:
        """
        Get list of available signals/variables for this house.

        Returns:
            List of signal names with their IDs
        """
        if not self.bms_token:
            print("ERROR: Not authenticated")
            return []

        # Make sure we're logged in to Arrigo
        if not self.arrigo_session.headers.get('Authorization'):
            self.login_to_arrigo()

        # Discover signals if not already done
        if not self.signal_map:
            self.discover_signals()

        # Return formatted list
        result = []
        for signal_id, info in self.signal_map.items():
            result.append(f"{info['field_name']}: {info['arrigo_name']} = {info['current_value']} {info['unit']}")

        return result

    def login_to_arrigo(self) -> bool:
        """
        Login to the Arrigo server and set up authentication.

        Uses the BMS token as Bearer token for Arrigo API access.

        Returns:
            True if login successful
        """
        print(f"\nConnecting to Arrigo server: {self.arrigo_host}")

        if not self.bms_token:
            print("  ERROR: No BMS token available")
            return False

        # Set Bearer token auth for Arrigo API
        self.arrigo_session.headers.update({
            "Authorization": f"Bearer {self.bms_token}",
            "Content-Type": "application/json",
        })

        try:
            # Test connection
            login_url = f"{self.arrigo_url}/api/login"
            response = self.arrigo_session.get(login_url, timeout=30)

            if response.status_code == 200:
                data = response.json()
                print(f"  Connected to: {data.get('mainProject', 'unknown')}")
                self.log(f"Service status: {data.get('serviceStatus')}")
                return True
            else:
                print(f"  Warning: Arrigo login returned {response.status_code}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"  ERROR: Could not reach Arrigo server: {e}")
            return False

    def test_graphql_query(self):
        """
        Test the GraphQL API with a simple query to understand the schema.
        """
        print(f"\nTesting GraphQL at: {self.graphql_url}")
        print(f"BMS Token: {self.bms_token[:20]}..." if self.bms_token else "No BMS token")

        # Try different authentication methods
        print("\n0. Testing different auth methods...")

        # Method A: BMS token as Bearer
        print("\n   A. Trying Bearer token auth...")
        self.arrigo_session.headers.update({
            "Authorization": f"Bearer {self.bms_token}"
        })
        try:
            response = self.arrigo_session.get(f"{self.arrigo_url}/api/login", timeout=10)
            print(f"      Status: {response.status_code}")
        except Exception as e:
            print(f"      Error: {e}")

        # Method B: Session token from HomeSide
        print("\n   B. Trying HomeSide session token...")
        self.arrigo_session.headers.update({
            "Authorization": self.session_token
        })
        try:
            response = self.arrigo_session.get(f"{self.arrigo_url}/api/login", timeout=10)
            print(f"      Status: {response.status_code}")
            if response.status_code == 200:
                print(f"      Response: {response.text[:200]}")
        except Exception as e:
            print(f"      Error: {e}")

        # Method C: Try with uid in header
        print("\n   C. Trying with uid header...")
        self.arrigo_session.headers.update({
            "X-Arrigo-Uid": self.uid or "",
            "Authorization": self.bms_token
        })
        try:
            response = self.arrigo_session.get(f"{self.arrigo_url}/api/login", timeout=10)
            print(f"      Status: {response.status_code}")
        except Exception as e:
            print(f"      Error: {e}")

        # Test 1: Full introspection query to discover available queries
        print("\n1. Testing schema introspection (available queries)...")
        introspection_query = """
        {
            __schema {
                queryType {
                    name
                    fields {
                        name
                        description
                        args {
                            name
                            type { name kind ofType { name kind } }
                        }
                    }
                }
            }
        }
        """

        try:
            response = self.arrigo_session.post(
                self.graphql_url,
                json={"query": introspection_query},
                timeout=30
            )
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                print(f"   Response: {json.dumps(result, indent=2)[:1000]}")
            else:
                print(f"   Body: {response.text[:500]}")
        except Exception as e:
            print(f"   Error: {e}")

        # Test 2: Try a simple historical data query
        print("\n2. Testing historical data query...")

        # Use a short time range for testing
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)

        test_query = """
        query GetHistoricalData($deviceId: String!, $signalName: String!, $startTime: Long!, $endTime: Long!, $resolution: String!) {
            historicalData(
                deviceId: $deviceId
                signalName: $signalName
                startTime: $startTime
                endTime: $endTime
                resolution: $resolution
            ) {
                timestamp
                value
                status
                quality
            }
        }
        """

        variables = {
            "deviceId": self.username,
            "signalName": "Ute Temperatur",
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "resolution": "1h"
        }

        print(f"   Query variables: {json.dumps(variables, indent=2)}")

        try:
            response = self.arrigo_session.post(
                self.graphql_url,
                json={"query": test_query, "variables": variables},
                timeout=30
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:2000]}")
        except Exception as e:
            print(f"   Error: {e}")

        # Test 3: Try without deviceId parameter
        print("\n3. Testing query without deviceId...")

        alt_query = """
        query GetHistoricalData($signalName: String!, $startTime: Long!, $endTime: Long!, $resolution: String!) {
            historicalData(
                signalName: $signalName
                startTime: $startTime
                endTime: $endTime
                resolution: $resolution
            ) {
                timestamp
                value
            }
        }
        """

        alt_variables = {
            "signalName": "Ute Temperatur",
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "resolution": "1h"
        }

        try:
            response = self.arrigo_session.post(
                self.graphql_url,
                json={"query": alt_query, "variables": alt_variables},
                timeout=30
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:2000]}")
        except Exception as e:
            print(f"   Error: {e}")

    def fetch_historical_data(
        self,
        days_back: int = 90,
        resolution_minutes: int = 60
    ) -> List[Dict]:
        """
        Fetch historical data for all relevant signals.

        Args:
            days_back: Number of days of history to fetch
            resolution_minutes: Data resolution in minutes

        Returns:
            List of data points with timestamps
        """
        if not self.bms_token:
            print("ERROR: Not authenticated")
            return []

        # Login to Arrigo server
        if not self.login_to_arrigo():
            return []

        # Discover available signals
        if not self.discover_signals():
            return []

        print(f"\nFetching {days_back} days of historical data...")

        # Calculate time range
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days_back)

        print(f"  Time range: {start_time.date()} to {end_time.date()}")
        print(f"  Resolution: {resolution_minutes} minutes")

        # Fetch via GraphQL
        return self._fetch_via_graphql(start_time, end_time, resolution_minutes)

    def _fetch_via_graphql(
        self,
        start_time: datetime,
        end_time: datetime,
        resolution_minutes: int
    ) -> List[Dict]:
        """
        Fetch historical data via Arrigo GraphQL API using analogsHistory query.
        """
        if not self.signal_map:
            print("  ERROR: No signals discovered")
            return []

        # Get all signal IDs we want to fetch
        signal_ids = list(self.signal_map.keys())

        print(f"\n  Fetching history for {len(signal_ids)} signals...")

        # GraphQL query for historical data
        query = '''
        query GetHistory($filter: AnalogEventFilter) {
            analogsHistory(first: 50000, filter: $filter) {
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
        '''

        # Resolution in seconds
        resolution_seconds = resolution_minutes * 60

        variables = {
            'filter': {
                'signalId': signal_ids,
                'ranges': [{
                    'from': start_time.isoformat(),
                    'to': end_time.isoformat()
                }],
                'timeLength': resolution_seconds
            }
        }

        self.log(f"Query variables: {json.dumps(variables, indent=2)}")

        try:
            response = self.arrigo_session.post(
                self.graphql_url,
                json={'query': query, 'variables': variables},
                timeout=300  # 5 minutes for large queries
            )

            if response.status_code != 200:
                print(f"  ERROR: GraphQL returned {response.status_code}")
                self.log(f"Response: {response.text[:500]}")
                return []

            result = response.json()

            if 'errors' in result:
                print(f"  ERROR: GraphQL errors: {result['errors']}")
                return []

            items = result.get('data', {}).get('analogsHistory', {}).get('items', [])
            total = result.get('data', {}).get('analogsHistory', {}).get('totalCount', 0)

            print(f"  Received {len(items)} data points (total available: {total})")

            # Convert to our format
            all_data = []
            signal_counts = {}

            for item in items:
                signal_id = item['signalId']
                if signal_id not in self.signal_map:
                    continue

                field_name = self.signal_map[signal_id]['field_name']
                value = item.get('value')

                if value is not None:
                    # Parse timestamp
                    time_str = item['time']
                    try:
                        timestamp = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    except:
                        timestamp = datetime.fromisoformat(time_str)

                    all_data.append({
                        'timestamp': timestamp,
                        'field': field_name,
                        'value': float(value)
                    })

                    # Count per signal
                    signal_counts[field_name] = signal_counts.get(field_name, 0) + 1

            # Print summary per signal
            print("\n  Data points per signal:")
            for field_name, count in sorted(signal_counts.items()):
                print(f"    {field_name}: {count}")

            print(f"\n  Total data points: {len(all_data)}")
            return all_data

        except requests.exceptions.Timeout:
            print("  ERROR: Query timed out")
            return []
        except Exception as e:
            print(f"  ERROR: {e}")
            self.log(f"Exception: {type(e).__name__}: {e}")
            return []


        # Check for partial matches (signals with device ID suffix)
        for key, value in SIGNAL_MAPPING.items():
            if signal_name.startswith(key):
                return value

        # Default: return cleaned signal name
        return signal_name.lower().replace(' ', '_').replace(',', '')

class InfluxDBImporter:
    """Imports historical data to InfluxDB."""

    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        house_id: str
    ):
        if not INFLUX_AVAILABLE:
            raise ImportError("influxdb_client not installed")

        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = bucket
        self.org = org
        self.house_id = house_id

    def import_data(self, data: List[Dict], dry_run: bool = False) -> Tuple[int, int]:
        """
        Import data to InfluxDB.

        Args:
            data: List of data points
            dry_run: If True, don't actually write

        Returns:
            Tuple of (success_count, error_count)
        """
        if not data:
            print("No data to import")
            return 0, 0

        # Group data by timestamp for efficient writing
        data_by_time = {}
        for point in data:
            ts = point['timestamp']
            if ts not in data_by_time:
                data_by_time[ts] = {}
            data_by_time[ts][point['field']] = point['value']

        print(f"\nImporting {len(data_by_time)} time points to InfluxDB...")

        if dry_run:
            print("  (DRY RUN - no data will be written)")

        success_count = 0
        error_count = 0

        # Write to both thermal_history and heating_system measurements
        for timestamp, fields in data_by_time.items():
            try:
                # Only import if we have the core fields for thermal analysis
                if 'room_temperature' in fields and 'outdoor_temperature' in fields:
                    if not dry_run:
                        # Write to thermal_history (for thermal analyzer)
                        thermal_point = Point("thermal_history") \
                            .tag("house_id", self.house_id) \
                            .field("room_temperature", round(float(fields['room_temperature']), 2)) \
                            .field("outdoor_temperature", round(float(fields['outdoor_temperature']), 2)) \
                            .time(timestamp, WritePrecision.S)

                        if 'supply_temp' in fields:
                            thermal_point.field("supply_temp", round(float(fields['supply_temp']), 2))
                        if 'return_temp' in fields:
                            thermal_point.field("return_temp", round(float(fields['return_temp']), 2))

                        self.write_api.write(bucket=self.bucket, org=self.org, record=thermal_point)

                        # Also write to heating_system for dashboards
                        heating_point = Point("heating_system") \
                            .tag("house_id", self.house_id) \
                            .time(timestamp, WritePrecision.S)

                        for field, value in fields.items():
                            heating_point.field(field, round(float(value), 2))

                        self.write_api.write(bucket=self.bucket, org=self.org, record=heating_point)

                    success_count += 1

            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  Error writing point: {e}")

        print(f"  Imported: {success_count}, Errors: {error_count}")
        return success_count, error_count

    def close(self):
        """Close InfluxDB connection."""
        self.client.close()


def main():
    parser = argparse.ArgumentParser(
        description='Import historical heating data from Arrigo API to InfluxDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Dry run - see what would be imported
    python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90 --dry-run

    # Import 3 months of data
    python3 import_historical_data.py --username FC2000232581 --password "xxx" --days 90

    # Import for specific house (if user has multiple)
    python3 import_historical_data.py --username FC2000232581 --password "xxx" --house-index 0
        """
    )

    # Authentication
    parser.add_argument('--username', required=True, help='HomeSide username (e.g., FC2000232581)')
    parser.add_argument('--password', required=True, help='HomeSide password')

    # Data options
    parser.add_argument('--days', type=int, default=90, help='Days of history to fetch (default: 90)')
    parser.add_argument('--resolution', type=int, default=60, help='Resolution in minutes (default: 60)')
    parser.add_argument('--house-index', type=int, default=0, help='House index if multiple (default: 0)')

    # InfluxDB options
    parser.add_argument('--influx-url', default='http://localhost:8086', help='InfluxDB URL')
    parser.add_argument('--influx-token', default=os.getenv('INFLUXDB_TOKEN', ''))
    parser.add_argument('--influx-org', default='homeside', help='InfluxDB organization')
    parser.add_argument('--influx-bucket', default='heating', help='InfluxDB bucket')
    parser.add_argument('--house-id', help='House ID for InfluxDB tags (auto-detected if not specified)')

    # Operation modes
    parser.add_argument('--dry-run', action='store_true', help='Show what would be imported without writing')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--list-signals', action='store_true', help='List available signals and exit')
    parser.add_argument('--test-graphql', action='store_true', help='Test GraphQL query and show raw response')
    parser.add_argument('--arrigo-host', default=DEFAULT_ARRIGO_HOST,
                        help=f'Arrigo server hostname (default: {DEFAULT_ARRIGO_HOST})')

    args = parser.parse_args()

    print("=" * 60)
    print("Historical Data Import Tool")
    print("=" * 60)
    print()

    # Create client and authenticate
    client = ArrigoHistoricalClient(
        username=args.username,
        password=args.password,
        arrigo_host=args.arrigo_host,
        verbose=args.verbose
    )

    if not client.authenticate():
        sys.exit(1)

    if not client.select_house(args.house_index):
        sys.exit(1)

    # List signals mode
    if args.list_signals:
        print("\nAvailable signals (from HomeSide API):")
        signals = client.get_available_signals()
        for signal in sorted(signals):
            print(f"  - {signal}")
        sys.exit(0)

    # Test GraphQL mode - helps debug the API schema
    if args.test_graphql:
        print("\nTesting GraphQL query...")
        client.login_to_arrigo()
        client.test_graphql_query()
        sys.exit(0)

    # Determine house ID for InfluxDB
    house_id = args.house_id
    if not house_id:
        # Extract from client_id (e.g., "38/xxx/HEM_FJV_149/HEM_FJV_Villa_149" -> "HEM_FJV_Villa_149")
        if client.client_id:
            parts = client.client_id.split('/')
            house_id = parts[-1] if parts else client.client_id
        else:
            house_id = f"house_{args.username}"

    print(f"\nHouse ID for InfluxDB: {house_id}")

    # Fetch historical data
    data = client.fetch_historical_data(
        days_back=args.days,
        resolution_minutes=args.resolution
    )

    if not data:
        print("\nNo historical data retrieved.")
        print("\nTroubleshooting:")
        print("1. The Arrigo GraphQL API may require direct server access")
        print("2. If you know the Arrigo server hostname, use --arrigo-host")
        print("3. Check if historical data is available in the HomeSide web interface")
        sys.exit(1)

    print(f"\nRetrieved {len(data)} data points")

    # Preview data
    print("\nData preview (first 5 points):")
    for point in data[:5]:
        print(f"  {point['timestamp']} | {point['field']}: {point['value']}")

    # Import to InfluxDB
    if args.dry_run:
        print("\n[DRY RUN] Would import to InfluxDB:")
        print(f"  URL: {args.influx_url}")
        print(f"  Bucket: {args.influx_bucket}")
        print(f"  House ID: {house_id}")
        print(f"  Points: {len(data)}")
    else:
        if not INFLUX_AVAILABLE:
            print("\nERROR: influxdb_client not installed. Cannot import.")
            print("Install with: pip install influxdb-client")
            sys.exit(1)

        try:
            importer = InfluxDBImporter(
                url=args.influx_url,
                token=args.influx_token,
                org=args.influx_org,
                bucket=args.influx_bucket,
                house_id=house_id
            )

            success, errors = importer.import_data(data, dry_run=args.dry_run)
            importer.close()

            if success > 0:
                print(f"\nSuccess! Imported {success} time points to InfluxDB")
                print("The thermal analyzer will use this data on next restart.")

        except Exception as e:
            print(f"\nERROR: Failed to import to InfluxDB: {e}")
            sys.exit(1)

    print()


if __name__ == "__main__":
    main()

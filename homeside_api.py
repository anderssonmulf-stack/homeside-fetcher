#!/usr/bin/env python3
"""
HomeSide API Client
Handles authentication and data fetching from HomeSide heating system API
"""

import requests
import time
import json
import os
from datetime import datetime, timezone


def load_variables_config(config_path='variables_config.json'):
    """
    Load variable configuration from JSON file

    Returns:
        tuple: (api_names_list, field_mapping_dict, var_count)
    """
    try:
        # Try current directory first, then /app (Docker path)
        paths = [config_path, f'/app/{config_path}']
        config = None

        for path in paths:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    config = json.load(f)
                break

        if not config:
            raise FileNotFoundError(f"Config file not found in {paths}")

        variables = config['variables']
        api_names = [v['api_name'] for v in variables]
        field_mapping = {v['api_name']: v['field_name'] for v in variables}

        return api_names, field_mapping, len(variables)

    except Exception as e:
        # Fallback to hardcoded list if config file not found
        print(f"⚠ Could not load variables config: {e}")
        print("  Using fallback variable list")

        # Fallback hardcoded list
        api_names = [
            'MEAN_GT_RUM_1_Output',
            'AI_GT_UTE_Output',
            'MEDEL_GT_UTE_24h_Average',
            'AI_VS1_GT_TILL_1_Output',
            'AI_VS1_GT_RETUR_1_Output',
            'AI_VV1_TAPP_GT41_Output',
            'AI_GP_EXP1_1_Output',
            'POOL_AKTIV_Input',
            'VMM1_AKTIV_Input',
            'FORC_BORTALAGE',
            'KU_VS1_GT_TILL_1_SetPoint',
            'KU_VS1_GT_TILL_1_BORTREST_SetPoint'
        ]

        field_mapping = {
            'MEAN_GT_RUM_1_Output': 'room_temperature',
            'AI_GT_UTE_Output': 'outdoor_temperature',
            'MEDEL_GT_UTE_24h_Average': 'outdoor_temp_24h_avg',
            'AI_VS1_GT_TILL_1_Output': 'supply_temp',
            'AI_VS1_GT_RETUR_1_Output': 'return_temp',
            'AI_VV1_TAPP_GT41_Output': 'hot_water_temp',
            'AI_GP_EXP1_1_Output': 'system_pressure',
            'POOL_AKTIV_Input': 'electric_heater',
            'VMM1_AKTIV_Input': 'heat_recovery',
            'FORC_BORTALAGE': 'away_mode',
            'KU_VS1_GT_TILL_1_SetPoint': 'target_temp_setpoint',
            'KU_VS1_GT_TILL_1_BORTREST_SetPoint': 'away_temp_setpoint'
        }

        return api_names, field_mapping, len(api_names)


class HomeSideAPI:
    def __init__(self, session_token, clientid, logger, username=None, password=None, debug_mode=False, seq_logger=None):
        self.base_url = "https://homeside.systeminstallation.se"
        self.session_token = session_token
        self.clientid = clientid
        self.logger = logger
        self.username = username
        self.password = password
        self.debug_mode = debug_mode
        self.seq_logger = seq_logger

        # BMS token data (obtained from getarrigobmstoken)
        self.bms_token = None
        self.uid = None
        self.extend_uid = None
        self.refresh_token = None

        # House info (from auto-discovery)
        self.house_name = None
        self.houses = []

        # Track when session token was set/refreshed
        self.session_token_updated_at = datetime.now(timezone.utc).isoformat()
        self.session_token_source = "env"  # "env" or "api"

        # Load variable configuration
        self.target_vars, self.field_mapping, self.var_count = load_variables_config()
        if self.debug_mode:
            self.logger.info(f"Loaded {self.var_count} variables from config")

        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Authorization': session_token
        })

    def refresh_session_token(self):
        """Refresh the session token using direct API authentication"""
        if not self.username or not self.password:
            self.logger.error("Cannot refresh token: No credentials provided")
            return False

        self.logger.info("Attempting to refresh session token via API...")

        try:
            # Authenticate via API (discovered from browser network inspection)
            auth_url = f"{self.base_url}/api/v2/authorize/account"
            payload = {
                "user": {
                    "Account": "homeside",
                    "UserName": self.username.replace(" ", ""),  # Remove spaces
                    "Password": self.password
                },
                "lang": "sv"
            }

            response = requests.post(auth_url, json=payload, timeout=30)

            if response.status_code == 200:
                data = response.json()
                new_token = data.get('querykey')

                if new_token:
                    self.session_token = new_token
                    self.session.headers.update({'Authorization': new_token})
                    self.session_token_updated_at = datetime.now(timezone.utc).isoformat()
                    self.session_token_source = "api"
                    self.logger.info("Session token refreshed successfully via API")

                    if self.seq_logger:
                        self.seq_logger.log_token_refresh(success=True, method='API', username=self.username)

                    # Auto-discover client ID if not set
                    if not self.clientid:
                        self.discover_client_id()

                    return True
                else:
                    self.logger.error("API response missing 'querykey'")
            else:
                self.logger.error(f"API authentication failed: HTTP {response.status_code}")

        except Exception as e:
            self.logger.error(f"Failed to refresh session token: {str(e)}")

        if self.seq_logger:
            self.seq_logger.log_token_refresh(success=False, username=self.username)

        return False


    def cleanup(self):
        """Clean up resources"""
        # No cleanup needed - using direct API authentication
        pass

    def discover_client_id(self):
        """
        Auto-discover the client ID by fetching the user's house list.
        Uses the /api/v2/housefidlist endpoint.

        Returns:
            bool: True if client ID discovered, False otherwise
        """
        try:
            response = self.session.post(
                f"{self.base_url}/api/v2/housefidlist",
                json={},
                timeout=30
            )
            response.raise_for_status()

            self.houses = response.json()

            if self.houses:
                # Use the first house (most users have one)
                house = self.houses[0]
                self.clientid = house.get("restapiurl")
                self.house_name = house.get("name")

                if self.clientid:
                    self.logger.info(f"Auto-discovered client ID: {self.clientid}")
                    print(f"✓ Auto-discovered client ID for: {self.house_name}")

                    # Update seq_logger with discovered client_id
                    if self.seq_logger:
                        self.seq_logger.set_client_id(self.clientid)
                        self.seq_logger.log(
                            f"Client ID auto-discovered: {self.house_name}",
                            level='Information',
                            properties={
                                'EventType': 'ClientIdDiscovered',
                                'HouseName': self.house_name,
                                'HouseCount': len(self.houses)
                            }
                        )
                    return True

            self.logger.warning("No houses found for this user")
            return False

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to discover client ID: {str(e)}")
            print(f"✗ Failed to discover client ID: {e}")
            return False

    def get_bms_token(self, retry_on_auth_error=True):
        """Get BMS token required for heating data access"""
        # If no session token, try to get one first
        if not self.session_token:
            if not self.refresh_session_token():
                self.logger.error("Cannot get BMS token: No session token and login failed")
                return False

        # Auto-discover client ID if not set
        if not self.clientid:
            if not self.discover_client_id():
                self.logger.error("Cannot get BMS token: No client ID and auto-discovery failed")
                return False

        try:
            payload = {"clientid": self.clientid}
            response = self.session.post(
                f"{self.base_url}/api/v2/housearrigobmsapi/getarrigobmstoken",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            self.bms_token = data.get('token')
            self.uid = data.get('uid')
            self.extend_uid = data.get('extendUid')
            self.refresh_token = data.get('refreshToken')

            if self.debug_mode:
                self.logger.info(f"BMS token obtained successfully")
            print(f"✓ BMS token obtained at {datetime.now()}")
            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and retry_on_auth_error:
                self.logger.warning("Session token expired (401), attempting to refresh...")
                print("⚠ Session token expired, refreshing...")
                if self.refresh_session_token():
                    # Retry once with new token
                    return self.get_bms_token(retry_on_auth_error=False)
            self.logger.error(f"Failed to get BMS token: {str(e)}")
            print(f"✗ Failed to get BMS token: {e}")
            return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get BMS token: {str(e)}")
            print(f"✗ Failed to get BMS token: {e}")
            return False

    def get_heating_data(self, retry_on_auth_error=True):
        """Fetch current heating system variables"""
        if not self.bms_token:
            self.logger.error("BMS token not available, call get_bms_token first")
            return None

        try:
            # Build complete payload with all required fields
            payload = {
                "uid": self.uid,
                "clientid": self.clientid,
                "extendUid": self.extend_uid,
                "token": self.bms_token
            }

            # Time the API call to see if longer timeout helps
            start_time = time.time()

            response = self.session.post(
                f"{self.base_url}/api/v2/housearrigobmsapi/getducvariables",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            elapsed_time = time.time() - start_time

            data = response.json()
            var_count = len(data.get('variables', []))

            if self.debug_mode:
                self.logger.info(f"Heating data fetched in {elapsed_time:.2f}s ({var_count} variables)")
                print(f"⏱  API response time: {elapsed_time:.2f}s ({var_count} variables)")

            # Check for "Pending" values - API may need time to fetch data
            pending_count = sum(1 for v in data.get('variables', []) if v.get('type') == 'Pending' or v.get('value') is None)
            ready_count = var_count - pending_count

            # Helper function to check if we have all target variables
            def has_all_targets(data):
                variables = {}
                for var in data.get('variables', []):
                    if var.get('value') is not None:
                        short_name = var['variable'].split('.')[-1]
                        variables[short_name] = var['value']
                return sum(1 for target in self.target_vars if target in variables)

            # Smart polling: If many values pending, keep polling until we get all targets OR timeout
            if pending_count > var_count / 2 and retry_on_auth_error:
                targets_found = has_all_targets(data)
                if self.debug_mode:
                    self.logger.info(f"API returned {pending_count}/{var_count} pending values, {targets_found}/{self.var_count} targets found")
                    print(f"⏳ {pending_count}/{var_count} values pending, {targets_found}/{self.var_count} targets found")

                max_wait = 60  # Max 60 seconds
                poll_interval = 5  # Check every 5 seconds
                elapsed = 0

                while elapsed < max_wait and targets_found < self.var_count:
                    if self.debug_mode:
                        print(f"⏳ Waiting {poll_interval}s (elapsed: {elapsed}s, found: {targets_found}/{self.var_count})...")
                    time.sleep(poll_interval)
                    elapsed += poll_interval

                    # Poll again
                    response = self.session.post(
                        f"{self.base_url}/api/v2/housearrigobmsapi/getducvariables",
                        json=payload,
                        timeout=30
                    )
                    response.raise_for_status()
                    data = response.json()

                    # Check how many targets we have now
                    targets_found = has_all_targets(data)

                    if targets_found == self.var_count:
                        if self.debug_mode:
                            self.logger.info(f"✓ All {self.var_count} targets found after {elapsed}s")
                            print(f"✓ All {self.var_count} targets found after {elapsed}s!")
                        break

                # Final count
                var_count = len(data.get('variables', []))
                pending_count = sum(1 for v in data.get('variables', []) if v.get('type') == 'Pending' or v.get('value') is None)
                ready_count = var_count - pending_count

                if targets_found < self.var_count:
                    if self.debug_mode:
                        self.logger.info(f"Polling stopped after {elapsed}s: {ready_count}/{var_count} values ready, {targets_found}/{self.var_count} targets found")
                        print(f"⏱ Polling stopped: {ready_count}/{var_count} ready, {targets_found}/{self.var_count} targets found")

            # Check if API returned 0 variables - indicates expired token
            if var_count == 0 and retry_on_auth_error:
                self.logger.warning("API returned 0 variables - session token likely expired")
                print(f"⚠ API returned 0 variables (HTTP {response.status_code}), token likely expired, refreshing...")

                # Try refreshing session token
                if self.refresh_session_token():
                    # Also need to get new BMS token with new session token
                    if self.get_bms_token(retry_on_auth_error=False):
                        # Retry data fetch once with new tokens
                        return self.get_heating_data(retry_on_auth_error=False)

                # If refresh failed, log error and return empty data
                self.logger.error("Failed to refresh tokens after getting 0 variables")
                print("✗ Token refresh failed, no data available")
                return None

            return data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and retry_on_auth_error:
                # Session token might be expired, try refreshing
                self.logger.warning("Got 401 when fetching data, session token may be expired")
                print("⚠ Session token expired (401), refreshing...")
                if self.refresh_session_token():
                    # Also need to get new BMS token with new session token
                    if self.get_bms_token(retry_on_auth_error=False):
                        # Retry data fetch once with new tokens
                        return self.get_heating_data(retry_on_auth_error=False)

            self.logger.warning(f"Failed to fetch heating data (HTTP {e.response.status_code}): {str(e)}")
            print(f"⚠ Data fetch failed (HTTP {e.response.status_code}): {e}")
            return None

        except requests.exceptions.RequestException as e:
            # Timeouts are normal behavior when API is slow - log as info, not warning
            if 'timed out' in str(e).lower():
                self.logger.info(f"API timeout (normal behavior): {str(e)}")
                print(f"ℹ️  API timeout, will refresh and retry")
            else:
                self.logger.warning(f"Failed to fetch heating data (network): {str(e)}")
                print(f"⚠ Data fetch failed (network): {e}")
            return None

    def extract_key_values(self, raw_data):
        """Extract only useful heating values from API response"""
        if not raw_data or 'variables' not in raw_data:
            return None

        # Build dictionary of all variables
        variables = {}
        for var in raw_data['variables']:
            var_name = var['variable'].split('.')[-1]  # Get short name
            variables[var_name] = var['value']

        # Log variable count (only in debug mode - reduces noise)
        total_vars = len(variables)
        if self.debug_mode:
            var_names = list(variables.keys())[:10]
            self.logger.info(f"API returned {total_vars} variables. Sample names: {var_names}")
            print(f"📊 API returned {total_vars} variables")

        # Extract values using field mapping from config
        extracted = {'timestamp': datetime.now(timezone.utc).isoformat()}

        for api_name, field_name in self.field_mapping.items():
            if api_name in variables:
                extracted[field_name] = variables[api_name]

        # Log how many values we found vs expected (keep print for user visibility)
        non_none_count = sum(1 for v in extracted.values() if v is not None) - 1  # -1 for timestamp
        if self.debug_mode:
            self.logger.info(f"Found {non_none_count}/{self.var_count} expected values")
        print(f"✓ Matched {non_none_count}/{self.var_count} expected values")

        # Remove None values to save memory
        filtered = {k: v for k, v in extracted.items() if v is not None}

        # If we got no matching data, return None to trigger token refresh
        if len(filtered) == 1:  # Only timestamp
            self.logger.warning(f"No heating values found in API response (got {total_vars} total variables but 0 matches)")
            print(f"⚠ No matching variables found (0/{total_vars})")
            return None  # Return None to trigger token refresh in main loop

        return filtered

    def display_data(self, data):
        """Display extracted data in readable format"""
        if not data:
            print("No data to display")
            return

        print("\n" + "="*70)
        print(f"HEATING SYSTEM DATA - {data.get('timestamp', datetime.now())}")
        print("="*70)

        # Temperatures
        if 'room_temperature' in data:
            print(f"  Room Temperature:     {data['room_temperature']:.2f}°C")
        if 'target_temp_setpoint' in data:
            print(f"  Target Setpoint:      {data['target_temp_setpoint']:.2f}°C")
        if 'away_temp_setpoint' in data:
            print(f"  Away Mode Setpoint:   {data['away_temp_setpoint']:.2f}°C")
        if 'outdoor_temperature' in data:
            print(f"  Outdoor Temperature:  {data['outdoor_temperature']:.2f}°C")
        if 'outdoor_temp_24h_avg' in data:
            print(f"  Outdoor 24h Average:  {data['outdoor_temp_24h_avg']:.2f}°C")
        if 'supply_temp' in data:
            print(f"  Supply Temperature:   {data['supply_temp']:.2f}°C")
        if 'return_temp' in data:
            print(f"  Return Temperature:   {data['return_temp']:.2f}°C")
        if 'hot_water_temp' in data:
            print(f"  Hot Water Temp:       {data['hot_water_temp']:.2f}°C")
        if 'system_pressure' in data:
            print(f"  System Pressure:      {data['system_pressure']:.2f} bar")

        # Status flags
        if 'electric_heater' in data:
            print(f"  Electric Heater:      {data['electric_heater']}")
        if 'heat_recovery' in data:
            print(f"  Heat Recovery:        {data['heat_recovery']}")
        if 'away_mode' in data:
            print(f"  Away Mode:            {data['away_mode']}")

        print("="*70 + "\n")

    def write_value(self, path: str, value, retry_on_auth_error=True):
        """
        Write a value to the HomeSide API using the save endpoint.

        Args:
            path: Variable path (e.g., "Cwl.Advise.A[70]" for heat curve at 0°C outdoor)
            value: Value to set (will be converted to string)
            retry_on_auth_error: Whether to retry after refreshing token on 401

        Returns:
            bool: True if write was successful, False otherwise

        Heat curve indices (Cwl.Advise.A[64] through Cwl.Advise.A[73]):
            64-69: Heat curve points at various outdoor temperatures
            70: Target supply temperature at 0°C outdoor
            71-73: Heat curve points at higher outdoor temperatures
        """
        if not self.bms_token:
            self.logger.error("BMS token not available, call get_bms_token first")
            return False

        try:
            # Build the arrigoBMSTokenAndUid JSON string (must be escaped JSON)
            token_uid_data = {
                "uid": self.uid,
                "token": self.bms_token,
                "refreshToken": self.refresh_token or "",
                "extendUid": self.extend_uid
            }

            payload = {
                "clientid": self.clientid,
                "arrigoBMSTokenAndUid": json.dumps(token_uid_data),
                "input": [
                    {"Name": path, "Value": str(value)}
                ]
            }

            if self.debug_mode:
                self.logger.info(f"Writing {path} = {value}")
                print(f"📝 Writing {path} = {value}")

            response = self.session.post(
                f"{self.base_url}/api/v2/housearrigobmsapi/save",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            if self.debug_mode:
                self.logger.info(f"Write successful: {response.status_code}")
                print(f"✓ Write successful")

            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and retry_on_auth_error:
                self.logger.warning("Session token expired (401) during write, attempting to refresh...")
                print("⚠ Session token expired, refreshing...")
                if self.refresh_session_token():
                    if self.get_bms_token(retry_on_auth_error=False):
                        return self.write_value(path, value, retry_on_auth_error=False)
            self.logger.error(f"Failed to write value: {str(e)}")
            print(f"✗ Write failed: {e}")
            return False

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to write value (network): {str(e)}")
            print(f"✗ Write failed (network): {e}")
            return False

    def write_heat_curve_point(self, index: int, temperature: float):
        """
        Convenience method to write a heat curve point.

        Args:
            index: Heat curve index (64-73)
            temperature: Target supply temperature in °C

        Returns:
            bool: True if write was successful
        """
        if not 64 <= index <= 73:
            self.logger.error(f"Heat curve index must be 64-73, got {index}")
            return False

        path = f"Cwl.Advise.A[{index}]"
        return self.write_value(path, temperature)

"""
Fetcher Deployer - Automated container deployment for new customers

Handles:
- Creating customer profiles
- Creating environment files
- Deploying Docker containers
- Managing container lifecycle
"""

import os
import json
import subprocess
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

import docker
from docker.errors import NotFound, APIError

# Configure logging
logger = logging.getLogger(__name__)


class FetcherDeployer:
    """Manages fetcher container deployment for customers."""

    def __init__(self):
        self.docker_client = docker.from_env()
        self.project_root = '/opt/dev/homeside-fetcher'
        self.envs_dir = os.path.join(self.project_root, 'envs')
        self.profiles_dir = os.path.join(self.project_root, 'profiles')
        self.image_name = 'homeside-fetcher-homeside-fetcher:latest'
        self.network_name = os.environ.get('DOCKER_NETWORK', 'dryckesmail_beer-network')

    def deploy_fetcher(
        self,
        customer_id: str,
        friendly_name: str,
        homeside_username: str,
        homeside_password: str,
        latitude: float = 56.66157,
        longitude: float = 12.77318,
        meter_ids: List[str] = None
    ) -> Dict[str, Any]:
        """
        Deploy a new fetcher container for a customer.

        Args:
            customer_id: HomeSide client ID (e.g., "HEM_FJV_Villa_149")
            friendly_name: Human-readable name (e.g., "Daggis8")
            homeside_username: HomeSide login username
            homeside_password: HomeSide login password
            latitude: Location latitude for weather (default: Halmstad)
            longitude: Location longitude for weather
            meter_ids: List of energy meter IDs for this house

        Returns:
            Dict with status and details
        """
        result = {
            'success': False,
            'customer_id': customer_id,
            'friendly_name': friendly_name,
            'steps': []
        }

        try:
            # Step 1: Create customer profile
            profile_created = self._create_customer_profile(customer_id, friendly_name, meter_ids=meter_ids or [])
            result['steps'].append({
                'step': 'create_profile',
                'success': profile_created,
                'path': os.path.join(self.profiles_dir, f'{customer_id}.json')
            })
            if not profile_created:
                return result

            # Step 2: Create environment file
            env_path = self._create_env_file(
                customer_id, friendly_name,
                homeside_username, homeside_password,
                latitude, longitude
            )
            result['steps'].append({
                'step': 'create_env',
                'success': bool(env_path),
                'path': env_path
            })
            if not env_path:
                return result

            # Step 3: Start Docker container
            container_name = f'homeside-fetcher-{customer_id}'
            container = self._start_container(customer_id, env_path)
            result['steps'].append({
                'step': 'start_container',
                'success': bool(container),
                'container_name': container_name,
                'container_id': container.short_id if container else None
            })
            if not container:
                return result

            result['success'] = True
            result['container_name'] = container_name
            logger.info(f"Successfully deployed fetcher for {friendly_name} ({customer_id})")

        except Exception as e:
            logger.error(f"Failed to deploy fetcher for {customer_id}: {e}")
            result['error'] = str(e)

        return result

    def _create_customer_profile(self, customer_id: str, friendly_name: str, meter_ids: List[str] = None) -> bool:
        """Create a new customer profile with default settings."""
        profile_path = os.path.join(self.profiles_dir, f'{customer_id}.json')

        # Don't overwrite existing profile
        if os.path.exists(profile_path):
            logger.info(f"Profile already exists for {customer_id}")
            # Update meter_ids if provided and profile exists
            if meter_ids:
                try:
                    with open(profile_path, 'r') as f:
                        existing = json.load(f)
                    # Only update if meter_ids not already set
                    if not existing.get('meter_ids'):
                        existing['meter_ids'] = meter_ids
                        with open(profile_path, 'w') as f:
                            json.dump(existing, f, indent=2)
                        logger.info(f"Updated meter_ids for {customer_id}: {meter_ids}")
                except Exception as e:
                    logger.warning(f"Failed to update meter_ids for {customer_id}: {e}")
            return True

        profile = {
            "schema_version": 1,
            "customer_id": customer_id,
            "friendly_name": friendly_name,
            "meter_ids": meter_ids or [],
            "building": {
                "description": "",
                "thermal_response": "medium"
            },
            "comfort": {
                "target_indoor_temp": 22.0,
                "acceptable_deviation": 1.0
            },
            "heating_system": {
                "response_time_minutes": 30,
                "max_supply_temp": 55
            },
            "learned": {
                "thermal_coefficient": None,
                "thermal_coefficient_confidence": 0.0,
                "hourly_bias": {},
                "samples_since_last_update": 0,
                "total_samples": 0,
                "next_update_at_samples": 24,
                "updated_at": None
            }
        }

        try:
            os.makedirs(self.profiles_dir, exist_ok=True)
            with open(profile_path, 'w') as f:
                json.dump(profile, f, indent=2)
            logger.info(f"Created profile for {customer_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to create profile: {e}")
            return False

    def _create_env_file(
        self,
        customer_id: str,
        friendly_name: str,
        homeside_username: str,
        homeside_password: str,
        latitude: float,
        longitude: float
    ) -> Optional[str]:
        """Create environment file for the fetcher container."""
        env_path = os.path.join(self.envs_dir, f'{customer_id}.env')

        # Read shared settings from main .env
        shared_settings = self._get_shared_settings()

        env_content = f"""# Auto-generated environment file for {friendly_name}
# Created: {datetime.now().isoformat()}

# HomeSide Credentials
HOMESIDE_USERNAME={homeside_username}
HOMESIDE_PASSWORD={homeside_password}
HOMESIDE_CLIENTID=

# Site Identity
FRIENDLY_NAME={friendly_name}
DISPLAY_NAME_SOURCE=friendly_name

# Polling
POLL_INTERVAL_MINUTES=5

# Seq Logging
SEQ_URL={shared_settings.get('SEQ_URL', 'http://seq:5341')}
SEQ_API_KEY={shared_settings.get('SEQ_API_KEY', '')}

# Logging
LOG_LEVEL=INFO
DEBUG_MODE=false

# InfluxDB
INFLUXDB_URL={shared_settings.get('INFLUXDB_URL', 'http://influxdb:8086')}
INFLUXDB_TOKEN={shared_settings.get('INFLUXDB_TOKEN', '')}
INFLUXDB_ORG={shared_settings.get('INFLUXDB_ORG', 'homeside')}
INFLUXDB_BUCKET={shared_settings.get('INFLUXDB_BUCKET', 'heating')}
INFLUXDB_ENABLED=true

# Location for weather
LATITUDE={latitude}
LONGITUDE={longitude}
"""

        try:
            os.makedirs(self.envs_dir, exist_ok=True)
            with open(env_path, 'w') as f:
                f.write(env_content)
            # Restrict permissions
            os.chmod(env_path, 0o600)
            logger.info(f"Created env file: {env_path}")
            return env_path
        except Exception as e:
            logger.error(f"Failed to create env file: {e}")
            return None

    def _get_shared_settings(self) -> Dict[str, str]:
        """Read shared settings from the main .env file."""
        main_env = os.path.join(self.project_root, '.env')
        settings = {}

        if os.path.exists(main_env):
            with open(main_env, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, _, value = line.partition('=')
                        settings[key.strip()] = value.strip()

        return settings

    def _start_container(self, customer_id: str, env_path: str) -> Optional[Any]:
        """Start a Docker container for the customer."""
        container_name = f'homeside-fetcher-{customer_id}'

        # Check if container already exists
        try:
            existing = self.docker_client.containers.get(container_name)
            if existing.status == 'running':
                logger.info(f"Container {container_name} already running")
                return existing
            else:
                # Remove stopped container
                existing.remove()
                logger.info(f"Removed stopped container {container_name}")
        except NotFound:
            pass  # Container doesn't exist, that's fine

        try:
            # Read env file content
            env_vars = {}
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, _, value = line.partition('=')
                        env_vars[key.strip()] = value.strip()

            container = self.docker_client.containers.run(
                self.image_name,
                name=container_name,
                detach=True,
                restart_policy={'Name': 'unless-stopped'},
                environment=env_vars,
                network=self.network_name,
                labels={
                    'com.homeside.customer_id': customer_id,
                    'com.homeside.managed': 'true',
                    'com.homeside.created': datetime.now().isoformat()
                },
                healthcheck={
                    'test': ['CMD', 'python', '-c',
                             "import os; exit(0 if any('HSF_Fetcher' in open(f'/proc/{p}/cmdline').read() for p in os.listdir('/proc') if p.isdigit() and os.path.exists(f'/proc/{p}/cmdline')) else 1)"],
                    'interval': 30000000000,  # 30s in nanoseconds
                    'timeout': 10000000000,   # 10s in nanoseconds
                    'retries': 3,
                    'start_period': 60000000000  # 60s in nanoseconds
                },
                log_config={
                    'type': 'json-file',
                    'config': {
                        'max-size': '10m',
                        'max-file': '3'
                    }
                }
            )
            logger.info(f"Started container {container_name}: {container.short_id}")
            return container

        except APIError as e:
            logger.error(f"Docker API error starting container: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to start container: {e}")
            return None

    def stop_fetcher(self, customer_id: str) -> bool:
        """Stop and remove a customer's fetcher container."""
        container_name = f'homeside-fetcher-{customer_id}'

        try:
            container = self.docker_client.containers.get(container_name)
            container.stop(timeout=10)
            container.remove()
            logger.info(f"Stopped and removed container {container_name}")
            return True
        except NotFound:
            logger.warning(f"Container {container_name} not found")
            return True  # Already gone, that's fine
        except Exception as e:
            logger.error(f"Failed to stop container {container_name}: {e}")
            return False

    def get_fetcher_status(self, customer_id: str) -> Dict[str, Any]:
        """Get the status of a customer's fetcher container."""
        container_name = f'homeside-fetcher-{customer_id}'

        try:
            container = self.docker_client.containers.get(container_name)
            return {
                'exists': True,
                'status': container.status,
                'running': container.status == 'running',
                'container_id': container.short_id,
                'created': container.attrs.get('Created', ''),
                'labels': container.labels
            }
        except NotFound:
            return {
                'exists': False,
                'status': 'not_found',
                'running': False
            }
        except Exception as e:
            return {
                'exists': False,
                'status': 'error',
                'running': False,
                'error': str(e)
            }

    def list_managed_fetchers(self) -> list:
        """List all fetcher containers managed by this system."""
        try:
            containers = self.docker_client.containers.list(
                all=True,
                filters={'label': 'com.homeside.managed=true'}
            )
            return [
                {
                    'name': c.name,
                    'status': c.status,
                    'customer_id': c.labels.get('com.homeside.customer_id', ''),
                    'created': c.labels.get('com.homeside.created', '')
                }
                for c in containers
            ]
        except Exception as e:
            logger.error(f"Failed to list containers: {e}")
            return []

    # =========================================================================
    # Offboarding Methods
    # =========================================================================

    def soft_offboard(self, customer_id: str) -> Dict[str, Any]:
        """
        Phase 1: Stop data collection but retain data.
        - Stop Docker container
        - Keep profile, env file, InfluxDB data
        """
        result = {
            'success': False,
            'customer_id': customer_id,
            'steps': []
        }

        try:
            # Stop the container (but don't remove env/profile)
            container_stopped = self.stop_fetcher(customer_id)
            result['steps'].append({
                'step': 'stop_container',
                'success': container_stopped
            })

            result['success'] = container_stopped
            if container_stopped:
                logger.info(f"Soft offboarded customer {customer_id} - container stopped")

        except Exception as e:
            logger.error(f"Failed to soft offboard {customer_id}: {e}")
            result['error'] = str(e)

        return result

    def hard_offboard(self, customer_id: str, gui_username: str = None) -> Dict[str, Any]:
        """
        Phase 2: Complete data removal.
        - Remove Docker container (if exists)
        - Delete customer profile
        - Delete environment file
        - Delete InfluxDB data
        - Remove htpasswd entry (if gui_username provided)
        """
        result = {
            'success': True,  # Start optimistic, mark false on critical failure
            'customer_id': customer_id,
            'steps': []
        }

        try:
            # Step 1: Remove container (if exists)
            container_removed = self.stop_fetcher(customer_id)
            result['steps'].append({
                'step': 'remove_container',
                'success': container_removed
            })

            # Step 2: Delete customer profile
            profile_deleted = self.delete_customer_profile(customer_id)
            result['steps'].append({
                'step': 'delete_profile',
                'success': profile_deleted
            })

            # Step 3: Delete environment file
            env_deleted = self.delete_env_file(customer_id)
            result['steps'].append({
                'step': 'delete_env',
                'success': env_deleted
            })

            # Step 4: Remove .env credential lines (HOUSE_{id}_*)
            env_creds_removed = self.remove_env_credentials(customer_id)
            result['steps'].append({
                'step': 'remove_env_credentials',
                'success': env_creds_removed
            })

            # Step 5: Delete InfluxDB data
            influx_deleted = self.delete_influxdb_data(customer_id)
            result['steps'].append({
                'step': 'delete_influxdb',
                'success': influx_deleted
            })

            # Step 6: Re-sync Dropbox meter CSV
            try:
                import sys
                sys.path.insert(0, self.project_root)
                from dropbox_sync import sync_meters
                dropbox_synced = sync_meters()
            except Exception as e:
                logger.warning(f"Dropbox sync failed (non-fatal): {e}")
                dropbox_synced = False
            result['steps'].append({
                'step': 'dropbox_sync',
                'success': dropbox_synced
            })

            # Step 7: Remove htpasswd entry (if username provided)
            if gui_username:
                htpasswd_removed = delete_htpasswd_entry(gui_username)
                result['steps'].append({
                    'step': 'delete_htpasswd',
                    'success': htpasswd_removed
                })

            # Check if all critical steps succeeded
            failed_steps = [s for s in result['steps'] if not s['success']]
            if failed_steps:
                result['success'] = False
                result['failed_steps'] = [s['step'] for s in failed_steps]

            logger.info(f"Hard offboarded customer {customer_id}: {result}")

        except Exception as e:
            logger.error(f"Failed to hard offboard {customer_id}: {e}")
            result['success'] = False
            result['error'] = str(e)

        return result

    def delete_customer_profile(self, customer_id: str) -> bool:
        """Delete profiles/{customer_id}.json and {customer_id}_signals.json."""
        success = True

        for suffix in [f'{customer_id}.json', f'{customer_id}_signals.json']:
            path = os.path.join(self.profiles_dir, suffix)
            if not os.path.exists(path):
                logger.info(f"Profile {path} already deleted")
                continue
            try:
                os.remove(path)
                logger.info(f"Deleted: {path}")
            except Exception as e:
                logger.error(f"Failed to delete {path}: {e}")
                success = False

        return success

    def remove_env_credentials(self, customer_id: str) -> bool:
        """Remove HOUSE_{customer_id}_* lines (and preceding comment) from .env."""
        env_path = os.path.join(self.project_root, '.env')
        if not os.path.exists(env_path):
            return True

        try:
            with open(env_path, 'r') as f:
                lines = f.readlines()

            prefix = f'HOUSE_{customer_id}_'
            remove_indices = set()
            for i, line in enumerate(lines):
                if line.strip().startswith(prefix):
                    remove_indices.add(i)
                    if i > 0 and lines[i - 1].strip().startswith('#'):
                        remove_indices.add(i - 1)

            if not remove_indices:
                logger.info(f"No .env entries found for {customer_id}")
                return True

            with open(env_path, 'w') as f:
                for i, line in enumerate(lines):
                    if i not in remove_indices:
                        f.write(line)

            logger.info(f"Removed {len(remove_indices)} .env lines for {customer_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to remove .env entries for {customer_id}: {e}")
            return False

    def delete_env_file(self, customer_id: str) -> bool:
        """Delete envs/{customer_id}.env"""
        env_path = os.path.join(self.envs_dir, f'{customer_id}.env')

        if not os.path.exists(env_path):
            logger.info(f"Env file {env_path} already deleted")
            return True

        try:
            os.remove(env_path)
            logger.info(f"Deleted env file: {env_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete env file {env_path}: {e}")
            return False

    def delete_influxdb_data(self, customer_id: str) -> bool:
        """Delete all time-series data for a customer from InfluxDB."""
        try:
            from influxdb_client import InfluxDBClient
            from datetime import timezone

            settings = self._get_shared_settings()

            # Check if InfluxDB settings are configured
            influx_url = settings.get('INFLUXDB_URL')
            influx_token = settings.get('INFLUXDB_TOKEN')
            influx_org = settings.get('INFLUXDB_ORG')
            influx_bucket = settings.get('INFLUXDB_BUCKET')

            if not all([influx_url, influx_token, influx_org, influx_bucket]):
                logger.warning("InfluxDB not fully configured, skipping data deletion")
                return True  # Not a failure, just not configured

            client = InfluxDBClient(
                url=influx_url,
                token=influx_token,
                org=influx_org
            )

            delete_api = client.delete_api()

            # Delete all data matching this customer's house_id
            # Use very wide time range to catch all data
            start = datetime(2020, 1, 1, tzinfo=timezone.utc)
            stop = datetime(2099, 12, 31, tzinfo=timezone.utc)

            # Delete from bucket using house_id tag
            # The predicate uses InfluxDB's delete predicate syntax
            predicate = f'house_id="{customer_id}"'

            delete_api.delete(
                start=start,
                stop=stop,
                predicate=predicate,
                bucket=influx_bucket
            )

            logger.info(f"Deleted InfluxDB data for house_id={customer_id}")
            client.close()
            return True

        except ImportError:
            logger.warning("influxdb_client not installed, skipping InfluxDB deletion")
            return True
        except Exception as e:
            logger.error(f"Failed to delete InfluxDB data for {customer_id}: {e}")
            return False


def create_htpasswd_entry(username: str, password: str) -> bool:
    """
    Add user to nginx htpasswd file for web access.

    Uses stdin (-i flag) to pass password safely, avoiding shell interpretation
    of special characters.

    Requires sudo access to htpasswd command.
    """
    try:
        # Use -i flag to read password from stdin - handles special characters safely
        result = subprocess.run(
            ['sudo', 'htpasswd', '-i', '/etc/nginx/.htpasswd', username],
            input=password,
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            logger.info(f"Created htpasswd entry for {username}")
            return True
        else:
            logger.error(f"htpasswd failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("htpasswd command timed out")
        return False
    except Exception as e:
        logger.error(f"Failed to create htpasswd entry: {e}")
        return False


def extract_customer_id_from_client_path(client_path: str) -> str:
    """
    Extract customer_id from full HomeSide client path.

    Example:
        "38/Account/HEM_FJV_149/HEM_FJV_Villa_149" -> "HEM_FJV_Villa_149"
    """
    if not client_path:
        return ""
    parts = client_path.split('/')
    return parts[-1] if parts else ""


def delete_htpasswd_entry(username: str) -> bool:
    """
    Remove user from nginx htpasswd file.

    Requires sudo access to htpasswd command.
    """
    try:
        result = subprocess.run(
            ['sudo', 'htpasswd', '-D', '/etc/nginx/.htpasswd', username],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            logger.info(f"Removed htpasswd entry for {username}")
            return True
        else:
            # User might not exist in htpasswd, which is fine
            if 'not found' in result.stderr.lower():
                logger.info(f"User {username} not found in htpasswd (already removed)")
                return True
            logger.error(f"htpasswd -D failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("htpasswd command timed out")
        return False
    except Exception as e:
        logger.error(f"Failed to remove htpasswd entry: {e}")
        return False

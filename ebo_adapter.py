"""
EBO BMS Adapter

Wraps EboApi to match the interface that building_fetcher.py expects,
so EBO buildings can be polled using the same fetch_and_write() loop as Arrigo.

Duck-type interface required by building_fetcher.py:
    client.login() -> bool
    client.discover_signals() -> bool
    client.signal_map: dict[signal_id -> {current_value, name, unit}]
    client.get_alarms(first=100) -> list

Signal paths use three strategies:
    - Direct: "/Site/Folder/Variable/Value" — read via get_objects on the object path
    - IO Bus: "/Site/IO Bus/PointName/Value" — read via batch browse of IO Bus folder
    - Trend log fallback: For signals unreachable via API (e.g. LonWorks DUC points),
      reads the latest trend log record as a near-live value.

The adapter auto-detects IO Bus paths and batches them into a single folder read.
"""

import struct
import logging
import time
from datetime import datetime, timezone, timedelta
from ebo_api import EboApi, EboApiError


def _hex_to_double(hexval):
    """Convert EBO hex-encoded IEEE 754 double to float."""
    if isinstance(hexval, str) and hexval.startswith('0x'):
        try:
            return struct.unpack('d', struct.pack('Q', int(hexval, 16)))[0]
        except (ValueError, struct.error):
            return None
    if isinstance(hexval, (int, float)):
        return float(hexval)
    return None


class EboBmsAdapter:
    """Adapter making EboApi compatible with building_fetcher's BMS interface."""

    def __init__(self, base_url, username, password, domain="",
                 logger=None, verbose=False, verify_ssl=True):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.domain = domain
        self.logger = logger or logging.getLogger(__name__)
        self.verbose = verbose
        self.verify_ssl = verify_ssl

        self.ebo = EboApi(base_url, username, password, domain,
                          verify_ssl=verify_ssl)
        self.signal_map = {}       # signal_id -> {current_value, name, unit}
        self._signal_paths = []    # configured signal paths (with /Value suffix)
        self._trend_fallbacks = {} # signal_path -> trend_log_path (for unreachable signals)
        self._history_client = None

    def login(self) -> bool:
        """Authenticate and initialize EBO session.

        Retries on 'already logged on' (code 131094) with a short delay,
        since multiple fetchers sharing the same credentials may collide
        during startup.
        """
        for attempt in range(3):
            try:
                self.ebo.login()
                self.ebo.web_entry()
                self.logger.info(f"EBO: Logged in to {self.base_url}")
                return True
            except EboApiError as e:
                if e.code == 131094 and attempt < 2:
                    delay = 5 * (attempt + 1)
                    self.logger.info(f"EBO: Session conflict, retrying in {delay}s (attempt {attempt + 1}/3)")
                    time.sleep(delay)
                    continue
                self.logger.error(f"EBO login failed: {e}")
                return False
            except Exception as e:
                self.logger.error(f"EBO login failed: {e}")
                return False
        return False

    def configure_signals(self, signal_paths: list):
        """Set which signal paths to read (call before first discover_signals).

        Args:
            signal_paths: list of EBO property paths ending in /Value
        """
        self._signal_paths = list(signal_paths)

    def configure_trend_fallbacks(self, fallbacks: dict):
        """Set trend log fallback paths for signals unreachable via get_objects.

        Args:
            fallbacks: dict of signal_path -> trend_log_path
        """
        self._trend_fallbacks = dict(fallbacks)

    def _relogin(self) -> bool:
        """Re-authenticate to EBO. Returns True on success.

        If 'already logged on' (131094) and we already have a session token,
        the existing session is still valid — treat as success.
        """
        try:
            self.ebo.login()
            self.ebo.web_entry()
            self.logger.info(f"EBO: Re-authenticated to {self.base_url}")
            return True
        except EboApiError as e:
            if e.code == 131094 and self.ebo.session_token:
                self.logger.info("EBO: Session still active (already logged on), continuing")
                return True
            self.logger.warning(f"EBO: Re-login failed: {e}")
            return False
        except Exception as e:
            self.logger.warning(f"EBO: Re-login failed: {e}")
            return False

    def discover_signals(self) -> bool:
        """Read current values via get_objects, populate self.signal_map.

        Uses two strategies:
        1. IO Bus paths: batch-read via get_objects on the IO Bus folder
        2. Other paths: batch-read via get_objects on individual object paths

        Only re-authenticates when a read fails, to avoid invalidating
        sessions shared across fetchers using the same credentials.
        """
        if not self._signal_paths:
            self.logger.warning("EBO: No signal paths configured")
            return False

        return self._do_discover(retry_on_fail=True)

    def _do_discover(self, retry_on_fail=True) -> bool:
        """Internal discover implementation with optional auth-retry."""
        try:
            # Classify paths: IO Bus vs server variables
            io_bus_paths = {}  # io_bus_folder -> {point_name -> original_signal_path}
            direct_paths = []  # (object_path, original_signal_path)

            for signal_path in self._signal_paths:
                # Strip /Value suffix to get object path
                obj_path = signal_path.rsplit('/Value', 1)[0] if signal_path.endswith('/Value') else signal_path

                if '/IO Bus/' in obj_path:
                    # IO Bus point: extract folder and point name
                    # Path format: /Site/IO Bus/PointName or /Site/IO Bus/Module/PointName
                    parts = obj_path.split('/IO Bus/', 1)
                    io_folder = parts[0] + '/IO Bus'
                    point_name = obj_path.split('/')[-1]  # last segment is the point name
                    io_bus_paths.setdefault(io_folder, {})[point_name] = signal_path
                else:
                    direct_paths.append((obj_path, signal_path))

            read_count = 0

            self.logger.debug(f"EBO discover: {len(io_bus_paths)} IO Bus folder(s), {len(direct_paths)} direct path(s)")

            # Strategy 1: Batch-read IO Bus folders
            for io_folder, points in io_bus_paths.items():
                try:
                    result = self.ebo.get_objects([io_folder], levels=2, include_hidden=True)
                    res = result.get('GetObjectsRes', result)
                    for r in res.get('results', []):
                        name = r.get('name', '')
                        if name in points:
                            props = r.get('properties', {})
                            value_prop = props.get('Value', {})
                            raw_val = value_prop.get('value')
                            unit = value_prop.get('unitDisplayName', '')
                            decoded = _hex_to_double(raw_val)
                            if decoded is not None:
                                self.signal_map[points[name]] = {
                                    'current_value': decoded,
                                    'name': name,
                                    'unit': unit,
                                }
                                read_count += 1
                except Exception as e:
                    self.logger.warning(f"EBO: Failed to read IO Bus {io_folder}: {e}")

            self.logger.debug(f"EBO: IO Bus read {read_count} signals")

            # Strategy 2: Batch-read server variables via get_objects
            if direct_paths:
                obj_paths = [op for op, sp in direct_paths]
                try:
                    result = self.ebo.get_objects(obj_paths, levels=0, include_hidden=True)
                    res = result.get('GetObjectsRes', result)

                    # Build lookup from returned path -> result
                    result_map = {}
                    for r in res.get('results', []):
                        rpath = r.get('path', '')
                        rname = r.get('name', '')
                        full_path = f"{rpath}/{rname}" if rpath else rname
                        result_map[full_path] = r

                    for obj_path, signal_path in direct_paths:
                        r = result_map.get(obj_path)
                        if not r:
                            continue
                        props = r.get('properties', {})
                        value_prop = props.get('Value', {})
                        raw_val = value_prop.get('value')
                        unit = value_prop.get('unitDisplayName', '')
                        name = r.get('name', obj_path.split('/')[-1])
                        decoded = _hex_to_double(raw_val)
                        if decoded is not None:
                            self.signal_map[signal_path] = {
                                'current_value': decoded,
                                'name': name,
                                'unit': unit,
                            }
                            read_count += 1
                except Exception as e:
                    self.logger.warning(f"EBO: Failed to read server variables: {e}")

            # Strategy 3: Trend log fallback for signals not yet read
            if self._trend_fallbacks:
                missing = [sp for sp in self._signal_paths
                           if sp not in self.signal_map and sp in self._trend_fallbacks]
                if missing:
                    trend_count = self._read_trend_fallbacks(missing)
                    read_count += trend_count

            self.logger.info(f"EBO: Read {read_count}/{len(self._signal_paths)} signals")

            # If we got zero signals and haven't retried yet, re-authenticate and try once more
            if read_count == 0 and retry_on_fail:
                self.logger.info("EBO: Got 0 signals, re-authenticating and retrying...")
                if self._relogin():
                    return self._do_discover(retry_on_fail=False)

            return read_count > 0

        except Exception as e:
            self.logger.error(f"EBO discover_signals failed: {e}")
            if retry_on_fail:
                self.logger.info("EBO: Retrying after re-authentication...")
                if self._relogin():
                    return self._do_discover(retry_on_fail=False)
            return False

    def _read_trend_fallbacks(self, missing_paths: list) -> int:
        """Read latest trend log records for signals not reachable via get_objects."""
        if not self._history_client:
            try:
                from ebo_history import EboHistoryClient
                self._history_client = EboHistoryClient(
                    base_url=self.base_url,
                    csrf_token=self.ebo.session_token,
                    session=self.ebo.session,
                )
            except Exception as e:
                self.logger.warning(f"EBO: Failed to create history client: {e}")
                return 0

        # Update history client session after re-login
        self._history_client.csrf_token = self.ebo.session_token
        self._history_client.session = self.ebo.session

        count = 0
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=30)
        start_us = str(int(start.timestamp() * 1_000_000))
        end_us = str(int(now.timestamp() * 1_000_000))

        from ebo_history import UNIT_NONE

        for signal_path in missing_paths:
            trend_log = self._trend_fallbacks[signal_path]
            try:
                records = self._history_client.read_trend_log(
                    log_path=trend_log,
                    log_unit_id=UNIT_NONE,
                    display_unit_id=UNIT_NONE,
                    max_records=5,
                    page_size=5,
                    start_time_utc=start_us,
                    end_time_utc=end_us,
                )
                if records:
                    latest = records[-1]
                    self.signal_map[signal_path] = {
                        'current_value': latest.value,
                        'name': trend_log.split('/')[-1],
                        'unit': '',
                    }
                    count += 1
                    self.logger.debug(
                        f"EBO trend fallback: {trend_log.split('/')[-1]} = {latest.value:.2f}")
            except Exception as e:
                self.logger.warning(f"EBO: Trend fallback failed for {trend_log}: {e}")

        if count:
            self.logger.debug(f"EBO: Trend fallback read {count}/{len(missing_paths)} signals")
        return count

    def get_alarms(self, first=100):
        """EBO alarm fetch — not implemented initially."""
        return []

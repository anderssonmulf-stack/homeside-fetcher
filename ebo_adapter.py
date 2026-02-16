"""
EBO BMS Adapter

Wraps EboApi to match the interface that building_fetcher.py expects,
so EBO buildings can be polled using the same fetch_and_write() loop as Arrigo.

Duck-type interface required by building_fetcher.py:
    client.login() -> bool
    client.discover_signals() -> bool
    client.signal_map: dict[signal_id -> {current_value, name, unit}]
    client.get_alarms(first=100) -> list

Signal paths use two formats in building configs:
    - Direct: "/Site/Folder/Variable/Value" — read via get_objects on the object path
    - IO Bus: "/Site/IO Bus/PointName/Value" — read via batch browse of IO Bus folder

The adapter auto-detects IO Bus paths and batches them into a single folder read.
"""

import struct
import logging
from ebo_api import EboApi


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

    def login(self) -> bool:
        """Authenticate and initialize EBO session."""
        try:
            self.ebo.login()
            self.ebo.web_entry()
            self.logger.info(f"EBO: Logged in to {self.base_url}")
            return True
        except Exception as e:
            self.logger.error(f"EBO login failed: {e}")
            return False

    def configure_signals(self, signal_paths: list):
        """Set which signal paths to read (call before first discover_signals).

        Args:
            signal_paths: list of EBO property paths ending in /Value
        """
        self._signal_paths = list(signal_paths)

    def discover_signals(self) -> bool:
        """Read current values via get_objects, populate self.signal_map.

        Uses two strategies:
        1. IO Bus paths: batch-read via get_objects on the IO Bus folder
        2. Other paths: batch-read via get_objects on individual object paths
        """
        if not self._signal_paths:
            self.logger.warning("EBO: No signal paths configured")
            return False

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

            if self.verbose:
                self.logger.debug(f"EBO: Read {read_count}/{len(self._signal_paths)} signals")

            return read_count > 0

        except Exception as e:
            self.logger.error(f"EBO discover_signals failed: {e}")
            return False

    def get_alarms(self, first=100):
        """EBO alarm fetch — not implemented initially."""
        return []

"""
EBO BMS Adapter

Wraps EboApi to match the interface that building_fetcher.py expects,
so EBO buildings can be polled using the same fetch_and_write() loop as Arrigo.

Duck-type interface required by building_fetcher.py:
    client.login() -> bool
    client.discover_signals() -> bool
    client.signal_map: dict[signal_id -> {current_value, name, unit}]
    client.get_alarms(first=100) -> list
"""

import logging
from ebo_api import EboApi


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
        self._subscription_handle = None
        self._subscription_paths = []  # ordered list matching subscription indices

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
        """Set which signal paths to subscribe to (call before first discover_signals)."""
        self._subscription_paths = list(signal_paths)

    def discover_signals(self) -> bool:
        """Read current values via subscription, populate self.signal_map."""
        if not self._subscription_paths:
            self.logger.warning("EBO: No signal paths configured")
            return False

        try:
            if not self._subscription_handle:
                # First call: create subscription with all configured signal paths
                result = self.ebo.create_subscription(self._subscription_paths)
                self._subscription_handle = result.get('handle')
                items = result.get('items', [])
            else:
                # Subsequent calls: poll existing subscription
                result = self.ebo.read_subscription(self._subscription_handle)
                items = result.get('items', [])

            parsed = EboApi.parse_subscription_items(items)

            for i, entry in enumerate(parsed):
                if i >= len(self._subscription_paths):
                    break
                path = self._subscription_paths[i]
                self.signal_map[path] = {
                    'current_value': entry['value'],
                    'name': path.split('/')[-1],
                    'unit': entry['unit'],
                }

            if self.verbose:
                self.logger.debug(f"EBO: Read {len(parsed)} signals")

            return True

        except Exception as e:
            self.logger.error(f"EBO discover_signals failed: {e}")
            # Reset subscription handle so next call creates a fresh one
            self._subscription_handle = None
            return False

    def get_alarms(self, first=100):
        """EBO alarm fetch — not implemented initially."""
        return []

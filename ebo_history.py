"""
EBO (EcoStruxure Building Operation) — Historical Data Client

Provides functions for fetching historical trend log data from EBO servers.
Assumes login/session management is already handled externally.

Usage:
    from ebo_history import EboHistoryClient

    client = EboHistoryClient(base_url="https://ebo.halmstad.se", csrf_token="28024217")

    # Discover chart series
    series = client.get_chart_config("/Kattegattgymnasiet 20942 AS3/Effektstyrning/Diagram/Effektstyrning")

    # Fetch historical data for a single log
    records = client.read_trend_log(
        log_path="/Kattegattgymnasiet 20942 AS3/Effektstyrning/Trendloggar/GT1_BB_Logg",
        log_unit_id=2621441,
        display_unit_id=2621441,  # Use 2621441 for °C, 2621443 for °F
        max_records=10000,
    )

    # Fetch all logs from a chart
    all_data = client.read_all_chart_logs(
        chart_path="/Kattegattgymnasiet 20942 AS3/Effektstyrning/Diagram/Effektstyrning",
        max_records_per_log=10000,
    )
"""

import base64
import struct
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Unit ID constants
# ---------------------------------------------------------------------------
UNIT_PERCENT = 2097153
UNIT_CELSIUS = 2621441
UNIT_FAHRENHEIT = 2621443
UNIT_KW = 52494337
UNIT_NONE = 65537


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class TrendRecord:
    """A single historical data point."""
    timestamp_ms: int
    value: float
    sequence_nr: int
    status: int

    @property
    def timestamp_utc(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ms / 1000, tz=timezone.utc)

    @property
    def timestamp_iso(self) -> str:
        return self.timestamp_utc.isoformat()


@dataclass
class TrendLogSeries:
    """Metadata for a trend log series (from GetInitialTrendData)."""
    name: str
    description: str
    display_log_path: str
    display_signal_path: str
    unit_name: str
    configured_unit_id: int
    display_unit_id: int
    point_display_unit_id: int
    point_configured_unit_id: int
    y_axis_location: int  # 0=left, 1=right

    @property
    def log_array_path(self) -> str:
        """Full path including /LogArray suffix for ReadTimeBasedTrend."""
        return f"{self.display_log_path}/LogArray"


@dataclass
class TrendPage:
    """Result of a single ReadTimeBasedTrend call."""
    records: list[TrendRecord]
    first_record_us: int  # microseconds — from METADATA
    last_record_us: int   # microseconds — from METADATA
    total_in_page: int
    more_available: bool


# ---------------------------------------------------------------------------
# Value decoding
# ---------------------------------------------------------------------------
def decode_ebo_value(val) -> float:
    """Decode EBO value — base64-encoded IEEE 754 LE double, or plain number."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        padded = val + "=" * (-len(val) % 4)
        raw = base64.b64decode(padded)
        return struct.unpack("<d", raw)[0]
    return float(val)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class EboHistoryClient:
    """Client for reading historical trend data from EBO."""

    def __init__(
        self,
        base_url: str,
        csrf_token: str,
        session: Optional[requests.Session] = None,
        timeout: int = 30,
    ):
        """
        Args:
            base_url: EBO server URL, e.g. "https://ebo.halmstad.se"
            csrf_token: x-csrf-token value from login flow
            session: Optional existing requests.Session (for shared auth/cookies)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.csrf_token = csrf_token
        self.timeout = timeout
        self.session = session or requests.Session()

        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "x-csrf-token": self.csrf_token,
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/",
            }
        )

    @property
    def _endpoint(self) -> str:
        return f"{self.base_url}/json/POST"

    def _post(self, payload: dict) -> dict:
        """Send a command to the EBO JSON endpoint."""
        resp = self.session.post(self._endpoint, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # GetInitialTrendData — discover chart configuration
    # ------------------------------------------------------------------
    def get_chart_config(self, chart_path: str) -> list[TrendLogSeries]:
        """
        Get the trend log series configured in a chart view.

        Args:
            chart_path: Path to the chart object, e.g.
                "/Kattegattgymnasiet 20942 AS3/Effektstyrning/Diagram/Effektstyrning"

        Returns:
            List of TrendLogSeries with log paths, unit IDs, etc.
        """
        payload = {
            "command": "GetInitialTrendData",
            "path": chart_path,
            "viewType": "TrendChart",
        }
        result = self._post(payload)

        series_list = []
        for item in result.get("GetInitialTrendDataRes", []):
            series_list.append(
                TrendLogSeries(
                    name=item.get("Name", ""),
                    description=item.get("Description", ""),
                    display_log_path=item.get("DisplayLog", ""),
                    display_signal_path=item.get("DisplaySignal", ""),
                    unit_name=item.get("DisplayLogUnitName", ""),
                    configured_unit_id=int(item.get("ConfiguredLogUnitId", 0)),
                    display_unit_id=int(item.get("DisplayLogUnitId", 0)),
                    point_display_unit_id=int(item.get("PointDisplayUnitId", 0)),
                    point_configured_unit_id=int(item.get("PointConfiguredUnitId", 0)),
                    y_axis_location=int(item.get("YAxisLocation", 0)),
                )
            )
        logger.info("Found %d series in chart %s", len(series_list), chart_path)
        return series_list

    # ------------------------------------------------------------------
    # ReadTimeBasedTrend — fetch one page of historical data
    # ------------------------------------------------------------------
    def _read_trend_page(
        self,
        log_array_path: str,
        log_unit_id: int,
        display_unit_id: int,
        num_records: int = 4000,
        end_time_utc: str = "0",
        start_time_utc: str = "0",
        reverse: bool = True,
    ) -> TrendPage:
        """
        Fetch a single page of trend data.

        Args:
            log_array_path: Full path including /LogArray suffix
            log_unit_id: Storage unit ID (ConfiguredLogUnitId)
            display_unit_id: Display unit ID — determines the unit of returned values.
                Use ConfiguredLogUnitId for native unit (e.g. °C), or
                DisplayLogUnitId for display unit (e.g. °F)
            num_records: Number of records to request (max observed: 4000)
            end_time_utc: Upper time bound in microseconds (str). "0" = no limit (latest)
            start_time_utc: Lower time bound in microseconds (str). "0" = no limit (oldest)
            reverse: True = newest first (standard pagination direction)
        """
        payload = {
            "command": "ReadTimeBasedTrend",
            "path": log_array_path,
            "id": 0,
            "handle": 0,
            "deliveryType": 0,
            "startTime": "1970-01-01T00:00:00.000Z",
            "endTime": "1970-01-01T00:00:00.000Z",
            "startTimeUtc": start_time_utc,
            "endTimeUtc": end_time_utc,
            "reverse": reverse,
            "numberOfRequestedRecords": num_records,
            "filter": "",
            "logUnitId": log_unit_id,
            "logDisplayUnitId": display_unit_id,
            "pointDisplayUnitId": display_unit_id,
            "pointUnitId": log_unit_id,
        }

        result = self._post(payload)

        metadata = result.get("METADATA", [{}])[0]
        first_record_us = int(metadata.get("firstRecord", "0"))
        last_record_us = int(metadata.get("lastRecord", "0"))
        nbr_records = int(metadata.get("nbrOfRecords", "0"))
        more_available = metadata.get("moreDataAvailable", "0") == "1"

        records = []
        for array_res in result.get("ReadArrayRes", []):
            for row in array_res.get("data", []):
                try:
                    ts = row[0]
                    # Skip metadata rows (e.g. 'trend.record.TLogEventRecord')
                    if not isinstance(ts, (int, float)):
                        continue
                    ts_ms = int(ts)
                    # Skip obviously invalid timestamps (before year 2000)
                    if ts_ms < 946684800000:
                        continue
                    val = decode_ebo_value(row[1])
                    if len(row) >= 5:
                        seq = int(row[3]) if isinstance(row[3], (int, float)) else 0
                        status = int(row[4]) if isinstance(row[4], (int, float)) else 0
                    else:
                        seq, status = 0, 0
                    records.append(TrendRecord(
                        timestamp_ms=ts_ms, value=val,
                        sequence_nr=seq, status=status,
                    ))
                except (ValueError, TypeError, IndexError):
                    continue  # Skip unparseable rows

        return TrendPage(
            records=records,
            first_record_us=first_record_us,
            last_record_us=last_record_us,
            total_in_page=nbr_records,
            more_available=more_available,
        )

    # ------------------------------------------------------------------
    # Read full trend log with automatic pagination
    # ------------------------------------------------------------------
    def read_trend_log(
        self,
        log_path: str,
        log_unit_id: int,
        display_unit_id: int,
        max_records: int = 50000,
        page_size: int = 4000,
        start_time_utc: Optional[str] = None,
        end_time_utc: Optional[str] = None,
    ) -> list[TrendRecord]:
        """
        Read historical data from a trend log with automatic pagination.

        The log_path should NOT include /LogArray — it will be appended.

        Args:
            log_path: Path to the trend log, e.g.
                "/Kattegattgymnasiet 20942 AS3/Effektstyrning/Trendloggar/GT1_BB_Logg"
            log_unit_id: Storage unit (ConfiguredLogUnitId from chart config)
            display_unit_id: Display unit — set equal to log_unit_id for native units
            max_records: Safety limit on total records to fetch
            page_size: Records per API call (recommended: 4000)
            start_time_utc: Optional lower bound (microseconds, str). None = no limit
            end_time_utc: Optional upper bound (microseconds, str). None = latest data

        Returns:
            List of TrendRecord sorted chronologically (oldest first)
        """
        log_array_path = f"{log_path}/LogArray"
        all_records: list[TrendRecord] = []
        current_end_utc = end_time_utc or "0"
        current_start_utc = start_time_utc or "0"
        page_num = 0

        while len(all_records) < max_records:
            page_num += 1
            logger.info(
                "Fetching page %d for %s (endTimeUtc=%s, collected=%d)",
                page_num, log_path.split("/")[-1], current_end_utc, len(all_records),
            )

            page = self._read_trend_page(
                log_array_path=log_array_path,
                log_unit_id=log_unit_id,
                display_unit_id=display_unit_id,
                num_records=page_size,
                end_time_utc=current_end_utc,
                start_time_utc=current_start_utc,
                reverse=True,
            )

            all_records.extend(page.records)
            logger.info(
                "  Got %d records (total: %d), more_available=%s",
                len(page.records), len(all_records), page.more_available,
            )

            if not page.more_available or len(page.records) == 0:
                break

            # Pagination: set endTimeUtc to lastRecord - 1 (microseconds)
            current_end_utc = str(page.last_record_us - 1)

        # Sort chronologically (oldest first) since we read in reverse
        all_records.sort(key=lambda r: r.timestamp_ms)
        logger.info("Total records fetched for %s: %d", log_path.split("/")[-1], len(all_records))
        return all_records

    # ------------------------------------------------------------------
    # Convenience: read all logs in a chart
    # ------------------------------------------------------------------
    def read_all_chart_logs(
        self,
        chart_path: str,
        max_records_per_log: int = 50000,
        page_size: int = 4000,
        use_native_units: bool = True,
    ) -> dict[str, list[TrendRecord]]:
        """
        Discover all trend logs in a chart and fetch their historical data.

        Args:
            chart_path: Path to the chart view
            max_records_per_log: Max records per individual log
            page_size: Records per API call
            use_native_units: If True, returns values in configured unit (e.g. °C).
                If False, returns in display unit (e.g. °F).

        Returns:
            Dict mapping series name -> list of TrendRecord
        """
        series_list = self.get_chart_config(chart_path)
        result = {}

        for series in series_list:
            display_uid = (
                series.configured_unit_id if use_native_units else series.display_unit_id
            )
            logger.info(
                "Reading %s (%s) in %s...",
                series.name,
                series.description,
                series.unit_name if not use_native_units else "native unit",
            )
            records = self.read_trend_log(
                log_path=series.display_log_path,
                log_unit_id=series.configured_unit_id,
                display_unit_id=display_uid,
                max_records=max_records_per_log,
                page_size=page_size,
            )
            result[series.name] = records

        return result

    # ------------------------------------------------------------------
    # Helper: browse objects
    # ------------------------------------------------------------------
    def get_objects(
        self,
        path: str,
        property_names: Optional[list[str]] = None,
        levels: int = 0,
    ) -> dict:
        """
        Browse/read objects in the EBO object tree.

        Args:
            path: Object path
            property_names: Properties to include (e.g. ["Value", "DESCR"])
            levels: Depth of children to include (0 = just this object)
        """
        path_entry = {"path": path}
        if property_names:
            path_entry["propertyNames"] = property_names

        payload = {
            "command": "GetObjects",
            "paths": [path_entry],
            "levels": levels,
            "includeHidden": True,
            "dbMode": True,
            "includeAggregated": False,
        }
        return self._post(payload)

    def find_logs_for_point(self, point_value_path: str) -> dict:
        """
        Find which trend logs and charts reference a specific point.

        Args:
            point_value_path: Path to the point's Value property, e.g.
                "/..../Effekt_signal/Value"
        """
        payload = {
            "command": "GetContextMenuBackReferences",
            "path": point_value_path,
            "isobject": False,
        }
        return self._post(payload)

    def get_multi_property(self, property_paths: list[str]) -> dict:
        """Read multiple properties in a single call."""
        payload = {
            "command": "GetMultiProperty",
            "data": property_paths,
        }
        return self._post(payload)

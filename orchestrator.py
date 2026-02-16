#!/usr/bin/env python3
"""
Orchestrator — single Docker container that manages all fetcher subprocesses.

Scans profiles/ for private homes (→ HSF_Fetcher.py) and buildings/ for
commercial buildings (→ building_fetcher.py).  Rescans every 60 s to
pick up additions, removals and config changes without a container restart.

Environment variables:
    Shared infra (INFLUXDB_*, SEQ_*, LATITUDE, LONGITUDE, DROPBOX_*, …)
    are inherited by every child.

    Per-house credentials use the pattern:
        HOUSE_<customer_id>_USERNAME / HOUSE_<customer_id>_PASSWORD
    Per-building credentials:
        BUILDING_<building_id>_USERNAME / BUILDING_<building_id>_PASSWORD
"""

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------
SCAN_INTERVAL = 60          # seconds between directory rescans
PROFILES_DIR = "profiles"
BUILDINGS_DIR = "buildings"
OFFBOARDED_FILE = "offboarded.json"
RESTART_BACKOFF_BASE = 10   # seconds — doubles on each consecutive crash
RESTART_BACKOFF_MAX = 300   # cap at 5 minutes


# ---------------------------------------------------------------------------
#  Data classes
# ---------------------------------------------------------------------------
POLL_OFFSET_STEP = 10          # seconds between each child's poll offset


@dataclass
class Child:
    config_path: str            # e.g. "profiles/HEM_FJV_Villa_149.json"
    config_id: str              # e.g. "HEM_FJV_Villa_149"
    friendly_name: str
    kind: str                   # "house" or "building"
    poll_offset: int = 0        # seconds to stagger poll start
    process: subprocess.Popen | None = None
    consecutive_crashes: int = 0
    last_start: float = 0.0
    backoff_until: float = 0.0


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [orchestrator] {msg}", flush=True)


def load_json(path: str) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        log(f"Failed to load {path}: {e}")
        return None


def _env_key(prefix: str, config_id: str, suffix: str) -> str:
    """Build an env-var name like HOUSE_HEM_FJV_Villa_149_USERNAME."""
    return f"{prefix}_{config_id}_{suffix}"


# ---------------------------------------------------------------------------
#  Subprocess management
# ---------------------------------------------------------------------------
def build_house_env(config_id: str, friendly_name: str, poll_offset: int = 0) -> dict:
    """Return an env dict for a private-home fetcher subprocess."""
    env = os.environ.copy()
    env["HOMESIDE_USERNAME"] = os.getenv(_env_key("HOUSE", config_id, "USERNAME"), "")
    env["HOMESIDE_PASSWORD"] = os.getenv(_env_key("HOUSE", config_id, "PASSWORD"), "")
    env["HOMESIDE_CLIENTID"] = ""
    env["FRIENDLY_NAME"] = friendly_name
    env["DISPLAY_NAME_SOURCE"] = "friendly_name"
    env["POLL_INTERVAL_MINUTES"] = os.getenv("POLL_INTERVAL_MINUTES", "5")
    env["INFLUXDB_ENABLED"] = os.getenv("INFLUXDB_ENABLED", "true")
    env["POLL_OFFSET_SECONDS"] = str(poll_offset)
    return env


def build_building_env(config_id: str, poll_offset: int = 0) -> dict:
    """Return an env dict for a commercial-building fetcher subprocess.

    Resolves credentials in order:
      1. credential_ref from building config (e.g. EBO_HK_CRED1 → EBO_HK_CRED1_USERNAME)
      2. Legacy per-building env vars (BUILDING_<id>_USERNAME)
    Sets generic BMS_USERNAME/BMS_PASSWORD/BMS_DOMAIN plus ARRIGO_* aliases.
    """
    env = os.environ.copy()

    # Try credential_ref from building config first
    username = ""
    password = ""
    domain = ""
    config = load_json(os.path.join(BUILDINGS_DIR, f"{config_id}.json"))
    if config:
        cred_ref = config.get("connection", {}).get("credential_ref", "")
        if cred_ref:
            username = os.getenv(f"{cred_ref}_USERNAME", "")
            password = os.getenv(f"{cred_ref}_PASSWORD", "")
            domain = os.getenv(f"{cred_ref}_DOMAIN", "")

    # Fall back to legacy BUILDING_<id>_USERNAME/PASSWORD
    if not username:
        username = os.getenv(_env_key("BUILDING", config_id, "USERNAME"), "")
    if not password:
        password = os.getenv(_env_key("BUILDING", config_id, "PASSWORD"), "")

    # Generic BMS vars (used by building_fetcher for any BMS type)
    env["BMS_USERNAME"] = username
    env["BMS_PASSWORD"] = password
    env["BMS_DOMAIN"] = domain
    # Arrigo aliases for backwards compatibility
    env["ARRIGO_USERNAME"] = username
    env["ARRIGO_PASSWORD"] = password
    env["POLL_OFFSET_SECONDS"] = str(poll_offset)
    return env


def spawn_child(child: Child) -> None:
    """Start (or restart) the subprocess for *child*."""
    if child.kind == "house":
        env = build_house_env(child.config_id, child.friendly_name, child.poll_offset)
        cmd = [sys.executable, "-u", "HSF_Fetcher.py"]
    else:
        env = build_building_env(child.config_id, child.poll_offset)
        cmd = [sys.executable, "-u", "building_fetcher.py",
               "--building", child.config_id]

    child.process = subprocess.Popen(cmd, env=env)
    child.last_start = time.monotonic()
    log(f"Spawned {child.kind} '{child.friendly_name}' (pid {child.process.pid}, offset {child.poll_offset}s)")


def stop_child(child: Child, timeout: float = 10.0) -> None:
    """Send SIGTERM and wait; SIGKILL as last resort."""
    if child.process is None or child.process.poll() is not None:
        return
    pid = child.process.pid
    log(f"Stopping '{child.friendly_name}' (pid {pid})")
    child.process.terminate()
    try:
        child.process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        log(f"  SIGKILL '{child.friendly_name}' (pid {pid})")
        child.process.kill()
        child.process.wait(timeout=5)


# ---------------------------------------------------------------------------
#  Directory scanning
# ---------------------------------------------------------------------------
def scan_configs() -> dict[str, dict]:
    """
    Return a dict keyed by config_id with metadata for every valid config.
    """
    found: dict[str, dict] = {}

    # --- Private homes (profiles/*.json) ---
    profiles = Path(PROFILES_DIR)
    if profiles.is_dir():
        for p in sorted(profiles.glob("HEM_FJV_Villa_*.json")):
            if "_signals.json" in p.name:
                continue
            data = load_json(str(p))
            if not data:
                continue
            cid = data.get("customer_id", p.stem)
            env_user = os.getenv(_env_key("HOUSE", cid, "USERNAME"), "")
            if not env_user:
                log(f"Skipping {p.name}: no HOUSE_{cid}_USERNAME in env")
                continue
            found[cid] = {
                "path": str(p),
                "kind": "house",
                "friendly_name": data.get("friendly_name", cid),
            }

    # --- Commercial buildings (buildings/*.json) ---
    buildings = Path(BUILDINGS_DIR)
    if buildings.is_dir():
        for p in sorted(buildings.glob("*.json")):
            if '_signals.json' in p.name:
                continue
            data = load_json(str(p))
            if not data:
                continue
            bid = data.get("building_id", p.stem)

            # Check for credentials: credential_ref first, then legacy BUILDING_<id>
            cred_ref = data.get("connection", {}).get("credential_ref", "")
            if cred_ref:
                env_user = os.getenv(f"{cred_ref}_USERNAME", "")
                if not env_user:
                    log(f"Skipping {p.name}: no {cred_ref}_USERNAME in env")
                    continue
            else:
                env_user = os.getenv(_env_key("BUILDING", bid, "USERNAME"), "")
                if not env_user:
                    log(f"Skipping {p.name}: no BUILDING_{bid}_USERNAME in env")
                    continue

            found[bid] = {
                "path": str(p),
                "kind": "building",
                "friendly_name": data.get("friendly_name", bid),
            }

    return found


# ---------------------------------------------------------------------------
#  Reconciliation loop
# ---------------------------------------------------------------------------
def reconcile(children: dict[str, Child], configs: dict[str, dict]) -> None:
    """
    Compare running children against discovered configs.
    Start new, stop removed, restart changed.
    """
    current_ids = set(children.keys())
    desired_ids = set(configs.keys())

    # --- New configs → spawn (with staggered poll offsets) ---
    # Assign offsets based on total child count to spread writes in time
    next_offset = len(children) * POLL_OFFSET_STEP
    for cid in sorted(desired_ids - current_ids):
        cfg = configs[cid]
        child = Child(
            config_path=cfg["path"],
            config_id=cid,
            friendly_name=cfg["friendly_name"],
            kind=cfg["kind"],
            poll_offset=next_offset,
        )
        spawn_child(child)
        children[cid] = child
        next_offset += POLL_OFFSET_STEP

    # --- Removed configs → stop ---
    for cid in current_ids - desired_ids:
        stop_child(children[cid])
        log(f"Removed '{children[cid].friendly_name}'")
        del children[cid]


def check_crashed(children: dict[str, Child]) -> None:
    """Restart children that have exited unexpectedly."""
    now = time.monotonic()
    for child in children.values():
        if child.process is None:
            continue
        rc = child.process.poll()
        if rc is None:
            continue  # still running

        # Process has exited
        if now < child.backoff_until:
            continue  # still in backoff

        child.consecutive_crashes += 1
        backoff = min(RESTART_BACKOFF_BASE * (2 ** (child.consecutive_crashes - 1)),
                      RESTART_BACKOFF_MAX)
        log(f"'{child.friendly_name}' exited (rc={rc}), "
            f"crash #{child.consecutive_crashes}, restarting in {backoff}s")
        child.backoff_until = now + backoff

        # Reset crash counter if the child ran for >10 min before crashing
        if now - child.last_start > 600:
            child.consecutive_crashes = 1

        spawn_child(child)


# ---------------------------------------------------------------------------
#  Scheduled purge of offboarded entities
# ---------------------------------------------------------------------------
def check_purge_schedule() -> None:
    """Check offboarded.json and purge InfluxDB data for entries past their grace period."""
    if not os.path.exists(OFFBOARDED_FILE):
        return

    try:
        with open(OFFBOARDED_FILE) as f:
            data = json.load(f)
    except Exception as e:
        log(f"Failed to read {OFFBOARDED_FILE}: {e}")
        return

    pending = data.get("pending_purge", [])
    if not pending:
        return

    now = datetime.now(timezone.utc)
    purged_list = data.get("purged", [])
    still_pending = []
    changed = False

    for entry in pending:
        purge_after = datetime.fromisoformat(entry["purge_after"])
        if now < purge_after:
            days_left = (purge_after - now).days
            log(f"Pending purge: {entry['id']} ({entry.get('friendly_name', '')}) "
                f"— {days_left} day(s) remaining")
            still_pending.append(entry)
            continue

        # Time to purge
        tag_name = entry.get("influx_tag", "house_id")
        entity_id = entry["id"]
        log(f"Purging InfluxDB data: {tag_name}={entity_id} "
            f"({entry.get('friendly_name', '')})")

        try:
            from influxdb_client import InfluxDBClient
            url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
            token = os.getenv("INFLUXDB_TOKEN", "")
            org = os.getenv("INFLUXDB_ORG", "homeside")
            bucket = os.getenv("INFLUXDB_BUCKET", "heating")

            client = InfluxDBClient(url=url, token=token, org=org)
            client.delete_api().delete(
                start=datetime(2020, 1, 1, tzinfo=timezone.utc),
                stop=datetime(2099, 12, 31, tzinfo=timezone.utc),
                predicate=f'{tag_name}="{entity_id}"',
                bucket=bucket,
            )
            client.close()

            purged_list.append({
                "id": entity_id,
                "friendly_name": entry.get("friendly_name", ""),
                "purged_at": now.isoformat(),
            })
            changed = True
            log(f"Purged InfluxDB data: {tag_name}={entity_id}")
        except Exception as e:
            log(f"Failed to purge {entity_id}, will retry next day: {e}")
            still_pending.append(entry)

    if changed:
        data["pending_purge"] = still_pending
        data["purged"] = purged_list
        try:
            with open(OFFBOARDED_FILE, "w") as f:
                json.dump(data, f, indent=2)
                f.write("\n")
            log(f"Updated {OFFBOARDED_FILE}: "
                f"{len(still_pending)} pending, {len(purged_list)} purged")
        except Exception as e:
            log(f"Failed to write {OFFBOARDED_FILE}: {e}")


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------
def main() -> None:
    children: dict[str, Child] = {}
    shutdown = False

    def handle_signal(signum, _frame):
        nonlocal shutdown
        sig_name = signal.Signals(signum).name
        log(f"Received {sig_name}, shutting down all children")
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log("Orchestrator starting")
    log(f"Scanning {PROFILES_DIR}/ and {BUILDINGS_DIR}/ every {SCAN_INTERVAL}s")

    # Initial scan and spawn
    configs = scan_configs()
    if not configs:
        log("WARNING: No valid configs found — waiting for configs to appear")
    else:
        log(f"Found {len(configs)} config(s): "
            + ", ".join(f"{v['friendly_name']} ({k})" for k, v in configs.items()))
    reconcile(children, configs)

    # Run purge check on startup
    last_purge_date = datetime.now(timezone.utc).date()
    check_purge_schedule()

    # Main loop
    try:
        while not shutdown:
            time.sleep(SCAN_INTERVAL)
            if shutdown:
                break

            # Rescan directories
            configs = scan_configs()
            reconcile(children, configs)

            # Check for crashed processes
            check_crashed(children)

            # Daily purge check for offboarded entities
            today = datetime.now(timezone.utc).date()
            if today != last_purge_date:
                check_purge_schedule()
                last_purge_date = today

    except KeyboardInterrupt:
        shutdown = True

    # Graceful shutdown
    log(f"Stopping {len(children)} child process(es)")
    for child in children.values():
        stop_child(child)
    log("Orchestrator exiting")


if __name__ == "__main__":
    main()

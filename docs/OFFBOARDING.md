# Offboarding Guide

Complete guide for removing entities (buildings, houses) and users from BVPro. Covers both soft offboarding (graceful, reversible) and hard removal (immediate, permanent).

---

## Table of Contents

1. [Overview](#overview)
2. [Soft Offboarding (Recommended)](#soft-offboarding-recommended)
3. [Hard Removal](#hard-removal)
4. [User Offboarding via Web GUI](#user-offboarding-via-web-gui)
5. [Automatic Purge Process](#automatic-purge-process)
6. [Manual Operations](#manual-operations)
7. [offboarded.json Reference](#offboardedjson-reference)
8. [What Gets Deleted](#what-gets-deleted)
9. [Troubleshooting](#troubleshooting)

---

## Overview

There are **two paths** to offboard entities, and **two scopes** (entity-level and user-level):

| Method | Data Collection | Config/Credentials | InfluxDB Data | Reversible? |
|---|---|---|---|---|
| **Soft offboard** | Stops within 60s | Removed immediately | Kept for 30 days (configurable) | Yes, within grace period |
| **Hard remove** | Stops within 60s | Removed immediately | Deleted immediately | No |

| Scope | What it covers |
|---|---|
| **Entity** (house/building) | Config JSON, .env credentials, InfluxDB data, Dropbox meters |
| **User** (web GUI account) | User account, htpasswd entry, assigned houses (triggers entity offboard) |

---

## Soft Offboarding (Recommended)

Soft offboarding is the recommended approach. It stops data collection immediately but defers InfluxDB data deletion by a grace period (default 30 days), allowing recovery if needed.

### Via CLI (`remove_customer.py`)

```bash
# Soft offboard a house (30-day grace period)
python3 remove_customer.py HEM_FJV_Villa_99 --soft

# Soft offboard a building
python3 remove_customer.py TE236_HEM_Kontor --soft --type building

# Custom grace period (60 days)
python3 remove_customer.py HEM_FJV_Villa_99 --soft --days 60

# Dry run first
python3 remove_customer.py HEM_FJV_Villa_99 --soft --dry-run
```

### What Happens (step by step)

```
remove_customer.py --soft
    │
    ├── Step 1: Delete config JSON
    │   ├── House: profiles/HEM_FJV_Villa_99.json
    │   │         profiles/HEM_FJV_Villa_99_signals.json
    │   └── Building: buildings/TE236_HEM_Kontor.json
    │
    ├── Step 2: Remove .env credential lines
    │   ├── House: HOUSE_HEM_FJV_Villa_99_USERNAME, _PASSWORD
    │   └── Building: BUILDING_TE236_HEM_Kontor_USERNAME, _PASSWORD
    │
    └── Step 3: Add entry to offboarded.json
        └── { id, type, friendly_name, purge_after, influx_tag }
```

**After these steps:**
- The orchestrator detects the missing config within 60 seconds and stops the subprocess
- The entity no longer appears in new meter request syncs (Dropbox)
- InfluxDB data remains intact until the purge date
- The orchestrator checks `offboarded.json` daily and purges expired entries

### Cancelling a Soft Offboard

To cancel before the purge date, you need to:

1. **Restore config and credentials** -- recreate the config JSON and `.env` lines
2. **Remove from offboarded.json** -- delete the entry from the `pending_purge` array

```bash
# Edit offboarded.json and remove the entry from pending_purge
nano offboarded.json
```

The orchestrator will pick up the restored config on the next 60-second scan.

---

## Hard Removal

Hard removal deletes everything immediately with no grace period. Use this when you're certain you want to permanently remove all data.

### Via CLI (`remove_customer.py`)

```bash
# Hard remove a house
python3 remove_customer.py HEM_FJV_Villa_99

# Hard remove a building
python3 remove_customer.py TE236_HEM_Kontor --type building

# Skip confirmation prompt
python3 remove_customer.py HEM_FJV_Villa_99 --force

# Dry run first
python3 remove_customer.py HEM_FJV_Villa_99 --dry-run
```

### What Happens (step by step)

```
remove_customer.py (hard)
    │
    ├── Step 1: Delete all InfluxDB data
    │   └── DELETE predicate: house_id="HEM_FJV_Villa_99"
    │       (or building_id="TE236_HEM_Kontor" for buildings)
    │
    ├── Step 2: Delete config JSON
    │   ├── House: profiles/HEM_FJV_Villa_99.json
    │   │         profiles/HEM_FJV_Villa_99_signals.json
    │   └── Building: buildings/TE236_HEM_Kontor.json
    │
    ├── Step 3: Remove .env credential lines
    │   ├── House: HOUSE_HEM_FJV_Villa_99_USERNAME, _PASSWORD
    │   └── Building: BUILDING_TE236_HEM_Kontor_USERNAME, _PASSWORD
    │
    └── Step 4: Re-sync Dropbox meter CSV
        └── Removes the entity's meter IDs from SvenskEB_DH.csv
```

**This is irreversible.** All historical InfluxDB data (heating_system, energy_meter, thermal_history, temperature_forecast, etc.) is permanently deleted.

---

## User Offboarding via Web GUI

The web GUI at `bvpro.hem.se` provides admin-level user management with its own two-phase offboarding.

### Soft Delete (Admin Panel)

**Route:** `POST /admin/users/<username>/soft-delete`

1. Admin clicks "Disable" on a user in `/admin/users`
2. System performs for each house assigned to the user:
   - Calls `FetcherDeployer.soft_offboard()` -- stops the Docker container
3. Marks user as `role: "deleted"` with a `purge_after` date (30 days)
4. User can no longer log in

**What is preserved:** profile JSON, `.env` credentials, InfluxDB data, htpasswd entry.

### Restore (within grace period)

**Route:** `POST /admin/users/<username>/restore`

1. Admin clicks "Restore" on a deleted user
2. User role is set back to `user`
3. Data collection containers are restarted
4. User can log in again

### Hard Delete (permanent)

**Route:** `POST /admin/users/<username>/hard-delete`

Requires typing the username to confirm. For each assigned house:

1. Calls `FetcherDeployer.hard_offboard()`:
   - Stops and removes Docker container
   - Deletes customer profile JSON and signals JSON
   - Deletes per-customer env file (`envs/<customer_id>.env`)
   - Removes `.env` credential lines (`HOUSE_<id>_*`)
   - Deletes all InfluxDB data (`house_id="<id>"`)
2. Deletes user account from `users.json`
3. Removes htpasswd entry from `/etc/nginx/.htpasswd`

### Automatic Purge of Expired Users

**Route:** `POST /admin/purge-deleted`

Admin can trigger batch purging of all users whose 30-day grace period has expired. This performs hard delete on each expired user.

---

## Automatic Purge Process

The orchestrator handles deferred InfluxDB purges automatically.

### How It Works

```
orchestrator.py (main loop)
    │
    ├── Every day (at midnight UTC):
    │   └── check_purge_schedule()
    │       │
    │       ├── Read offboarded.json
    │       ├── For each entry in pending_purge:
    │       │   ├── If purge_after > now: log days remaining, skip
    │       │   └── If purge_after <= now:
    │       │       ├── Delete InfluxDB data: {influx_tag}="{id}"
    │       │       ├── Move entry to purged[] audit list
    │       │       └── Write updated offboarded.json
    │       └── On failure: keep in pending_purge, retry next day
    │
    └── Also checks on startup
```

### InfluxDB Delete Predicate

The purge uses the `influx_tag` field from the offboarded entry to construct the delete:

| Entity type | Tag | Delete predicate |
|---|---|---|
| House | `house_id` | `house_id="HEM_FJV_Villa_99"` |
| Building | `building_id` | `building_id="TE236_HEM_Kontor"` |

This deletes **all measurements** matching that tag: `heating_system`, `energy_meter`, `thermal_history`, `temperature_forecast`, `building_system`, `building_alarms`, etc.

### Retry on Failure

If InfluxDB is down or the delete fails, the entry stays in `pending_purge` and is retried on the next daily check.

---

## Manual Operations

### Force Immediate Purge of a Soft-Offboarded Entity

```bash
# This does a hard remove (bypasses the grace period)
python3 remove_customer.py HEM_FJV_Villa_99 --force
python3 remove_customer.py TE236_HEM_Kontor --type building --force
```

### Check Pending Purges

```bash
cat offboarded.json | python3 -m json.tool
```

### Verify InfluxDB Data is Gone

```bash
# Check for remaining data (should return empty)
influx query 'from(bucket: "heating")
  |> range(start: -1y)
  |> filter(fn: (r) => r.house_id == "HEM_FJV_Villa_99")
  |> limit(n: 1)'
```

### Manually Sync Dropbox After Removal

```bash
python3 dropbox_sync.py
```

This rebuilds the meter request file, excluding any meters whose entities no longer have config files.

---

## offboarded.json Reference

This file tracks entities pending InfluxDB purge and an audit log of completed purges. It is gitignored (deployment-specific).

### Structure

```json
{
  "pending_purge": [
    {
      "id": "TE236_HEM_Kontor",
      "type": "building",
      "friendly_name": "HEM Kontor TE236",
      "offboarded_at": "2026-02-14T06:00:00+00:00",
      "purge_after": "2026-03-16T06:00:00+00:00",
      "influx_tag": "building_id"
    }
  ],
  "purged": [
    {
      "id": "HEM_FJV_Villa_99",
      "friendly_name": "TestHouse",
      "purged_at": "2026-01-15T08:00:00+00:00"
    }
  ]
}
```

### Field Reference

| Field | Description |
|---|---|
| `id` | Entity identifier (customer_id or building_id) |
| `type` | `house` or `building` |
| `friendly_name` | Human-readable name (for logging) |
| `offboarded_at` | When the soft offboard was initiated |
| `purge_after` | When the orchestrator should delete InfluxDB data |
| `influx_tag` | `house_id` or `building_id` -- determines the delete predicate |

### Audit Trail

Completed purges are moved to the `purged` array with a `purged_at` timestamp. This provides an audit trail of when data was permanently deleted.

---

## What Gets Deleted

### Entity Artifacts

| Artifact | Soft Offboard | Hard Remove | Location |
|---|---|---|---|
| Config JSON | Deleted | Deleted | `profiles/<id>.json` or `buildings/<id>.json` |
| Signals JSON (houses) | Deleted | Deleted | `profiles/<id>_signals.json` |
| `.env` credentials | Deleted | Deleted | Lines matching `HOUSE_<id>_*` or `BUILDING_<id>_*` |
| Per-customer env file | Kept | Deleted | `envs/<id>.env` (web GUI deployments) |
| InfluxDB data | Kept (grace period) | Deleted immediately | All measurements tagged with entity ID |
| Dropbox meter CSV | Not re-synced | Re-synced | `/data/SvenskEB_DH.csv` |
| Docker container | Stops within 60s | Stopped and removed | `homeside-fetcher-<id>` |

### InfluxDB Measurements Affected

All measurements using the entity's tag are purged:

| Entity Type | Tag | Measurements |
|---|---|---|
| House | `house_id` | `heating_system`, `energy_meter`, `thermal_history`, `temperature_forecast`, `forecast_accuracy`, `learned_parameters`, `heat_curve_baseline` |
| Building | `building_id` | `building_system`, `building_alarms`, `energy_meter` |

### User Artifacts (Web GUI)

| Artifact | Soft Delete | Hard Delete |
|---|---|---|
| User account (`users.json`) | Role set to `deleted` | Entry removed |
| htpasswd entry | Kept | Removed |
| Assigned houses | Containers stopped | Full hard offboard per house |

---

## Troubleshooting

### Entity still collecting data after offboard

The orchestrator scans every 60 seconds. If the subprocess is still running:
- Check that the config JSON was actually deleted
- Check that the `.env` credentials were removed
- The orchestrator won't spawn a new process without both

### Purge didn't happen on expected date

- Check orchestrator logs: `docker logs homeside-orchestrator 2>&1 | grep purge`
- Verify `offboarded.json` has the correct `purge_after` date
- Check if InfluxDB was reachable (failed purges stay in `pending_purge` and retry next day)

### Need to recover data after soft offboard

Within the grace period:
1. Recreate the config JSON (from backup or manually)
2. Re-add `.env` credentials
3. Remove the entry from `offboarded.json` > `pending_purge`
4. The orchestrator will resume data collection within 60 seconds

### User was soft-deleted but should be restored

Admin can restore via the web GUI: `/admin/users/<username>/restore`
This resets the role to `user` and restarts data collection.

### CLI Reference

```
python3 remove_customer.py <entity_id> [options]

Arguments:
  entity_id              ID to remove (e.g., HEM_FJV_Villa_149, TE236_HEM_Kontor)

Options:
  --type house|building  Entity type (default: house)
  --soft                 Defer InfluxDB deletion, write to offboarded.json
  --days N               Grace period for --soft (default: 30)
  --force                Skip confirmation prompt
  --dry-run              Show what would happen without doing it
```

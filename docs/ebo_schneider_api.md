# EBO Halmstad (Schneider Electric EcoStruxure Building Operation) API

## System Info
- **System:** Schneider Electric EcoStruxure Building Operation (EBO)
- **Base URL:** `https://ebo.halmstad.se`
- **Version:** 6.0.4.90
- **Customer:** Halmstads Kommun (HK)
- **HAR capture date:** 2026-02-11

## Authentication Flow (SxWDigest Protocol)

Reverse-engineered from `login.js` in the EBO WebStation bundle. Uses "SxWDigest" (Schneider eXtended Web Digest) — a custom challenge-response protocol. No cookies — entirely header-based.

### Overview
1. Fetch the HTML page → get initial CSRF token from `<input type="hidden" id="csrf" value=":01000000...">`
2. `POST /vp/Challenge` → get challenge nonce
3. `POST /webstation/LoginSettings` → get RSA public key (for HTTPS password encryption)
4. Compute SHA-256 digest of `username + domain + password + "webstation/vp/Login" + challenge`
5. Over HTTPS: also encrypt password with RSA-OAEP/AES-CBC (BB8/C3PO/R2D2 params)
6. `POST /webstation/vp/Login` with `Authorization: SxWDigest UID=...,DOM=...,NV=...,DIG=...,BB8=...,C3PO=...,R2D2=...`
7. Server returns session token → use as `x-csrf-token` for all subsequent requests

### Step 1: Get CSRF Token
```
GET /
→ Parse HTML for: <input type="hidden" id="csrf" value=":01000000{hex}">
```
This is the initial anti-forgery token, sent as `X-CSRF-Token` header on all auth requests.

### Step 2: Challenge
```
POST /vp/Challenge
Headers: X-CSRF-Token: :01000000{hex}
Response: {"challenge": "<random_nonce_string>"}
```

### Step 3: Login Settings (RSA Public Key)
```
POST /webstation/LoginSettings
Headers: X-CSRF-Token: :01000000{hex}
Response: JSON with RSA public key (JWK format) + server config
```

### Step 4: Compute Digest & Login
```python
# Digest computation
digest_input = username + domain + password + "webstation/vp/Login" + challenge
digest = sha256(digest_input).hexdigest()

# Authorization header
auth = f"SxWDigest UID={username},DOM={domain},NV={challenge},DIG={digest}"

# Over HTTPS, also add encrypted password:
# BB8 = AES-128-CBC encrypted password (Base64)
# C3PO = RSA-OAEP encrypted AES key (Base64)
# R2D2 = AES IV (Base64)
auth += f",BB8={aes_encrypted_pwd},C3PO={rsa_encrypted_key},R2D2={iv}"
```

```
POST /webstation/vp/Login
Headers:
  Authorization: SxWDigest UID=<user>,DOM=<domain>,NV=<challenge>,DIG=<sha256>,...
  X-CSRF-Token: :01000000{hex}
Response: {"token": "<session_token>"}
```

### Post-Login
All subsequent requests use the session token:
```
x-csrf-token: {session_token}
```

### Password Encryption Detail (HTTPS only)
Over HTTPS, the password is additionally encrypted using a hybrid scheme:
1. UTF-8 encode password → Base64 encode → convert to bytes
2. Generate random AES-128-CBC key + random 16-byte IV
3. Encrypt the Base64-encoded password with AES-CBC
4. Fetch RSA public key from `/webstation/LoginSettings`
5. Encrypt the AES key with RSA-OAEP (SHA-1)
6. Send as: `BB8={aes_ciphertext}`, `C3PO={rsa_encrypted_aes_key}`, `R2D2={iv}`

### Error Codes
| Code | Meaning |
|------|---------|
| `131073` | Wrong domain, username, or password |
| `131077` | User account has expired |
| `131094` | A user is already logged on |
| `1048592` | No valid client license |

### Key Auth Observations
- No cookies — session maintained via `x-csrf-token` header only
- Initial CSRF token (`:01000000{hex}`) is from the HTML page, NOT computed from credentials
- Credentials go in the `Authorization` header during login, not in the CSRF token
- Session token is a simple numeric/string value returned by the login endpoint
- Domain name defaults to "SBO" (seen in HAR: `User.domain: "SBO"`)

## API Structure

All data API calls use a single endpoint:
```
POST /json/POST
Content-Type: application/json
x-csrf-token: {session_token}
```
Request body always has a `command` field plus command-specific parameters.

## API Commands

### Session Init: WebEntry
```json
{
  "command": "WebEntry",
  "clientLanguage": "en-US",
  "clientLocale": "en-US",
  "clientSystemOfMeasurement": 3
}
```
Returns user info, permissions, workspace config.

### Batch Property Read: GetMultiProperty
```json
{
  "command": "GetMultiProperty",
  "data": [
    "/EC1/path/to/property1",
    "/EC1/path/to/property2"
  ]
}
```

### Object Tree Browse: GetObjects
```json
{
  "command": "GetObjects",
  "paths": [{"path": "/EC1/some/path"}],
  "levels": 1,
  "includeHidden": true,
  "dbMode": true,
  "includeAggregated": false
}
```
**Important:** `paths` must be a list of dicts with `"path"` key, NOT plain strings.

### Live Data Subscriptions
```json
// Create
{"command": "CreateSubscription", "propertyPaths": ["/path/to/Value"]}
// Response: {"handle": 1538840522, "items": [...]}

// Add more paths
{"command": "AddToSubscription", "handle": 1538840522, "propertyPaths": [...]}

// Poll (most frequent call)
{"command": "ReadSubscription", "handle": 1538840522}

// Remove paths
{"command": "RemoveFromSubscription", "handle": 1538840522, "indices": [5, 6, 7]}
```

### Graphics & Navigation
```json
{"command": "GetGraphicsInfo", "path": "/EC1/Översikt/Bilder/Startsida"}
{"command": "GetWorkspaceLayout", "workspace": "Läsbehörighet", "path": "..."}
{"command": "GetPanelLayout", "path": "/Kattegattgymnasiet 20942 AS3/VP1/UC"}
{"command": "ClientRefresh", "bookmark": -1}
```

### Type Metadata
```json
{"command": "GetPropertyTypes", "typeNames": ["alarm.pt.DisabledCause"], "serverPath": "/..."}
{"command": "GetTypes", "typeNames": ["udt.xxx..."], "serverPath": "/..."}
```

### Static Resources (TGML Graphics)
```
GET /tgmlStorage/{server}/{type_id}
```

## Sites & Buildings

| Site | Automation Servers |
|------|-------------------|
| EC1 (Central/Översikt) | Central server |
| Kattegattgymnasiet 20942 | AS2, AS3, AS4, AS5, AS6, AS7, AS8, AS9 |
| Sannarpsgymnasiet 20949 | (single) |

## Sample Data Paths (263 total observed)

```
/Kattegattgymnasiet 20942 AS3/!Modbus RTU/VS1-P1A/Modbus Signaler/Pump data/Speed/Value
/Kattegattgymnasiet 20942 AS2/IO Bus/04_AO-8-V/TAFA1-SV1/Value
/EC1/Översikt/Soluppgång och solnedgång/Variabler/Sunrise/Value
/EC1/Översikt/Variabler/Beredskap/måndag/DESCR
```

Categories: System, Overview (Översikt), HVAC values, pump speeds, temperatures, valve positions, Modbus signals.

## Required Headers for All Requests
```
Origin: https://ebo.halmstad.se
Referer: https://ebo.halmstad.se/
Accept: */*
Content-Type: application/json  (for /json/POST calls)
x-csrf-token: {session_token}
```

## Alternative API Access Methods

### EWS (EcoStruxure Web Services) — SOAP API
- Endpoint: `https://<server>/EcoStruxure/DataExchange`
- Uses standard HTTP Digest Auth (SHA-256), realm `ews@SxWBM`
- Python `requests.auth.HTTPDigestAuth` works natively
- Requires EWS to be enabled in EBO server settings

### Client API (JavaScript)
- `https://<server>/publicweb/client_api.js`
- Documentation: `https://<server>/publicweb/client_api.txt`
- Requires authenticated WebStation session (VP auth)

### SmartConnector REST Gateway
- OAuth2/Bearer auth — requires SmartConnector add-on
- Not confirmed available on this installation

## Value Encoding

Subscription values are returned as **IEEE 754 hex-encoded doubles**:
```
"value": "0x405b6f7ce3333333"  →  109.74 (°F) or 43.1 (°C)
```

Decode with:
```python
import struct
raw = int("0x405b6f7ce3333333", 16)
value = struct.unpack('!d', raw.to_bytes(8, 'big'))[0]  # → 43.1
```

### Unit System
- `clientSystemOfMeasurement: 0` = SI/Metric (°C, kPa, etc.)
- `clientSystemOfMeasurement: 3` = US/Imperial (°F, psi, etc.)
- Set in WebEntry call

## JSON Encoding

EBO server **does NOT handle JSON unicode escapes** (`\u00F6` for ö, etc.).
Swedish characters must be sent as raw UTF-8, not escaped.

```python
# WRONG — EBO strips unicode escapes:
requests.post(url, json=data)  # uses ensure_ascii=True

# CORRECT — raw UTF-8:
requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'),
              headers={'Content-Type': 'application/json; charset=utf-8'})
```

## Verified Signal Paths (Kattegattgymnasiet 20942)

| Signal | Path | Unit |
|--------|------|------|
| Outdoor temp | `.../AS9/IO Bus/05_UI-16/GTU_2/Value` | °C |
| Dampened outdoor | `.../AS3/VS1/Variabler/GTU_Däm/Value` | °C |
| Supply temp VS1 | `.../AS3/IO Bus/AS-B-24 onboard IO/VS1-GT1/Value` | °C |
| Return temp VS1 | `.../AS3/IO Bus/AS-B-24 onboard IO/VS1-GT2/Value` | °C |
| Supply setpoint VS1 | `.../AS3/VS1/Variabler/GT1_BB/Value` | °C |
| Supply temp VS2 | `.../AS3/IO Bus/AS-B-24 onboard IO/VS2-GT1/Value` | °C |
| Return temp VS2 | `.../AS3/IO Bus/AS-B-24 onboard IO/VS2-GT2/Value` | °C |
| Hot water temp | `.../AS3/IO Bus/AS-B-24 onboard IO/VV1-GT1/Value` | °C |
| Hot water return | `.../AS3/IO Bus/AS-B-24 onboard IO/VV1-GT2/Value` | °C |
| System pressure VS1 | `.../AS3/IO Bus/AS-B-24 onboard IO/VS1-GP1/Value` | Pa |
| Expansion pressure | `.../AS3/IO Bus/AS-B-24 onboard IO/VS1-EXP-GP1/Value` | kPa |
| Valve position VS1 | `.../AS3/IO Bus/AS-B-24 onboard IO/VS1-SV1/Value` | % |
| Primary supply | `.../AS3/VP1/Variabler Externa/GT1T/Value` | °C |
| Primary return | `.../AS3/VP1/Variabler Externa/GT1R/Value` | °C |
| Daily avg outdoor | `.../AS3/VS1/Variabler/MEDUTEFÖDAG/Value` | °C |

All paths prefixed with `/Kattegattgymnasiet 20942`.

## Building Inventory (Halmstads Kommun)

- **40 schools** (Skolor) — including Kattegattgymnasiet, Sannarpsgymnasiet, Sturegymnasiet
- **74 preschools** (Förskolor)
- Additional: Fritid (leisure), Omsorg (care), Övrigt (other)
- Menu navigation via graphics info at `/EC1/Översikt/Bilder/Meny Översikt/{category}`

## Historical Data (Trend Logs)

### Overview

EBO stores historical data in **trend logs** (`trend.TLog` for periodic, `trend.TLogChangeOfValue` for change-triggered).
Two commands are needed:

1. **`GetInitialTrendData`** — discover chart series (log paths, unit IDs)
2. **`ReadTimeBasedTrend`** — fetch actual historical data with pagination

Implementation: `ebo_history.py` (`EboHistoryClient`)

### GetInitialTrendData — Discover Chart Series

```json
{
  "command": "GetInitialTrendData",
  "path": "/Kattegattgymnasiet 20942 AS3/Effektstyrning/Diagram/Effektstyrning",
  "viewType": "TrendChart"
}
```

Returns `GetInitialTrendDataRes` array with series metadata:
- `DisplayLog` — trend log path (used for `ReadTimeBasedTrend`)
- `DisplaySignal` — live signal path
- `ConfiguredLogUnitId` — storage unit ID
- `DisplayLogUnitId` — display unit ID
- `DisplayLogUnitName` — unit string (e.g. "°C", "kW", "%")

### ReadTimeBasedTrend — Fetch Historical Data

```json
{
  "command": "ReadTimeBasedTrend",
  "path": "/...trend_log_path.../LogArray",
  "id": 0,
  "handle": 0,
  "deliveryType": 0,
  "startTime": "1970-01-01T00:00:00.000Z",
  "endTime": "1970-01-01T00:00:00.000Z",
  "startTimeUtc": "0",
  "endTimeUtc": "0",
  "reverse": true,
  "numberOfRequestedRecords": 4000,
  "filter": "",
  "logUnitId": 2621441,
  "logDisplayUnitId": 2621441,
  "pointDisplayUnitId": 2621441,
  "pointUnitId": 2621441
}
```

**Key details:**
- Path MUST end with `/LogArray` suffix
- `startTimeUtc`/`endTimeUtc` are **microseconds** as strings. `"0"` = no limit
- `numberOfRequestedRecords` max is 4000 per page
- `reverse: true` = newest first (standard pagination direction)
- `startTime`/`endTime` fields are ignored when UTC variants are provided (set to epoch)

**Response structure:**
```json
{
  "METADATA": [{"firstRecord": "1696358548318000", "lastRecord": "...", "nbrOfRecords": "4000", "moreDataAvailable": "1"}],
  "ReadArrayRes": [{"data": [[timestamp_ms, value, ?, seq_nr, status], ...]}]
}
```

**Value encoding:** Values are either plain numbers (int/float) or **base64-encoded IEEE 754 little-endian doubles**.

```python
import base64, struct
def decode_ebo_value(val):
    if isinstance(val, (int, float)):
        return float(val)
    padded = val + "=" * (-len(val) % 4)
    return struct.unpack("<d", base64.b64decode(padded))[0]
```

**Pagination:** When `moreDataAvailable == "1"`, use `lastRecord - 1` as next `endTimeUtc`.

**Gotchas:**
- Some data rows contain metadata strings like `'trend.record.TLogEventRecord'` — skip rows where timestamp is not numeric
- Filter out timestamps before year 2000 (epoch-zero records)

### Unit ID Constants

| Constant | Value | Unit |
|----------|-------|------|
| `UNIT_CELSIUS` | 2621441 | °C |
| `UNIT_FAHRENHEIT` | 2621443 | °F |
| `UNIT_PERCENT` | 2097153 | % |
| `UNIT_KW` | 52494337 | kW |
| `UNIT_NONE` | 65537 | (dimensionless) |

### Python Client Usage

```python
from ebo_api import EboApi
from ebo_history import EboHistoryClient, UNIT_CELSIUS

# Login
api = EboApi("https://ebo.halmstad.se", username, password)
api.login()
api.web_entry()

# Create history client using the session
client = EboHistoryClient(
    base_url="https://ebo.halmstad.se",
    csrf_token=api.session_token,
    session=api.session,
)

# Discover chart series
series = client.get_chart_config(
    "/Kattegattgymnasiet 20942 AS3/Effektstyrning/Diagram/Effektstyrning"
)

# Fetch historical data
records = client.read_trend_log(
    log_path=series[0].display_log_path,
    log_unit_id=series[0].configured_unit_id,
    display_unit_id=series[0].configured_unit_id,
    max_records=50000,
)
```

## Kattegattgymnasiet 20942 — Building Structure

### Automation Servers

| Server | Building Part | Subsystems |
|--------|---------------|------------|
| AS3 | HD1 (Huvuddel 1) | VS1, VS2, VV1, KV1, VP1, Effektstyrning |
| AS9 | HD2 (Huvuddel 2) | VS4, VV2, VV3, Effektstyrning |
| AS2 | Översikt (Overview) | Central navigation, graphics |
| AS4-AS8 | Various | Not fully explored |

### Known Charts

| Chart Path | Series |
|------------|--------|
| `.../AS3/Effektstyrning/Diagram/Effektstyrning` | 9 series: outdoor temp, supply setpoint, room temp avg, power control signal, VMM1_EF (HD1 heat meter), valve positions |
| `.../AS9/Effektstyrning/Diagram/Effektstyrning` | 10 series: same pattern as AS3 but for HD2, plus VV2, VV3, VS4 valves |
| `.../AS3/VP1/Diagram/VP1` | 2 series: primary supply temp (GT1T), primary return temp (GT1R) |

### Energy Meter Trend Logs (VMM1_EF = Värmemängdsmätare Effekt)

| Meter | Log Path | Type | Data Range | Resolution |
|-------|----------|------|------------|------------|
| HD1 | `.../AS3/Effektstyrning/Trendloggar/Kattegattgymnasiet HD1-VMM1_EF_Logg` | TLogChangeOfValue | Oct–Dec 2025 | ~6 min |
| HD2 | `.../AS9/Effektstyrning/Trendloggar/VMM1_EF_Logg` | TLogChangeOfValue | Sep–Nov 2025 | ~6 min |

**Note:** Both meters are `TLogChangeOfValue` type (log on significant change, not periodic). Data currently stale (stops late 2025).

### All Available Trend Logs

**AS3 Effektstyrning (12 logs):**

| Log | Description | Type |
|-----|-------------|------|
| GTU_Logg | Outdoor temperature | TLog (periodic) |
| GT1_BB_Logg | Supply setpoint from application | TLog |
| GT5_Medel_Logg | Room temperature average | TLog |
| Effekt_styrsignal_Logg | Control signal from HEM MQTT | TLog |
| Effekt_GT1_BB_Logg | Output supply setpoint | TLog |
| GT1_BB_Diff_Logg | Current setpoint reduction | TLog |
| Effekt_DT_Logg | Runtime | TLog |
| Kattegattgymnasiet HD1-VMM1_EF_Logg | Heat meter power | TLogChangeOfValue |
| Effekt_D_Logg | Power control active indicator | TLogChangeOfValue |
| Effektstyrning_Aktiv_Logg | Manual control 0/1 | TLogChangeOfValue |
| Effekt_Max_Logg | Max power limit | TLogChangeOfValue |
| GT1_Min_Logg | Min supply setpoint limit | TLogChangeOfValue |

**AS3 VS1 (22 logs):** Supply/return temps, pressures, pump status, valve positions, damped outdoor temp

**AS3 VS2 (13 logs):** Supply/return temps, pressures, pump status, valve positions

**AS3 VV1 (10 logs):** Hot water temps (10s and 1min intervals), valve positions, VVC pump

**AS3 KV1 (3 logs):** Cold water flow, accumulated volume, monthly consumption

**AS3 VP1 (2 logs):** Primary supply/return temps (GT1T, GT1R) — currently empty

**AS9 Effektstyrning (12 logs):** Mirror of AS3 structure for HD2 building part

### Data Availability & Resolution

| Signal Type | Example | Records | Time Span | Resolution |
|------------|---------|---------|-----------|------------|
| Temperatures (TLog) | GTU, GT1_BB, GT5_Medel | ~13,000 | 90 days | 10 min |
| Valve positions (TLog) | VV1-SV1 (1min interval) | ~28,000 | 90 days | 2 min |
| Heat meter (TLogChangeOfValue) | VMM1_EF | ~4,000 | 35–60 days | ~6 min |

## Source JS Files
- `login.js` — Authentication class `P` with full SxWDigest flow
- `6474_split.js` (module 83477) — Pure JS SHA-256 implementation
- CSRF token read from `<input type="hidden" id="csrf">` in the HTML page

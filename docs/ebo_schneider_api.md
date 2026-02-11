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
  "paths": ["/EC1/some/path"],
  "levels": 1,
  "includeHidden": true,
  "dbMode": true,
  "includeAggregated": false
}
```

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

## Source JS Files
- `login.js` — Authentication class `P` with full SxWDigest flow
- `6474_split.js` (module 83477) — Pure JS SHA-256 implementation
- CSRF token read from `<input type="hidden" id="csrf">` in the HTML page

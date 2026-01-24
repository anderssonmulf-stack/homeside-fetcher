#!/usr/bin/env python3
"""
Dump actual variable names returned by API to understand inconsistency
"""

import os
import json
import requests
import time

def load_env():
    env_vars = {}
    try:
        with open('/opt/dev/homeside-fetcher/.env', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
    except Exception as e:
        print(f"Warning: Could not load .env file: {e}")
    return env_vars

env = load_env()
username = env.get('HOMESIDE_USERNAME', 'FC 2000232581')
password = env.get('HOMESIDE_PASSWORD', 'k8H3qd')
clientid = env.get('HOMESIDE_CLIENTID', '38/Sysinst10/HEM_FJV_149/HEM_FJV_Villa_149')
base_url = "https://homeside.systeminstallation.se"

print("=" * 80)
print("VARIABLE DUMP - Testing API Consistency")
print("=" * 80)
print()

# Step 1: Authenticate
print("Step 1: Getting session token...")
auth_url = f"{base_url}/api/v2/authorize/account"
payload = {
    "user": {
        "Account": "homeside",
        "UserName": username.replace(" ", ""),
        "Password": password
    },
    "lang": "sv"
}

response = requests.post(auth_url, json=payload, timeout=30)
session_token = response.json().get('querykey')
print(f"✓ Got session token: ...{session_token[-12:]}")
print()

# Step 2: Get BMS token
print("Step 2: Getting BMS token...")
session = requests.Session()
session.headers.update({'Authorization': session_token})

payload = {"clientid": clientid}
response = session.post(
    f"{base_url}/api/v2/housearrigobmsapi/getarrigobmstoken",
    json=payload,
    timeout=30
)
data = response.json()
bms_token = data.get('token')
uid = data.get('uid')
extend_uid = data.get('extendUid')
print(f"✓ Got BMS token")
print(f"  uid: {uid}")
print(f"  extendUid: {extend_uid}")
print()

# Expected short variable names (last segment after splitting by '.')
expected_short_names = [
    'AI_ZON1_GT_RUM_1_Output',
    'GT_UteTEMP_LM1_Output',
    'GT_Ute_24_Output',
    'GT_FRAML_LM1_Output',
    'GT_RETURL_LM1_Output',
    'GT_VV_Output',
    'PT_ANLDR_VVC_LM1_Output',
    'POOL_AKTIV_Input',
    'VV_AKTIV_Input',
    'BORTA_Input'
]

print("Expected short variable names:")
for name in expected_short_names:
    print(f"  - {name}")
print()

# Step 3: Fetch variables MULTIPLE times
print("=" * 80)
print("Fetching variables 5 times to check consistency...")
print("=" * 80)

for attempt in range(1, 6):
    print(f"\n--- Attempt #{attempt} ---")

    payload = {
        "clientid": clientid,
        "uid": uid,
        "extendUid": extend_uid,
        "token": bms_token
    }

    try:
        start_time = time.time()
        response = session.post(
            f"{base_url}/api/v2/housearrigobmsapi/getducvariables",
            json=payload,
            timeout=30
        )
        elapsed = time.time() - start_time

        data = response.json()
        variables = data.get('variables', [])

        print(f"Response time: {elapsed:.2f}s")
        print(f"Total variables: {len(variables)}")

        # Extract all short names (last segment after splitting by '.')
        returned_short_names = []
        for v in variables:
            full_name = v.get('variable', '')
            if '.' in full_name:
                short_name = full_name.split('.')[-1]
                returned_short_names.append(short_name)

        # Check which expected variables we found
        found = []
        for expected in expected_short_names:
            if expected in returned_short_names:
                found.append(expected)

        print(f"Found {len(found)}/{len(expected_short_names)} expected variables:")
        for name in found:
            print(f"  ✓ {name}")

        missing = [n for n in expected_short_names if n not in found]
        if missing:
            print(f"Missing {len(missing)} variables:")
            for name in missing:
                print(f"  ✗ {name}")

        # Save full response for this attempt
        with open(f'/opt/dev/homeside-fetcher/api_response_{attempt}.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Full response saved to api_response_{attempt}.json")

    except Exception as e:
        print(f"ERROR: {e}")

    # Wait 2 seconds between attempts
    if attempt < 5:
        time.sleep(2)

print()
print("=" * 80)
print("DONE - Check api_response_*.json files for full details")
print("=" * 80)

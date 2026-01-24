#!/usr/bin/env python3
"""
Debug script to see all variables returned by the API
"""

import os
import json
import requests
from collections import Counter

# Load credentials
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
print("VARIABLE DEBUG SCRIPT")
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

response = requests.post(auth_url, json=payload, timeout=10)
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
    timeout=10
)
data = response.json()
bms_token = data.get('token')
uid = data.get('uid')
extend_uid = data.get('extendUid')
print(f"✓ Got BMS token")
print()

# Step 3: Get variables - MULTIPLE TIMES
print("Step 3: Fetching variables 3 times to check consistency...")
print("-" * 80)

for attempt in range(1, 4):
    print(f"\nAttempt #{attempt}")
    print("-" * 40)

    payload = {
        "clientid": clientid,
        "uid": uid,
        "extendUid": extend_uid,
        "token": bms_token
    }

    response = session.post(
        f"{base_url}/api/v2/housearrigobmsapi/getducvariables",
        json=payload,
        timeout=10
    )

    data = response.json()
    variables = data.get('variables', [])

    print(f"Total variables returned: {len(variables)}")

    # Expected variable names
    expected_vars = {
        'RoomTempValue': 'Room Temperature',
        'OutdoorTempValue': 'Outdoor Temperature',
        'OutdoorAvg24Value': 'Outdoor 24h Average',
        'SupplyValue': 'Supply Temperature',
        'ReturnValue': 'Return Temperature',
        'HotWaterValue': 'Hot Water Temperature',
        'OperatingPressureValue': 'System Pressure',
        'AddHeatPowerValue': 'Electric Heater',
        'HeatRecoveryValue': 'Heat Recovery',
        'VacationModeValue': 'Away Mode'
    }

    # Check which expected variables we found
    found = {}
    not_found = []

    for var in variables:
        var_name = var.get('variableName')
        if var_name in expected_vars:
            found[var_name] = var.get('variableValue')

    for var_name in expected_vars:
        if var_name not in found:
            not_found.append(var_name)

    print(f"Found {len(found)}/{len(expected_vars)} expected variables:")
    for var_name, value in found.items():
        print(f"  ✓ {expected_vars[var_name]:25} = {value}")

    if not_found:
        print(f"\nMissing {len(not_found)} variables:")
        for var_name in not_found:
            print(f"  ✗ {expected_vars[var_name]}")

    # Show sample of all variable names (first 30)
    print(f"\nSample of all variable names (first 30):")
    for i, var in enumerate(variables[:30]):
        print(f"  {i+1:3}. {var.get('variableName')}")

print()
print("=" * 80)
print("Checking for patterns in variable names...")
print("=" * 80)

# Get one more full fetch to analyze
payload = {
    "clientid": clientid,
    "uid": uid,
    "extendUid": extend_uid,
    "token": bms_token
}

response = session.post(
    f"{base_url}/api/v2/housearrigobmsapi/getducvariables",
    json=payload,
    timeout=10
)

data = response.json()
variables = data.get('variables', [])

# Look for variables containing our keywords
keywords = ['room', 'outdoor', 'supply', 'return', 'hot', 'water', 'pressure', 'heat', 'vacation']

print("\nSearching for variables containing keywords:")
for keyword in keywords:
    matches = [v for v in variables if keyword.lower() in v.get('variableName', '').lower()]
    if matches:
        print(f"\n'{keyword}' matches ({len(matches)}):")
        for var in matches[:10]:  # Show first 10
            print(f"  - {var.get('variableName'):40} = {var.get('variableValue')}")

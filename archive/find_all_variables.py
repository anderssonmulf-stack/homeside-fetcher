#!/usr/bin/env python3
"""
Find the correct variable names for all 10 values
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
username = env.get('HOMESIDE_USERNAME')
password = env.get('HOMESIDE_PASSWORD')
clientid = env.get('HOMESIDE_CLIENTID')
base_url = "https://homeside.systeminstallation.se"

# Authenticate
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

# Get BMS token
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

# Get variables
payload = {
    "clientid": clientid,
    "uid": uid,
    "extendUid": extend_uid,
    "token": bms_token
}

response = session.post(
    f"{base_url}/api/v2/housearrigobmsapi/getducvariables",
    json=payload,
    timeout=30
)
data = response.json()
variables = data.get('variables', [])

print("=" * 80)
print("SEARCHING FOR HEATING SYSTEM VARIABLES")
print("=" * 80)
print()

# Search for each type of variable
searches = [
    ("Room Temperature", ["GT_RUM", "ROOM", "RUM"], ["AI_ZON1_GT_RUM"], [20, 23]),
    ("Outdoor Temperature", ["GT_UTE", "OUTDOOR", "AI_GT_UTE"], ["24h", "MEDEL"], [3, 6]),
    ("Outdoor 24h Average", ["24h", "MEDEL_GT_UTE"], [], [3, 6]),
    ("Supply Temperature", ["GT_TILL", "FRAML", "SUPPLY"], ["RETUR", "CURVE", "ADAPT"], [25, 35]),
    ("Return Temperature", ["GT_RETUR", "RETURN"], ["CURVE", "ADAPT"], [25, 30]),
    ("Hot Water Temperature", ["VV", "TAPP", "GT41"], ["AKTIV", "VMM", "CURVE"], [20, 50]),
    ("System Pressure", ["GP_EXP", "TRYCK"], ["AKTIV", "CURVE", "ADAPT"], [0.5, 2]),
    ("Electric Heater", ["POOL_AKTIV", "ELM1_AKTIV"], [], [True, False]),
    ("Heat Recovery", ["VV_AKTIV", "VMM1_AKTIV"], [], [True, False]),
    ("Away Mode", ["BORTA", "AWAY"], ["TIMER", "CURVE"], [True, False])
]

for description, include_terms, exclude_terms, expected_range in searches:
    print(f"\n{description}:")
    print("-" * 40)

    candidates = []
    for v in variables:
        if v.get('value') is None:
            continue

        name = v.get('variable', '')
        short = name.split('.')[-1]
        value = v.get('value')

        # Check if any include term is in the name
        if not any(term.upper() in short.upper() for term in include_terms):
            continue

        # Check if any exclude term is in the name
        if any(term.upper() in short.upper() for term in exclude_terms):
            continue

        candidates.append((short, value))

    # Show all candidates
    if candidates:
        for short, value in candidates:
            # Highlight if value is in expected range
            in_range = ""
            if isinstance(value, bool):
                if value in expected_range:
                    in_range = " ✓"
            elif isinstance(value, (int, float)):
                if len(expected_range) == 2 and expected_range[0] <= value <= expected_range[1]:
                    in_range = " ✓"

            print(f"  {short:45} = {value}{in_range}")
    else:
        print("  ✗ No candidates found")

print()
print("=" * 80)

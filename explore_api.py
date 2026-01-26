#!/usr/bin/env python3
"""
HomeSide API Explorer
Probes the API for potential historical data endpoints.

Usage:
    python explore_api.py
"""

import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv


def authenticate(base_url, username, password):
    """Authenticate and return session token."""
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
    response.raise_for_status()
    data = response.json()
    return data.get('querykey')


def get_client_id(base_url, session_token):
    """Get client ID from house list."""
    response = requests.post(
        f"{base_url}/api/v2/housefidlist",
        headers={'Authorization': session_token},
        json={},
        timeout=30
    )
    response.raise_for_status()
    houses = response.json()
    if houses:
        return houses[0].get('restapiurl'), houses[0].get('name')
    return None, None


def get_bms_token(base_url, session_token, client_id):
    """Get BMS token for API calls."""
    response = requests.post(
        f"{base_url}/api/v2/housearrigobmsapi/getarrigobmstoken",
        headers={'Authorization': session_token},
        json={"clientid": client_id},
        timeout=30
    )
    response.raise_for_status()
    data = response.json()
    return {
        'token': data.get('token'),
        'uid': data.get('uid'),
        'extendUid': data.get('extendUid'),
        'refreshToken': data.get('refreshToken')
    }


def probe_endpoint(base_url, session_token, endpoint, payload, description):
    """Try an endpoint and report results."""
    url = f"{base_url}{endpoint}"
    print(f"\n{'='*60}")
    print(f"Probing: {endpoint}")
    print(f"Description: {description}")
    print(f"Payload: {json.dumps(payload, indent=2)[:200]}...")

    try:
        response = requests.post(
            url,
            headers={'Authorization': session_token, 'Content-Type': 'application/json'},
            json=payload,
            timeout=30
        )

        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            try:
                data = response.json()
                print(f"Response type: {type(data).__name__}")
                if isinstance(data, dict):
                    print(f"Keys: {list(data.keys())}")
                    # Show sample of data
                    for key in list(data.keys())[:5]:
                        val = data[key]
                        if isinstance(val, list):
                            print(f"  {key}: list[{len(val)}]")
                            if val:
                                print(f"    Sample: {str(val[0])[:100]}...")
                        else:
                            print(f"  {key}: {str(val)[:100]}")
                elif isinstance(data, list):
                    print(f"List with {len(data)} items")
                    if data:
                        print(f"Sample: {str(data[0])[:200]}...")
                return data
            except json.JSONDecodeError:
                print(f"Response (not JSON): {response.text[:200]}")
        else:
            print(f"Error response: {response.text[:200]}")

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

    return None


def main():
    load_dotenv()

    base_url = "https://homeside.systeminstallation.se"
    username = os.getenv('HOMESIDE_USERNAME')
    password = os.getenv('HOMESIDE_PASSWORD')

    if not username or not password:
        print("ERROR: Set HOMESIDE_USERNAME and HOMESIDE_PASSWORD in .env")
        return

    print("HomeSide API Explorer")
    print("=" * 60)

    # Authenticate
    print("\n1. Authenticating...")
    session_token = authenticate(base_url, username, password)
    print(f"   Token: ...{session_token[-8:]}")

    # Get client ID
    print("\n2. Getting client ID...")
    client_id, house_name = get_client_id(base_url, session_token)
    print(f"   House: {house_name}")
    print(f"   Client ID: {client_id}")

    # Get BMS token
    print("\n3. Getting BMS token...")
    bms = get_bms_token(base_url, session_token, client_id)
    print(f"   BMS Token: ...{bms['token'][-8:]}")

    # Base payload for BMS API calls
    base_payload = {
        "uid": bms['uid'],
        "clientid": client_id,
        "extendUid": bms['extendUid'],
        "token": bms['token']
    }

    # Time range for history queries
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    week_ago = now - timedelta(days=7)

    # =========================================================================
    # Probe potential history endpoints
    # =========================================================================

    print("\n" + "=" * 60)
    print("PROBING POTENTIAL HISTORY ENDPOINTS")
    print("=" * 60)

    # Common history endpoint patterns
    history_endpoints = [
        ("/api/v2/housearrigobmsapi/gethistory", "History endpoint"),
        ("/api/v2/housearrigobmsapi/getduchistory", "DUC history"),
        ("/api/v2/housearrigobmsapi/gettrend", "Trend data"),
        ("/api/v2/housearrigobmsapi/gettrenddata", "Trend data v2"),
        ("/api/v2/housearrigobmsapi/getlog", "Log data"),
        ("/api/v2/housearrigobmsapi/getlogs", "Logs"),
        ("/api/v2/housearrigobmsapi/getchartdata", "Chart data"),
        ("/api/v2/housearrigobmsapi/getgraphdata", "Graph data"),
        ("/api/v2/housearrigobmsapi/getstatistics", "Statistics"),
        ("/api/v2/housearrigobmsapi/getvariablehistory", "Variable history"),
        ("/api/v2/history", "Generic history"),
        ("/api/v2/trends", "Trends"),
    ]

    for endpoint, desc in history_endpoints:
        # Try with basic payload
        probe_endpoint(base_url, session_token, endpoint, base_payload, desc)

        # Try with time range
        payload_with_time = {
            **base_payload,
            "from": yesterday.isoformat(),
            "to": now.isoformat(),
            "startTime": yesterday.isoformat(),
            "endTime": now.isoformat(),
            "start": int(yesterday.timestamp() * 1000),
            "end": int(now.timestamp() * 1000),
        }
        probe_endpoint(base_url, session_token, endpoint, payload_with_time, f"{desc} (with time range)")

    # Try getting variable list first to find variable paths
    print("\n" + "=" * 60)
    print("GETTING VARIABLE PATHS FOR HISTORY QUERIES")
    print("=" * 60)

    response = requests.post(
        f"{base_url}/api/v2/housearrigobmsapi/getducvariables",
        headers={'Authorization': session_token},
        json=base_payload,
        timeout=30
    )

    if response.status_code == 200:
        data = response.json()
        variables = data.get('variables', [])

        # Find temperature variable for history test
        temp_var = None
        for var in variables:
            if 'GT_UTE' in var.get('variable', '') or 'outdoor' in var.get('variable', '').lower():
                temp_var = var
                break

        if temp_var:
            print(f"\nFound outdoor temp variable: {temp_var['variable']}")

            # Try history with specific variable
            var_history_payloads = [
                {
                    **base_payload,
                    "variable": temp_var['variable'],
                    "path": temp_var.get('path'),
                },
                {
                    **base_payload,
                    "variables": [temp_var['variable']],
                    "from": yesterday.isoformat(),
                    "to": now.isoformat(),
                },
                {
                    **base_payload,
                    "paths": [temp_var.get('path')],
                    "startTime": int(yesterday.timestamp()),
                    "endTime": int(now.timestamp()),
                },
            ]

            for payload in var_history_payloads:
                probe_endpoint(
                    base_url, session_token,
                    "/api/v2/housearrigobmsapi/gethistory",
                    payload,
                    f"History for {temp_var['variable']}"
                )

    # Try to discover available API endpoints
    print("\n" + "=" * 60)
    print("PROBING API DISCOVERY ENDPOINTS")
    print("=" * 60)

    discovery_endpoints = [
        ("/api/v2/swagger", "Swagger docs"),
        ("/api/v2/openapi", "OpenAPI spec"),
        ("/api/v2", "API root"),
        ("/api/v2/endpoints", "Endpoints list"),
        ("/api/v2/housearrigobmsapi", "BMS API root"),
        ("/api/v2/housearrigobmsapi/help", "BMS API help"),
        ("/api/v2/housearrigobmsapi/methods", "BMS API methods"),
    ]

    for endpoint, desc in discovery_endpoints:
        # Try GET first
        try:
            response = requests.get(
                f"{base_url}{endpoint}",
                headers={'Authorization': session_token},
                timeout=10
            )
            print(f"\nGET {endpoint}: {response.status_code}")
            if response.status_code == 200:
                print(f"  Response: {response.text[:300]}...")
        except:
            pass

        # Then POST
        probe_endpoint(base_url, session_token, endpoint, {}, desc)

    print("\n" + "=" * 60)
    print("EXPLORATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()

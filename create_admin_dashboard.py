#!/usr/bin/env python3
"""
Creates an admin version of the Homeside Heating System dashboard
with a house_id variable selector for multi-user support.
"""

import json
import re
import requests
import sys

GRAFANA_URL = "http://localhost:3000"
GRAFANA_USER = "admin"
GRAFANA_PASS = "temppass123"

def add_house_id_filter(query: str) -> str:
    """Add house_id filter to Flux query after the range() call."""
    # Pattern: find the first filter after range, insert house_id filter before it
    # We want to add: |> filter(fn: (r) => r["house_id"] == "${house_id}")

    house_id_filter = '|> filter(fn: (r) => r["house_id"] == "${house_id}")'

    # Find position after range() call
    range_pattern = r'(\|>\s*range\([^)]+\))'
    match = re.search(range_pattern, query)

    if match:
        # Insert house_id filter after range()
        insert_pos = match.end()
        modified = query[:insert_pos] + '\n  ' + house_id_filter + query[insert_pos:]
        return modified

    return query


def create_admin_dashboard():
    # Load original dashboard
    with open('/tmp/homeside_dashboard_original.json', 'r') as f:
        data = json.load(f)

    dashboard = data['dashboard']

    # Change title and UID for the new dashboard
    dashboard['title'] = 'Homeside Heating System - Admin'
    dashboard['uid'] = 'homeside-admin-multiuser'
    dashboard['id'] = None  # Let Grafana assign new ID
    dashboard['tags'] = ['heating', 'homeside', 'admin']

    # Add house_id template variable
    house_id_variable = {
        "current": {
            "selected": True,
            "text": "",
            "value": ""
        },
        "datasource": {
            "type": "influxdb",
            "uid": "dfb5s9mkiv5kwf"
        },
        "definition": 'import "influxdata/influxdb/schema"\nschema.tagValues(bucket: "heating", tag: "house_id")',
        "description": "Select which house/user to view",
        "hide": 0,
        "includeAll": False,
        "label": "House",
        "multi": False,
        "name": "house_id",
        "options": [],
        "query": 'import "influxdata/influxdb/schema"\nschema.tagValues(bucket: "heating", tag: "house_id")',
        "refresh": 1,  # Refresh on dashboard load
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,  # Alphabetical asc
        "type": "query"
    }

    dashboard['templating'] = {"list": [house_id_variable]}

    # Modify all panel queries to include house_id filter
    for panel in dashboard.get('panels', []):
        for target in panel.get('targets', []):
            if 'query' in target:
                original_query = target['query']
                modified_query = add_house_id_filter(original_query)
                target['query'] = modified_query
                print(f"Modified query in panel: {panel.get('title', 'Unknown')}")

    # Prepare payload for Grafana API
    payload = {
        "dashboard": dashboard,
        "overwrite": True,
        "message": "Created admin dashboard with house_id variable"
    }

    # Save modified dashboard to file for reference
    with open('/tmp/homeside_dashboard_admin.json', 'w') as f:
        json.dump(payload, f, indent=2)

    print("\nSaved modified dashboard to /tmp/homeside_dashboard_admin.json")

    # Import to Grafana
    print("\nImporting dashboard to Grafana...")
    response = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        auth=(GRAFANA_USER, GRAFANA_PASS),
        headers={"Content-Type": "application/json"},
        json=payload
    )

    if response.status_code == 200:
        result = response.json()
        print(f"Dashboard created successfully!")
        print(f"URL: {GRAFANA_URL}{result.get('url', '')}")
        print(f"UID: {result.get('uid', '')}")
        return True
    else:
        print(f"Failed to create dashboard: {response.status_code}")
        print(response.text)
        return False


if __name__ == "__main__":
    success = create_admin_dashboard()
    sys.exit(0 if success else 1)

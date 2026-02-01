"""
Helper functions for updating Grafana dashboards.
"""

import json
import os
import logging

logger = logging.getLogger(__name__)

DASHBOARD_PATH = '/opt/dev/homeside-fetcher/grafana/dashboards/homeside-3.0.json'
PROFILES_DIR = '/opt/dev/homeside-fetcher/profiles'


def get_house_mappings():
    """Read all customer profiles and return friendly_name -> customer_id mappings."""
    mappings = []
    
    if os.path.exists(PROFILES_DIR):
        for filename in os.listdir(PROFILES_DIR):
            if filename.endswith('.json'):
                customer_id = filename[:-5]
                try:
                    with open(os.path.join(PROFILES_DIR, filename)) as f:
                        profile = json.load(f)
                    mappings.append({
                        'text': profile.get('friendly_name') or customer_id,
                        'value': customer_id
                    })
                except Exception as e:
                    logger.error(f"Failed to read profile {filename}: {e}")
                    mappings.append({'text': customer_id, 'value': customer_id})
    
    # Sort by friendly name
    mappings.sort(key=lambda x: x['text'].lower())
    return mappings


def update_dashboard_house_variable():
    """Update the Grafana dashboard's house_id variable with current customer profiles."""
    try:
        mappings = get_house_mappings()
        
        if not mappings:
            logger.warning("No house mappings found")
            return False
        
        with open(DASHBOARD_PATH) as f:
            dashboard = json.load(f)
        
        # Build options list
        options = []
        for i, m in enumerate(mappings):
            options.append({
                'selected': i == 0,  # First one is default
                'text': m['text'],
                'value': m['value']
            })
        
        # Build query string (format: "Name1 : value1, Name2 : value2")
        query = ', '.join([f"{m['text']} : {m['value']}" for m in mappings])
        
        # Update the variable
        dashboard['templating']['list'] = [
            {
                'current': {
                    'selected': True,
                    'text': mappings[0]['text'],
                    'value': mappings[0]['value']
                },
                'description': 'Select which house to view',
                'hide': 0,
                'includeAll': False,
                'label': 'House',
                'multi': False,
                'name': 'house_id',
                'options': options,
                'query': query,
                'skipUrlSync': False,
                'type': 'custom'
            }
        ]
        
        with open(DASHBOARD_PATH, 'w') as f:
            json.dump(dashboard, f, indent=2)
        
        logger.info(f"Updated Grafana dashboard with {len(mappings)} houses")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update Grafana dashboard: {e}")
        return False


if __name__ == '__main__':
    # Can be run manually: python grafana_helper.py
    logging.basicConfig(level=logging.INFO)
    if update_dashboard_house_variable():
        print("Dashboard updated successfully")
    else:
        print("Failed to update dashboard")

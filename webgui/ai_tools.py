"""
AI Assistant Tool Functions for BVPro Web GUI.
Each function is callable by the Anthropic tool_use API and returns
data from InfluxDB, profiles, or building configs.
"""

import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')
PROFILES_DIR = os.path.join(os.path.dirname(__file__), '..', 'profiles')
BUILDINGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'buildings')


def _get_influx():
    from influx_reader import get_influx_reader
    return get_influx_reader()


def _load_profile(house_id: str) -> dict:
    """Load a customer profile JSON."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from customer_profile import CustomerProfile
    try:
        profile = CustomerProfile.load(house_id, PROFILES_DIR)
        return profile.to_dict()
    except Exception as e:
        return {'error': str(e)}


def _load_building(building_id: str) -> dict:
    """Load a building config JSON."""
    filepath = os.path.join(BUILDINGS_DIR, f"{building_id}.json")
    if not os.path.exists(filepath):
        return {'error': f'Building config not found: {building_id}'}
    with open(filepath, 'r') as f:
        return json.load(f)


# =========================================================================
# Read-only tools (all roles)
# =========================================================================

def get_current_status(house_id: str) -> dict:
    """Get the latest heating data for a house."""
    influx = _get_influx()
    data = influx.get_latest_heating_data(house_id)
    if not data:
        return {'error': f'No recent data for {house_id}'}
    return data


def get_energy_consumption(entity_id: str, days: int = 30, entity_type: str = 'house') -> dict:
    """Get energy consumption history (daily totals)."""
    influx = _get_influx()
    if entity_type == 'building':
        return influx.get_building_energy_consumption(entity_id, days=days)
    return influx.get_energy_consumption_history(entity_id, days=days)


def get_energy_separation(entity_id: str, days: int = 30, entity_type: str = 'house') -> dict:
    """Get heating vs DHW energy breakdown."""
    influx = _get_influx()
    tag = 'building_id' if entity_type == 'building' else 'house_id'
    return influx.get_energy_separation(entity_id, days=days, entity_tag=tag)


def get_temperature_history(entity_id: str, hours: int = 168, entity_type: str = 'house') -> dict:
    """Get temperature history (indoor, outdoor, supply, return)."""
    influx = _get_influx()
    if entity_type == 'building':
        return {'data': influx.get_building_temperature_history(entity_id, hours=hours)}
    return {'data': influx.get_temperature_history(entity_id, hours=hours)}


def get_weather_history(house_id: str, hours: int = 168) -> dict:
    """Get weather observation history."""
    influx = _get_influx()
    return {'data': influx.get_weather_history(house_id, hours=hours)}


def get_data_availability(entity_id: str, days: int = 30, entity_type: str = 'house') -> dict:
    """Get daily data point counts to find gaps."""
    influx = _get_influx()
    if entity_type == 'building':
        return influx.get_building_data_availability(entity_id, days=days)
    return influx.get_data_availability(entity_id, days=days)


def get_profile_info(house_id: str) -> dict:
    """Get house profile settings and learned parameters."""
    data = _load_profile(house_id)
    if 'error' in data:
        return data
    # Return a clean summary
    return {
        'customer_id': data.get('customer_id'),
        'friendly_name': data.get('friendly_name'),
        'building': data.get('building'),
        'comfort': data.get('comfort'),
        'heating_system': data.get('heating_system'),
        'meter_ids': data.get('meter_ids', []),
        'energy_separation': data.get('energy_separation'),
        'learned': data.get('learned'),
    }


def get_building_info(building_id: str) -> dict:
    """Get building config and signal list."""
    data = _load_building(building_id)
    if 'error' in data:
        return data
    # Return summary (omit verbose signal details)
    signal_count = len(data.get('analog_signals', {})) + len(data.get('digital_signals', {}))
    return {
        'building_id': data.get('building_id'),
        'friendly_name': data.get('friendly_name'),
        'building_type': data.get('building_type'),
        'meter_ids': data.get('meter_ids', []),
        'energy_separation': data.get('energy_separation'),
        'connection': data.get('connection'),
        'poll_interval_minutes': data.get('poll_interval_minutes'),
        'signal_count': signal_count,
        'alarm_monitoring': data.get('alarm_monitoring'),
    }


def compare_periods(entity_id: str, period1_start: str, period1_end: str,
                    period2_start: str, period2_end: str,
                    entity_type: str = 'house') -> dict:
    """
    Compare energy and temperatures between two date ranges.
    Dates should be YYYY-MM-DD format.
    """
    influx = _get_influx()
    tag = 'building_id' if entity_type == 'building' else 'house_id'

    if not influx.client:
        return {'error': 'No InfluxDB connection'}

    results = {}
    try:
        query_api = influx.client.query_api()

        for label, start, end in [('period1', period1_start, period1_end),
                                   ('period2', period2_start, period2_end)]:
            # Energy from energy_separated
            energy_query = f'''
                from(bucket: "{influx.bucket}")
                |> range(start: {start}T00:00:00Z, stop: {end}T23:59:59Z)
                |> filter(fn: (r) => r["_measurement"] == "energy_separated")
                |> filter(fn: (r) => r["{tag}"] == "{entity_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''
            tables = query_api.query(energy_query, org=influx.org)

            total_energy = 0
            total_heating = 0
            total_dhw = 0
            day_count = 0

            for table in tables:
                for record in table.records:
                    actual = record.values.get('total_energy_kwh') or record.values.get('actual_energy_kwh') or 0
                    total_energy += actual
                    total_heating += record.values.get('heating_energy_kwh', 0) or 0
                    total_dhw += record.values.get('dhw_energy_kwh', 0) or 0
                    day_count += 1

            # Average outdoor temp from heating_system or building_system
            measurement = 'building_system' if entity_type == 'building' else 'heating_system'
            outdoor_field = 'outdoor_temp_fvc' if entity_type == 'building' else 'outdoor_temperature'
            temp_query = f'''
                from(bucket: "{influx.bucket}")
                |> range(start: {start}T00:00:00Z, stop: {end}T23:59:59Z)
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => r["{tag}"] == "{entity_id}")
                |> filter(fn: (r) => r["_field"] == "{outdoor_field}")
                |> mean()
            '''
            temp_tables = query_api.query(temp_query, org=influx.org)
            avg_outdoor = None
            for table in temp_tables:
                for record in table.records:
                    avg_outdoor = round(record.get_value(), 1) if record.get_value() is not None else None

            results[label] = {
                'start': start,
                'end': end,
                'days': day_count,
                'total_energy_kwh': round(total_energy, 1),
                'heating_kwh': round(total_heating, 1),
                'dhw_kwh': round(total_dhw, 1),
                'avg_outdoor_temp': avg_outdoor,
            }

        # Calculate comparison
        p1 = results['period1']
        p2 = results['period2']
        if p1['total_energy_kwh'] > 0 and p2['total_energy_kwh'] > 0:
            pct_change = round((p2['total_energy_kwh'] - p1['total_energy_kwh']) / p1['total_energy_kwh'] * 100, 1)
            results['comparison'] = {
                'energy_change_kwh': round(p2['total_energy_kwh'] - p1['total_energy_kwh'], 1),
                'energy_change_pct': pct_change,
            }

        return results

    except Exception as e:
        return {'error': str(e)}


def check_data_gaps(entity_id: str, days: int = 14, entity_type: str = 'house') -> dict:
    """Find days with missing or sparse data."""
    availability = get_data_availability(entity_id, days=days, entity_type=entity_type)
    if 'error' in availability:
        return availability

    gaps = []
    for day in availability.get('data', []):
        count = day.get('count', 0)
        if count == 0:
            gaps.append({'date': day.get('date'), 'status': 'no_data', 'points': 0})
        elif count < 200:  # Expect ~288 points/day at 5-min intervals
            gaps.append({'date': day.get('date'), 'status': 'sparse', 'points': count})

    return {
        'entity_id': entity_id,
        'days_checked': days,
        'gaps_found': len(gaps),
        'gaps': gaps,
    }


def diagnose_missing_separation(entity_id: str, date: str, entity_type: str = 'house') -> dict:
    """
    Diagnose why energy separation is missing for a specific date.
    Date should be YYYY-MM-DD format.
    """
    # 1. Check profile/config
    if entity_type == 'building':
        config = _load_building(entity_id)
    else:
        config = _load_profile(entity_id)

    if 'error' in config:
        return {'diagnosis': f'Could not load config: {config["error"]}'}

    es = config.get('energy_separation', {})

    if not es.get('enabled', False):
        return {
            'diagnosis': 'Energy separation is disabled for this entity.',
            'fix': 'Enable energy_separation.enabled in the profile/config.'
        }

    meter_ids = config.get('meter_ids', [])
    if not meter_ids:
        return {
            'diagnosis': 'No energy meter ID is configured.',
            'fix': 'Add a meter_id to the profile/config.'
        }

    # 2. Check if meter data exists for that date
    influx = _get_influx()
    if not influx.client:
        return {'diagnosis': 'Cannot connect to InfluxDB.'}

    try:
        query_api = influx.client.query_api()

        meter_query = f'''
            from(bucket: "{influx.bucket}")
            |> range(start: {date}T00:00:00Z, stop: {date}T23:59:59Z)
            |> filter(fn: (r) => r["_measurement"] == "energy_meter")
            |> filter(fn: (r) => r["_field"] == "consumption")
            |> count()
        '''
        tables = query_api.query(meter_query, org=influx.org)
        meter_count = 0
        for table in tables:
            for record in table.records:
                meter_count += record.get_value() or 0

        if meter_count == 0:
            return {
                'diagnosis': f'No meter data imported for {date}.',
                'fix': 'Meter data may not have been exported yet. The import runs daily at 08:00.'
            }

        # 3. Check if energy_separated exists
        tag = 'building_id' if entity_type == 'building' else 'house_id'
        sep_query = f'''
            from(bucket: "{influx.bucket}")
            |> range(start: {date}T00:00:00Z, stop: {date}T23:59:59Z)
            |> filter(fn: (r) => r["_measurement"] == "energy_separated")
            |> filter(fn: (r) => r["{tag}"] == "{entity_id}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        sep_tables = query_api.query(sep_query, org=influx.org)
        sep_records = []
        for table in sep_tables:
            for record in table.records:
                sep_records.append(record.values)

        if sep_records:
            rec = sep_records[0]
            if rec.get('no_breakdown'):
                coverage = rec.get('data_coverage', 0)
                return {
                    'diagnosis': f'Data exists but coverage was {coverage:.0%}, below the 80% threshold.',
                    'detail': 'The day was marked no_breakdown because outdoor temperature data was insufficient.',
                }
            return {
                'diagnosis': 'Energy separation data exists for this date.',
                'data': {
                    'total_kwh': rec.get('total_energy_kwh') or rec.get('actual_energy_kwh'),
                    'heating_kwh': rec.get('heating_energy_kwh'),
                    'dhw_kwh': rec.get('dhw_energy_kwh'),
                }
            }

        # 4. Separation hasn't run
        return {
            'diagnosis': f'Meter data exists ({meter_count} hours) but energy separation has not run yet for {date}.',
            'fix': 'The energy pipeline runs daily at 08:00. It should process this date on the next run.'
        }

    except Exception as e:
        return {'diagnosis': f'Error querying InfluxDB: {str(e)}'}


# =========================================================================
# Admin-only tools
# =========================================================================

def get_system_overview() -> dict:
    """Get summary of all houses and buildings."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from customer_profile import CustomerProfile

    houses = []
    if os.path.exists(PROFILES_DIR):
        for filename in os.listdir(PROFILES_DIR):
            if filename.endswith('.json') and '_signals.json' not in filename:
                house_id = filename[:-5]
                try:
                    profile = CustomerProfile.load(house_id, PROFILES_DIR)
                    houses.append({
                        'id': house_id,
                        'friendly_name': profile.friendly_name,
                        'energy_separation_enabled': profile.energy_separation.enabled if hasattr(profile, 'energy_separation') else False,
                    })
                except Exception:
                    houses.append({'id': house_id, 'friendly_name': house_id})

    buildings = []
    if os.path.exists(BUILDINGS_DIR):
        for filename in os.listdir(BUILDINGS_DIR):
            if filename.endswith('.json') and not filename.endswith('.template'):
                building_id = filename[:-5]
                try:
                    with open(os.path.join(BUILDINGS_DIR, filename), 'r') as f:
                        bdata = json.load(f)
                    buildings.append({
                        'id': building_id,
                        'friendly_name': bdata.get('friendly_name', building_id),
                        'energy_separation_enabled': bdata.get('energy_separation', {}).get('enabled', False),
                    })
                except Exception:
                    buildings.append({'id': building_id, 'friendly_name': building_id})

    return {'houses': houses, 'buildings': buildings}


# =========================================================================
# Write tools (user+admin with edit permission)
# =========================================================================

def update_comfort_settings(house_id: str, target_indoor_temp: float = None,
                           acceptable_deviation: float = None) -> dict:
    """Update comfort settings in a house profile."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from customer_profile import CustomerProfile

    try:
        profile = CustomerProfile.load(house_id, PROFILES_DIR)
        changes = {}

        if target_indoor_temp is not None:
            if not 15 <= target_indoor_temp <= 28:
                return {'error': 'target_indoor_temp must be between 15 and 28'}
            old = profile.comfort.target_indoor_temp
            profile.comfort.target_indoor_temp = target_indoor_temp
            changes['target_indoor_temp'] = {'old': old, 'new': target_indoor_temp}

        if acceptable_deviation is not None:
            if not 0.5 <= acceptable_deviation <= 3.0:
                return {'error': 'acceptable_deviation must be between 0.5 and 3.0'}
            old = profile.comfort.acceptable_deviation
            profile.comfort.acceptable_deviation = acceptable_deviation
            changes['acceptable_deviation'] = {'old': old, 'new': acceptable_deviation}

        if changes:
            profile.save()
            return {'success': True, 'changes': changes}
        return {'message': 'No changes specified.'}

    except Exception as e:
        return {'error': str(e)}


def update_building_description(house_id: str, friendly_name: str = None,
                                description: str = None) -> dict:
    """Update friendly name or description in a house profile."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from customer_profile import CustomerProfile

    try:
        profile = CustomerProfile.load(house_id, PROFILES_DIR)
        changes = {}

        if friendly_name is not None:
            old = profile.friendly_name
            profile.friendly_name = friendly_name
            changes['friendly_name'] = {'old': old, 'new': friendly_name}

        if description is not None:
            old = profile.building.description if hasattr(profile.building, 'description') else ''
            profile.building.description = description
            changes['description'] = {'old': old, 'new': description}

        if changes:
            profile.save()
            return {'success': True, 'changes': changes}
        return {'message': 'No changes specified.'}

    except Exception as e:
        return {'error': str(e)}


# =========================================================================
# Tool registry — maps tool names to functions and metadata
# =========================================================================

TOOL_DEFINITIONS = [
    {
        'name': 'get_current_status',
        'description': 'Get the latest heating system readings for a house (temperatures, pressure, modes). Use this when the user asks about current status or live data.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'house_id': {'type': 'string', 'description': 'House ID, e.g. HEM_FJV_Villa_149'},
            },
            'required': ['house_id'],
        },
        'requires_access': 'house',
        'write': False,
    },
    {
        'name': 'get_energy_consumption',
        'description': 'Get daily energy consumption history (total kWh per day). Use this for energy usage questions.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'House or building ID'},
                'days': {'type': 'integer', 'description': 'Number of days (default 30, max 365)', 'default': 30},
                'entity_type': {'type': 'string', 'enum': ['house', 'building'], 'default': 'house'},
            },
            'required': ['entity_id'],
        },
        'requires_access': 'entity',
        'write': False,
    },
    {
        'name': 'get_energy_separation',
        'description': 'Get heating vs domestic hot water (DHW) energy breakdown per day. Shows how total energy is split between heating and hot water.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'House or building ID'},
                'days': {'type': 'integer', 'description': 'Number of days (default 30)', 'default': 30},
                'entity_type': {'type': 'string', 'enum': ['house', 'building'], 'default': 'house'},
            },
            'required': ['entity_id'],
        },
        'requires_access': 'entity',
        'write': False,
    },
    {
        'name': 'get_temperature_history',
        'description': 'Get temperature history (indoor, outdoor, supply, return temperatures over time).',
        'input_schema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'House or building ID'},
                'hours': {'type': 'integer', 'description': 'Number of hours (default 168 = 7 days)', 'default': 168},
                'entity_type': {'type': 'string', 'enum': ['house', 'building'], 'default': 'house'},
            },
            'required': ['entity_id'],
        },
        'requires_access': 'entity',
        'write': False,
    },
    {
        'name': 'get_weather_history',
        'description': 'Get weather observation history (temperature, wind, humidity from SMHI).',
        'input_schema': {
            'type': 'object',
            'properties': {
                'house_id': {'type': 'string', 'description': 'House ID'},
                'hours': {'type': 'integer', 'description': 'Number of hours (default 168)', 'default': 168},
            },
            'required': ['house_id'],
        },
        'requires_access': 'house',
        'write': False,
    },
    {
        'name': 'get_data_availability',
        'description': 'Get daily data point counts. Useful for finding days with missing or sparse data.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'House or building ID'},
                'days': {'type': 'integer', 'description': 'Number of days (default 30)', 'default': 30},
                'entity_type': {'type': 'string', 'enum': ['house', 'building'], 'default': 'house'},
            },
            'required': ['entity_id'],
        },
        'requires_access': 'entity',
        'write': False,
    },
    {
        'name': 'get_profile_info',
        'description': 'Get house profile: comfort settings, heating system config, learned thermal parameters, energy separation settings.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'house_id': {'type': 'string', 'description': 'House ID'},
            },
            'required': ['house_id'],
        },
        'requires_access': 'house',
        'write': False,
    },
    {
        'name': 'get_building_info',
        'description': 'Get building config: connection details, signal count, energy separation settings, alarm monitoring.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'building_id': {'type': 'string', 'description': 'Building ID'},
            },
            'required': ['building_id'],
        },
        'requires_access': 'building',
        'write': False,
    },
    {
        'name': 'compare_periods',
        'description': 'Compare energy consumption and temperatures between two date ranges. Use YYYY-MM-DD dates.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'House or building ID'},
                'period1_start': {'type': 'string', 'description': 'Start date of first period (YYYY-MM-DD)'},
                'period1_end': {'type': 'string', 'description': 'End date of first period (YYYY-MM-DD)'},
                'period2_start': {'type': 'string', 'description': 'Start date of second period (YYYY-MM-DD)'},
                'period2_end': {'type': 'string', 'description': 'End date of second period (YYYY-MM-DD)'},
                'entity_type': {'type': 'string', 'enum': ['house', 'building'], 'default': 'house'},
            },
            'required': ['entity_id', 'period1_start', 'period1_end', 'period2_start', 'period2_end'],
        },
        'requires_access': 'entity',
        'write': False,
    },
    {
        'name': 'check_data_gaps',
        'description': 'Find days with missing or sparse heating/sensor data.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'House or building ID'},
                'days': {'type': 'integer', 'description': 'Number of days to check (default 14)', 'default': 14},
                'entity_type': {'type': 'string', 'enum': ['house', 'building'], 'default': 'house'},
            },
            'required': ['entity_id'],
        },
        'requires_access': 'entity',
        'write': False,
    },
    {
        'name': 'diagnose_missing_separation',
        'description': 'Diagnose why energy separation data is missing for a specific date. Checks: enabled?, meter data?, coverage?, pipeline run?',
        'input_schema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'House or building ID'},
                'date': {'type': 'string', 'description': 'Date to diagnose (YYYY-MM-DD)'},
                'entity_type': {'type': 'string', 'enum': ['house', 'building'], 'default': 'house'},
            },
            'required': ['entity_id', 'date'],
        },
        'requires_access': 'entity',
        'write': False,
    },
    {
        'name': 'get_system_overview',
        'description': 'Get a summary of all houses and buildings in the system. Admin only.',
        'input_schema': {
            'type': 'object',
            'properties': {},
        },
        'requires_access': 'admin',
        'write': False,
    },
    {
        'name': 'update_comfort_settings',
        'description': 'Change comfort settings (target indoor temperature, acceptable deviation) for a house.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'house_id': {'type': 'string', 'description': 'House ID'},
                'target_indoor_temp': {'type': 'number', 'description': 'New target indoor temperature (15-28)'},
                'acceptable_deviation': {'type': 'number', 'description': 'New acceptable deviation (0.5-3.0)'},
            },
            'required': ['house_id'],
        },
        'requires_access': 'house_edit',
        'write': True,
    },
    {
        'name': 'update_building_description',
        'description': 'Update the friendly name or description of a house.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'house_id': {'type': 'string', 'description': 'House ID'},
                'friendly_name': {'type': 'string', 'description': 'New friendly name'},
                'description': {'type': 'string', 'description': 'New building description'},
            },
            'required': ['house_id'],
        },
        'requires_access': 'house_edit',
        'write': True,
    },
    {
        'name': 'create_support_ticket',
        'description': 'Escalate a question to support by creating a ticket. Use when you cannot resolve the user\'s issue.',
        'input_schema': {
            'type': 'object',
            'properties': {
                'summary': {'type': 'string', 'description': 'Brief summary of the issue'},
                'details': {'type': 'string', 'description': 'Detailed description including what was tried'},
            },
            'required': ['summary'],
        },
        'requires_access': 'none',
        'write': False,
    },
]

# Map tool names to functions
TOOL_FUNCTIONS = {
    'get_current_status': get_current_status,
    'get_energy_consumption': get_energy_consumption,
    'get_energy_separation': get_energy_separation,
    'get_temperature_history': get_temperature_history,
    'get_weather_history': get_weather_history,
    'get_data_availability': get_data_availability,
    'get_profile_info': get_profile_info,
    'get_building_info': get_building_info,
    'compare_periods': compare_periods,
    'check_data_gaps': check_data_gaps,
    'diagnose_missing_separation': diagnose_missing_separation,
    'get_system_overview': get_system_overview,
    'update_comfort_settings': update_comfort_settings,
    'update_building_description': update_building_description,
    # create_support_ticket is handled specially in ai_assistant.py
}

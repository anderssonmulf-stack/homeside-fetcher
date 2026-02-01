#!/usr/bin/env python3
"""
Svenskeb Settings GUI - Flask Web Application
Main entry point for the heating system settings interface.
"""

import os
import sys
import secrets

# Add parent directory to path for customer_profile module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from functools import wraps
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from auth import UserManager, require_login, require_role
from audit import AuditLogger
from email_service import EmailService
from fetcher_deployer import FetcherDeployer, create_htpasswd_entry, delete_htpasswd_entry, extract_customer_id_from_client_path
from grafana_helper import update_dashboard_house_variable

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))

# Initialize services
user_manager = UserManager()
audit_logger = AuditLogger()
email_service = EmailService()


# =============================================================================
# Context Processor - inject variables into all templates
# =============================================================================

@app.context_processor
def inject_pending_count():
    """Inject pending user count for admin badge"""
    if session.get('user_role') == 'admin':
        users = user_manager.get_all_users()
        pending_count = sum(1 for u in users.values() if u.get('role') == 'pending')
        return {'pending_count': pending_count}
    return {'pending_count': 0}


def get_houses_with_names():
    """Get all houses with their friendly names"""
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    houses = []

    if os.path.exists(profiles_dir):
        for filename in os.listdir(profiles_dir):
            if filename.endswith('.json'):
                house_id = filename[:-5]
                try:
                    profile = CustomerProfile.load(house_id, profiles_dir)
                    houses.append({
                        'id': house_id,
                        'friendly_name': profile.friendly_name or house_id
                    })
                except Exception:
                    houses.append({'id': house_id, 'friendly_name': house_id})

    return houses


# =============================================================================
# Authentication Routes
# =============================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    """User login page"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        password = request.form.get('password', '')

        user = user_manager.authenticate(username, password)

        if user:
            if user['role'] == 'pending':
                flash('Your account is awaiting admin approval.', 'warning')
                return render_template('login.html')

            session['user_id'] = username
            session['user_name'] = user['name']
            session['user_role'] = user['role']
            session['user_houses'] = user['houses']

            audit_logger.log('UserLogin', username, {'ip': request.remote_addr})
            flash(f'Welcome, {user["name"]}!', 'success')
            return redirect(url_for('dashboard'))
        else:
            audit_logger.log('LoginFailed', username or 'unknown', {'ip': request.remote_addr})
            flash('Invalid username or password.', 'error')

    return render_template('login.html')


@app.route('/logout')
def logout():
    """Log out current user"""
    username = session.get('user_id', 'unknown')
    audit_logger.log('UserLogout', username, {})
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


def _verify_homeside_credentials(homeside_username: str, homeside_password: str) -> dict:
    """
    Verify HomeSide credentials and return result.
    Returns dict with 'valid' bool and optional 'customer_id', 'house_name', 'error'.
    """
    import requests

    try:
        # Remove spaces from username (HomeSide quirk)
        username_clean = homeside_username.replace(' ', '')

        auth_url = "https://homeside.systeminstallation.se/api/v2/authorize/account"
        payload = {
            "user": {
                "Account": "homeside",
                "UserName": username_clean,
                "Password": homeside_password
            },
            "lang": "sv"
        }

        response = requests.post(auth_url, json=payload, timeout=15)
        if response.status_code != 200:
            return {'valid': False, 'error': f'HomeSide API error (HTTP {response.status_code})'}

        result = response.json()
        session_token = result.get('querykey')

        if not session_token:
            return {'valid': False, 'error': 'Invalid HomeSide username or password'}

        # Get house list to extract customer_id
        house_response = requests.post(
            "https://homeside.systeminstallation.se/api/v2/housefidlist",
            json={},
            headers={'Authorization': session_token},
            timeout=15
        )

        if house_response.status_code == 200:
            houses = house_response.json()
            if houses:
                house = houses[0]
                client_path = house.get('restapiurl', '')
                customer_id = extract_customer_id_from_client_path(client_path)
                return {
                    'valid': True,
                    'customer_id': customer_id,
                    'house_name': house.get('name', ''),
                    'client_path': client_path
                }

        return {'valid': True, 'customer_id': '', 'house_name': ''}

    except requests.Timeout:
        return {'valid': False, 'error': 'HomeSide API timeout - please try again'}
    except Exception as e:
        return {'valid': False, 'error': f'Could not verify HomeSide credentials: {str(e)}'}


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Self-registration for new users"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip().lower()
        note = request.form.get('note', '').strip()

        # HomeSide credentials
        homeside_username = request.form.get('homeside_username', '').strip()
        homeside_password = request.form.get('homeside_password', '').strip()
        house_friendly_name = request.form.get('house_friendly_name', '').strip()

        # Validation
        errors = []
        if len(username) < 3:
            errors.append('Username must be at least 3 characters.')
        if len(password) < 8:
            errors.append('Password must be at least 8 characters.')
        if password != confirm_password:
            errors.append('Passwords do not match.')
        if not name:
            errors.append('Name is required.')
        if not email or '@' not in email:
            errors.append('Valid email is required.')
        if not homeside_username:
            errors.append('HomeSide username is required.')
        if not homeside_password:
            errors.append('HomeSide password is required.')
        if not house_friendly_name:
            errors.append('House name is required.')
        if user_manager.user_exists(username):
            errors.append('Username already taken.')
        if user_manager.email_exists(email):
            errors.append('Email already registered.')

        # Verify HomeSide credentials before allowing registration
        homeside_verified = None
        if homeside_username and homeside_password and not errors:
            homeside_verified = _verify_homeside_credentials(homeside_username, homeside_password)
            if not homeside_verified['valid']:
                errors.append(homeside_verified.get('error', 'Invalid HomeSide credentials'))

        if errors:
            for error in errors:
                flash(error, 'error')
            return render_template('register.html',
                                   username=username, name=name, email=email, note=note,
                                   homeside_username=homeside_username,
                                   homeside_password=homeside_password,
                                   house_friendly_name=house_friendly_name)

        # Create pending user with verified customer_id
        verified_customer_id = ''
        if homeside_verified and homeside_verified.get('valid'):
            verified_customer_id = homeside_verified.get('customer_id', '')

        user_manager.create_user(
            username=username,
            password=password,
            name=name,
            email=email,
            role='pending',
            houses=[],
            registration_note=note,
            homeside_username=homeside_username,
            homeside_password=homeside_password,
            house_friendly_name=house_friendly_name,
            verified_customer_id=verified_customer_id
        )

        audit_logger.log('UserRegistered', username, {'email': email, 'house_name': house_friendly_name})

        # Create htpasswd entry for Grafana access (same credentials as webgui)
        htpasswd_created = create_htpasswd_entry(username, password)
        if htpasswd_created:
            audit_logger.log('HtpasswdCreated', username, {'for_grafana': True})

        # Notify admins
        email_service.notify_admins_new_registration(username, name, email, note)

        flash('Registration submitted! An admin will review your application.', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')


# =============================================================================
# Dashboard & Main Pages
# =============================================================================

@app.route('/')
@require_login
def dashboard():
    """Main dashboard - shows user's houses and recent activity"""
    user_houses = session.get('user_houses', [])
    role = session.get('user_role')

    # Get houses this user can see
    if role == 'admin' or '*' in user_houses:
        houses = user_manager.get_all_houses()
    else:
        houses = user_houses

    # Get recent changes for user's houses
    recent_changes = audit_logger.get_recent_changes(houses, limit=10)

    return render_template('dashboard.html',
                           houses=houses,
                           recent_changes=recent_changes)


@app.route('/house/<house_id>')
@require_login
def house_detail(house_id):
    """View/edit settings for a specific house"""
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        flash('You do not have access to this house.', 'error')
        return redirect(url_for('dashboard'))

    # Load house profile
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
    except FileNotFoundError:
        flash('House profile not found.', 'error')
        return redirect(url_for('dashboard'))

    can_edit = user_manager.can_edit_house(session.get('user_id'), house_id)

    # Get real-time data from InfluxDB
    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    realtime_data = influx.get_latest_heating_data(house_id)
    forecast_data = influx.get_forecast_summary(house_id)

    # Get change history for this house
    changes = audit_logger.get_house_changes(house_id, limit=20)

    return render_template('house_detail.html',
                           profile=profile,
                           house_id=house_id,
                           can_edit=can_edit,
                           changes=changes,
                           realtime=realtime_data,
                           forecast=forecast_data)


@app.route('/house/<customer_id>/dashboard')
@require_login
def house_dashboard(customer_id):
    """Display embedded Grafana dashboard for a house."""
    if not user_manager.can_access_house(session.get('user_id'), customer_id):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))

    # Get house info from profile
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    try:
        profile = CustomerProfile.load(customer_id, profiles_dir)
        friendly_name = profile.friendly_name or customer_id
    except FileNotFoundError:
        friendly_name = customer_id

    # Get all houses for this user (for house selector)
    user = user_manager.get_user(session.get('user_id'))
    user_houses = []
    if user:
        house_ids = user.get('houses', [])
        # Admin or wildcard user gets all houses
        if user.get('role') == 'admin' or '*' in house_ids:
            house_ids = user_manager.get_all_houses()

        for house_id in house_ids:
            if house_id != '*':
                try:
                    p = CustomerProfile.load(house_id, profiles_dir)
                    user_houses.append({
                        'id': house_id,
                        'name': p.friendly_name or house_id
                    })
                except FileNotFoundError:
                    user_houses.append({
                        'id': house_id,
                        'name': house_id
                    })

    # Sort by name for consistent display
    user_houses.sort(key=lambda x: x['name'].lower())

    return render_template('house_dashboard.html',
        customer_id=customer_id,
        friendly_name=friendly_name,
        user_houses=user_houses,
        grafana_base='/grafana',
        dashboard_uid='homeside-v3'
    )


@app.route('/house/<house_id>/graphs')
@require_login
def house_graphs(house_id):
    """Display data availability and graphs for a house."""
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))

    # Get house info from profile
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
        friendly_name = profile.friendly_name or house_id
    except FileNotFoundError:
        friendly_name = house_id

    # Get data availability
    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    availability = influx.get_data_availability(house_id, days=30)

    # Build Plotly chart data - using heatmap for data availability
    import plotly.graph_objects as go
    import json

    fig = go.Figure()

    if availability['categories']:
        # Collect all unique dates
        all_dates = set()
        for cat in availability['categories']:
            for d in cat['data']:
                all_dates.add(d['date'])
        all_dates = sorted(all_dates)

        # Build heatmap data matrix
        category_names = [c['name'] for c in availability['categories']]
        category_types = [c['type'] for c in availability['categories']]

        # Create z-values matrix (categories x dates)
        z_values = []
        for cat in availability['categories']:
            date_to_count = {d['date']: d['count'] for d in cat['data']}
            row = [date_to_count.get(date, 0) for date in all_dates]
            z_values.append(row)

        # Custom colorscale: white (0) to blue (measured)
        # We'll use annotations to show predicted vs measured distinction
        fig.add_trace(go.Heatmap(
            z=z_values,
            x=all_dates,
            y=category_names,
            colorscale=[
                [0, '#f8f9fa'],      # No data - light gray
                [0.01, '#e3f2fd'],   # Very few points - very light blue
                [0.25, '#90caf9'],   # Some data - light blue
                [0.5, '#42a5f5'],    # Good coverage - medium blue
                [1, '#1565c0']       # Full coverage - dark blue
            ],
            showscale=True,
            colorbar=dict(
                title=dict(text='Data points', side='right')
            ),
            hovertemplate='%{x}<br>%{y}: %{z} points<extra></extra>'
        ))

        # Add markers for predicted data types (dotted border effect via annotations)
        for i, cat_type in enumerate(category_types):
            if cat_type == 'predicted':
                # Add a subtle indicator that this is predicted data
                fig.add_annotation(
                    x=-0.02,
                    y=category_names[i],
                    xref='paper',
                    yref='y',
                    text='*',
                    showarrow=False,
                    font=dict(size=14, color='#9b59b6')
                )

    # Update layout
    fig.update_layout(
        title=None,
        xaxis=dict(
            title='Date',
            showgrid=True,
            gridcolor='rgba(0,0,0,0.1)',
            tickangle=-45
        ),
        yaxis=dict(
            title=None,
            autorange='reversed'  # First category at top
        ),
        height=max(300, len(availability['categories']) * 40 + 150),
        margin=dict(l=130, r=80, t=20, b=80),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='#ffffff',
        font=dict(family='system-ui, -apple-system, sans-serif'),
        dragmode='pan'
    )

    # Add config for interactivity
    graph_config = {
        'displayModeBar': True,
        'modeBarButtonsToRemove': ['lasso2d', 'select2d', 'autoScale2d'],
        'displaylogo': False,
        'scrollZoom': True
    }

    graph_json = json.dumps(fig.to_dict())
    config_json = json.dumps(graph_config)

    return render_template('house_graphs.html',
        house_id=house_id,
        friendly_name=friendly_name,
        graph_json=graph_json,
        config_json=config_json,
        availability=availability
    )


@app.route('/api/house/<house_id>/data-availability')
@require_login
def api_data_availability(house_id):
    """API endpoint for data availability chart (for dynamic updates)."""
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)  # Clamp between 7 and 365

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    availability = influx.get_data_availability(house_id, days=days)

    return jsonify(availability)


@app.route('/api/house/<house_id>/effective-temperature')
@require_login
def api_effective_temperature(house_id):
    """
    API endpoint for effective temperature chart.
    Returns actual temp, effective temp, and effect breakdown.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)  # Default 7 days
    hours = min(max(hours, 24), 720)  # Clamp between 1 and 30 days

    # Get house location from profile
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
        latitude = getattr(profile, 'latitude', None)
        longitude = getattr(profile, 'longitude', None)
    except FileNotFoundError:
        latitude = None
        longitude = None

    # Fallback to environment or default (Linköping area)
    if latitude is None:
        latitude = float(os.environ.get('LATITUDE', '58.41'))
    if longitude is None:
        longitude = float(os.environ.get('LONGITUDE', '15.62'))

    # Get weather history
    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    weather_data = influx.get_weather_history(house_id, hours=hours)

    if not weather_data:
        return jsonify({'error': 'No weather data available', 'data': []})

    # Calculate effective temperatures
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from energy_models import get_weather_model
    from energy_models.weather_energy_model import WeatherConditions
    from datetime import datetime

    model = get_weather_model('simple')

    results = []
    for w in weather_data:
        if w.get('temperature') is None:
            continue

        try:
            timestamp = datetime.fromisoformat(w['timestamp'].replace('Z', '+00:00'))

            conditions = WeatherConditions(
                timestamp=timestamp,
                temperature=w['temperature'],
                wind_speed=w.get('wind_speed') or 0,
                humidity=w.get('humidity') or 50,
                cloud_cover=w.get('cloud_cover', 4.0),  # From weather_forecast data
                latitude=latitude,
                longitude=longitude
            )

            eff = model.effective_temperature(conditions)

            results.append({
                'timestamp': w['timestamp'],
                'timestamp_display': w['timestamp_swedish'],
                'actual_temp': round(w['temperature'], 1),
                'effective_temp': round(eff.effective_temp, 1),
                'wind_effect': round(eff.wind_effect, 2),
                'humidity_effect': round(eff.humidity_effect, 2),
                'solar_effect': round(eff.solar_effect, 2),
                'sun_elevation': round(eff.sun_elevation, 1) if eff.sun_elevation else None,
                'wind_speed': w.get('wind_speed'),
                'humidity': w.get('humidity'),
                'cloud_cover': w.get('cloud_cover'),
            })
        except Exception as e:
            continue

    return jsonify({
        'data': results,
        'model_version': model.model_version,
        'location': {'latitude': latitude, 'longitude': longitude}
    })


@app.route('/house/<house_id>/settings', methods=['POST'])
@require_login
def update_house_settings(house_id):
    """Update settings for a house"""
    if not user_manager.can_edit_house(session.get('user_id'), house_id):
        flash('You do not have permission to edit this house.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
    except FileNotFoundError:
        flash('House profile not found.', 'error')
        return redirect(url_for('dashboard'))

    # Track changes
    changes = {}

    # Update friendly name
    new_friendly_name = request.form.get('friendly_name', '').strip()
    if new_friendly_name and new_friendly_name != profile.friendly_name:
        changes['friendly_name'] = {
            'old': profile.friendly_name,
            'new': new_friendly_name
        }
        profile.friendly_name = new_friendly_name

    # Update building description
    new_description = request.form.get('building_description', '').strip()
    if new_description != profile.building.description:
        changes['building_description'] = {
            'old': profile.building.description,
            'new': new_description
        }
        profile.building.description = new_description

    # Update comfort settings
    new_target = request.form.get('target_indoor_temp', type=float)
    if new_target and new_target != profile.comfort.target_indoor_temp:
        changes['target_indoor_temp'] = {
            'old': profile.comfort.target_indoor_temp,
            'new': new_target
        }
        profile.comfort.target_indoor_temp = new_target

    new_deviation = request.form.get('acceptable_deviation', type=float)
    if new_deviation and new_deviation != profile.comfort.acceptable_deviation:
        changes['acceptable_deviation'] = {
            'old': profile.comfort.acceptable_deviation,
            'new': new_deviation
        }
        profile.comfort.acceptable_deviation = new_deviation

    if changes:
        profile.save()

        # Log each change
        for setting, values in changes.items():
            audit_logger.log('SettingChanged', session.get('user_id'), {
                'house_id': house_id,
                'setting': setting,
                'old_value': values['old'],
                'new_value': values['new'],
                'source': 'gui'
            })

        flash('Settings updated successfully.', 'success')
    else:
        flash('No changes made.', 'info')

    return redirect(url_for('house_detail', house_id=house_id))


# =============================================================================
# Energy Import Routes
# =============================================================================

ENERGY_TYPE_LABELS = {
    'fjv_total': 'Fjärrvärme kWh (Total)',
    'fjv_heating': 'Fjärrvärme kWh (Heating)',
    'fjv_tapwater': 'Fjärrvärme kWh (Tap Water)',
}


@app.route('/house/<house_id>/energy/upload', methods=['POST'])
@require_login
def upload_energy_data(house_id):
    """Handle energy CSV upload and show dry run preview"""
    if not user_manager.can_edit_house(session.get('user_id'), house_id):
        flash('You do not have permission to import data for this house.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Get profile for friendly name
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
        house_name = profile.friendly_name or house_id
    except FileNotFoundError:
        flash('House profile not found.', 'error')
        return redirect(url_for('dashboard'))

    # Validate file upload
    if 'energy_file' not in request.files:
        flash('No file selected.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    file = request.files['energy_file']
    if file.filename == '':
        flash('No file selected.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Check file extension
    if not file.filename.lower().endswith('.csv'):
        flash('Only CSV files are supported.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Check file size (max 1MB)
    file_content = file.read()
    if len(file_content) > 1024 * 1024:
        flash('File too large. Maximum size is 1MB.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Get energy type
    energy_type = request.form.get('energy_type', 'fjv_total')
    if energy_type not in ENERGY_TYPE_LABELS:
        energy_type = 'fjv_total'

    # Run dry run
    from energy_import_service import get_energy_importer
    importer = get_energy_importer()
    result = importer.dry_run(house_id, file_content, energy_type)

    # Store parsed data in a temp file (session cookie has 4KB limit)
    if result['success'] and result['new_rows'] > 0:
        import json
        import uuid
        import tempfile

        # Create unique file for this import
        import_id = str(uuid.uuid4())
        temp_dir = os.path.join(tempfile.gettempdir(), 'svenskeb_imports')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f'{import_id}.json')

        # Save parsed data to temp file
        import_data = {
            'house_id': house_id,
            'energy_type': energy_type,
            'data': [
                {
                    'timestamp': row['timestamp'].isoformat(),
                    'value': row['value'],
                    'energy_type': row['energy_type']
                }
                for row in result['parsed_data']
            ],
            'new_kwh': result['new_kwh']
        }
        with open(temp_file, 'w') as f:
            json.dump(import_data, f)

        # Store only the import ID in session
        session['pending_import_id'] = import_id

    return render_template('energy_import.html',
        house_id=house_id,
        house_name=house_name,
        energy_type=energy_type,
        energy_type_label=ENERGY_TYPE_LABELS.get(energy_type, energy_type),
        result=result
    )


@app.route('/house/<house_id>/energy/confirm', methods=['POST'])
@require_login
def confirm_energy_import(house_id):
    """Confirm and execute the energy data import"""
    if not user_manager.can_edit_house(session.get('user_id'), house_id):
        flash('You do not have permission to import data for this house.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Retrieve import ID from session
    import_id = session.pop('pending_import_id', None)
    if not import_id:
        flash('Import session expired. Please try again.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Load parsed data from temp file
    import json
    import tempfile
    temp_dir = os.path.join(tempfile.gettempdir(), 'svenskeb_imports')
    temp_file = os.path.join(temp_dir, f'{import_id}.json')

    try:
        with open(temp_file, 'r') as f:
            pending = json.load(f)
        # Clean up temp file
        os.remove(temp_file)
    except FileNotFoundError:
        flash('Import session expired. Please try again.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Verify house_id matches
    if pending['house_id'] != house_id:
        flash('Import session mismatch. Please try again.', 'error')
        return redirect(url_for('house_detail', house_id=house_id))

    # Convert ISO strings back to datetime objects
    from datetime import datetime
    parsed_data = []
    for row in pending['data']:
        parsed_data.append({
            'timestamp': datetime.fromisoformat(row['timestamp']),
            'value': row['value'],
            'energy_type': row['energy_type']
        })

    # Execute import
    from energy_import_service import get_energy_importer
    importer = get_energy_importer()
    result = importer.import_data(house_id, parsed_data)

    if result['success']:
        # Log the import
        audit_logger.log('EnergyDataImported', session.get('user_id'), {
            'house_id': house_id,
            'energy_type': pending['energy_type'],
            'rows_imported': result['rows_written'],
            'total_kwh': pending['new_kwh']
        })
        flash(f"Successfully imported {result['rows_written']} rows ({pending['new_kwh']:.1f} kWh).", 'success')
    else:
        flash(f"Import failed: {result['error']}", 'error')

    return redirect(url_for('house_detail', house_id=house_id))


# =============================================================================
# Admin Routes
# =============================================================================

@app.route('/admin/users')
@require_role('admin')
def admin_users():
    """Admin: User management page"""
    users = user_manager.get_all_users()
    pending_count = sum(1 for u in users.values() if u['role'] == 'pending')
    deleted_users = user_manager.get_deleted_users()
    pending_purge = user_manager.get_users_pending_purge()
    all_houses = get_houses_with_names()
    return render_template('admin_users.html',
                           users=users,
                           pending_count=pending_count,
                           deleted_users=deleted_users,
                           pending_purge_count=len(pending_purge),
                           all_houses=all_houses)


@app.route('/admin/users/<username>/approve', methods=['POST'])
@require_role('admin')
def admin_approve_user(username):
    """Admin: Approve a pending user and deploy their fetcher"""
    houses = request.form.getlist('houses')
    # Filter out empty strings
    houses = [h for h in houses if h]
    role = request.form.get('role', 'user')
    customer_id = request.form.get('customer_id', '')

    # Validate role
    if role not in ['user', 'viewer', 'admin']:
        role = 'user'

    # Get user before approval to access HomeSide credentials
    user = user_manager.get_user(username)
    if not user:
        flash(f'User {username} not found.', 'error')
        return redirect(url_for('admin_users'))

    # Use verified_customer_id from registration, or try to discover it
    if not customer_id:
        customer_id = user.get('verified_customer_id', '')

    if not customer_id and user.get('homeside_username') and user.get('homeside_password'):
        discovered = _discover_customer_id(
            user.get('homeside_username'),
            user.get('homeside_password')
        )
        if discovered:
            customer_id = discovered.get('customer_id', '')

    # Add discovered customer_id to houses list (even before deploy attempt)
    if customer_id and customer_id not in houses:
        houses.append(customer_id)

    if user_manager.approve_user(username, houses, role=role, approved_by=session.get('user_id')):
        # Refresh user data after approval
        user = user_manager.get_user(username)

        audit_logger.log('UserApproved', session.get('user_id'), {
            'approved_user': username,
            'role': role,
            'houses': houses,
            'customer_id': customer_id
        })

        # Deploy fetcher container if we have HomeSide credentials and customer_id
        deploy_result = None
        if customer_id and user.get('homeside_username') and user.get('homeside_password'):
            deployer = FetcherDeployer()
            deploy_result = deployer.deploy_fetcher(
                customer_id=customer_id,
                friendly_name=user.get('house_friendly_name', customer_id),
                homeside_username=user.get('homeside_username'),
                homeside_password=user.get('homeside_password')
            )

            if deploy_result.get('success'):
                audit_logger.log('FetcherDeployed', session.get('user_id'), {
                    'customer_id': customer_id,
                    'container_name': deploy_result.get('container_name'),
                    'for_user': username
                })
                # Update Grafana dashboard to include new house
                update_dashboard_house_variable()
            else:
                audit_logger.log('FetcherDeployFailed', session.get('user_id'), {
                    'customer_id': customer_id,
                    'error': deploy_result.get('error', 'Unknown'),
                    'for_user': username
                })

        # Send welcome email
        email_service.send_welcome_email(user)

        if deploy_result and deploy_result.get('success'):
            flash(f'User {username} approved and fetcher deployed for {customer_id}.', 'success')
        elif customer_id:
            flash(f'User {username} approved but fetcher deployment failed. Check logs.', 'warning')
        else:
            flash(f'User {username} approved. No HomeSide credentials for auto-deployment.', 'info')
    else:
        flash(f'Failed to approve user {username}.', 'error')

    return redirect(url_for('admin_users'))


def _discover_customer_id(homeside_username: str, homeside_password: str) -> dict:
    """Discover customer_id by authenticating with HomeSide API."""
    import requests

    try:
        # Remove spaces from username (HomeSide quirk)
        username = homeside_username.replace(' ', '')

        auth_url = "https://homeside.systeminstallation.se/api/v2/authorize/account"
        payload = {
            "user": {
                "Account": "homeside",
                "UserName": username,
                "Password": homeside_password
            },
            "lang": "sv"
        }

        response = requests.post(auth_url, json=payload, timeout=15)
        if response.status_code != 200:
            return {}

        result = response.json()
        session_token = result.get('querykey')
        if not session_token:
            return {}

        # Get house list
        house_response = requests.post(
            "https://homeside.systeminstallation.se/api/v2/housefidlist",
            json={},
            headers={'Authorization': session_token},
            timeout=15
        )

        if house_response.status_code == 200:
            houses = house_response.json()
            if houses:
                house = houses[0]
                client_path = house.get('restapiurl', '')
                return {
                    'customer_id': extract_customer_id_from_client_path(client_path),
                    'client_path': client_path,
                    'house_name': house.get('name', '')
                }
    except Exception:
        pass

    return {}


@app.route('/admin/users/<username>/reject', methods=['POST'])
@require_role('admin')
def admin_reject_user(username):
    """Admin: Reject a pending user"""
    reason = request.form.get('reason', '')

    user = user_manager.get_user(username)
    if user:
        audit_logger.log('UserRejected', session.get('user_id'), {
            'rejected_user': username,
            'reason': reason
        })

        # Send rejection email
        email_service.send_rejection_email(user, reason)

        user_manager.delete_user(username)
        flash(f'User {username} has been rejected.', 'success')
    else:
        flash(f'User {username} not found.', 'error')

    return redirect(url_for('admin_users'))


@app.route('/admin/users/<username>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_edit_user(username):
    """Admin: Edit user details"""
    user = user_manager.get_user(username)
    if not user:
        flash('User not found.', 'error')
        return redirect(url_for('admin_users'))

    if request.method == 'POST':
        new_role = request.form.get('role')
        new_houses = request.form.getlist('houses')
        # Filter out empty strings
        new_houses = [h for h in new_houses if h]

        changes = {}
        if new_role != user['role']:
            changes['role'] = {'old': user['role'], 'new': new_role}
        if set(new_houses) != set(user['houses']):
            changes['houses'] = {'old': user['houses'], 'new': new_houses}

        if changes:
            user_manager.update_user(username, role=new_role, houses=new_houses)

            audit_logger.log('UserModified', session.get('user_id'), {
                'modified_user': username,
                'changes': changes
            })

            flash(f'User {username} updated.', 'success')

        return redirect(url_for('admin_users'))

    all_houses = get_houses_with_names()
    return render_template('admin_edit_user.html', user=user, username=username, all_houses=all_houses)


@app.route('/admin/users/<username>/soft-delete', methods=['POST'])
@require_role('admin')
def admin_soft_delete_user(username):
    """Admin: Soft delete - disable account, stop data collection"""
    user = user_manager.get_user(username)
    if not user:
        flash('User not found.', 'error')
        return redirect(url_for('admin_users'))

    # Prevent deleting yourself
    if username == session.get('user_id'):
        flash('You cannot delete your own account.', 'error')
        return redirect(url_for('admin_users'))

    # Stop fetcher container(s)
    deployer = FetcherDeployer()
    customer_ids = [h for h in user.get('houses', []) if h != '*']
    for customer_id in customer_ids:
        deployer.soft_offboard(customer_id)

    # Mark user as deleted
    user_manager.soft_delete_user(username)

    audit_logger.log('UserSoftDeleted', session.get('user_id'), {
        'deleted_user': username,
        'customer_ids': customer_ids,
        'scheduled_purge': '30 days'
    })

    flash(f'User {username} disabled. Data will be permanently deleted in 30 days.', 'warning')
    return redirect(url_for('admin_users'))


@app.route('/admin/users/<username>/restore', methods=['POST'])
@require_role('admin')
def admin_restore_user(username):
    """Admin: Restore a soft-deleted user"""
    user = user_manager.get_user(username)
    if not user or user.get('role') != 'deleted':
        flash('User not found or not deleted.', 'error')
        return redirect(url_for('admin_users'))

    # Restore user
    user_manager.restore_user(username, role='user')

    # Restart fetcher container(s)
    deployer = FetcherDeployer()
    customer_ids = [h for h in user.get('houses', []) if h != '*']
    for customer_id in customer_ids:
        # Re-deploy using stored credentials
        deployer.deploy_fetcher(
            customer_id=customer_id,
            friendly_name=user.get('house_friendly_name', customer_id),
            homeside_username=user.get('homeside_username', ''),
            homeside_password=user.get('homeside_password', '')
        )

    audit_logger.log('UserRestored', session.get('user_id'), {
        'restored_user': username,
        'customer_ids': customer_ids
    })

    flash(f'User {username} has been restored.', 'success')
    return redirect(url_for('admin_users'))


@app.route('/admin/users/<username>/hard-delete', methods=['POST'])
@require_role('admin')
def admin_hard_delete_user(username):
    """Admin: Permanent deletion - remove all data"""
    confirm = request.form.get('confirm_delete', '')

    if confirm != username:
        flash('Please confirm by typing the username exactly.', 'error')
        return redirect(url_for('admin_edit_user', username=username))

    user = user_manager.get_user(username)
    if not user:
        flash('User not found.', 'error')
        return redirect(url_for('admin_users'))

    # Prevent deleting yourself
    if username == session.get('user_id'):
        flash('You cannot delete your own account.', 'error')
        return redirect(url_for('admin_users'))

    deployer = FetcherDeployer()
    customer_ids = [h for h in user.get('houses', []) if h != '*']

    for customer_id in customer_ids:
        deployer.hard_offboard(customer_id, username)

    # Delete user account
    user_manager.delete_user(username)

    # Remove htpasswd entry
    delete_htpasswd_entry(username)

    audit_logger.log('UserHardDeleted', session.get('user_id'), {
        'deleted_user': username,
        'customer_ids': customer_ids
    })

    flash(f'User {username} and all data permanently deleted.', 'success')
    return redirect(url_for('admin_users'))


@app.route('/admin/purge-deleted', methods=['POST'])
@require_role('admin')
def admin_purge_deleted():
    """Admin: Purge all users past their scheduled deletion date"""
    pending = user_manager.get_users_pending_purge()
    deployer = FetcherDeployer()
    purged = []

    for user in pending:
        username = user['username']
        customer_ids = [h for h in user.get('houses', []) if h != '*']

        for customer_id in customer_ids:
            deployer.hard_offboard(customer_id, username)

        user_manager.delete_user(username)
        delete_htpasswd_entry(username)
        purged.append(username)

    if purged:
        audit_logger.log('BulkPurge', session.get('user_id'), {
            'purged_users': purged
        })
        flash(f'Purged {len(purged)} users: {", ".join(purged)}', 'success')
    else:
        flash('No users pending purge.', 'info')

    return redirect(url_for('admin_users'))


# =============================================================================
# Email Action Routes (token-based, no login required)
# =============================================================================

@app.route('/action/<token>/<action>')
def handle_action(token, action):
    """Handle email action links (approve/decline)"""
    if action not in ['approve', 'decline']:
        flash('Invalid action.', 'error')
        return render_template('action_result.html', success=False)

    pending_action = user_manager.get_pending_action(token)

    if not pending_action:
        flash('This link has expired or is invalid.', 'error')
        return render_template('action_result.html', success=False)

    # Process the action
    result = user_manager.complete_action(token, action)

    audit_logger.log('ActionCompleted', pending_action.get('user_id', 'unknown'), {
        'action_type': pending_action.get('type'),
        'action': action,
        'house_id': pending_action.get('house_id')
    })

    return render_template('action_result.html',
                           success=True,
                           action=action,
                           action_type=pending_action.get('type'))


# =============================================================================
# API Routes (for AJAX)
# =============================================================================

@app.route('/api/grafana/houses')
def api_grafana_houses():
    """
    Public API endpoint for Grafana to get house list with friendly names.
    Returns format compatible with Grafana Infinity plugin.
    No authentication required - only returns public mapping data.
    """
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    houses = []

    if os.path.exists(profiles_dir):
        for filename in os.listdir(profiles_dir):
            if filename.endswith('.json'):
                customer_id = filename[:-5]
                try:
                    profile = CustomerProfile.load(customer_id, profiles_dir)
                    houses.append({
                        'text': profile.friendly_name or customer_id,
                        'value': customer_id
                    })
                except Exception:
                    houses.append({
                        'text': customer_id,
                        'value': customer_id
                    })

    # Sort by friendly name
    houses.sort(key=lambda x: x['text'].lower())

    return jsonify(houses)


@app.route('/api/activity')
@require_login
def api_activity():
    """Get recent activity for user's houses"""
    user_houses = session.get('user_houses', [])
    role = session.get('user_role')

    if role == 'admin' or '*' in user_houses:
        houses = None  # All houses
    else:
        houses = user_houses

    changes = audit_logger.get_recent_changes(houses, limit=50)
    return jsonify(changes)


@app.route('/api/test-homeside-credentials', methods=['POST'])
@require_role('admin')
def test_homeside_credentials():
    """Test HomeSide API credentials"""
    import requests

    data = request.get_json()
    username = data.get('username', '').replace(' ', '')
    password = data.get('password', '')

    if not username or not password:
        return jsonify({'success': False, 'error': 'Missing credentials'})

    try:
        auth_url = "https://homeside.systeminstallation.se/api/v2/authorize/account"
        payload = {
            "user": {
                "Account": "homeside",
                "UserName": username,
                "Password": password
            },
            "lang": "sv"
        }

        response = requests.post(auth_url, json=payload, timeout=15)

        if response.status_code == 200:
            result = response.json()
            if result.get('querykey'):
                # Try to get house list
                session_token = result.get('querykey')
                house_response = requests.post(
                    "https://homeside.systeminstallation.se/api/v2/housefidlist",
                    json={},
                    headers={'Authorization': session_token},
                    timeout=15
                )
                if house_response.status_code == 200:
                    houses = house_response.json()
                    if houses:
                        house = houses[0]
                        house_name = house.get('name', 'Unknown')
                        client_path = house.get('restapiurl', '')
                        customer_id = extract_customer_id_from_client_path(client_path)
                        return jsonify({
                            'success': True,
                            'client_id': house_name,
                            'client_path': client_path,
                            'customer_id': customer_id
                        })
                return jsonify({'success': True, 'client_id': 'Connected', 'client_path': '', 'customer_id': ''})
            else:
                return jsonify({'success': False, 'error': 'Invalid response'})
        elif response.status_code == 401:
            return jsonify({'success': False, 'error': 'Invalid credentials'})
        else:
            return jsonify({'success': False, 'error': f'HTTP {response.status_code}'})

    except requests.Timeout:
        return jsonify({'success': False, 'error': 'Connection timeout'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# =============================================================================
# Error Handlers
# =============================================================================

@app.errorhandler(404)
def not_found(e):
    return render_template('error.html', error='Page not found', code=404), 404


@app.errorhandler(500)
def server_error(e):
    return render_template('error.html', error='Server error', code=500), 500


# =============================================================================
# Main
# =============================================================================

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

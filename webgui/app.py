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
from theme import get_theme
from fetcher_deployer import FetcherDeployer, create_htpasswd_entry, delete_htpasswd_entry, extract_customer_id_from_client_path

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))

# Initialize services
site_theme = get_theme()
user_manager = UserManager()
audit_logger = AuditLogger(app_name=site_theme['audit_app_name'])
email_service = EmailService(theme=site_theme)


# =============================================================================
# Context Processor - inject variables into all templates
# =============================================================================

@app.context_processor
def inject_theme():
    """Inject theme variables into all templates"""
    return {'theme': site_theme}


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
            if filename.endswith('.json') and '_signals.json' not in filename:
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

            session['user_id'] = user['username']  # Use actual username from DB
            session['user_name'] = user['name']
            session['user_role'] = user['role']
            session['user_houses'] = user.get('houses', [])
            session['verified_customer_id'] = user.get('verified_customer_id', '')

            audit_logger.log('UserLogin', user['username'], {'ip': request.remote_addr})
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


@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Request a password reset email"""
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()

        if not email or '@' not in email:
            flash('Please enter a valid email address.', 'error')
            return render_template('forgot_password.html')

        # Try to create reset token (returns None if email not found)
        token = user_manager.create_password_reset_token(email)

        if token:
            # Email exists - send reset email
            user = user_manager.get_user_by_email(email)
            email_service.send_password_reset_email(email, user['name'], token)
            audit_logger.log('PasswordResetRequested', email, {'ip': request.remote_addr})

        # Always show the same message for security (don't reveal if email exists)
        flash('If an account with that email exists, we have sent password reset instructions.', 'info')
        return redirect(url_for('login'))

    return render_template('forgot_password.html')


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    """Reset password using a valid token"""
    # Validate token first
    token_data = user_manager.validate_password_reset_token(token)

    if not token_data:
        flash('This password reset link is invalid or has expired.', 'error')
        return redirect(url_for('forgot_password'))

    if request.method == 'POST':
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')

        errors = []
        if len(password) < 8:
            errors.append('Password must be at least 8 characters.')
        if password != confirm_password:
            errors.append('Passwords do not match.')

        if errors:
            for error in errors:
                flash(error, 'error')
            return render_template('reset_password.html', token=token)

        # Reset the password
        if user_manager.reset_password_with_token(token, password):
            audit_logger.log('PasswordReset', token_data['username'], {'ip': request.remote_addr})
            flash('Your password has been reset successfully. You can now log in.', 'success')
            return redirect(url_for('login'))
        else:
            flash('Failed to reset password. Please try again.', 'error')
            return redirect(url_for('forgot_password'))

    return render_template('reset_password.html', token=token)


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
        meter_number = request.form.get('meter_number', '').strip()

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
                                   house_friendly_name=house_friendly_name,
                                   meter_number=meter_number)

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
            verified_customer_id=verified_customer_id,
            meter_number=meter_number
        )

        audit_logger.log('UserRegistered', username, {'email': email, 'house_name': house_friendly_name})

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
    verified_customer_id = session.get('verified_customer_id', '')
    role = session.get('user_role')

    # Get houses this user can see
    if role == 'admin' or '*' in user_houses:
        house_ids = user_manager.get_all_houses()
    else:
        # Start with assigned houses
        house_ids = list(user_houses)
        # Add own house if not already included
        if verified_customer_id and verified_customer_id not in house_ids:
            house_ids.append(verified_customer_id)

    # Get houses with friendly names
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    houses = []
    for house_id in house_ids:
        if house_id == '*':
            continue
        try:
            profile = CustomerProfile.load(house_id, profiles_dir)
            houses.append({
                'id': house_id,
                'name': profile.friendly_name or house_id
            })
        except FileNotFoundError:
            houses.append({'id': house_id, 'name': house_id})

    # Sort by name
    houses.sort(key=lambda x: x['name'].lower())

    # Add "All houses" virtual card at the beginning (only if multiple houses)
    if len(houses) > 1:
        houses.insert(0, {'id': '__all__', 'name': 'All houses', 'is_aggregate': True})

    # Get buildings this user can see (admins and wildcard users see all)
    buildings = []
    if role == 'admin' or '*' in user_houses:
        building_ids = user_manager.get_all_buildings()
        buildings_dir = os.path.join(os.path.dirname(__file__), '..', 'buildings')
        for building_id in building_ids:
            filepath = os.path.join(buildings_dir, f"{building_id}.json")
            try:
                import json as _json
                with open(filepath, 'r') as f:
                    bdata = _json.load(f)
                buildings.append({
                    'id': building_id,
                    'name': bdata.get('friendly_name') or building_id,
                    'type': bdata.get('building_type', 'commercial'),
                })
            except Exception:
                buildings.append({'id': building_id, 'name': building_id, 'type': 'commercial'})
        buildings.sort(key=lambda x: x['name'].lower())

        if len(buildings) > 1:
            buildings.insert(0, {'id': '__all__', 'name': 'All buildings', 'is_aggregate': True, 'type': 'aggregate'})

    # Get recent changes for user's houses
    recent_changes = audit_logger.get_recent_changes(house_ids, limit=10)

    return render_template('dashboard.html',
                           houses=houses,
                           buildings=buildings,
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

    # Check if this is the user's own house (for showing/hiding settings)
    # Admins can always see settings
    is_own_house = (
        session.get('user_role') == 'admin' or
        session.get('verified_customer_id') == house_id
    )

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
                           is_own_house=is_own_house,
                           changes=changes,
                           realtime=realtime_data,
                           forecast=forecast_data,
                           current_house_name=profile.friendly_name or house_id)


@app.route('/house/<house_id>/graphs')
@require_login
def house_graphs(house_id):
    """Display data availability and graphs for a house."""
    # Handle aggregate "All houses" view
    is_aggregate = (house_id == '__all__')

    if not is_aggregate and not user_manager.can_access_house(session.get('user_id'), house_id):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))

    if is_aggregate:
        friendly_name = 'All houses'
        availability = {'categories': []}
        realtime_data = None
    else:
        # Get house info from profile
        from customer_profile import CustomerProfile
        profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
        try:
            profile = CustomerProfile.load(house_id, profiles_dir)
            friendly_name = profile.friendly_name or house_id
        except FileNotFoundError:
            friendly_name = house_id

        # Get data availability and real-time data
        from influx_reader import get_influx_reader
        influx = get_influx_reader()
        availability = influx.get_data_availability(house_id, days=30)
        realtime_data = influx.get_latest_heating_data(house_id)

    # Build Plotly chart data - using heatmap for data availability
    import plotly.graph_objects as go
    import json

    graph_json = '{}'
    config_json = '{}'

    if not is_aggregate:
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
        availability=availability,
        realtime=realtime_data,
        current_house_name=friendly_name,
        is_aggregate=is_aggregate
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

    # Get house location and ML2 coefficients from profile
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    solar_coefficient = None
    wind_coefficient = None
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
        latitude = getattr(profile, 'latitude', None)
        longitude = getattr(profile, 'longitude', None)
        # Get ML2 coefficients from learned parameters
        if hasattr(profile, 'learned') and profile.learned:
            wc = getattr(profile.learned, 'weather_coefficients', None)
            if wc:
                solar_coefficient = getattr(wc, 'solar_coefficient_ml2', None)
                wind_coefficient = getattr(wc, 'wind_coefficient_ml2', None)
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

    # Debug: check connection
    print(f"[DEBUG] InfluxDB client: {influx.client is not None}, URL: {influx.url}")

    weather_data = influx.get_weather_history(house_id, hours=hours)

    print(f"[DEBUG] Weather data for {house_id}: {len(weather_data)} points")

    if not weather_data:
        return jsonify({'error': 'No weather data available', 'data': [], 'debug': {'hours': hours, 'house_id': house_id}})

    # Calculate effective temperatures using ML2 coefficients if available
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from energy_models.weather_energy_model import SimpleWeatherModel, WeatherConditions
    from datetime import datetime

    # Create model with ML2 coefficients (or defaults if not available)
    model_kwargs = {}
    if solar_coefficient is not None:
        model_kwargs['solar_coefficient'] = solar_coefficient
    if wind_coefficient is not None:
        model_kwargs['wind_coefficient'] = wind_coefficient
    model = SimpleWeatherModel(**model_kwargs)

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


@app.route('/api/house/<house_id>/forecast-effective-temperature')
@require_login
def api_forecast_effective_temperature(house_id):
    """
    API endpoint for forecast effective temperature (12h ahead).
    Used for predicting control parameters.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours_ahead = request.args.get('hours', 12, type=int)
    hours_ahead = min(max(hours_ahead, 1), 48)

    # Get house location and ML2 coefficients from profile
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    solar_coefficient = None
    wind_coefficient = None
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
        latitude = getattr(profile, 'latitude', None)
        longitude = getattr(profile, 'longitude', None)
        # Get ML2 coefficients from learned parameters
        if hasattr(profile, 'learned') and profile.learned:
            wc = getattr(profile.learned, 'weather_coefficients', None)
            if wc:
                solar_coefficient = getattr(wc, 'solar_coefficient_ml2', None)
                wind_coefficient = getattr(wc, 'wind_coefficient_ml2', None)
    except FileNotFoundError:
        latitude = None
        longitude = None

    if latitude is None:
        latitude = float(os.environ.get('LATITUDE', '58.41'))
    if longitude is None:
        longitude = float(os.environ.get('LONGITUDE', '15.62'))

    # Get forecast data directly from SMHI (includes wind, humidity, cloud cover)
    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    forecast_data = influx.get_smhi_forecast(latitude, longitude, hours_ahead=hours_ahead)

    if not forecast_data:
        return jsonify({'error': 'No forecast data available', 'data': []})

    # Calculate effective temperatures using ML2 coefficients if available
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from energy_models.weather_energy_model import SimpleWeatherModel, WeatherConditions
    from datetime import datetime

    # Create model with ML2 coefficients (or defaults if not available)
    model_kwargs = {}
    if solar_coefficient is not None:
        model_kwargs['solar_coefficient'] = solar_coefficient
    if wind_coefficient is not None:
        model_kwargs['wind_coefficient'] = wind_coefficient
    model = SimpleWeatherModel(**model_kwargs)

    results = []
    for f in forecast_data:
        if f.get('temperature') is None:
            continue

        try:
            timestamp = datetime.fromisoformat(f['timestamp'].replace('Z', '+00:00'))

            conditions = WeatherConditions(
                timestamp=timestamp,
                temperature=f['temperature'],
                wind_speed=f.get('wind_speed', 3.0),  # Real forecast wind
                humidity=f.get('humidity', 60.0),     # Real forecast humidity (if available)
                cloud_cover=f.get('cloud_cover', 4.0),
                latitude=latitude,
                longitude=longitude
            )

            eff = model.effective_temperature(conditions)

            results.append({
                'timestamp': f['timestamp'],
                'timestamp_display': f['timestamp_swedish'],
                'forecast_temp': round(f['temperature'], 1),
                'effective_temp': round(eff.effective_temp, 1),
                'wind_effect': round(eff.wind_effect, 2),
                'humidity_effect': round(eff.humidity_effect, 2),
                'solar_effect': round(eff.solar_effect, 2),
                'sun_elevation': round(eff.sun_elevation, 1) if eff.sun_elevation else None,
                'wind_speed': f.get('wind_speed'),
                'humidity': f.get('humidity'),
                'cloud_cover': f.get('cloud_cover'),
                'is_forecast': True
            })
        except Exception as e:
            continue

    return jsonify({
        'data': results,
        'model_version': model.model_version,
        'hours_ahead': hours_ahead
    })


@app.route('/api/house/<house_id>/historical-forecast-effective-temperature')
@require_login
def api_historical_forecast_effective_temperature(house_id):
    """
    API endpoint for historical forecast effective temperature.
    Returns what was predicted (effective temp from forecast data) for past hours,
    allowing comparison with actual measured values.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)

    # Get house location and ML2 coefficients from profile
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
    solar_coefficient = None
    wind_coefficient = None
    try:
        profile = CustomerProfile.load(house_id, profiles_dir)
        latitude = getattr(profile, 'latitude', None)
        longitude = getattr(profile, 'longitude', None)
        if hasattr(profile, 'learned') and profile.learned:
            wc = getattr(profile.learned, 'weather_coefficients', None)
            if wc:
                solar_coefficient = getattr(wc, 'solar_coefficient_ml2', None)
                wind_coefficient = getattr(wc, 'wind_coefficient_ml2', None)
    except FileNotFoundError:
        latitude = None
        longitude = None

    if latitude is None:
        latitude = float(os.environ.get('LATITUDE', '58.41'))
    if longitude is None:
        longitude = float(os.environ.get('LONGITUDE', '15.62'))

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    forecast_data = influx.get_historical_forecast_weather(house_id, hours=hours)

    if not forecast_data:
        return jsonify({'data': []})

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from energy_models.weather_energy_model import SimpleWeatherModel, WeatherConditions
    from datetime import datetime

    model_kwargs = {}
    if solar_coefficient is not None:
        model_kwargs['solar_coefficient'] = solar_coefficient
    if wind_coefficient is not None:
        model_kwargs['wind_coefficient'] = wind_coefficient
    model = SimpleWeatherModel(**model_kwargs)

    results = []
    for f in forecast_data:
        if f.get('temperature') is None:
            continue

        try:
            timestamp = datetime.fromisoformat(f['timestamp'].replace('Z', '+00:00'))

            conditions = WeatherConditions(
                timestamp=timestamp,
                temperature=f['temperature'],
                wind_speed=f.get('wind_speed', 3.0),
                humidity=f.get('humidity', 60.0),
                cloud_cover=f.get('cloud_cover', 4.0),
                latitude=latitude,
                longitude=longitude
            )

            eff = model.effective_temperature(conditions)

            results.append({
                'timestamp': f['timestamp'],
                'timestamp_display': f['timestamp_swedish'],
                'forecast_temp': round(f['temperature'], 1),
                'effective_temp': round(eff.effective_temp, 1),
                'wind_effect': round(eff.wind_effect, 2),
                'humidity_effect': round(eff.humidity_effect, 2),
                'solar_effect': round(eff.solar_effect, 2),
                'is_historical_forecast': True
            })
        except Exception:
            continue

    return jsonify({
        'data': results,
        'model_version': model.model_version,
    })


@app.route('/api/house/<house_id>/energy-forecast')
@require_login
def api_energy_forecast(house_id):
    """
    API endpoint for hourly heating energy forecast (24h ahead).

    Returns predicted kWh per hour for demand response and planning.
    This data enables:
    - Homeowner energy cost estimation
    - Energy company load aggregation
    - Demand shifting from peak to off-peak hours
    """
    if house_id != '__all__' and not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 24, type=int)
    hours = min(max(hours, 1), 72)  # Clamp between 1 and 72 hours

    from influx_reader import get_influx_reader
    influx = get_influx_reader()

    if house_id == '__all__':
        data = influx.get_energy_forecast_all(hours=hours)
    else:
        data = influx.get_energy_forecast(house_id, hours=hours)

    return jsonify(data)


@app.route('/api/house/<house_id>/temperature-history')
@require_login
def api_temperature_history(house_id):
    """
    API endpoint for primary/secondary temperature comparison chart.
    Returns DH side and house side temperatures.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)  # Clamp between 1 and 30 days

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    data = influx.get_temperature_history(house_id, hours=hours)

    if not data:
        return jsonify({'error': 'No temperature data available', 'data': []})

    return jsonify({
        'data': data,
        'hours': hours
    })


@app.route('/api/house/<house_id>/supply-return-forecast')
@require_login
def api_supply_return_forecast(house_id):
    """
    API endpoint for Supply & Return temperatures with heat curve and forecast.
    Returns historical data and future forecast predictions.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)  # Clamp between 1 and 30 days

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_supply_return_with_forecast(house_id, hours=hours)

    return jsonify(result)


@app.route('/api/house/<house_id>/energy-consumption')
@require_login
def api_energy_consumption(house_id):
    """
    API endpoint for energy consumption chart.
    Returns consumption data aggregated by day/hour/month.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)  # Clamp between 7 and 365 days

    aggregation = request.args.get('aggregation', 'daily')
    if aggregation not in ['hourly', 'daily', 'monthly']:
        aggregation = 'daily'

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_energy_consumption_history(house_id, days=days, aggregation=aggregation)

    # data_source is set by influx_reader ('imported' or 'live')
    return jsonify(result)


@app.route('/api/house/<house_id>/energy-separated')
@require_login
def api_energy_separated(house_id):
    """
    API endpoint for energy separation chart (heating vs DHW).
    Returns calibrated separation based on k × degree-hours model.
    """
    if house_id != '__all__' and not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)  # Clamp between 7 and 365 days

    from influx_reader import get_influx_reader
    influx = get_influx_reader()

    if house_id == '__all__':
        result = influx.get_energy_separation_all(days=days)
    else:
        result = influx.get_energy_separation(house_id, days=days)

    return jsonify(result)


@app.route('/api/house/<house_id>/energy-signature-hourly')
@require_login
def api_energy_signature_hourly(house_id):
    """
    API endpoint for hourly energy signature data (consumption vs outdoor temp).
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 90, type=int)
    days = min(max(days, 7), 365)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_energy_signature_hourly(house_id, days=days)

    return jsonify(result)


@app.route('/api/house/<house_id>/k-value-history')
@require_login
def api_k_value_history(house_id):
    """
    API endpoint for k-value calibration history.
    Returns historical k-values for convergence visualization.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_k_value_history(house_id, days=days)

    return jsonify(result)


@app.route('/api/house/<house_id>/efficiency-metrics')
@require_login
def api_efficiency_metrics(house_id):
    """
    API endpoint for efficiency metrics chart.
    Returns delta T, power, flow rate, and efficiency ratio.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    data = influx.get_efficiency_metrics(house_id, hours=hours)

    if not data:
        return jsonify({'error': 'No efficiency data available', 'data': []})

    # Calculate summary statistics
    valid_dh_delta = [d['dh_delta_t'] for d in data if d['dh_delta_t'] is not None]
    valid_house_delta = [d['house_delta_t'] for d in data if d['house_delta_t'] is not None]
    valid_power = [d['dh_power'] for d in data if d['dh_power'] is not None]

    summary = {
        'avg_dh_delta_t': round(sum(valid_dh_delta) / len(valid_dh_delta), 1) if valid_dh_delta else None,
        'avg_house_delta_t': round(sum(valid_house_delta) / len(valid_house_delta), 1) if valid_house_delta else None,
        'avg_power': round(sum(valid_power) / len(valid_power), 1) if valid_power else None,
        'max_power': round(max(valid_power), 1) if valid_power else None,
        'min_power': round(min(valid_power), 1) if valid_power else None,
    }

    return jsonify({
        'data': data,
        'summary': summary,
        'hours': hours
    })


@app.route('/api/house/<house_id>/power-history')
@require_login
def api_power_history(house_id):
    """
    API endpoint for real-time power consumption chart.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_power_history(house_id, hours=hours)

    data = result.get('data', [])
    data_source = result.get('data_source')

    if not data:
        return jsonify({'error': 'No power data available', 'data': [], 'data_source': None})

    # Calculate energy estimate (integrate power over time)
    # Approximate using trapezoidal rule
    total_kwh = 0
    if len(data) >= 2:
        for i in range(1, len(data)):
            prev = data[i-1]
            curr = data[i]
            try:
                from datetime import datetime
                t1 = datetime.fromisoformat(prev['timestamp'].replace('Z', '+00:00'))
                t2 = datetime.fromisoformat(curr['timestamp'].replace('Z', '+00:00'))
                hours_diff = (t2 - t1).total_seconds() / 3600
                avg_power = (prev['dh_power'] + curr['dh_power']) / 2
                total_kwh += avg_power * hours_diff
            except Exception:
                continue

    return jsonify({
        'data': data,
        'estimated_kwh': round(total_kwh, 1),
        'hours': hours,
        'data_source': data_source
    })


@app.route('/api/house/<house_id>/cost-estimate')
@require_login
def api_cost_estimate(house_id):
    """
    API endpoint for cost estimation based on energy consumption.
    Uses configurable price per kWh.
    """
    if not user_manager.can_access_house(session.get('user_id'), house_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)

    # Default district heating price in SEK/kWh (typical Swedish price)
    # Can be made configurable per house in the future
    price_per_kwh = request.args.get('price', 1.20, type=float)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    energy_result = influx.get_energy_consumption_history(house_id, days=days, aggregation='daily')

    # Get data source from energy query (imported or live)
    data_source = energy_result.get('data_source')

    # Calculate costs
    cost_data = []
    total_cost = 0
    total_kwh = 0

    for energy_type, entries in energy_result.get('data', {}).items():
        for entry in entries:
            kwh = entry['value']
            cost = kwh * price_per_kwh
            cost_data.append({
                'timestamp': entry['timestamp'],
                'timestamp_display': entry['timestamp_display'],
                'kwh': kwh,
                'cost': round(cost, 2),
                'energy_type': energy_type
            })
            total_kwh += kwh
            total_cost += cost

    # Calculate daily average
    daily_avg_kwh = total_kwh / days if days > 0 else 0
    daily_avg_cost = total_cost / days if days > 0 else 0

    # Project monthly cost
    monthly_projection = daily_avg_cost * 30

    return jsonify({
        'data': cost_data,
        'summary': {
            'total_kwh': round(total_kwh, 1),
            'total_cost': round(total_cost, 2),
            'daily_avg_kwh': round(daily_avg_kwh, 1),
            'daily_avg_cost': round(daily_avg_cost, 2),
            'monthly_projection': round(monthly_projection, 2),
            'price_per_kwh': price_per_kwh,
            'currency': 'SEK'
        },
        'days': days,
        'data_source': data_source
    })


# =============================================================================
# Building Routes
# =============================================================================

@app.route('/building/<building_id>/graphs')
@require_login
def building_graphs(building_id):
    """Display data graphs for a building."""
    is_aggregate = (building_id == '__all__')

    if not is_aggregate and not user_manager.can_access_building(session.get('user_id'), building_id):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))

    if is_aggregate:
        friendly_name = 'All buildings'
        availability = {'categories': []}
        realtime_data = None
        signal_map = {}
    else:
        # Get building info from config
        import json as _json
        buildings_dir = os.path.join(os.path.dirname(__file__), '..', 'buildings')
        filepath = os.path.join(buildings_dir, f"{building_id}.json")

        try:
            with open(filepath, 'r') as f:
                bdata = _json.load(f)
            friendly_name = bdata.get('friendly_name') or building_id
        except (FileNotFoundError, _json.JSONDecodeError):
            friendly_name = building_id
            bdata = {}

        # Build signal_map for tooltips
        signal_map = {}
        for key, sig in bdata.get('analog_signals', {}).items():
            fn = sig.get('field_name')
            if fn:
                signal_map[fn] = {
                    'key': key,
                    'signal_id': sig.get('signal_id', ''),
                    'trend_log': sig.get('trend_log', ''),
                    'category': sig.get('category', ''),
                }

        # Get data availability and real-time data
        from influx_reader import get_influx_reader
        influx = get_influx_reader()
        availability = influx.get_building_data_availability(building_id, days=30)
        realtime_data = influx.get_latest_building_data(building_id)

    # Build Plotly chart data for availability heatmap
    import plotly.graph_objects as go
    import json

    fig = go.Figure()

    if not is_aggregate and availability['categories']:
        all_dates = set()
        for cat in availability['categories']:
            for d in cat['data']:
                all_dates.add(d['date'])
        all_dates = sorted(all_dates)

        category_names = [c['name'] for c in availability['categories']]

        z_values = []
        for cat in availability['categories']:
            date_to_count = {d['date']: d['count'] for d in cat['data']}
            row = [date_to_count.get(date, 0) for date in all_dates]
            z_values.append(row)

        fig.add_trace(go.Heatmap(
            z=z_values,
            x=all_dates,
            y=category_names,
            colorscale=[
                [0, '#f8f9fa'],
                [0.01, '#e3f2fd'],
                [0.25, '#90caf9'],
                [0.5, '#42a5f5'],
                [1, '#1565c0']
            ],
            showscale=True,
            colorbar=dict(title=dict(text='Data points', side='right')),
            hovertemplate='%{x}<br>%{y}: %{z} points<extra></extra>'
        ))

    fig.update_layout(
        title=None,
        xaxis=dict(title='Date', showgrid=True, gridcolor='rgba(0,0,0,0.1)', tickangle=-45),
        yaxis=dict(title=None, autorange='reversed'),
        height=max(300, len(availability.get('categories', [])) * 40 + 150),
        margin=dict(l=130, r=80, t=20, b=80),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='#ffffff',
        font=dict(family='system-ui, -apple-system, sans-serif'),
        dragmode='pan'
    )

    graph_config = {
        'displayModeBar': True,
        'modeBarButtonsToRemove': ['lasso2d', 'select2d', 'autoScale2d'],
        'displaylogo': False,
        'scrollZoom': True
    }

    graph_json = json.dumps(fig.to_dict())
    config_json = json.dumps(graph_config)

    return render_template('building_graphs.html',
        building_id=building_id,
        friendly_name=friendly_name,
        graph_json=graph_json,
        config_json=config_json,
        availability=availability,
        realtime=realtime_data,
        signal_map=signal_map,
        current_building_name=friendly_name,
        is_aggregate=is_aggregate
    )


@app.route('/api/building/<building_id>/data-availability')
@require_login
def api_building_data_availability(building_id):
    """API endpoint for building data availability chart."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    availability = influx.get_building_data_availability(building_id, days=days)

    return jsonify(availability)


@app.route('/api/building/<building_id>/supply-return')
@require_login
def api_building_supply_return(building_id):
    """API endpoint for building supply/return temperatures."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_building_supply_return(building_id, hours=hours)

    return jsonify(result)


@app.route('/api/building/<building_id>/temperature-history')
@require_login
def api_building_temperature_history(building_id):
    """API endpoint for building primary vs secondary temperature comparison."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    data = influx.get_building_temperature_history(building_id, hours=hours)

    if not data:
        return jsonify({'error': 'No temperature data available', 'data': []})

    return jsonify({'data': data, 'hours': hours})


@app.route('/api/building/<building_id>/energy-consumption')
@require_login
def api_building_energy_consumption(building_id):
    """API endpoint for building energy consumption chart."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)

    aggregation = request.args.get('aggregation', 'daily')
    if aggregation not in ['hourly', 'daily', 'monthly']:
        aggregation = 'daily'

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_building_energy_consumption(building_id, days=days, aggregation=aggregation)

    return jsonify(result)


@app.route('/api/building/<building_id>/efficiency-metrics')
@require_login
def api_building_efficiency_metrics(building_id):
    """API endpoint for building efficiency metrics (delta T, flow)."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 168, type=int)
    hours = min(max(hours, 24), 720)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    data = influx.get_building_efficiency_metrics(building_id, hours=hours)

    return jsonify({'data': data, 'hours': hours})


@app.route('/api/building/<building_id>/cost-estimate')
@require_login
def api_building_cost_estimate(building_id):
    """API endpoint for building cost estimation based on energy consumption."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)
    price_per_kwh = request.args.get('price', 1.20, type=float)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    energy_result = influx.get_building_energy_consumption(building_id, days=days, aggregation='daily')

    data_source = energy_result.get('data_source')

    cost_data = []
    total_cost = 0
    total_kwh = 0

    for energy_type, entries in energy_result.get('data', {}).items():
        for entry in entries:
            kwh = entry['value']
            cost = kwh * price_per_kwh
            cost_data.append({
                'timestamp': entry['timestamp'],
                'timestamp_display': entry['timestamp_display'],
                'kwh': kwh,
                'cost': round(cost, 2),
                'energy_type': energy_type
            })
            total_kwh += kwh
            total_cost += cost

    daily_avg_kwh = total_kwh / days if days > 0 else 0
    daily_avg_cost = total_cost / days if days > 0 else 0
    monthly_projection = daily_avg_cost * 30

    return jsonify({
        'data': cost_data,
        'summary': {
            'total_kwh': round(total_kwh, 1),
            'total_cost': round(total_cost, 2),
            'daily_avg_kwh': round(daily_avg_kwh, 1),
            'daily_avg_cost': round(daily_avg_cost, 2),
            'monthly_projection': round(monthly_projection, 2),
            'price_per_kwh': price_per_kwh,
            'currency': 'SEK'
        },
        'days': days,
        'data_source': data_source
    })


@app.route('/api/building/<building_id>/energy-separated')
@require_login
def api_building_energy_separated(building_id):
    """API endpoint for building energy separation (heating vs DHW)."""
    if building_id != '__all__' and not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    if building_id == '__all__':
        result = influx.get_building_energy_separation_all(days=days)
    else:
        result = influx.get_building_energy_separation(building_id, days=days)
    return jsonify(result)


@app.route('/api/building/<building_id>/k-value-history')
@require_login
def api_building_k_value_history(building_id):
    """API endpoint for building k-value calibration history."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 7), 365)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_k_value_history(building_id, days=days, entity_tag="building_id")
    return jsonify(result)


@app.route('/api/building/<building_id>/energy-forecast')
@require_login
def api_building_energy_forecast(building_id):
    """API endpoint for building energy forecast using k-value + SMHI weather."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    hours = request.args.get('hours', 24, type=int)
    hours = min(max(hours, 1), 72)

    # Load building config to get k-value and location
    buildings_dir = os.path.join(os.path.dirname(__file__), '..', 'buildings')
    config_path = os.path.join(buildings_dir, f'{building_id}.json')

    try:
        import json
        with open(config_path, 'r') as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return jsonify({'forecast': [], 'summary': {}, 'error': 'Building config not found'})

    k_value = config.get('energy_separation', {}).get('heat_loss_k')
    assumed_indoor = config.get('energy_separation', {}).get('assumed_indoor_temp', 21.0)
    lat = config.get('location', {}).get('latitude')
    lon = config.get('location', {}).get('longitude')

    if not k_value:
        return jsonify({'forecast': [], 'summary': {}, 'error': 'No k-value calibrated yet'})

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_building_energy_forecast(
        building_id, hours=hours,
        k_value=k_value, assumed_indoor_temp=assumed_indoor,
        latitude=lat, longitude=lon
    )
    return jsonify(result)


@app.route('/building/<building_id>/info')
@require_login
def building_info(building_id):
    """Technician info page for a building — signal mapping, events, config."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))

    import json as _json
    buildings_dir = os.path.join(os.path.dirname(__file__), '..', 'buildings')
    filepath = os.path.join(buildings_dir, f"{building_id}.json")

    try:
        with open(filepath, 'r') as f:
            bdata = _json.load(f)
        friendly_name = bdata.get('friendly_name') or building_id
    except (FileNotFoundError, _json.JSONDecodeError):
        flash('Building config not found.', 'error')
        return redirect(url_for('dashboard'))

    # Build signal table data
    signals = []
    for key, sig in bdata.get('analog_signals', {}).items():
        signals.append({
            'key': key,
            'field_name': sig.get('field_name', ''),
            'signal_id': sig.get('signal_id', ''),
            'unit': sig.get('unit', ''),
            'category': sig.get('category', ''),
            'live_source': sig.get('live_source', 'live'),
            'trend_log': sig.get('trend_log', ''),
            'writable': sig.get('write_on_change', False),
        })
    signals.sort(key=lambda s: (s['category'], s['key']))

    # Get realtime data for "last value" column
    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    realtime_data = influx.get_latest_building_data(building_id)

    # Get events
    events_result = influx.get_building_events(building_id, days=30)

    # Connection info
    connection = bdata.get('connection', {})
    energy_sep = bdata.get('energy_separation', {})
    location = bdata.get('location', {})

    # Detect architecture from signal paths
    has_io_bus = any('/IO Bus/' in sig.get('signal_id', '') for sig in bdata.get('analog_signals', {}).values())

    # Cross-building reference: load all building configs
    cross_ref = []
    if os.path.isdir(buildings_dir):
        for fname in sorted(os.listdir(buildings_dir)):
            if not fname.endswith('.json'):
                continue
            bid = fname[:-5]
            try:
                with open(os.path.join(buildings_dir, fname), 'r') as f:
                    other = _json.load(f)
                other_signals = other.get('analog_signals', {})
                field_names = sorted(set(
                    s.get('field_name', '') for s in other_signals.values() if s.get('field_name')
                ))
                cross_ref.append({
                    'id': bid,
                    'name': other.get('friendly_name') or bid,
                    'system': other.get('connection', {}).get('system', 'arrigo'),
                    'signal_count': len(other_signals),
                    'field_names': field_names,
                })
            except Exception:
                pass

    return render_template('building_info.html',
        building_id=building_id,
        friendly_name=friendly_name,
        bdata=bdata,
        signals=signals,
        realtime=realtime_data,
        events=events_result.get('events', []),
        connection=connection,
        energy_sep=energy_sep,
        location=location,
        has_io_bus=has_io_bus,
        cross_ref=cross_ref,
        current_building_name=friendly_name
    )


@app.route('/api/building/<building_id>/events')
@require_login
def api_building_events(building_id):
    """API endpoint for building operational events."""
    if not user_manager.can_access_building(session.get('user_id'), building_id):
        return jsonify({'error': 'Access denied'}), 403

    days = request.args.get('days', 30, type=int)
    days = min(max(days, 1), 365)

    from influx_reader import get_influx_reader
    influx = get_influx_reader()
    result = influx.get_building_events(building_id, days=days)
    return jsonify(result)


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

    # Update meter_ids (comma-separated input)
    new_meter_ids_str = request.form.get('meter_ids', '').strip()
    if new_meter_ids_str is not None:
        # Parse comma-separated meter IDs, strip whitespace, filter empty
        new_meter_ids = [m.strip() for m in new_meter_ids_str.split(',') if m.strip()]
        old_meter_ids = profile.meter_ids or []
        if set(new_meter_ids) != set(old_meter_ids):
            changes['meter_ids'] = {
                'old': old_meter_ids,
                'new': new_meter_ids
            }
            profile.meter_ids = new_meter_ids

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

        # If meter_ids changed, sync to Dropbox
        if 'meter_ids' in changes:
            try:
                from dropbox_sync import sync_meters
                sync_meters()
            except Exception as e:
                # Don't fail the request if sync fails
                logger.warning(f"Failed to sync meters to Dropbox: {e}")

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
        temp_dir = os.path.join(tempfile.gettempdir(), 'bvpro_imports')
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
    temp_dir = os.path.join(tempfile.gettempdir(), 'bvpro_imports')
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

        # Sync meter requests to Dropbox (updates from_date based on imported data)
        try:
            from dropbox_sync import sync_meters
            sync_meters()
            print(f"[INFO] Synced meter requests to Dropbox after energy import for {house_id}")
        except Exception as e:
            print(f"[WARNING] Failed to sync meters to Dropbox: {e}")

        # Run energy separation immediately after import
        try:
            from datetime import datetime, timedelta
            from customer_profile import CustomerProfile
            import glob as glob_mod
            profile_files = glob_mod.glob(f'profiles/*{house_id}*.json') or glob_mod.glob('profiles/*.json')
            for pf in profile_files:
                profile = CustomerProfile.load_by_path(pf)
                if profile.customer_id != house_id:
                    continue
                if not profile.energy_separation.enabled:
                    break

                from heating_energy_calibrator import HeatingEnergyCalibrator
                cal_days = profile.energy_separation.calibration_days or 30
                start_date = (datetime.now() - timedelta(days=cal_days)).strftime('%Y-%m-%d')
                wc = profile.learned.weather_coefficients
                solar_coeff = wc.solar_coefficient_ml2 if (wc.solar_confidence_ml2 or 0) >= 0.2 else None
                wind_coeff = wc.wind_coefficient_ml2 if (wc.solar_confidence_ml2 or 0) >= 0.2 else None
                calibrator = HeatingEnergyCalibrator(
                    influx_url=os.environ.get('INFLUXDB_URL', 'http://localhost:8086'),
                    influx_token=os.environ.get('INFLUXDB_TOKEN'),
                    influx_org=os.environ.get('INFLUXDB_ORG', 'homeside'),
                    influx_bucket=os.environ.get('INFLUXDB_BUCKET', 'heating'),
                    latitude=float(os.environ.get('LATITUDE', 58.41)),
                    longitude=float(os.environ.get('LONGITUDE', 15.62)),
                    solar_coefficient=solar_coeff,
                    wind_coefficient=wind_coeff
                )
                try:
                    analyses, used_k = calibrator.analyze(
                        house_id=house_id,
                        start_date=start_date,
                        calibrated_k=None,
                        k_percentile=profile.energy_separation.k_percentile or 15,
                        quiet=True
                    )
                    if analyses:
                        written = calibrator.write_to_influx(house_id, analyses, used_k)
                        print(f"[INFO] Energy separation done for {house_id}: {written} days (k={used_k:.4f})")
                finally:
                    calibrator.close()
                break
        except Exception as e:
            print(f"[WARNING] Post-import energy separation failed: {e}")

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


@app.route('/admin/users/add', methods=['GET', 'POST'])
@require_role('admin')
def admin_add_user():
    """Admin: Create a new user account directly"""
    all_houses = get_houses_with_names()

    if request.method == 'POST':
        import re
        from datetime import datetime

        username = request.form.get('username', '').strip()
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        role = request.form.get('role', 'user')
        houses = [h for h in request.form.getlist('houses') if h]

        # HomeSide credentials (optional)
        homeside_username = request.form.get('homeside_username', '').strip()
        homeside_password = request.form.get('homeside_password', '').strip()
        house_friendly_name = request.form.get('house_friendly_name', '').strip()
        meter_number = request.form.get('meter_number', '').strip()

        send_welcome_email = request.form.get('send_welcome_email') == '1'

        # Preserve form values for re-render on error
        form = {
            'username': username, 'name': name, 'email': email,
            'role': role, 'houses': houses,
            'homeside_username': homeside_username,
            'homeside_password': homeside_password,
            'house_friendly_name': house_friendly_name,
            'meter_number': meter_number,
            'send_welcome_email': '1' if send_welcome_email else '0',
        }

        # If no password provided, user will set it via email link
        needs_invite = not password

        # Validation
        errors = []
        if len(username) < 3:
            errors.append('Username must be at least 3 characters.')
        elif not re.match(r'^[a-zA-Z0-9._-]+$', username):
            errors.append('Username may only contain letters, numbers, dots, hyphens, and underscores.')
        if not name:
            errors.append('Name is required.')
        if not email or '@' not in email:
            errors.append('Valid email is required.')
        if password and len(password) < 8:
            errors.append('Password must be at least 8 characters.')
        if password and password != confirm_password:
            errors.append('Passwords do not match.')
        if role not in ('user', 'viewer', 'admin'):
            errors.append('Invalid role.')
        if user_manager.user_exists(username):
            errors.append('Username already taken.')
        if user_manager.email_exists(email):
            errors.append('Email already registered.')

        # Verify HomeSide credentials if provided
        verified_customer_id = ''
        if homeside_username and homeside_password and not errors:
            result = _verify_homeside_credentials(homeside_username, homeside_password)
            if result['valid']:
                verified_customer_id = result.get('customer_id', '')
                # Auto-add discovered house to the houses list
                if verified_customer_id and verified_customer_id not in houses:
                    houses.append(verified_customer_id)
            else:
                errors.append(result.get('error', 'Invalid HomeSide credentials'))

        if errors:
            for error in errors:
                flash(error, 'error')
            return render_template('admin_add_user.html', all_houses=all_houses, form=form)

        # If no password, generate a random one (user will set via invite link)
        actual_password = password or secrets.token_urlsafe(24)

        # Create user with chosen role (active immediately)
        user_manager.create_user(
            username=username,
            password=actual_password,
            name=name,
            email=email,
            role=role,
            houses=houses,
            homeside_username=homeside_username,
            homeside_password=homeside_password,
            house_friendly_name=house_friendly_name,
            verified_customer_id=verified_customer_id,
            meter_number=meter_number,
        )

        # Mark as approved by current admin
        user_manager.update_user(
            username,
            approved_by=session.get('user_id'),
            approved_at=datetime.utcnow().isoformat() + 'Z',
        )

        # Audit log
        audit_logger.log('UserCreatedByAdmin', session.get('user_id'), {
            'created_user': username,
            'role': role,
            'houses': houses,
            'has_homeside': bool(homeside_username),
            'invite_link': needs_invite,
        })

        if needs_invite:
            # Create a 48-hour password setup token and send invite email
            token = user_manager.create_password_reset_token(email, expires_minutes=2880)
            user = user_manager.get_user(username)
            if token and user:
                email_sent = email_service.send_invite_email(user, token)
                if email_sent:
                    audit_logger.log('InviteEmailSent', session.get('user_id'), {
                        'to_user': username, 'to_email': email,
                    })
                    flash(f'User {username} created. Invite email sent to {email}.', 'success')
                else:
                    audit_logger.log('InviteEmailFailed', session.get('user_id'), {
                        'to_user': username, 'to_email': email,
                    })
                    setup_url = url_for('reset_password', token=token, _external=True)
                    flash(f'User {username} created but invite email failed. Setup link: {setup_url}', 'warning')
            else:
                setup_url = url_for('reset_password', token=token, _external=True) if token else None
                if setup_url:
                    flash(f'User {username} created. Could not send email. Setup link: {setup_url}', 'warning')
                else:
                    flash(f'User {username} created but invite email could not be sent.', 'warning')
        else:
            # Admin set a password — optionally send welcome email
            if send_welcome_email:
                user = user_manager.get_user(username)
                if user:
                    email_sent = email_service.send_welcome_email(user)
                    if not email_sent:
                        audit_logger.log('WelcomeEmailFailed', session.get('user_id'), {
                            'to_user': username, 'to_email': email,
                        })
                        flash(f'User {username} created but welcome email failed to send.', 'warning')
                        return redirect(url_for('admin_users'))
            flash(f'User {username} created successfully.', 'success')

        return redirect(url_for('admin_users'))

    # GET
    form = {}
    return render_template('admin_add_user.html', all_houses=all_houses, form=form)


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
            # Get meter_number if provided during registration
            meter_ids = []
            if user.get('meter_number'):
                meter_ids = [user.get('meter_number')]

            deploy_result = deployer.deploy_fetcher(
                customer_id=customer_id,
                friendly_name=user.get('house_friendly_name', customer_id),
                homeside_username=user.get('homeside_username'),
                homeside_password=user.get('homeside_password'),
                meter_ids=meter_ids
            )

            if deploy_result.get('success'):
                audit_logger.log('FetcherDeployed', session.get('user_id'), {
                    'customer_id': customer_id,
                    'container_name': deploy_result.get('container_name'),
                    'for_user': username
                })
                # Sync meter IDs to Dropbox if provided
                if meter_ids:
                    try:
                        from dropbox_sync import sync_meters
                        sync_meters()
                        print(f"[INFO] Synced meter IDs to Dropbox for {customer_id}")
                    except Exception as e:
                        print(f"[WARNING] Failed to sync meters to Dropbox: {e}")
            else:
                audit_logger.log('FetcherDeployFailed', session.get('user_id'), {
                    'customer_id': customer_id,
                    'error': deploy_result.get('error', 'Unknown'),
                    'for_user': username
                })

        # Send welcome email
        email_sent = email_service.send_welcome_email(user)
        if not email_sent:
            audit_logger.log('WelcomeEmailFailed', session.get('user_id'), {
                'to_user': username, 'to_email': user.get('email', ''),
            })

        if deploy_result and deploy_result.get('success'):
            msg = f'User {username} approved and fetcher deployed for {customer_id}.'
            if not email_sent:
                msg += ' (Welcome email failed to send)'
            flash(msg, 'success')
        elif customer_id:
            flash(f'User {username} approved but fetcher deployment failed. Check logs.', 'warning')
        else:
            msg = f'User {username} approved. No HomeSide credentials for auto-deployment.'
            if not email_sent:
                msg += ' (Welcome email failed to send)'
            flash(msg, 'info')
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


@app.route('/admin/users/<username>/resend-invite', methods=['POST'])
@require_role('admin')
def admin_resend_invite(username):
    """Admin: Resend invite email with a fresh password setup link"""
    user = user_manager.get_user(username)
    if not user:
        flash(f'User {username} not found.', 'error')
        return redirect(url_for('admin_users'))

    email = user.get('email', '')
    if not email:
        flash(f'User {username} has no email address.', 'error')
        return redirect(url_for('admin_users'))

    # Create a fresh 48-hour token
    token = user_manager.create_password_reset_token(email, expires_minutes=2880)
    if not token:
        flash(f'Could not create invite token for {username}.', 'error')
        return redirect(url_for('admin_users'))

    email_sent = email_service.send_invite_email(user, token)
    if email_sent:
        audit_logger.log('InviteEmailResent', session.get('user_id'), {
            'to_user': username, 'to_email': email,
        })
        flash(f'Invite email resent to {email}.', 'success')
    else:
        audit_logger.log('InviteEmailFailed', session.get('user_id'), {
            'to_user': username, 'to_email': email,
        })
        setup_url = url_for('reset_password', token=token, _external=True)
        flash(f'Email failed to send. Manual setup link: {setup_url}', 'warning')

    return redirect(url_for('admin_users'))


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
# AI Assistant Routes
# =============================================================================

@app.route('/api/assistant/chat', methods=['POST'])
@require_login
def api_assistant_chat():
    """AI assistant chat endpoint."""
    from ai_assistant import get_assistant, load_conversation, save_conversation, _check_rate_limit
    import uuid

    # Check API key configured
    assistant = get_assistant(admin=(session.get('user_role') == 'admin'))
    if not assistant:
        return jsonify({'error': 'AI assistant is not configured (missing API key).'}), 503

    # Rate limit
    username = session.get('user_id', '')
    if not _check_rate_limit(username):
        return jsonify({'error': 'Du har natt gransen for antal meddelanden per timme.'}), 429

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Invalid request'}), 400

    message = (data.get('message') or '').strip()
    if not message:
        return jsonify({'error': 'Empty message'}), 400
    if len(message) > 2000:
        return jsonify({'error': 'Message too long (max 2000 characters)'}), 400

    conversation_id = data.get('conversation_id') or str(uuid.uuid4())

    # Build user context
    role = session.get('user_role', 'viewer')
    user_houses_raw = session.get('user_houses', [])
    verified_customer_id = session.get('verified_customer_id', '')

    # Resolve accessible houses
    from customer_profile import CustomerProfile
    profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')

    if role == 'admin' or '*' in user_houses_raw:
        house_ids = user_manager.get_all_houses()
    else:
        house_ids = list(user_houses_raw)
        if verified_customer_id and verified_customer_id not in house_ids:
            house_ids.append(verified_customer_id)

    accessible_houses = []
    for hid in house_ids:
        if hid == '*':
            continue
        try:
            profile = CustomerProfile.load(hid, profiles_dir)
            accessible_houses.append({'id': hid, 'friendly_name': profile.friendly_name or hid})
        except Exception:
            accessible_houses.append({'id': hid, 'friendly_name': hid})

    # Resolve accessible buildings
    accessible_buildings = []
    if role == 'admin' or '*' in user_houses_raw:
        building_ids = user_manager.get_all_buildings()
        buildings_dir = os.path.join(os.path.dirname(__file__), '..', 'buildings')
        for bid in building_ids:
            try:
                import json as _json
                with open(os.path.join(buildings_dir, f"{bid}.json"), 'r') as f:
                    bdata = _json.load(f)
                accessible_buildings.append({'id': bid, 'friendly_name': bdata.get('friendly_name', bid)})
            except Exception:
                accessible_buildings.append({'id': bid, 'friendly_name': bid})

    user_context = {
        'role': role,
        'username': username,
        'accessible_houses': accessible_houses,
        'accessible_buildings': accessible_buildings,
        'can_edit_fn': lambda hid: user_manager.can_edit_house(username, hid),
    }

    # Pass current page entity context so the assistant knows what the user is viewing
    viewing_house = data.get('house_id', '')
    viewing_building = data.get('building_id', '')
    viewing_name = data.get('entity_name', '')
    if viewing_house or viewing_building:
        user_context['viewing_entity'] = {
            'house_id': viewing_house,
            'building_id': viewing_building,
            'name': viewing_name,
        }

    # Load conversation history (keep last 10 messages for context)
    history = load_conversation(username, conversation_id)
    history = history[-10:]
    history.append({'role': 'user', 'content': message})

    try:
        response_text = assistant.chat(history, user_context)
    except Exception as e:
        print(f"AI assistant error: {e}")
        return jsonify({'error': 'Ett fel uppstod. Forsok igen.'}), 500

    history.append({'role': 'assistant', 'content': response_text})

    # Check for support ticket
    support_ticket = bool(user_context.get('_support_ticket'))
    if support_ticket:
        ticket = user_context['_support_ticket']
        try:
            user = user_manager.get_user(username)
            user_email = user.get('email', '') if user else ''
            user_name = user.get('name', username) if user else username
            email_service.send_support_ticket(
                user_name=user_name,
                user_email=user_email,
                summary=ticket.get('summary', ''),
                details=ticket.get('details', ''),
                transcript=history,
            )
        except Exception as e:
            print(f"Failed to send support ticket email: {e}")

    # Save conversation
    save_conversation(username, conversation_id, history, support_ticket=support_ticket)

    # Audit log write operations
    if user_context.get('_support_ticket'):
        audit_logger.log('SupportTicket', username, {
            'summary': user_context['_support_ticket'].get('summary', ''),
        })

    return jsonify({
        'response': response_text,
        'conversation_id': conversation_id,
    })


@app.route('/api/assistant/history')
@require_login
def api_assistant_history():
    """Get the user's recent chat conversations."""
    from ai_assistant import get_user_conversations
    username = session.get('user_id', '')
    conversations = get_user_conversations(username)
    return jsonify({'conversations': conversations})


@app.route('/api/assistant/conversation/<conversation_id>')
@require_login
def api_assistant_conversation(conversation_id):
    """Get a specific conversation's messages."""
    from ai_assistant import load_conversation
    username = session.get('user_id', '')
    messages = load_conversation(username, conversation_id)
    return jsonify({'messages': messages, 'conversation_id': conversation_id})


@app.context_processor
def inject_ai_enabled():
    """Inject AI assistant availability into all templates."""
    ai_enabled = bool(os.environ.get('ANTHROPIC_API_KEY'))
    return {'ai_enabled': ai_enabled}


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

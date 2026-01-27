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

        if errors:
            for error in errors:
                flash(error, 'error')
            return render_template('register.html',
                                   username=username, name=name, email=email, note=note,
                                   homeside_username=homeside_username,
                                   homeside_password=homeside_password,
                                   house_friendly_name=house_friendly_name)

        # Create pending user
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
            house_friendly_name=house_friendly_name
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
# Admin Routes
# =============================================================================

@app.route('/admin/users')
@require_role('admin')
def admin_users():
    """Admin: User management page"""
    users = user_manager.get_all_users()
    pending_count = sum(1 for u in users.values() if u['role'] == 'pending')
    all_houses = get_houses_with_names()
    return render_template('admin_users.html', users=users, pending_count=pending_count, all_houses=all_houses)


@app.route('/admin/users/<username>/approve', methods=['POST'])
@require_role('admin')
def admin_approve_user(username):
    """Admin: Approve a pending user"""
    houses = request.form.getlist('houses')
    role = request.form.get('role', 'user')

    # Validate role
    if role not in ['user', 'viewer', 'admin']:
        role = 'user'

    if user_manager.approve_user(username, houses, role=role, approved_by=session.get('user_id')):
        user = user_manager.get_user(username)

        audit_logger.log('UserApproved', session.get('user_id'), {
            'approved_user': username,
            'role': role,
            'houses': houses
        })

        # Send welcome email
        email_service.send_welcome_email(user)

        flash(f'User {username} has been approved as {role}.', 'success')
    else:
        flash(f'Failed to approve user {username}.', 'error')

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
                        house_name = houses[0].get('Housename', 'Unknown')
                        return jsonify({'success': True, 'client_id': house_name})
                return jsonify({'success': True, 'client_id': 'Connected'})
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

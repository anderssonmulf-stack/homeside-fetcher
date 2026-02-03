"""
Authentication and User Management for Svenskeb Settings GUI
"""

import json
import os
import secrets
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional
import bcrypt
from flask import session, redirect, url_for, flash, request


USERS_FILE = os.path.join(os.path.dirname(__file__), 'users.json')


def require_login(f):
    """Decorator: Require user to be logged in"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


def require_role(*roles):
    """Decorator: Require user to have one of the specified roles"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash('Please log in to access this page.', 'warning')
                return redirect(url_for('login', next=request.url))

            user_role = session.get('user_role')
            if user_role not in roles:
                flash('You do not have permission to access this page.', 'error')
                return redirect(url_for('dashboard'))

            return f(*args, **kwargs)
        return decorated_function
    return decorator


class UserManager:
    """Manages user accounts, authentication, and permissions"""

    def __init__(self, users_file: str = USERS_FILE):
        self.users_file = users_file
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        """Create users file with default admin if it doesn't exist"""
        if not os.path.exists(self.users_file):
            default_data = {
                "users": {},
                "pending_actions": {}
            }
            self._save_data(default_data)

    def _load_data(self) -> Dict:
        """Load users data from JSON file"""
        try:
            with open(self.users_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"users": {}, "pending_actions": {}}

    def _save_data(self, data: Dict):
        """Save users data to JSON file"""
        with open(self.users_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify a password against its hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except Exception:
            return False

    # =========================================================================
    # User CRUD Operations
    # =========================================================================

    def create_user(self, username: str, password: str, name: str, email: str,
                    role: str = 'pending', houses: List[str] = None,
                    registration_note: str = '',
                    homeside_username: str = '',
                    homeside_password: str = '',
                    house_friendly_name: str = '',
                    verified_customer_id: str = '',
                    meter_number: str = '') -> bool:
        """Create a new user account"""
        data = self._load_data()

        if username in data['users']:
            return False

        data['users'][username] = {
            'name': name,
            'email': email,
            'password_hash': self._hash_password(password),
            'role': role,
            'houses': houses or [],
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'approved_by': None,
            'approved_at': None,
            'registration_note': registration_note,
            'homeside_username': homeside_username,
            'homeside_password': homeside_password,
            'house_friendly_name': house_friendly_name,
            'verified_customer_id': verified_customer_id,
            'meter_number': meter_number
        }

        self._save_data(data)
        return True

    def get_user(self, username: str) -> Optional[Dict]:
        """Get user by username"""
        data = self._load_data()
        user = data['users'].get(username)
        if user:
            user = user.copy()
            user['username'] = username
        return user

    def get_all_users(self) -> Dict[str, Dict]:
        """Get all users"""
        data = self._load_data()
        return data['users']

    def update_user(self, username: str, **kwargs) -> bool:
        """Update user fields"""
        data = self._load_data()

        if username not in data['users']:
            return False

        for key, value in kwargs.items():
            if key == 'password':
                data['users'][username]['password_hash'] = self._hash_password(value)
            elif key in data['users'][username]:
                data['users'][username][key] = value

        self._save_data(data)
        return True

    def delete_user(self, username: str) -> bool:
        """Delete a user"""
        data = self._load_data()

        if username not in data['users']:
            return False

        del data['users'][username]
        self._save_data(data)
        return True

    def user_exists(self, username: str) -> bool:
        """Check if username exists"""
        data = self._load_data()
        return username in data['users']

    def email_exists(self, email: str) -> bool:
        """Check if email is already registered"""
        data = self._load_data()
        return any(u.get('email', '').lower() == email.lower()
                   for u in data['users'].values())

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user by email address"""
        data = self._load_data()
        email_lower = email.lower()
        for username, user in data['users'].items():
            if user.get('email', '').lower() == email_lower:
                user_copy = user.copy()
                user_copy['username'] = username
                return user_copy
        return None

    def get_user_by_homeside_username(self, homeside_username: str) -> Optional[Dict]:
        """Get user by HomeSide username (normalized, without spaces)"""
        data = self._load_data()
        # Normalize: remove spaces and convert to uppercase for comparison
        normalized = homeside_username.replace(' ', '').upper()
        for username, user in data['users'].items():
            stored_hs = user.get('homeside_username', '')
            if stored_hs and stored_hs.replace(' ', '').upper() == normalized:
                user_copy = user.copy()
                user_copy['username'] = username
                return user_copy
        return None

    # =========================================================================
    # Authentication
    # =========================================================================

    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """Authenticate user and return user data if successful.

        Supports login via:
        - Regular username with webgui password
        - HomeSide username (with or without space) with HomeSide password
        """
        # First try regular username with webgui password
        user = self.get_user(username)
        if user and self._verify_password(password, user.get('password_hash', '')):
            return user

        # Try HomeSide username with HomeSide password
        user = self.get_user_by_homeside_username(username)
        if user:
            stored_homeside_password = user.get('homeside_password', '')
            if stored_homeside_password and password == stored_homeside_password:
                return user

        return None

    # =========================================================================
    # Password Reset
    # =========================================================================

    def create_password_reset_token(self, email: str) -> Optional[str]:
        """Create a password reset token for the user with this email.

        Returns the token if created, None if email not found.
        Token is valid for 15 minutes.
        """
        user = self.get_user_by_email(email)
        if not user:
            return None

        data = self._load_data()
        token = secrets.token_urlsafe(32)

        # Store reset token in pending_actions
        if 'password_reset_tokens' not in data:
            data['password_reset_tokens'] = {}

        data['password_reset_tokens'][token] = {
            'username': user['username'],
            'email': email,
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'expires_at': (datetime.utcnow() + timedelta(minutes=15)).isoformat() + 'Z',
            'used': False
        }

        self._save_data(data)
        return token

    def validate_password_reset_token(self, token: str) -> Optional[Dict]:
        """Validate a password reset token.

        Returns the token data if valid, None if invalid or expired.
        """
        data = self._load_data()
        token_data = data.get('password_reset_tokens', {}).get(token)

        if not token_data:
            return None

        if token_data.get('used'):
            return None

        # Check expiration
        expires_at = datetime.fromisoformat(token_data['expires_at'].replace('Z', ''))
        if datetime.utcnow() > expires_at:
            return None

        return token_data

    def reset_password_with_token(self, token: str, new_password: str) -> bool:
        """Reset password using a valid token.

        Returns True if successful, False if token is invalid.
        """
        token_data = self.validate_password_reset_token(token)
        if not token_data:
            return False

        username = token_data['username']
        data = self._load_data()

        if username not in data['users']:
            return False

        # Update password
        data['users'][username]['password_hash'] = self._hash_password(new_password)

        # Mark token as used
        data['password_reset_tokens'][token]['used'] = True
        data['password_reset_tokens'][token]['used_at'] = datetime.utcnow().isoformat() + 'Z'

        self._save_data(data)
        return True

    def cleanup_expired_reset_tokens(self):
        """Remove expired password reset tokens (housekeeping)"""
        data = self._load_data()
        if 'password_reset_tokens' not in data:
            return

        now = datetime.utcnow()
        expired = []

        for token, token_data in data['password_reset_tokens'].items():
            expires_at = datetime.fromisoformat(token_data['expires_at'].replace('Z', ''))
            # Remove tokens older than 1 day (even if 15 min expired)
            if (now - expires_at).days >= 1:
                expired.append(token)

        for token in expired:
            del data['password_reset_tokens'][token]

        if expired:
            self._save_data(data)

    def approve_user(self, username: str, houses: List[str], approved_by: str, role: str = 'user') -> bool:
        """Approve a pending user"""
        data = self._load_data()

        if username not in data['users']:
            return False

        if data['users'][username]['role'] != 'pending':
            return False

        # Validate role
        if role not in ['user', 'viewer', 'admin']:
            role = 'user'

        data['users'][username]['role'] = role
        data['users'][username]['houses'] = houses
        data['users'][username]['approved_by'] = approved_by
        data['users'][username]['approved_at'] = datetime.utcnow().isoformat() + 'Z'

        self._save_data(data)
        return True

    # =========================================================================
    # Permissions
    # =========================================================================

    def can_access_house(self, username: str, house_id: str) -> bool:
        """Check if user can view a house.

        Access is granted if:
        - User is admin
        - User has wildcard access (*)
        - House is in user's assigned houses list
        - House matches user's verified_customer_id (their own house)
        """
        user = self.get_user(username)
        if not user:
            return False

        if user['role'] == 'admin':
            return True

        if '*' in user.get('houses', []):
            return True

        # Check assigned houses
        if house_id in user.get('houses', []):
            return True

        # Check own house (verified_customer_id)
        if user.get('verified_customer_id') == house_id:
            return True

        return False

    def can_edit_house(self, username: str, house_id: str) -> bool:
        """Check if user can edit a house.

        Edit permission is granted if:
        - User is admin
        - User has 'user' role AND house is their own (verified_customer_id)

        Viewers can never edit.
        Users can only edit their OWN house, not houses they have viewing access to.
        """
        user = self.get_user(username)
        if not user:
            return False

        # Viewers can never edit
        if user['role'] == 'viewer':
            return False

        # Admins can edit anything
        if user['role'] == 'admin':
            return True

        # Users can only edit their OWN house (verified_customer_id)
        # Not other houses they may have viewing access to
        if user['role'] == 'user':
            return user.get('verified_customer_id') == house_id

        return False

    def get_all_houses(self) -> List[str]:
        """Get list of all house IDs from customer profiles"""
        profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'profiles')
        houses = []

        if os.path.exists(profiles_dir):
            for filename in os.listdir(profiles_dir):
                if filename.endswith('.json'):
                    houses.append(filename[:-5])  # Remove .json

        return houses

    # =========================================================================
    # Pending Actions (for email confirm/decline)
    # =========================================================================

    def create_pending_action(self, action_type: str, house_id: str, user_id: str,
                               description: str, expires_hours: int = 24) -> str:
        """Create a pending action and return its token"""
        data = self._load_data()

        token = secrets.token_urlsafe(32)

        data['pending_actions'][token] = {
            'type': action_type,
            'house_id': house_id,
            'user_id': user_id,
            'description': description,
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'expires_at': (datetime.utcnow() + timedelta(hours=expires_hours)).isoformat() + 'Z',
            'status': 'pending'
        }

        self._save_data(data)
        return token

    def get_pending_action(self, token: str) -> Optional[Dict]:
        """Get a pending action by token"""
        data = self._load_data()
        action = data['pending_actions'].get(token)

        if not action:
            return None

        # Check expiration
        expires_at = datetime.fromisoformat(action['expires_at'].replace('Z', '+00:00'))
        if datetime.now(expires_at.tzinfo) > expires_at:
            return None

        if action['status'] != 'pending':
            return None

        return action

    def complete_action(self, token: str, result: str) -> bool:
        """Mark an action as completed"""
        data = self._load_data()

        if token not in data['pending_actions']:
            return False

        data['pending_actions'][token]['status'] = result
        data['pending_actions'][token]['completed_at'] = datetime.utcnow().isoformat() + 'Z'

        self._save_data(data)
        return True

    # =========================================================================
    # Soft Delete / Offboarding
    # =========================================================================

    def soft_delete_user(self, username: str) -> bool:
        """Mark user as deleted with 30-day grace period"""
        data = self._load_data()
        if username not in data['users']:
            return False

        now = datetime.utcnow()
        purge_date = now + timedelta(days=30)

        data['users'][username]['role'] = 'deleted'
        data['users'][username]['deleted_at'] = now.isoformat() + 'Z'
        data['users'][username]['scheduled_purge_at'] = purge_date.isoformat() + 'Z'

        self._save_data(data)
        return True

    def restore_user(self, username: str, role: str = 'user') -> bool:
        """Restore a soft-deleted user before purge date"""
        data = self._load_data()
        if username not in data['users']:
            return False
        if data['users'][username].get('role') != 'deleted':
            return False

        # Validate role
        if role not in ['user', 'viewer', 'admin']:
            role = 'user'

        data['users'][username]['role'] = role
        data['users'][username]['deleted_at'] = None
        data['users'][username]['scheduled_purge_at'] = None

        self._save_data(data)
        return True

    def get_users_pending_purge(self) -> List[Dict]:
        """Get users past their purge date"""
        data = self._load_data()
        now = datetime.utcnow()
        pending = []

        for username, user in data['users'].items():
            purge_at = user.get('scheduled_purge_at')
            if purge_at and user.get('role') == 'deleted':
                purge_date = datetime.fromisoformat(purge_at.replace('Z', ''))
                if now >= purge_date:
                    pending.append({'username': username, **user})

        return pending

    def get_deleted_users(self) -> List[Dict]:
        """Get all soft-deleted users (for admin UI) with days_left calculated"""
        data = self._load_data()
        now = datetime.utcnow()
        deleted = []

        for username, user in data['users'].items():
            if user.get('role') == 'deleted':
                user_data = {'username': username, **user}

                # Calculate days left until purge
                purge_at = user.get('scheduled_purge_at')
                if purge_at:
                    purge_date = datetime.fromisoformat(purge_at.replace('Z', ''))
                    days_left = (purge_date - now).days
                    user_data['days_left'] = days_left
                else:
                    user_data['days_left'] = 30

                deleted.append(user_data)

        return deleted

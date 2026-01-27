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
                    house_friendly_name: str = '') -> bool:
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
            'house_friendly_name': house_friendly_name
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

    # =========================================================================
    # Authentication
    # =========================================================================

    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """Authenticate user and return user data if successful"""
        user = self.get_user(username)

        if not user:
            return None

        if not self._verify_password(password, user.get('password_hash', '')):
            return None

        return user

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
        """Check if user can view a house"""
        user = self.get_user(username)
        if not user:
            return False

        if user['role'] == 'admin':
            return True

        if '*' in user.get('houses', []):
            return True

        return house_id in user.get('houses', [])

    def can_edit_house(self, username: str, house_id: str) -> bool:
        """Check if user can edit a house"""
        user = self.get_user(username)
        if not user:
            return False

        # Viewers can never edit
        if user['role'] == 'viewer':
            return False

        # Admins can edit anything
        if user['role'] == 'admin':
            return True

        # Users can edit their own houses
        if '*' in user.get('houses', []):
            return True

        return house_id in user.get('houses', [])

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

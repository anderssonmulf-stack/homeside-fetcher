#!/usr/bin/env python3
"""
Create initial admin user for Svenskeb Settings GUI
Run once to set up the first admin account.

Usage:
    python create_admin.py
"""

import sys
import getpass
from auth import UserManager
from theme import get_theme

def main():
    theme = get_theme()
    print("=" * 50)
    print(f"{theme['site_short_name']} - Create Admin User")
    print("=" * 50)
    print()

    user_manager = UserManager()

    # Get username
    username = input("Admin username: ").strip().lower()
    if not username or len(username) < 3:
        print("Error: Username must be at least 3 characters")
        sys.exit(1)

    if user_manager.user_exists(username):
        print(f"Error: User '{username}' already exists")
        sys.exit(1)

    # Get name
    name = input("Full name: ").strip()
    if not name:
        print("Error: Name is required")
        sys.exit(1)

    # Get email
    email = input("Email: ").strip().lower()
    if not email or '@' not in email:
        print("Error: Valid email is required")
        sys.exit(1)

    # Get password
    password = getpass.getpass("Password: ")
    if len(password) < 8:
        print("Error: Password must be at least 8 characters")
        sys.exit(1)

    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: Passwords do not match")
        sys.exit(1)

    # Create admin user
    success = user_manager.create_user(
        username=username,
        password=password,
        name=name,
        email=email,
        role='admin',
        houses=['*']  # Access to all houses
    )

    if success:
        print()
        print("=" * 50)
        print(f"Admin user '{username}' created successfully!")
        print(f"You can now log in at {theme['login_url_display']}")
        print("=" * 50)
    else:
        print("Error: Failed to create user")
        sys.exit(1)


if __name__ == '__main__':
    main()

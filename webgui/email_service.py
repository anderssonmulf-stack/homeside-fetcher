"""
Email Service for Svenskeb Settings GUI
Handles sending notification emails via SMTP (one.com)
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional


class EmailService:
    """Sends emails via SMTP"""

    def __init__(self, theme: dict = None):
        self.smtp_server = os.environ.get('SMTP_SERVER', 'send.one.com')
        self.smtp_port = int(os.environ.get('SMTP_PORT', '587'))
        self.smtp_user = os.environ.get('SMTP_USER', '')
        self.smtp_password = os.environ.get('SMTP_PASSWORD', '')
        self.from_email = os.environ.get('FROM_EMAIL', self.smtp_user)
        self.base_url = os.environ.get('BASE_URL', '')

        # Theme-aware branding
        theme = theme or {}
        self.email_prefix = theme.get('email_prefix', 'Svenskeb')
        self.email_system_name = theme.get('email_system_name', 'Svenskeb Heating System')
        self.from_name = os.environ.get('FROM_NAME', self.email_system_name)

        # Admin email(s) for notifications
        self.admin_emails = os.environ.get('ADMIN_EMAILS', '').split(',')

    def _send_email(self, to_email: str, subject: str, html_body: str, text_body: str = None):
        """Send an email via SMTP"""
        if not self.smtp_user or not self.smtp_password:
            print(f"Warning: SMTP not configured, would send to {to_email}: {subject}")
            return False

        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = to_email

            # Plain text version (fallback)
            if text_body:
                msg.attach(MIMEText(text_body, 'plain', 'utf-8'))

            # HTML version
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            return True

        except Exception as e:
            print(f"Error sending email to {to_email}: {e}")
            return False

    # =========================================================================
    # User Management Emails
    # =========================================================================

    def notify_admins_new_registration(self, username: str, name: str, email: str, note: str):
        """Notify admins about a new user registration"""
        subject = f"[{self.email_prefix}] New user registration: {name}"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2c3e50;">New User Registration</h2>

            <p>A new user has registered and is awaiting approval:</p>

            <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Username:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{username}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Name:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{name}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Email:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{email}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Note:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{note or '(none)'}</td>
                </tr>
            </table>

            <p>
                <a href="{self.base_url}/admin/users"
                   style="display: inline-block; padding: 12px 24px; background-color: #3498db;
                          color: white; text-decoration: none; border-radius: 4px;">
                    Review in Admin Panel
                </a>
            </p>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name}.
            </p>
        </body>
        </html>
        """

        text_body = f"""
New User Registration

Username: {username}
Name: {name}
Email: {email}
Note: {note or '(none)'}

Review at: {self.base_url}/admin/users
        """

        for admin_email in self.admin_emails:
            if admin_email.strip():
                self._send_email(admin_email.strip(), subject, html_body, text_body)

    def send_welcome_email(self, user: Dict):
        """Send welcome email to newly approved user"""
        subject = f"Welcome to {self.email_system_name}!"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #27ae60;">Welcome, {user['name']}!</h2>

            <p>Your account has been approved. You can now log in to access your heating system settings.</p>

            <p>
                <a href="{self.base_url}/login"
                   style="display: inline-block; padding: 12px 24px; background-color: #27ae60;
                          color: white; text-decoration: none; border-radius: 4px;">
                    Log In Now
                </a>
            </p>

            <h3>What you can do:</h3>
            <ul>
                <li>View your heating system status</li>
                <li>Adjust temperature settings</li>
                <li>View forecasts and history</li>
                <li>See system recommendations</li>
            </ul>

            <p>If you have any questions, please contact us.</p>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name}.
            </p>
        </body>
        </html>
        """

        text_body = f"""
Welcome, {user['name']}!

Your account has been approved. You can now log in at:
{self.base_url}/login

If you have any questions, please contact us.
        """

        return self._send_email(user['email'], subject, html_body, text_body)

    def send_invite_email(self, user: Dict, reset_token: str):
        """Send invite email to a user created by admin, with a password setup link"""
        subject = f"You've been invited to {self.email_system_name}"
        setup_url = f"{self.base_url}/reset-password/{reset_token}"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #27ae60;">Welcome, {user['name']}!</h2>

            <p>An account has been created for you on {self.email_system_name}.</p>

            <p>Your username is: <strong>{user['username']}</strong></p>

            <p>Please click the button below to set your password and get started:</p>

            <p style="margin: 30px 0;">
                <a href="{setup_url}"
                   style="display: inline-block; padding: 14px 28px; background-color: #27ae60;
                          color: white; text-decoration: none; border-radius: 4px;
                          font-weight: bold;">
                    Set Your Password
                </a>
            </p>

            <p style="color: #e74c3c; font-weight: bold;">
                This link will expire in 48 hours.
            </p>

            <h3>What you can do:</h3>
            <ul>
                <li>View your heating system status</li>
                <li>Adjust temperature settings</li>
                <li>View forecasts and history</li>
                <li>See system recommendations</li>
            </ul>

            <p>If you have any questions, please contact us.</p>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name}.<br>
                If you're having trouble clicking the button, copy and paste this URL into your browser:<br>
                <span style="word-break: break-all;">{setup_url}</span>
            </p>
        </body>
        </html>
        """

        text_body = f"""
Welcome, {user['name']}!

An account has been created for you on {self.email_system_name}.

Your username is: {user['username']}

Please visit the link below to set your password:
{setup_url}

This link will expire in 48 hours.

If you have any questions, please contact us.
        """

        return self._send_email(user['email'], subject, html_body, text_body)

    def send_rejection_email(self, user: Dict, reason: str):
        """Send rejection email to user"""
        subject = f"{self.email_prefix} Account Application"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2c3e50;">Account Application Update</h2>

            <p>Hi {user['name']},</p>

            <p>Unfortunately, your account application for {self.email_system_name}
               could not be approved at this time.</p>

            {f'<p><strong>Reason:</strong> {reason}</p>' if reason else ''}

            <p>If you believe this is an error or have questions, please contact us.</p>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name}.
            </p>
        </body>
        </html>
        """

        self._send_email(user['email'], subject, html_body)

    def send_password_reset_email(self, email: str, name: str, reset_token: str) -> bool:
        """Send password reset email with secure link.

        Args:
            email: User's email address
            name: User's display name
            reset_token: The password reset token

        Returns:
            True if email was sent successfully
        """
        subject = f"Password Reset Request - {self.email_prefix}"
        reset_url = f"{self.base_url}/reset-password/{reset_token}"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2c3e50;">Password Reset Request</h2>

            <p>Hi {name},</p>

            <p>We received a request to reset your password for your {self.email_prefix} account.</p>

            <p>Click the button below to set a new password:</p>

            <p style="margin: 30px 0;">
                <a href="{reset_url}"
                   style="display: inline-block; padding: 14px 28px; background-color: #3498db;
                          color: white; text-decoration: none; border-radius: 4px;
                          font-weight: bold;">
                    Reset Password
                </a>
            </p>

            <p style="color: #e74c3c; font-weight: bold;">
                This link will expire in 15 minutes.
            </p>

            <p>If you didn't request a password reset, you can safely ignore this email.
               Your password will remain unchanged.</p>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name}.<br>
                If you're having trouble clicking the button, copy and paste this URL into your browser:<br>
                <span style="word-break: break-all;">{reset_url}</span>
            </p>
        </body>
        </html>
        """

        text_body = f"""
Password Reset Request

Hi {name},

We received a request to reset your password for your {self.email_prefix} account.

Click this link to set a new password:
{reset_url}

This link will expire in 15 minutes.

If you didn't request a password reset, you can safely ignore this email.
Your password will remain unchanged.
        """

        return self._send_email(email, subject, html_body, text_body)

    # =========================================================================
    # Action Request Emails (confirm/decline)
    # =========================================================================

    def send_action_request(self, user: Dict, action_type: str, description: str,
                            approve_token: str, details: Dict = None):
        """
        Send an email requesting user to approve or decline an action

        Args:
            user: User dict with email and name
            action_type: Type of action (e.g., 'test_request', 'maintenance')
            description: Human-readable description of the request
            approve_token: Token for the action
            details: Additional details to show in email
        """
        subject = f"[{self.email_prefix}] Action Required: {action_type.replace('_', ' ').title()}"

        approve_url = f"{self.base_url}/action/{approve_token}/approve"
        decline_url = f"{self.base_url}/action/{approve_token}/decline"

        details_html = ""
        if details:
            details_html = "<ul>"
            for key, value in details.items():
                details_html += f"<li><strong>{key}:</strong> {value}</li>"
            details_html += "</ul>"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2c3e50;">Action Required</h2>

            <p>Hi {user['name']},</p>

            <p>{description}</p>

            {details_html}

            <p>Please choose one of the options below:</p>

            <div style="margin: 30px 0;">
                <a href="{approve_url}"
                   style="display: inline-block; padding: 14px 28px; background-color: #27ae60;
                          color: white; text-decoration: none; border-radius: 4px;
                          margin-right: 10px; font-weight: bold;">
                    ✓ Approve
                </a>
                <a href="{decline_url}"
                   style="display: inline-block; padding: 14px 28px; background-color: #e74c3c;
                          color: white; text-decoration: none; border-radius: 4px;
                          font-weight: bold;">
                    ✗ Decline
                </a>
            </div>

            <p style="color: #7f8c8d; font-size: 12px;">
                This link will expire in 24 hours. If you did not expect this email,
                please contact us.
            </p>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name}.
            </p>
        </body>
        </html>
        """

        text_body = f"""
Action Required

Hi {user['name']},

{description}

Approve: {approve_url}
Decline: {decline_url}

This link will expire in 24 hours.
        """

        return self._send_email(user['email'], subject, html_body, text_body)

    # =========================================================================
    # System Notification Emails
    # =========================================================================

    def send_support_ticket(self, user_name: str, user_email: str,
                           summary: str, transcript: list,
                           details: str = ''):
        """Send support ticket email to admins with chat transcript."""
        subject = f"[{self.email_prefix}] Support Ticket: {summary[:60]}"

        # Build transcript HTML
        transcript_html = ""
        for msg in transcript[-20:]:  # Last 20 messages
            role = msg.get('role', 'user')
            content = msg.get('content', '')
            if isinstance(content, str):
                content_escaped = content.replace('<', '&lt;').replace('>', '&gt;')
                label = 'User' if role == 'user' else 'Assistant'
                color = '#3498db' if role == 'user' else '#27ae60'
                transcript_html += f'<p><strong style="color:{color}">{label}:</strong> {content_escaped[:500]}</p>\n'

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #e74c3c;">Support Ticket</h2>

            <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">From:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{user_name} ({user_email})</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Summary:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{summary}</td>
                </tr>
                {'<tr><td style="padding: 10px; border: 1px solid #ddd; font-weight: bold; vertical-align: top;">Details:</td><td style="padding: 10px; border: 1px solid #ddd; white-space: pre-wrap;">' + details.replace("<", "&lt;").replace(">", "&gt;") + '</td></tr>' if details else ''}
            </table>

            <h3>Chat Transcript</h3>
            <div style="background: #f5f5f5; padding: 15px; border-radius: 8px; font-size: 14px;">
                {transcript_html}
            </div>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name} AI Assistant.
            </p>
        </body>
        </html>
        """

        details_text = f"\nDetails:\n{details}\n" if details else ""
        text_body = f"""
Support Ticket from {user_name} ({user_email})

Summary: {summary}
{details_text}
Chat transcript included in HTML version.
        """

        for admin_email in self.admin_emails:
            if admin_email.strip():
                self._send_email(admin_email.strip(), subject, html_body, text_body)

    def send_setting_changed_notification(self, user: Dict, house_id: str,
                                           setting: str, old_value, new_value,
                                           changed_by: str):
        """Notify user that a setting was changed (by admin or system)"""
        if changed_by == user.get('username'):
            return  # Don't notify about own changes

        subject = f"[{self.email_prefix}] Setting changed for your heating system"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2c3e50;">Setting Changed</h2>

            <p>Hi {user['name']},</p>

            <p>A setting for your heating system has been updated:</p>

            <table style="border-collapse: collapse; margin: 20px 0;">
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Setting:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{setting}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Previous value:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{old_value}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">New value:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{new_value}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold;">Changed by:</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{changed_by}</td>
                </tr>
            </table>

            <p>
                <a href="{self.base_url}/house/{house_id}"
                   style="display: inline-block; padding: 12px 24px; background-color: #3498db;
                          color: white; text-decoration: none; border-radius: 4px;">
                    View Details
                </a>
            </p>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 30px;">
                This is an automated message from {self.email_system_name}.
            </p>
        </body>
        </html>
        """

        self._send_email(user['email'], subject, html_body)

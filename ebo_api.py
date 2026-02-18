#!/usr/bin/env python3
"""
EBO (EcoStruxure Building Operation) API Client
Schneider Electric building automation system — SxWDigest authentication

Reverse-engineered from EBO WebStation login.js (version 6.0.4.90)
"""

import hashlib
import os
import re
import base64
import json
import struct
import requests
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend


class EboApiError(Exception):
    """EBO API error with optional error code"""
    ERROR_CODES = {
        131073: "Wrong domain, username, or password",
        131077: "User account has expired",
        131094: "A user is already logged on",
        1048592: "No valid client license",
    }

    def __init__(self, message, code=None):
        self.code = code
        if code and code in self.ERROR_CODES:
            message = f"{message}: {self.ERROR_CODES[code]} (code {code})"
        super().__init__(message)


class EboApi:
    """Client for Schneider Electric EcoStruxure Building Operation (EBO) WebStation API"""

    def __init__(self, base_url, username, password, domain="", verify_ssl=True):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.domain = domain
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.headers.update({
            'Origin': self.base_url,
            'Referer': f'{self.base_url}/',
            'Accept': '*/*',
        })
        self.csrf_token = None
        self.session_token = None
        self.rsa_public_key = None

    def login(self):
        """Full SxWDigest authentication flow. Returns session token."""
        # Step 1: Get initial CSRF token from HTML page
        self._fetch_csrf_token()

        # Step 2: Get challenge nonce
        challenge = self._get_challenge()

        # Step 3: Get RSA public key from login settings
        self._get_login_settings()

        # Step 4: Compute digest and login
        self.session_token = self._authorize(challenge)

        # Update session headers for subsequent requests
        self.session.headers.update({
            'X-CSRF-Token': self.session_token,
            'Content-Type': 'application/json',
        })

        return self.session_token

    def _fetch_csrf_token(self):
        """Step 1: Fetch the HTML page and extract the CSRF token from hidden input."""
        resp = self.session.get(self.base_url, verify=self.verify_ssl)
        resp.raise_for_status()

        # Look for: <input type="hidden" ... id="csrf" value=":01000000...">
        match = re.search(r'id="csrf"[^>]*value="([^"]+)"', resp.text)
        if not match:
            # Try alternative ordering
            match = re.search(r'value="([^"]+)"[^>]*id="csrf"', resp.text)
        if not match:
            raise EboApiError("Could not find CSRF token in HTML page")

        self.csrf_token = match.group(1)
        self.session.headers.update({'X-CSRF-Token': self.csrf_token})

    def _get_challenge(self):
        """Step 2: POST /vp/Challenge to get a nonce."""
        resp = self.session.post(
            f'{self.base_url}/vp/Challenge',
            data=b'',
            verify=self.verify_ssl
        )
        resp.raise_for_status()
        data = resp.json()
        challenge = data.get('challenge')
        if not challenge:
            raise EboApiError(f"No challenge in response: {data}")
        return challenge

    def _get_login_settings(self):
        """Step 3: POST /webstation/LoginSettings to get RSA public key."""
        resp = self.session.post(
            f'{self.base_url}/webstation/LoginSettings',
            data=b'',
            verify=self.verify_ssl
        )
        resp.raise_for_status()
        data = resp.json()

        # Extract RSA public key (JWK format) for password encryption
        # Response may nest it under 'LoginSettings' or at top level
        settings = data.get('LoginSettings', data)
        if isinstance(settings, dict):
            pk = settings.get('publicKey') or settings.get('PublicKey')
            if pk:
                self.rsa_public_key = pk

        return data

    def _authorize(self, challenge):
        """Step 4: Compute SHA-256 digest, encrypt password, and POST login."""
        login_path = "webstation/vp/Login"

        # Compute SHA-256 digest: username + domain + password + path + challenge
        # JS: e = "./webstation/vp/Login"; e.substring(1) = "/webstation/vp/Login"
        digest_path = "/" + login_path
        digest_input = self.username + self.domain + self.password + digest_path + challenge
        digest = hashlib.sha256(digest_input.encode('utf-8')).hexdigest()

        # Build authorization parameters
        params = [
            f"UID={requests.utils.quote(self.username)}",
            f"DOM={requests.utils.quote(self.domain)}",
            f"NV={challenge}",
            f"DIG={digest}",
        ]

        # Over HTTPS: encrypt password with RSA-OAEP + AES-128-CBC
        if self.base_url.startswith('https') and self.password and self.rsa_public_key:
            encrypted_params = self._encrypt_password()
            if encrypted_params:
                params.extend(encrypted_params)

        auth_header = "SxWDigest " + ",".join(params)

        resp = self.session.post(
            f'{self.base_url}/{login_path}',
            data=b'',
            headers={
                'Authorization': auth_header,
                'X-CSRF-Token': self.csrf_token,
            },
            verify=self.verify_ssl
        )
        resp.raise_for_status()
        data = resp.json()

        # Check for error — EBO returns: {"ERROR": "", "ErrMsg": "...", "ErrorCode": "131073", ...}
        if isinstance(data, dict):
            err_msg = data.get('ErrMsg') or data.get('error') or data.get('ERROR')
            err_code = data.get('ErrorCode') or data.get('errorCode') or data.get('code')
            status = data.get('Status', 'true')
            if status == 'false' or (err_code and str(err_code) != '0'):
                code = int(err_code) if err_code else None
                # 131094 = "already logged on" — our previous session is still valid,
                # keep using existing session_token + csrf_token instead of failing
                if code == 131094 and self.session_token:
                    return self.session_token
                raise EboApiError(f"Login failed: {err_msg or data}", code=code)

        # Extract session token
        token = None
        if isinstance(data, dict):
            token = data.get('token') or data.get('Token')

        # Also check response headers
        if not token:
            token = resp.headers.get('csrf-token') or resp.headers.get('X-CSRF-Token')

        if not token:
            raise EboApiError(f"No session token in login response: {data}")

        return str(token)

    def _encrypt_password(self):
        """Encrypt password with RSA-OAEP + AES-128-CBC hybrid scheme.

        Returns list of params: [BB8=..., C3PO=..., R2D2=...]
        BB8  = AES-encrypted password (Base64)
        C3PO = RSA-encrypted AES key (Base64)
        R2D2 = AES IV (Base64)
        """
        try:
            # Step 1: UTF-8 encode password, then Base64 encode
            pwd_b64 = base64.b64encode(self.password.encode('utf-8'))

            # Step 2: Generate random AES-128 key (16 bytes) and IV (16 bytes)
            aes_key = os.urandom(16)
            iv = os.urandom(16)

            # Step 3: AES-CBC encrypt the Base64-encoded password
            # Pad to AES block size (PKCS7)
            block_size = 16
            pad_len = block_size - (len(pwd_b64) % block_size)
            padded = pwd_b64 + bytes([pad_len] * pad_len)

            cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
            encryptor = cipher.encryptor()
            aes_ciphertext = encryptor.update(padded) + encryptor.finalize()

            # Step 4: Import RSA public key and encrypt the AES key
            rsa_key = self._import_rsa_key(self.rsa_public_key)
            rsa_ciphertext = rsa_key.encrypt(
                aes_key,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA1()),
                    algorithm=hashes.SHA1(),
                    label=None
                )
            )

            # Step 5: Base64 encode all outputs
            bb8 = base64.b64encode(aes_ciphertext).decode('ascii')
            c3po = base64.b64encode(rsa_ciphertext).decode('ascii')
            r2d2 = base64.b64encode(iv).decode('ascii')

            return [f"BB8={bb8}", f"C3PO={c3po}", f"R2D2={r2d2}"]

        except Exception as e:
            print(f"Warning: Password encryption failed ({e}), proceeding without encryption")
            return []

    def _import_rsa_key(self, key_data):
        """Import RSA public key from JWK or PEM format."""
        if isinstance(key_data, dict):
            # JWK format — extract modulus (n) and exponent (e)
            n_b64 = key_data.get('n', '')
            e_b64 = key_data.get('e', '')

            # JWK uses Base64url encoding
            n_bytes = base64.urlsafe_b64decode(n_b64 + '==')
            e_bytes = base64.urlsafe_b64decode(e_b64 + '==')

            n = int.from_bytes(n_bytes, 'big')
            e = int.from_bytes(e_bytes, 'big')

            return rsa.RSAPublicNumbers(e, n).public_key(default_backend())

        elif isinstance(key_data, str):
            if key_data.startswith('-----'):
                # PEM format
                return serialization.load_pem_public_key(
                    key_data.encode('utf-8'),
                    backend=default_backend()
                )
            else:
                # Try Base64-encoded DER
                der_bytes = base64.b64decode(key_data)
                return serialization.load_der_public_key(der_bytes, backend=default_backend())

        raise EboApiError(f"Unsupported RSA key format: {type(key_data)}")

    # --- Data API methods ---

    def _post_command(self, command_data):
        """Send a command to POST /json/POST and return the response."""
        if not self.session_token:
            raise EboApiError("Not logged in — call login() first")

        # Must use ensure_ascii=False — EBO server mangles JSON unicode escapes
        resp = self.session.post(
            f'{self.base_url}/json/POST',
            data=json.dumps(command_data, ensure_ascii=False).encode('utf-8'),
            headers={'Content-Type': 'application/json; charset=utf-8'},
            verify=self.verify_ssl,
            timeout=30
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def decode_value(hex_str):
        """Decode IEEE 754 hex string (e.g. '0x405b6f7ce3333333') to float."""
        if not hex_str or not isinstance(hex_str, str) or not hex_str.startswith('0x'):
            return hex_str  # Return as-is if not a hex float
        try:
            raw = int(hex_str, 16)
            return struct.unpack('!d', raw.to_bytes(8, 'big'))[0]
        except (ValueError, struct.error):
            return hex_str

    @staticmethod
    def parse_subscription_items(items):
        """Parse ReadSubscription items into a list of {index, value, unit, status} dicts."""
        parsed = []
        for item in items:
            prop = item.get('property', {})
            entry = {
                'index': item.get('index'),
                'value': EboApi.decode_value(prop.get('value')),
                'unit': prop.get('unitDisplayName', ''),
                'status': prop.get('status', -1),
                'forced': prop.get('forced', False),
                'type': item.get('type', ''),
            }
            parsed.append(entry)
        return parsed

    def web_entry(self):
        """Initialize session — returns user info, permissions, workspace config."""
        return self._post_command({
            "command": "WebEntry",
            "clientLanguage": "sv-SE",
            "clientLocale": "sv-SE",
            "clientSystemOfMeasurement": 0,  # 0=SI/Metric, 3=US/Imperial
        })

    def get_multi_property(self, paths):
        """Fetch multiple property values in one call.

        Args:
            paths: list of property paths, e.g. ["/EC1/path/to/Value"]
        """
        return self._post_command({
            "command": "GetMultiProperty",
            "data": paths,
        })

    def get_objects(self, paths, levels=1, include_hidden=False):
        """Browse the object tree.

        Args:
            paths: list of paths to browse
            levels: depth of tree traversal (1 = immediate children)
            include_hidden: include hidden objects
        """
        return self._post_command({
            "command": "GetObjects",
            "paths": paths,
            "levels": levels,
            "includeHidden": include_hidden,
            "dbMode": True,
            "includeAggregated": False,
        })

    def create_subscription(self, property_paths):
        """Create a live data subscription.

        Args:
            property_paths: list of property paths to subscribe to

        Returns:
            dict with 'handle' (subscription ID) and 'items' (initial values)
        """
        return self._post_command({
            "command": "CreateSubscription",
            "propertyPaths": property_paths,
        })

    def read_subscription(self, handle):
        """Poll a subscription for current values.

        Args:
            handle: subscription handle from create_subscription()
        """
        return self._post_command({
            "command": "ReadSubscription",
            "handle": handle,
        })

    def add_to_subscription(self, handle, property_paths):
        """Add more paths to an existing subscription."""
        return self._post_command({
            "command": "AddToSubscription",
            "handle": handle,
            "propertyPaths": property_paths,
        })

    def remove_from_subscription(self, handle, indices):
        """Remove paths from subscription by index."""
        return self._post_command({
            "command": "RemoveFromSubscription",
            "handle": handle,
            "indices": indices,
        })

    def get_graphics_info(self, path):
        """Get TGML graphics page metadata and data bindings."""
        return self._post_command({
            "command": "GetGraphicsInfo",
            "path": path,
        })

    def get_workspace_layout(self, workspace, path):
        """Get workspace layout configuration."""
        return self._post_command({
            "command": "GetWorkspaceLayout",
            "workspace": workspace,
            "path": path,
        })

    def get_panel_layout(self, path):
        """Get panel layout for a specific path."""
        return self._post_command({
            "command": "GetPanelLayout",
            "path": path,
        })

    def set_property(self, path, value):
        """Write a value to an EBO property.

        Tries SetMultiProperty (most likely EBO write command) first.
        If that fails, tries alternative command names.

        Args:
            path: Full property path (e.g. '/Site/VS1/Variabler/GT1_FS/Value')
            value: Value to write (float, int, bool, or string)

        Returns:
            dict with 'success' bool, 'response' dict, and 'command' used
        """
        # Try each command variant until one succeeds
        commands = [
            ("SetMultiProperty", {
                "command": "SetMultiProperty",
                "data": [{"path": path, "value": value}],
            }),
            ("SetMultiProperty_hex", {
                "command": "SetMultiProperty",
                "data": [{"path": path, "value": self.encode_value(value)}],
            }),
            ("SetProperty", {
                "command": "SetProperty",
                "path": path,
                "value": value,
            }),
            ("SetProperty_hex", {
                "command": "SetProperty",
                "path": path,
                "value": self.encode_value(value),
            }),
            ("ForceProperty", {
                "command": "ForceProperty",
                "data": [{"path": path, "value": value}],
            }),
            ("WriteProperty", {
                "command": "WriteProperty",
                "data": [{"path": path, "value": value}],
            }),
        ]

        last_error = None
        for name, cmd_data in commands:
            try:
                resp = self._post_command(cmd_data)
                # Check for error indicators in the response
                if isinstance(resp, dict):
                    err = resp.get('error') or resp.get('Error') or resp.get('ErrMsg')
                    if err:
                        last_error = f"{name}: {err}"
                        continue
                return {'success': True, 'response': resp, 'command': name}
            except Exception as e:
                last_error = f"{name}: {e}"
                continue

        return {'success': False, 'error': last_error, 'command': None}

    def set_property_direct(self, path, value, command="SetMultiProperty", hex_encode=False):
        """Write a value using a specific command name (skip auto-detection).

        Args:
            path: Full property path
            value: Value to write
            command: EBO command name to use
            hex_encode: If True, encode value as IEEE 754 hex string

        Returns:
            dict: Raw API response
        """
        write_value = self.encode_value(value) if hex_encode else value
        if command == "SetMultiProperty":
            return self._post_command({
                "command": command,
                "data": [{"path": path, "value": write_value}],
            })
        else:
            return self._post_command({
                "command": command,
                "path": path,
                "value": write_value,
            })

    @staticmethod
    def encode_value(value):
        """Encode a numeric value as IEEE 754 hex string (EBO format).

        E.g. 1.0 -> '0x3ff0000000000000'
        """
        try:
            raw = struct.pack('!d', float(value))
            return '0x' + raw.hex()
        except (ValueError, TypeError):
            return value

    def client_refresh(self, bookmark=-1):
        """Keep-alive / poll for server-side changes."""
        return self._post_command({
            "command": "ClientRefresh",
            "bookmark": bookmark,
        })

    def get_bookmark(self):
        """Get current bookmark from server (initial or refresh).

        Returns:
            int: Current bookmark value
        """
        result = self.client_refresh(bookmark=0)
        return result.get('ClientRefreshRes', result).get('bookmark', 0)

    def force_value(self, path, object_id, value, server_path, duration_seconds=3600,
                    bookmark=None, signatures=None):
        """Force a variable to a value with timed auto-release.

        Uses CommitOperations with 3 atomic sub-operations:
        1. Write the value
        2. Activate force (forced=true)
        3. Set auto-release timer (forcedUntil in milliseconds)

        Args:
            path: Full EBO object path (e.g. "/Server/VS1/Variabler/GT1_FS")
            object_id: Object GUID from EBO (e.g. "OzytXgOpT66fU0TxymXwQQ")
            value: Value to force (will be converted to string)
            server_path: Automation server path (e.g. "/Kattegattgymnasiet 20942 AS3")
            duration_seconds: Hold duration in seconds (default 3600 = 1 hour)
            bookmark: Session bookmark from ClientRefresh (auto-fetched if None)
            signatures: Electronic signature data if required, else None

        Returns:
            dict: API response (empty CommitOperationsRes on success)
        """
        if bookmark is None:
            bookmark = self.get_bookmark()

        value_str = str(value)
        duration_ms = int(duration_seconds * 1000)

        payload = {
            "command": "CommitOperations",
            "operations": [
                {
                    "kind": "SetProperty",
                    "id": object_id,
                    "name": "Value",
                    "value": value_str,
                    "isNull": False,
                    "path": path,
                },
                {
                    "kind": "SetProperty",
                    "id": object_id,
                    "name": "Value",
                    "forced": True,
                    "trueValue": value_str,
                    "value": value_str,
                    "path": path,
                },
                {
                    "kind": "SetProperty",
                    "id": object_id,
                    "name": "Value",
                    "forcedUntil": duration_ms,
                    "trueValue": value_str,
                    "value": value_str,
                    "path": path,
                },
            ],
            "bookmark": bookmark,
            "serverPath": server_path,
            "signatures": signatures,
        }

        return self._post_command(payload)

    def unforce_value(self, path, object_id, current_value, server_path,
                      bookmark=None, signatures=None):
        """UnForce (release) a variable, returning it to automatic control.

        Must be called before forcing a new value if a force is already active.

        Args:
            path: Full EBO object path
            object_id: Object GUID from EBO
            current_value: Current value of the variable (float, will be hex-encoded)
            server_path: Automation server path
            bookmark: Session bookmark (auto-fetched if None)
            signatures: Electronic signature data if required, else None

        Returns:
            dict: API response (empty CommitOperationsRes on success)
        """
        if bookmark is None:
            bookmark = self.get_bookmark()

        payload = {
            "command": "CommitOperations",
            "operations": [
                {
                    "kind": "SetProperty",
                    "id": object_id,
                    "name": "Value",
                    "forced": False,
                    "value": self.encode_value(float(current_value)),
                    "path": path,
                },
            ],
            "bookmark": bookmark,
            "serverPath": server_path,
            "signatures": signatures,
        }

        return self._post_command(payload)


def main():
    """Test login against an EBO server."""
    import argparse

    parser = argparse.ArgumentParser(description='EBO API login test')
    parser.add_argument('--url', required=True, help='Base URL (e.g. https://ebo.halmstad.se)')
    parser.add_argument('--username', required=True, help='Username')
    parser.add_argument('--password', required=True, help='Password')
    parser.add_argument('--domain', default='', help='Domain (default: empty)')
    parser.add_argument('--no-verify-ssl', action='store_true', help='Skip SSL verification')
    parser.add_argument('--explore', action='store_true', help='After login, explore object tree')
    args = parser.parse_args()

    api = EboApi(
        base_url=args.url,
        username=args.username,
        password=args.password,
        domain=args.domain,
        verify_ssl=not args.no_verify_ssl,
    )

    print(f"Connecting to {args.url}...")

    # Login
    print("Step 1: Fetching CSRF token...")
    api._fetch_csrf_token()
    print(f"  CSRF token: {api.csrf_token[:30]}...")

    print("Step 2: Getting challenge...")
    challenge = api._get_challenge()
    print(f"  Challenge: {challenge}")

    print("Step 3: Getting login settings...")
    settings = api._get_login_settings()
    print(f"  RSA key available: {api.rsa_public_key is not None}")
    print(f"  Settings keys: {list(settings.keys()) if isinstance(settings, dict) else 'N/A'}")

    print("Step 4: Authenticating...")
    token = api._authorize(challenge)
    api.session_token = token
    api.session.headers.update({
        'X-CSRF-Token': token,
        'Content-Type': 'application/json',
    })
    print(f"  Session token: {token}")
    print("Login successful!")

    # WebEntry
    print("\nInitializing session (WebEntry)...")
    entry = api.web_entry()
    if isinstance(entry, dict):
        web_entry_res = entry.get('WebEntryRes', entry)
        user = web_entry_res.get('User', {})
        print(f"  User: {user.get('name', 'N/A')}")
        print(f"  Domain: {user.get('domain', 'N/A')}")

    if args.explore:
        print("\nExploring object tree...")
        # Try browsing the root
        for root_path in ["/EC1", "/", ""]:
            try:
                result = api.get_objects([root_path], levels=1, include_hidden=True)
                print(f"\n  Objects at '{root_path}':")
                res = result.get('GetObjectsRes', result)
                objects = res.get('objects', []) if isinstance(res, dict) else []
                for obj in objects[:20]:
                    name = obj.get('name', obj.get('path', '?'))
                    print(f"    - {name}")
                if objects:
                    break
            except Exception as e:
                print(f"  Failed for '{root_path}': {e}")


if __name__ == '__main__':
    main()

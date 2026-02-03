#!/usr/bin/env python3
"""
Dropbox Client with OAuth Refresh Token Support

Provides a wrapper around the Dropbox SDK that handles automatic token refresh
using OAuth refresh tokens for long-lived access.

Usage:
    client = DropboxClient(
        app_key='your_app_key',
        app_secret='your_app_secret',
        refresh_token='your_refresh_token'
    )

    # Read a file
    content = client.read_file('/path/to/file.txt')

    # Write a file
    client.write_file('/path/to/file.txt', 'content')

    # List files in a folder
    files = client.list_folder('/folder')

    # Move a file
    client.move_file('/from/path.txt', '/to/path.txt')
"""

import logging
from typing import List, Optional

import dropbox
from dropbox.files import FileMetadata, FolderMetadata, WriteMode
from dropbox.exceptions import ApiError, AuthError

logger = logging.getLogger(__name__)


class DropboxClient:
    """
    Dropbox client with OAuth refresh token support.

    Uses refresh tokens for long-lived access instead of short-lived access tokens.
    The Dropbox SDK handles token refresh automatically.
    """

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        refresh_token: str
    ):
        """
        Initialize Dropbox client with OAuth credentials.

        Args:
            app_key: Dropbox app key from App Console
            app_secret: Dropbox app secret from App Console
            refresh_token: OAuth refresh token (obtained via setup_dropbox_auth.py)
        """
        self.app_key = app_key
        self.app_secret = app_secret
        self.refresh_token = refresh_token

        # Initialize with refresh token - SDK handles token refresh automatically
        self.dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret
        )

        logger.info("Dropbox client initialized with refresh token")

    def read_file(self, path: str) -> str:
        """
        Read file content from Dropbox.

        Args:
            path: Dropbox path (e.g., '/data/file.txt')

        Returns:
            File content as string (UTF-8 decoded)

        Raises:
            dropbox.exceptions.ApiError: If file not found or access denied
        """
        try:
            _, response = self.dbx.files_download(path)
            content = response.content.decode('utf-8-sig')  # Handle BOM
            logger.debug(f"Read file: {path} ({len(content)} bytes)")
            return content
        except ApiError as e:
            logger.error(f"Failed to read {path}: {e}")
            raise

    def read_file_bytes(self, path: str) -> bytes:
        """
        Read file content as bytes from Dropbox.

        Args:
            path: Dropbox path (e.g., '/data/file.csv')

        Returns:
            File content as bytes
        """
        try:
            _, response = self.dbx.files_download(path)
            logger.debug(f"Read file: {path} ({len(response.content)} bytes)")
            return response.content
        except ApiError as e:
            logger.error(f"Failed to read {path}: {e}")
            raise

    def write_file(self, path: str, content: str, overwrite: bool = True) -> FileMetadata:
        """
        Write content to a file in Dropbox.

        Args:
            path: Dropbox path (e.g., '/requests/meters.json')
            content: String content to write
            overwrite: If True, overwrites existing file; if False, auto-renames

        Returns:
            FileMetadata for the written file
        """
        try:
            mode = WriteMode.overwrite if overwrite else WriteMode.add
            data = content.encode('utf-8')

            metadata = self.dbx.files_upload(data, path, mode=mode)
            logger.info(f"Wrote file: {path} ({len(data)} bytes)")
            return metadata
        except ApiError as e:
            logger.error(f"Failed to write {path}: {e}")
            raise

    def write_file_bytes(self, path: str, data: bytes, overwrite: bool = True) -> FileMetadata:
        """
        Write bytes to a file in Dropbox.

        Args:
            path: Dropbox path
            data: Bytes to write
            overwrite: If True, overwrites existing file

        Returns:
            FileMetadata for the written file
        """
        try:
            mode = WriteMode.overwrite if overwrite else WriteMode.add
            metadata = self.dbx.files_upload(data, path, mode=mode)
            logger.info(f"Wrote file: {path} ({len(data)} bytes)")
            return metadata
        except ApiError as e:
            logger.error(f"Failed to write {path}: {e}")
            raise

    def list_folder(self, path: str, recursive: bool = False) -> List[FileMetadata]:
        """
        List files in a Dropbox folder.

        Args:
            path: Dropbox folder path (e.g., '/incoming')
            recursive: If True, lists files recursively

        Returns:
            List of FileMetadata objects (folders excluded)
        """
        try:
            # Handle root folder
            if path == '/':
                path = ''

            result = self.dbx.files_list_folder(path, recursive=recursive)
            files = []

            while True:
                for entry in result.entries:
                    if isinstance(entry, FileMetadata):
                        files.append(entry)

                if not result.has_more:
                    break

                result = self.dbx.files_list_folder_continue(result.cursor)

            logger.debug(f"Listed {len(files)} file(s) in {path or '/'}")
            return files

        except ApiError as e:
            if 'not_found' in str(e):
                logger.warning(f"Folder not found: {path}")
                return []
            logger.error(f"Failed to list {path}: {e}")
            raise

    def move_file(self, from_path: str, to_path: str, auto_rename: bool = True) -> FileMetadata:
        """
        Move a file within Dropbox.

        Args:
            from_path: Source path
            to_path: Destination path
            auto_rename: If True, auto-renames if destination exists

        Returns:
            FileMetadata for the moved file
        """
        try:
            metadata = self.dbx.files_move_v2(
                from_path,
                to_path,
                autorename=auto_rename
            )
            logger.info(f"Moved: {from_path} -> {to_path}")
            return metadata.metadata
        except ApiError as e:
            logger.error(f"Failed to move {from_path} to {to_path}: {e}")
            raise

    def delete_file(self, path: str) -> None:
        """
        Delete a file from Dropbox.

        Args:
            path: Path to file to delete
        """
        try:
            self.dbx.files_delete_v2(path)
            logger.info(f"Deleted: {path}")
        except ApiError as e:
            if 'not_found' in str(e):
                logger.warning(f"File not found (already deleted?): {path}")
                return
            logger.error(f"Failed to delete {path}: {e}")
            raise

    def create_folder(self, path: str) -> Optional[FolderMetadata]:
        """
        Create a folder in Dropbox.

        Args:
            path: Folder path to create

        Returns:
            FolderMetadata if created, None if already exists
        """
        try:
            result = self.dbx.files_create_folder_v2(path)
            logger.info(f"Created folder: {path}")
            return result.metadata
        except ApiError as e:
            if 'conflict' in str(e) or 'folder' in str(e).lower():
                logger.debug(f"Folder already exists: {path}")
                return None
            logger.error(f"Failed to create folder {path}: {e}")
            raise

    def ensure_folders(self, *paths: str) -> None:
        """
        Ensure multiple folders exist, creating them if needed.

        Args:
            *paths: Folder paths to ensure exist
        """
        for path in paths:
            self.create_folder(path)

    def file_exists(self, path: str) -> bool:
        """
        Check if a file exists in Dropbox.

        Args:
            path: File path to check

        Returns:
            True if file exists, False otherwise
        """
        try:
            self.dbx.files_get_metadata(path)
            return True
        except ApiError as e:
            if 'not_found' in str(e):
                return False
            raise

    def get_account_info(self) -> dict:
        """
        Get information about the connected Dropbox account.

        Returns:
            Dict with account info (name, email, etc.)
        """
        try:
            account = self.dbx.users_get_current_account()
            return {
                'name': account.name.display_name,
                'email': account.email,
                'account_id': account.account_id,
                'account_type': str(account.account_type),
            }
        except AuthError as e:
            logger.error(f"Authentication failed: {e}")
            raise


def create_client_from_env() -> Optional[DropboxClient]:
    """
    Create a DropboxClient from environment variables.

    Requires:
        DROPBOX_APP_KEY
        DROPBOX_APP_SECRET
        DROPBOX_REFRESH_TOKEN

    Returns:
        DropboxClient instance, or None if not configured
    """
    import os

    app_key = os.getenv('DROPBOX_APP_KEY')
    app_secret = os.getenv('DROPBOX_APP_SECRET')
    refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN')

    if not all([app_key, app_secret, refresh_token]):
        logger.warning("Dropbox not configured - missing DROPBOX_APP_KEY, DROPBOX_APP_SECRET, or DROPBOX_REFRESH_TOKEN")
        return None

    return DropboxClient(app_key, app_secret, refresh_token)

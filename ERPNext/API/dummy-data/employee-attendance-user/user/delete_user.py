#!/usr/bin/env python3
"""
ERPNext User Deletion Script
Deletes all users except specified protected users and system users.
Uses environment variables from .env file for configuration.
Author: ERPNext User Deletion Script
Version: 1.0.0
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

# ================================
# CONFIGURATION VARIABLES
# ================================
# Change these variables according to your setup
# Email of current user to protect
CURRENT_USER_EMAIL = "danarikramtirta@gmail.com"
PROTECTED_USERS = [
    "Administrator",
    "Guest",
    CURRENT_USER_EMAIL
]  # Users that will NOT be deleted

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    # Look for .env file in the API directory (parent of current directory)
    env_path = Path(__file__).parent.parent.parent / '.env'

    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print(f"✅ Loaded environment variables from {env_path}")
    else:
        print(f"⚠️ .env file not found at {env_path}")
        print("Using hardcoded values as fallback")


# Load environment variables
load_env_file()

# Configuration


class Config:
    # API Configuration (from .env file)
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL")
    COMPANY_NAME = os.getenv("COMPANY_NAME")
    COMPANY_ABBR = os.getenv("COMPANY_ABBR")

    # Retry settings
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # seconds


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set console handler encoding to handle unicode characters
for handler in logger.handlers:
    if isinstance(handler, StreamHandler):
        handler.stream.reconfigure(encoding='utf-8', errors='replace')


class ERPNextAPI:
    """Handles all API interactions with ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

        # Log the configuration being used
        logger.info(f"Using API configuration:")
        logger.info(f"  Base URL: {self.base_url}")
        logger.info(f"  Company: {Config.COMPANY_NAME}")
        logger.info(
            f"  API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT", "DELETE"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()

            # Handle DELETE requests that might not return JSON
            if method == "DELETE":
                return {"success": True}
            else:
                return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < Config.RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{Config.RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {Config.RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
                if hasattr(e, 'response') and e.response:
                    logger.error(f"Response content: {e.response.text}")
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        """Get list of documents"""
        params = {
            "limit_page_length": 1000
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)

        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def delete_doc(self, doctype: str, name: str) -> Dict:
        """Delete a document"""
        return self._make_request("DELETE", f"resource/{doctype}/{name}")


class UserDeletor:
    """Handles user deletion with safety checks"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_users = []
        self.protected_users = []
        self.failed_deletions = []

    def get_all_users(self):
        """Get all users from ERPNext"""
        logger.info("Fetching all users from ERPNext...")

        try:
            all_users = self.api.get_list("User",
                                          # Only enabled users
                                          filters=[["enabled", "=", 1]],
                                          fields=["name", "email", "first_name", "last_name", "enabled"])

            logger.info(f"Found {len(all_users)} total enabled users")
            return all_users

        except Exception as e:
            logger.error(f"Error fetching users: {str(e)}")
            return []

    def categorize_users(self, all_users):
        """Categorize users into protected and deletable"""
        deletable_users = []
        protected_users = []

        for user in all_users:
            user_email = user.get("email", "")
            user_name = user.get("name", "")

            # Check if user is in protected list (by email or name)
            is_protected = (
                user_email in PROTECTED_USERS or
                user_name in PROTECTED_USERS or
                user_email == CURRENT_USER_EMAIL
            )

            if is_protected:
                protected_users.append(user)
                logger.info(f"🔒 Protected user: {user_email} ({user_name})")
            else:
                deletable_users.append(user)
                logger.debug(f"🗑️ Deletable user: {user_email} ({user_name})")

        self.protected_users = protected_users

        logger.info(f"📊 User categorization:")
        logger.info(f"   - Protected users: {len(protected_users)}")
        logger.info(f"   - Deletable users: {len(deletable_users)}")

        return deletable_users

    def confirm_deletion(self, deletable_users):
        """Ask for user confirmation before deletion"""
        if not deletable_users:
            print("\n✅ No users to delete. All users are protected.")
            return False

        print(f"\n⚠️  WARNING: This will DELETE {len(deletable_users)} users!")
        print("\n🔒 Protected users (will NOT be deleted):")
        for user in self.protected_users:
            print(
                f"   - {user.get('email', 'No email')} ({user.get('name', 'No name')})")

        print(f"\n🗑️ Users to be DELETED ({len(deletable_users)}):")
        for i, user in enumerate(deletable_users[:10]):  # Show first 10
            print(
                f"   - {user.get('email', 'No email')} ({user.get('name', 'No name')})")

        if len(deletable_users) > 10:
            print(f"   ... and {len(deletable_users) - 10} more users")

        print(f"\n🎯 Current user email (protected): {CURRENT_USER_EMAIL}")

        response = input(
            f"\nAre you sure you want to DELETE {len(deletable_users)} users? Type 'DELETE' to confirm: ")

        return response == "DELETE"

    def delete_users(self, users_to_delete):
        """Delete the specified users"""
        logger.info(f"Starting deletion of {len(users_to_delete)} users...")

        deleted_count = 0
        failed_count = 0

        for i, user in enumerate(users_to_delete):
            user_email = user.get("email", "No email")
            user_name = user.get("name", "No name")

            try:
                logger.info(
                    f"🗑️ Deleting user {i+1}/{len(users_to_delete)}: {user_email} ({user_name})")

                # Attempt to delete the user
                self.api.delete_doc("User", user_name)

                self.deleted_users.append(user)
                deleted_count += 1

                logger.info(f"✅ Successfully deleted: {user_email}")

                # Small delay to avoid overwhelming the server
                time.sleep(0.5)

            except requests.exceptions.HTTPError as e:
                failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)

                if e.response and e.response.status_code == 417:
                    # Handle potential throttling
                    logger.warning(
                        f"⏳ Deletion throttled for {user_email}. Waiting 5 seconds...")
                    time.sleep(5)
                    try:
                        # Retry once
                        self.api.delete_doc("User", user_name)
                        self.deleted_users.append(user)
                        deleted_count += 1
                        logger.info(
                            f"✅ Successfully deleted (after retry): {user_email}")
                    except Exception as retry_error:
                        logger.error(
                            f"❌ Failed to delete {user_email} even after retry: {str(retry_error)}")
                        self.failed_deletions.append(
                            {"user": user, "error": str(retry_error)})
                elif e.response and e.response.status_code == 403:
                    logger.error(
                        f"❌ Permission denied for {user_email}: {error_msg}")
                    self.failed_deletions.append(
                        {"user": user, "error": f"Permission denied: {error_msg}"})
                elif e.response and e.response.status_code == 409:
                    logger.error(
                        f"❌ Cannot delete {user_email} (may have dependencies): {error_msg}")
                    self.failed_deletions.append(
                        {"user": user, "error": f"Has dependencies: {error_msg}"})
                else:
                    logger.error(
                        f"❌ Failed to delete {user_email}: {error_msg}")
                    self.failed_deletions.append(
                        {"user": user, "error": error_msg})

            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Failed to delete {user_email}: {str(e)}")
                self.failed_deletions.append({"user": user, "error": str(e)})

        logger.info(
            f"Deletion completed: {deleted_count} deleted, {failed_count} failed")
        return deleted_count, failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🗑️ ERPNext User Deletion Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {Config.BASE_URL}")
        print(f"🏢 Company: {Config.COMPANY_NAME}")
        print(
            f"🔑 Using API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")
        print(f"🔒 Current User (Protected): {CURRENT_USER_EMAIL}")
        print(f"🛡️ Protected Users: {', '.join(PROTECTED_USERS)}")
        print("=" * 80)

        try:
            # Step 1: Get all users
            all_users = self.get_all_users()
            if not all_users:
                print("❌ No users found or failed to fetch users")
                return

            # Step 2: Categorize users
            deletable_users = self.categorize_users(all_users)

            # Step 3: Confirm deletion
            if not self.confirm_deletion(deletable_users):
                print("Operation cancelled.")
                return

            # Step 4: Delete users
            deleted_count, failed_count = self.delete_users(deletable_users)

            # Step 5: Final summary
            print("\n" + "=" * 80)
            print("✅ USER DELETION COMPLETED!")
            print("=" * 80)
            print(f"📊 Summary:")
            print(f"   - Users deleted: {deleted_count}")
            print(f"   - Users failed: {failed_count}")
            print(f"   - Users protected: {len(self.protected_users)}")

            if self.failed_deletions:
                print(f"\n❌ Failed deletions ({len(self.failed_deletions)}):")
                # Show first 5 failures
                for failure in self.failed_deletions[:5]:
                    user = failure["user"]
                    error = failure["error"]
                    print(f"   - {user.get('email', 'No email')}: {error}")
                if len(self.failed_deletions) > 5:
                    print(
                        f"   ... and {len(self.failed_deletions) - 5} more failures")

            print("📋 Check the log file for detailed information:")
            print("   erpnext_user_deletion.log")
            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during user deletion: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have User deletion permissions")
            print(f"2. Verify ERPNext is running at {Config.BASE_URL}")
            print("3. Some users may have dependencies and cannot be deleted")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext User Deletion...")

    # Check if API credentials are set
    if not Config.API_KEY or not Config.API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in API/.env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    print(f"\n⚠️ WARNING: This script will delete ALL users except:")
    print(f"   - {CURRENT_USER_EMAIL} (current user)")
    print(f"   - Administrator (system user)")
    print(f"   - Guest (system user)")

    response = input(
        f"\nDo you want to proceed with user deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = UserDeletor()
        deletor.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext User Permissions Deletion Script
Deletes all user permissions from ERPNext system.
This should be run BEFORE deleting employees to clear permission dependencies.
Uses environment variables from .env file for configuration.
Author: ERPNext User Permissions Deletion Script
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
        logging.FileHandler(
            'erpnext_user_permissions_deletion.log', encoding='utf-8'),
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


class UserPermissionsDeletor:
    """Handles user permissions deletion with detailed reporting"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_permissions = []
        self.failed_deletions = []
        self.permission_stats = {}

    def get_all_user_permissions(self):
        """Get all user permissions from ERPNext"""
        logger.info("Fetching all user permissions from ERPNext...")

        try:
            all_permissions = self.api.get_list("User Permission",
                                                fields=["name", "user", "allow", "for_value", "creation"])

            logger.info(f"Found {len(all_permissions)} total user permissions")
            return all_permissions

        except Exception as e:
            logger.error(f"Error fetching user permissions: {str(e)}")
            return []

    def analyze_permissions(self, permissions):
        """Analyze permissions by type and user"""
        logger.info("Analyzing user permissions...")

        # Group by permission type
        by_type = {}
        by_user = {}

        for perm in permissions:
            perm_type = perm.get("allow", "Unknown")
            user = perm.get("user", "Unknown")

            # Count by type
            if perm_type not in by_type:
                by_type[perm_type] = []
            by_type[perm_type].append(perm)

            # Count by user
            if user not in by_user:
                by_user[user] = []
            by_user[user].append(perm)

        logger.info(f"📊 Permission Analysis:")
        logger.info(f"   - Unique users with permissions: {len(by_user)}")
        logger.info(f"   - Unique permission types: {len(by_type)}")

        # Show top permission types
        logger.info(f"   - Top permission types:")
        sorted_types = sorted(
            by_type.items(), key=lambda x: len(x[1]), reverse=True)
        for perm_type, perms in sorted_types[:5]:
            logger.info(f"     • {perm_type}: {len(perms)} permissions")

        # Show users with most permissions
        logger.info(f"   - Users with most permissions:")
        sorted_users = sorted(
            by_user.items(), key=lambda x: len(x[1]), reverse=True)
        for user, perms in sorted_users[:5]:
            logger.info(f"     • {user}: {len(perms)} permissions")

        return by_type, by_user

    def display_permissions_summary(self, permissions):
        """Display summary of permissions to be deleted"""
        if not permissions:
            print("\n✅ No user permissions found to delete.")
            return False

        print(
            f"\n⚠️  WARNING: This will DELETE {len(permissions)} user permissions!")

        # Analyze permissions
        by_type, by_user = self.analyze_permissions(permissions)

        print(f"\n📊 User Permissions Summary:")
        print(f"   - Total permissions: {len(permissions)}")
        print(f"   - Unique users affected: {len(by_user)}")
        print(f"   - Unique permission types: {len(by_type)}")

        print(f"\n🔑 Top Permission Types:")
        sorted_types = sorted(
            by_type.items(), key=lambda x: len(x[1]), reverse=True)
        for i, (perm_type, perms) in enumerate(sorted_types[:10]):
            print(f"   {i+1}. {perm_type}: {len(perms)} permissions")

        print(f"\n👥 Users with Most Permissions (top 10):")
        sorted_users = sorted(
            by_user.items(), key=lambda x: len(x[1]), reverse=True)
        for i, (user, perms) in enumerate(sorted_users[:10]):
            print(f"   {i+1}. {user}: {len(perms)} permissions")

        return True

    def confirm_deletion(self, permissions):
        """Ask for user confirmation before deletion"""
        if not self.display_permissions_summary(permissions):
            return False

        print(f"\n🚨 IMPORTANT:")
        print(f"   - This will remove ALL user permissions from the system")
        print(f"   - Users may lose access to specific records/documents")
        print(f"   - This action CANNOT be undone!")
        print(f"   - Run this BEFORE deleting employees to clear dependencies")

        response = input(
            f"\nAre you sure you want to DELETE ALL {len(permissions)} user permissions? Type 'DELETE ALL PERMISSIONS' to confirm: ")

        return response == "DELETE ALL PERMISSIONS"

    def delete_permissions(self, permissions_to_delete):
        """Delete the specified user permissions"""
        logger.info(
            f"Starting deletion of {len(permissions_to_delete)} user permissions...")

        deleted_count = 0
        failed_count = 0

        for i, permission in enumerate(permissions_to_delete):
            permission_name = permission.get("name", "Unknown")
            user = permission.get("user", "Unknown")
            perm_type = permission.get("allow", "Unknown")
            for_value = permission.get("for_value", "Unknown")

            try:
                logger.info(
                    f"🗑️ Deleting permission {i+1}/{len(permissions_to_delete)}: {user} -> {perm_type} ({for_value})")

                # Delete the user permission
                self.api.delete_doc("User Permission", permission_name)

                self.deleted_permissions.append(permission)
                deleted_count += 1

                # Track stats by permission type
                if perm_type not in self.permission_stats:
                    self.permission_stats[perm_type] = 0
                self.permission_stats[perm_type] += 1

                # Progress update every 50 deletions
                if deleted_count % 50 == 0:
                    logger.info(
                        f"✅ Progress: {deleted_count}/{len(permissions_to_delete)} permissions deleted")

                # Very small delay to avoid overwhelming the server
                time.sleep(0.1)

            except requests.exceptions.HTTPError as e:
                failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)

                if e.response and e.response.status_code == 417:
                    # Handle potential throttling
                    logger.warning(
                        f"⏳ Deletion throttled for permission {permission_name}. Waiting 3 seconds...")
                    time.sleep(3)
                    try:
                        # Retry once
                        self.api.delete_doc("User Permission", permission_name)
                        self.deleted_permissions.append(permission)
                        deleted_count += 1
                        logger.info(
                            f"✅ Successfully deleted (after retry): {user} -> {perm_type}")
                    except Exception as retry_error:
                        logger.error(
                            f"❌ Failed to delete permission even after retry: {str(retry_error)}")
                        self.failed_deletions.append(
                            {"permission": permission, "error": str(retry_error)})
                elif e.response and e.response.status_code == 403:
                    logger.error(
                        f"❌ Permission denied for {permission_name}: {error_msg}")
                    self.failed_deletions.append(
                        {"permission": permission, "error": f"Permission denied: {error_msg}"})
                elif e.response and e.response.status_code == 404:
                    logger.warning(
                        f"⚠️ Permission {permission_name} not found (may have been deleted already)")
                    # Don't count as failure since it's already gone
                    deleted_count += 1
                else:
                    logger.error(
                        f"❌ Failed to delete permission {permission_name}: {error_msg}")
                    self.failed_deletions.append(
                        {"permission": permission, "error": error_msg})

            except Exception as e:
                failed_count += 1
                logger.error(
                    f"❌ Failed to delete permission {permission_name}: {str(e)}")
                self.failed_deletions.append(
                    {"permission": permission, "error": str(e)})

        logger.info(
            f"User permissions deletion completed: {deleted_count} deleted, {failed_count} failed")
        return deleted_count, failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🔑 ERPNext User Permissions Deletion Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {Config.BASE_URL}")
        print(f"🏢 Company: {Config.COMPANY_NAME}")
        print(
            f"🔑 Using API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")
        print("⚠️  WARNING: This will delete ALL user permissions!")
        print("💡 Run this BEFORE deleting employees to clear permission dependencies")
        print("=" * 80)

        try:
            # Step 1: Get all user permissions
            all_permissions = self.get_all_user_permissions()
            if not all_permissions:
                print("✅ No user permissions found to delete")
                return

            # Step 2: Confirm deletion
            if not self.confirm_deletion(all_permissions):
                print("Operation cancelled.")
                return

            # Step 3: Delete user permissions
            deleted_count, failed_count = self.delete_permissions(
                all_permissions)

            # Step 4: Final summary
            print("\n" + "=" * 80)
            print("✅ USER PERMISSIONS DELETION COMPLETED!")
            print("=" * 80)
            print(f"📊 Summary:")
            print(f"   - Permissions deleted: {deleted_count}")
            print(f"   - Permissions failed: {failed_count}")

            if self.permission_stats:
                print(f"\n📋 Deleted permissions by type:")
                sorted_stats = sorted(
                    self.permission_stats.items(), key=lambda x: x[1], reverse=True)
                for perm_type, count in sorted_stats:
                    print(f"   - {perm_type}: {count}")

            if self.failed_deletions:
                print(f"\n❌ Failed deletions ({len(self.failed_deletions)}):")
                # Show first 5 failures
                for failure in self.failed_deletions[:5]:
                    permission = failure["permission"]
                    error = failure["error"]
                    user = permission.get("user", "Unknown")
                    perm_type = permission.get("allow", "Unknown")
                    print(f"   - {user} -> {perm_type}: {error}")
                if len(self.failed_deletions) > 5:
                    print(
                        f"   ... and {len(self.failed_deletions) - 5} more failures")

            print("\n💡 Next Steps:")
            print("   1. User permissions have been cleared")
            print("   2. You can now safely run the employee deletion script")
            print("   3. Check the log file for detailed information:")
            print("      erpnext_user_permissions_deletion.log")
            print("=" * 80)

        except Exception as e:
            logger.error(
                f"Fatal error during user permissions deletion: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have User Permission deletion rights")
            print(f"2. Verify ERPNext is running at {Config.BASE_URL}")
            print("3. Some permissions may be protected by the system")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext User Permissions Deletion...")

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

    print(f"\n⚠️ FINAL WARNING:")
    print(f"   This script will delete ALL user permissions in ERPNext")
    print(f"   This clears permission dependencies for employee deletion")
    print(f"   This action CANNOT be undone!")

    response = input(
        f"\nDo you want to proceed with user permissions deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = UserPermissionsDeletor()
        deletor.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

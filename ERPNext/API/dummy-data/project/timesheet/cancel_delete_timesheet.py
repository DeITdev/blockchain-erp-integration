#!/usr/bin/env python3
"""
ERPNext Timesheet Deletion Script
Cancels and deletes all timesheet records from ERPNext system.
First cancels submitted timesheets, then deletes all timesheets.
Uses environment variables from .env file for configuration.
Author: ERPNext Timesheet Deletion Script
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

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
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

# Configuration from environment variables
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
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        # Log the configuration being used
        logger.info(f"Using API configuration:")
        logger.info(f"  Base URL: {self.base_url}")
        logger.info(f"  Company: {COMPANY_NAME}")
        logger.info(f"  API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")

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
            if retry_count < RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
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

    def cancel_doc(self, doctype: str, name: str) -> Dict:
        """Cancel a document by setting docstatus to 2"""
        return self._make_request("PUT", f"resource/{doctype}/{name}", {"docstatus": 2})

    def delete_doc(self, doctype: str, name: str) -> Dict:
        """Delete a document"""
        return self._make_request("DELETE", f"resource/{doctype}/{name}")


class TimesheetDeletor:
    """Handles timesheet cancellation and deletion with safety checks"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.cancelled_timesheets = []
        self.deleted_timesheets = []
        self.failed_operations = []

    def get_all_timesheets(self):
        """Get all timesheets from ERPNext"""
        logger.info("Fetching all timesheets from ERPNext...")

        try:
            all_timesheets = self.api.get_list("Timesheet",
                                               filters={
                                                   "company": COMPANY_NAME},
                                               fields=["name", "employee", "docstatus", "total_hours"])

            logger.info(f"Found {len(all_timesheets)} total timesheets")
            return all_timesheets

        except Exception as e:
            logger.error(f"Error fetching timesheets: {str(e)}")
            return []

    def categorize_timesheets(self, all_timesheets):
        """Categorize timesheets by status"""
        draft_timesheets = []
        submitted_timesheets = []
        cancelled_timesheets = []

        for timesheet in all_timesheets:
            docstatus = timesheet.get("docstatus", 0)

            if docstatus == 0:
                draft_timesheets.append(timesheet)
            elif docstatus == 1:
                submitted_timesheets.append(timesheet)
            elif docstatus == 2:
                cancelled_timesheets.append(timesheet)

        logger.info(f"📊 Timesheet categorization:")
        logger.info(f"   - Draft (docstatus=0): {len(draft_timesheets)}")
        logger.info(
            f"   - Submitted (docstatus=1): {len(submitted_timesheets)}")
        logger.info(
            f"   - Cancelled (docstatus=2): {len(cancelled_timesheets)}")

        return draft_timesheets, submitted_timesheets, cancelled_timesheets

    def display_timesheets_summary(self, all_timesheets):
        """Display summary of timesheets to be deleted"""
        if not all_timesheets:
            print("\n✅ No timesheets found to delete.")
            return False

        print(
            f"\n⚠️  WARNING: This will DELETE {len(all_timesheets)} timesheets!")

        # Categorize timesheets
        draft_timesheets, submitted_timesheets, cancelled_timesheets = self.categorize_timesheets(
            all_timesheets)

        print(f"\n📊 Timesheet Summary:")
        print(f"   - Total timesheets: {len(all_timesheets)}")
        print(f"   - Draft timesheets: {len(draft_timesheets)}")
        print(f"   - Submitted timesheets: {len(submitted_timesheets)}")
        print(f"   - Cancelled timesheets: {len(cancelled_timesheets)}")

        # Calculate total hours
        total_hours = sum(timesheet.get("total_hours", 0)
                          for timesheet in all_timesheets)
        print(f"   - Total hours logged: {total_hours}")

        # Show sample timesheets
        print(f"\n📝 Sample timesheets to be deleted (first 10):")
        for i, timesheet in enumerate(all_timesheets[:10]):
            employee = timesheet.get("employee", "Unknown")
            docstatus = timesheet.get("docstatus", 0)
            total_hours = timesheet.get("total_hours", 0)

            # Status indicators
            status_indicator = {0: "📝 Draft", 1: "✅ Submitted",
                                2: "❌ Cancelled"}.get(docstatus, "❓ Unknown")

            print(
                f"   {i+1}. {employee} - {status_indicator} - ⏰ {total_hours} hours")

        if len(all_timesheets) > 10:
            print(f"   ... and {len(all_timesheets) - 10} more timesheets")

        print(f"\n🔄 Process:")
        print(f"   1. Cancel {len(submitted_timesheets)} submitted timesheets")
        print(f"   2. Delete all {len(all_timesheets)} timesheets")

        return True

    def confirm_deletion(self, all_timesheets):
        """Ask for user confirmation before deletion"""
        if not self.display_timesheets_summary(all_timesheets):
            return False

        print(f"\n🚨 IMPORTANT:")
        print(f"   - This will permanently delete ALL timesheet records")
        print(f"   - All time tracking data will be lost")
        print(f"   - Project time logs and billing data will be affected")
        print(f"   - This action CANNOT be undone!")

        response = input(
            f"\nAre you sure you want to DELETE ALL {len(all_timesheets)} timesheets? Type 'DELETE ALL TIMESHEETS' to confirm: ")
        return response == "DELETE ALL TIMESHEETS"

    def cancel_submitted_timesheets(self, submitted_timesheets):
        """Cancel all submitted timesheets (docstatus 1 → 2)"""
        if not submitted_timesheets:
            logger.info("No submitted timesheets to cancel.")
            return 0

        logger.info(
            f"Cancelling {len(submitted_timesheets)} submitted timesheets...")

        cancelled_count = 0

        for i, timesheet in enumerate(submitted_timesheets):
            timesheet_name = timesheet.get("name", "Unknown")
            employee = timesheet.get("employee", "Unknown")

            try:
                logger.info(
                    f"🔄 Cancelling timesheet {i+1}/{len(submitted_timesheets)}: {employee}")

                # Cancel the timesheet (docstatus 1 → 2)
                self.api.cancel_doc("Timesheet", timesheet_name)

                self.cancelled_timesheets.append(timesheet)
                cancelled_count += 1

                logger.info(f"✅ Successfully cancelled: {employee}")

                # Progress update every 5 cancellations
                if cancelled_count % 5 == 0:
                    logger.info(
                        f"📊 Cancellation progress: {cancelled_count}/{len(submitted_timesheets)} completed")

            except Exception as e:
                logger.error(
                    f"❌ Failed to cancel timesheet for {employee}: {str(e)}")
                self.failed_operations.append({
                    "operation": "cancel",
                    "timesheet": timesheet,
                    "error": str(e)
                })

        logger.info(f"Cancellation completed: {cancelled_count} cancelled")
        return cancelled_count

    def delete_all_timesheets(self, all_timesheets):
        """Delete all timesheets (after cancellation)"""
        logger.info(
            f"Starting deletion of {len(all_timesheets)} timesheets...")

        deleted_count = 0

        for i, timesheet in enumerate(all_timesheets):
            timesheet_name = timesheet.get("name", "Unknown")
            employee = timesheet.get("employee", "Unknown")

            try:
                logger.info(
                    f"🗑️ Deleting timesheet {i+1}/{len(all_timesheets)}: {employee}")

                # Delete the timesheet
                self.api.delete_doc("Timesheet", timesheet_name)

                self.deleted_timesheets.append(timesheet)
                deleted_count += 1

                logger.info(f"✅ Successfully deleted: {employee}")

                # Progress update every 10 deletions
                if deleted_count % 10 == 0:
                    logger.info(
                        f"📊 Deletion progress: {deleted_count}/{len(all_timesheets)} completed")

            except Exception as e:
                logger.error(
                    f"❌ Failed to delete timesheet for {employee}: {str(e)}")
                self.failed_operations.append({
                    "operation": "delete",
                    "timesheet": timesheet,
                    "error": str(e)
                })

        logger.info(f"Deletion completed: {deleted_count} deleted")
        return deleted_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🗑️ ERPNext Timesheet Deletion Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🔑 Using API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")
        print("⚠️  WARNING: This will delete ALL timesheets from the system!")
        print("🔄 Process: Cancel submitted timesheets → Delete all timesheets")
        print("=" * 80)

        try:
            # Step 1: Get all timesheets
            all_timesheets = self.get_all_timesheets()
            if not all_timesheets:
                print("✅ No timesheets found to delete")
                return

            # Step 2: Confirm deletion
            if not self.confirm_deletion(all_timesheets):
                print("Operation cancelled.")
                return

            # Step 3: Categorize timesheets
            draft_timesheets, submitted_timesheets, cancelled_timesheets = self.categorize_timesheets(
                all_timesheets)

            print("\n" + "=" * 80)
            print("🔄 STARTING TIMESHEET DELETION PROCESS")
            print("=" * 80)

            # Step 4: Cancel submitted timesheets first
            if submitted_timesheets:
                print(
                    f"\n📋 Step 1: Cancelling {len(submitted_timesheets)} submitted timesheets...")
                cancelled_count = self.cancel_submitted_timesheets(
                    submitted_timesheets)
                print(f"✅ Cancelled {cancelled_count} timesheets")
            else:
                print("\n📋 Step 1: No submitted timesheets to cancel")

            # Step 5: Delete all timesheets
            print(
                f"\n📋 Step 2: Deleting all {len(all_timesheets)} timesheets...")
            deleted_count = self.delete_all_timesheets(all_timesheets)

            # Step 6: Final summary
            print("\n" + "=" * 80)
            print("✅ TIMESHEET DELETION COMPLETED!")
            print("=" * 80)
            print(f"📊 Summary:")
            print(
                f"   - Timesheets cancelled: {len(self.cancelled_timesheets)}")
            print(f"   - Timesheets deleted: {deleted_count}")
            print(f"   - Total processed: {len(all_timesheets)}")
            print(f"   - Failed operations: {len(self.failed_operations)}")

            if self.failed_operations:
                print(
                    f"\n❌ Failed operations ({len(self.failed_operations)}):")
                # Show first 5 failures
                for failure in self.failed_operations[:5]:
                    operation = failure["operation"]
                    timesheet = failure["timesheet"]
                    error = failure["error"]
                    employee = timesheet.get("employee", "Unknown")
                    print(f"   - Failed to {operation}: {employee} - {error}")
                if len(self.failed_operations) > 5:
                    print(
                        f"   ... and {len(self.failed_operations) - 5} more failures")

            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during timesheet deletion: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print(
                "1. Check if API key/secret have Timesheet cancellation/deletion permissions")
            print(f"2. Verify ERPNext is running at {BASE_URL}")
            print("3. Some timesheets may have dependencies and cannot be deleted")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Timesheet Deletion...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in API/.env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    print(f"\n⚠️ WARNING: This script will delete ALL timesheets from ERPNext")
    print(f"   - First cancels all submitted timesheets")
    print(f"   - Then deletes all timesheet records")
    print(f"   - All time tracking data will be permanently lost")

    response = input(
        f"\nDo you want to proceed with timesheet deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = TimesheetDeletor()
        deletor.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

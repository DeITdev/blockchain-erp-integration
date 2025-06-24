#!/usr/bin/env python3
"""
ERPNext Attendance Deletion Script
Deletes all attendance records from the system.
Uses environment variables from .env file for configuration.
Author: ERPNext Attendance Deleter
Version: 1.0.0
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional
import sys

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

# Company filter
COMPANY = "PT Fiyansa Mulya"

# Simple logging - console only, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class AttendanceDeleter:
    """Deletes all attendance records from ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        self.deleted_count = 0
        self.failed_count = 0

        logger.info(f"🔗 API: {self.base_url}")
        logger.info(f"🏢 Company: {COMPANY}")
        logger.info(f"🔑 Key: {API_KEY[:8] if API_KEY else 'None'}...")

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
            if retry_count < 3:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/3) - Error: {e}")
                time.sleep(2)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after 3 attempts for {url}: {str(e)}")
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

    def get_all_attendance_records(self):
        """Fetch all attendance records"""
        logger.info("📋 Fetching all attendance records...")

        try:
            # Get all attendance records for the company
            attendance_records = self.get_list("Attendance",
                                               filters={"company": COMPANY},
                                               fields=["name", "employee", "attendance_date", "status"])

            logger.info(
                f"Found {len(attendance_records)} attendance records for {COMPANY}")

            if attendance_records:
                # Show some sample records
                logger.info("Sample attendance records:")
                for i, record in enumerate(attendance_records[:5]):
                    logger.info(
                        f"   {i+1}. {record.get('name')} - {record.get('attendance_date')} ({record.get('status')})")

                if len(attendance_records) > 5:
                    logger.info(
                        f"   ... and {len(attendance_records) - 5} more records")

            return attendance_records

        except Exception as e:
            logger.error(f"Error fetching attendance records: {str(e)}")
            return []

    def confirm_deletion(self, attendance_records):
        """Ask for user confirmation before deletion"""
        if not attendance_records:
            print("\n✅ No attendance records found to delete.")
            return False

        print(
            f"\n⚠️  WARNING: This will DELETE ALL {len(attendance_records)} attendance records!")
        print(f"\n🏢 Company: {COMPANY}")
        print(f"📊 Records to delete: {len(attendance_records)}")

        if attendance_records:
            print(f"\n📋 Sample records that will be deleted:")
            for i, record in enumerate(attendance_records[:10]):
                employee = record.get('employee', 'Unknown')
                date = record.get('attendance_date', 'Unknown')
                status = record.get('status', 'Unknown')
                print(f"   {i+1}. {employee} - {date} ({status})")

            if len(attendance_records) > 10:
                print(
                    f"   ... and {len(attendance_records) - 10} more records")

        print(f"\n🚨 THIS ACTION CANNOT BE UNDONE!")
        response = input(
            f"\nAre you sure you want to DELETE ALL {len(attendance_records)} attendance records? Type 'DELETE ALL' to confirm: ")

        return response == "DELETE ALL"

    def delete_attendance_records(self, records_to_delete):
        """Delete all attendance records"""
        print("\n" + "="*60)
        print("🗑️ Deleting Attendance Records")
        print("="*60)

        logger.info(
            f"Starting deletion of {len(records_to_delete)} attendance records...")

        for i, record in enumerate(records_to_delete):
            try:
                record_name = record.get("name")
                employee = record.get("employee", "Unknown")
                date = record.get("attendance_date", "Unknown")

                # Delete the attendance record
                self.delete_doc("Attendance", record_name)

                self.deleted_count += 1

                # Show progress every 50 deletions
                if self.deleted_count % 50 == 0:
                    print(
                        f"🗑️ Deleted {self.deleted_count}/{len(records_to_delete)} records...")

                logger.debug(f"Deleted: {record_name} ({employee} - {date})")

                # Small delay to avoid overwhelming the server
                time.sleep(0.1)

            except Exception as e:
                self.failed_count += 1
                logger.error(
                    f"❌ Failed to delete record {record.get('name', 'Unknown')}: {str(e)}")

                # Show failed deletions
                if self.failed_count <= 10:  # Show first 10 failures
                    print(
                        f"❌ Failed to delete: {record.get('name', 'Unknown')} - {str(e)[:50]}...")

        return self.deleted_count, self.failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🗑️ ERPNext Attendance Deletion Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY}")
        print("⚠️  THIS WILL DELETE ALL ATTENDANCE RECORDS!")
        print("=" * 80)

        try:
            # Get all attendance records
            attendance_records = self.get_all_attendance_records()

            # Confirm deletion
            if not self.confirm_deletion(attendance_records):
                print("Operation cancelled.")
                return

            # Delete records
            deleted_count, failed_count = self.delete_attendance_records(
                attendance_records)

            # Summary
            print("\n" + "="*60)
            print("📊 DELETION SUMMARY")
            print("="*60)
            print(f"✅ Successfully Deleted: {deleted_count} records")
            print(f"❌ Failed Deletions: {failed_count} records")
            print(f"📊 Total Processed: {len(attendance_records)} records")
            print(f"🏢 Company: {COMPANY}")

            if deleted_count > 0:
                print(
                    f"\n🎉 Successfully deleted {deleted_count} attendance records!")
                print("💡 The attendance system is now clean and ready for new data.")

            if failed_count > 0:
                print(f"\n⚠️ {failed_count} records could not be deleted.")
                print("This might be due to permissions or record dependencies.")

        except Exception as e:
            logger.error(f"Fatal error during attendance deletion: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Attendance Deletion...")

    # Check API credentials
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"\n🗑️ This will DELETE ALL attendance records for:")
    print(f"   🏢 Company: {COMPANY}")
    print(f"   🚨 This action CANNOT be undone!")
    print(f"   📊 All attendance data will be permanently removed")

    response = input(
        f"\nDo you want to proceed with attendance deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deleter = AttendanceDeleter()
        deleter.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Attendance Cancellation Script
Cancels all submitted attendance records in the system.
Uses environment variables from .env file for configuration.
Author: ERPNext Attendance Canceller
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


class AttendanceCanceller:
    """Cancels all submitted attendance records from ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        self.cancelled_count = 0
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

            # Handle DELETE/PUT requests that might not return JSON
            if method in ["DELETE", "PUT"]:
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

    def cancel_doc(self, doctype: str, name: str) -> Dict:
        """Cancel a document (set docstatus to 2)"""
        data = {
            "docstatus": 2  # 2 = Cancelled in ERPNext
        }
        return self._make_request("PUT", f"resource/{doctype}/{name}", data)

    def get_all_submitted_attendance_records(self):
        """Fetch all submitted attendance records"""
        logger.info("📋 Fetching all SUBMITTED attendance records...")

        try:
            # Get all submitted attendance records for the company (docstatus = 1 = Submitted)
            attendance_records = self.get_list("Attendance",
                                               filters={
                                                   "company": COMPANY, "docstatus": 1},
                                               fields=["name", "employee", "attendance_date", "status", "docstatus"])

            logger.info(
                f"Found {len(attendance_records)} SUBMITTED attendance records for {COMPANY}")

            if attendance_records:
                # Show some sample records
                logger.info("Sample attendance records to be cancelled:")
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

    def confirm_cancellation(self, attendance_records):
        """Ask for user confirmation before cancellation"""
        if not attendance_records:
            print("\n✅ No SUBMITTED attendance records found to cancel.")
            return False

        print(
            f"\n⚠️  WARNING: This will CANCEL ALL {len(attendance_records)} SUBMITTED attendance records!")
        print(f"\n🏢 Company: {COMPANY}")
        print(f"📊 SUBMITTED Records to cancel: {len(attendance_records)}")

        if attendance_records:
            print(f"\n📋 Sample records that will be cancelled:")
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
            f"\nAre you sure you want to CANCEL ALL {len(attendance_records)} SUBMITTED attendance records? Type 'CANCEL ALL' to confirm: ")

        return response == "CANCEL ALL"

    def cancel_attendance_records(self, records_to_cancel):
        """Cancel all submitted attendance records"""
        print("\n" + "="*60)
        print("❌ Cancelling Submitted Attendance Records")
        print("="*60)

        logger.info(
            f"Starting cancellation of {len(records_to_cancel)} submitted attendance records...")

        for i, record in enumerate(records_to_cancel):
            try:
                record_name = record.get("name")
                employee = record.get("employee", "Unknown")
                date = record.get("attendance_date", "Unknown")

                # Cancel the attendance record
                self.cancel_doc("Attendance", record_name)

                self.cancelled_count += 1

                # Show progress every 50 cancellations
                if self.cancelled_count % 50 == 0:
                    print(
                        f"❌ Cancelled {self.cancelled_count}/{len(records_to_cancel)} records...")

                logger.debug(f"Cancelled: {record_name} ({employee} - {date})")

                # Small delay to avoid overwhelming the server
                time.sleep(0.1)

            except Exception as e:
                self.failed_count += 1
                logger.error(
                    f"❌ Failed to cancel record {record.get('name', 'Unknown')}: {str(e)}")

                # Show failed cancellations
                if self.failed_count <= 10:  # Show first 10 failures
                    print(
                        f"❌ Failed to cancel: {record.get('name', 'Unknown')} - {str(e)[:50]}...")

        return self.cancelled_count, self.failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("❌ ERPNext SUBMITTED Attendance Cancellation Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY}")
        print("⚠️  THIS WILL CANCEL ALL SUBMITTED ATTENDANCE RECORDS!")
        print("=" * 80)

        try:
            # Get all submitted attendance records
            attendance_records = self.get_all_submitted_attendance_records()

            # Confirm cancellation
            if not self.confirm_cancellation(attendance_records):
                print("Operation cancelled.")
                return

            # Cancel records
            cancelled_count, failed_count = self.cancel_attendance_records(
                attendance_records)

            # Summary
            print("\n" + "="*60)
            print("📊 CANCELLATION SUMMARY")
            print("="*60)
            print(f"✅ Successfully cancelled: {cancelled_count} records")
            print(f"❌ Failed to cancel: {failed_count} records")
            print(
                f"📊 Total processed: {cancelled_count + failed_count} records")
            print("="*60)

            if failed_count > 0:
                logger.warning(
                    f"⚠️ {failed_count} records failed to cancel. Please check the logs above.")
            else:
                logger.info(
                    "✅ All attendance records have been successfully cancelled!")

        except Exception as e:
            logger.error(f"Fatal error during cancellation: {str(e)}")
            print(f"\n❌ Fatal error: {str(e)}")


if __name__ == "__main__":
    try:
        canceller = AttendanceCanceller()
        canceller.run()
    except KeyboardInterrupt:
        print("\n\n⚠️ Operation interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"\n❌ Fatal error: {str(e)}")
        sys.exit(1)

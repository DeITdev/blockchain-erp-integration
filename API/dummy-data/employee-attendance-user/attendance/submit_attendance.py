#!/usr/bin/env python3
"""
ERPNext Attendance Submission Script
Submits all draft attendance records after employee creation.
Uses environment variables from .env file for configuration.
Author: ERPNext Attendance Submitter
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


# Load environment variables
load_env_file()

# Configuration from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


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

        logger.info(f"🔗 API: {self.base_url}")
        logger.info(f"🏢 Company: {COMPANY_NAME}")
        logger.info(f"🔑 Key: {API_KEY[:8] if API_KEY else 'None'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
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
            "limit_page_length": 2000  # Increased to handle more records
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)

        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def submit_doc(self, doctype: str, name: str) -> Dict:
        """Submit a document by setting docstatus to 1"""
        return self._make_request("PUT", f"resource/{doctype}/{name}", {"docstatus": 1})


class AttendanceSubmitter:
    """Submits all draft attendance records"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.submitted_count = 0
        self.failed_count = 0
        self.failed_submissions = []

    def get_draft_attendance_records(self):
        """Get all draft attendance records (docstatus = 0)"""
        logger.info("📋 Fetching all draft attendance records...")

        try:
            # Get all draft attendance records for the company
            draft_attendance = self.api.get_list("Attendance",
                                                 filters={
                                                     "docstatus": 0,  # Draft status
                                                     "company": COMPANY_NAME
                                                 },
                                                 fields=["name", "employee", "attendance_date", "status"])

            logger.info(
                f"✅ Found {len(draft_attendance)} draft attendance records")
            return draft_attendance

        except Exception as e:
            logger.error(f"Error fetching draft attendance records: {str(e)}")
            return []

    def submit_attendance_records(self, attendance_records):
        """Submit all attendance records"""
        if not attendance_records:
            logger.info("No draft attendance records found to submit.")
            return

        total_records = len(attendance_records)
        logger.info(
            f"🚀 Starting submission of {total_records} attendance records...")

        print("\n" + "="*80)
        print("📝 SUBMITTING ATTENDANCE RECORDS")
        print("="*80)
        print(f"📊 Total Records to Submit: {total_records}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print("="*80)

        for i, attendance in enumerate(attendance_records):
            attendance_name = attendance.get("name")
            employee_name = attendance.get("employee", "Unknown")
            attendance_date = attendance.get("attendance_date", "Unknown")
            status = attendance.get("status", "Unknown")

            try:
                # Submit the attendance record
                self.api.submit_doc("Attendance", attendance_name)
                self.submitted_count += 1

                # Progress logging every 50 records
                if self.submitted_count % 50 == 0 or self.submitted_count == total_records:
                    progress_percent = (
                        self.submitted_count / total_records) * 100
                    print(
                        f"✅ Progress: {self.submitted_count}/{total_records} ({progress_percent:.1f}%) submitted")

                logger.debug(
                    f"✅ Submitted: {attendance_name} - {employee_name} - {attendance_date}")

                # Small delay to avoid overwhelming the server
                time.sleep(0.1)

            except requests.exceptions.HTTPError as e:
                self.failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)

                # Store failed submission details
                self.failed_submissions.append({
                    "name": attendance_name,
                    "employee": employee_name,
                    "date": attendance_date,
                    "status": status,
                    "error": error_msg
                })

                if e.response and e.response.status_code == 403:
                    logger.warning(
                        f"⚠️ Permission denied for {attendance_name}")
                elif e.response and e.response.status_code == 409:
                    logger.warning(
                        f"⚠️ Conflict for {attendance_name} (may already be submitted)")
                else:
                    logger.error(
                        f"❌ Failed to submit {attendance_name}: {error_msg}")

            except Exception as e:
                self.failed_count += 1
                error_msg = str(e)

                self.failed_submissions.append({
                    "name": attendance_name,
                    "employee": employee_name,
                    "date": attendance_date,
                    "status": status,
                    "error": error_msg
                })

                logger.error(
                    f"❌ Failed to submit {attendance_name}: {error_msg}")

        # Final summary
        print("\n" + "="*80)
        print("📊 SUBMISSION SUMMARY")
        print("="*80)
        print(f"✅ Successfully Submitted: {self.submitted_count}")
        print(f"❌ Failed: {self.failed_count}")
        print(
            f"📈 Success Rate: {(self.submitted_count / total_records * 100):.1f}%")

        if self.failed_submissions:
            print(f"\n❌ Failed Submissions ({len(self.failed_submissions)}):")
            # Show first 10 failures
            for i, failure in enumerate(self.failed_submissions[:10]):
                print(
                    f"   {i+1}. {failure['name']} - {failure['employee']} - {failure['date']}")
                print(f"      Error: {failure['error']}")

            if len(self.failed_submissions) > 10:
                print(
                    f"   ... and {len(self.failed_submissions) - 10} more failures")

        print("="*80)

        logger.info(
            f"Submission completed: {self.submitted_count} submitted, {self.failed_count} failed")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("📝 ERPNext Attendance Records Submitter")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🎯 Action: Submit all draft attendance records")
        print("=" * 80)

        try:
            # Step 1: Get all draft attendance records
            draft_records = self.get_draft_attendance_records()

            if not draft_records:
                print(
                    "✅ No draft attendance records found. All records may already be submitted.")
                return

            # Step 2: Confirm submission
            response = input(
                f"\nFound {len(draft_records)} draft attendance records. Submit all? (yes/no): ")
            if response.lower() != 'yes':
                print("Operation cancelled.")
                return

            # Step 3: Submit all records
            self.submit_attendance_records(draft_records)

            print(f"\n🎉 ATTENDANCE SUBMISSION COMPLETED!")
            if self.submitted_count > 0:
                print(
                    f"✅ {self.submitted_count} attendance records have been submitted successfully!")
            if self.failed_count > 0:
                print(
                    f"⚠️ {self.failed_count} records failed to submit - check logs for details")

        except Exception as e:
            logger.error(f"Fatal error during attendance submission: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Attendance Records Submission...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"\n📝 This script will submit ALL draft attendance records")
    print(f"🏢 Company: {COMPANY_NAME}")
    print(f"⚠️ This action will change docstatus from 0 (Draft) to 1 (Submitted)")

    try:
        submitter = AttendanceSubmitter()
        submitter.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

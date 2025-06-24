#!/usr/bin/env python3
"""
ERPNext Shift Assignment Submission Script
Submits all draft shift assignment records.
Uses environment variables from .env file for configuration.
Author: ERPNext Shift Assignment Submitter
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


class ShiftAssignmentSubmitter:
    """Submits all draft shift assignment records"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.submitted_count = 0
        self.failed_count = 0
        self.failed_submissions = []

    def get_draft_shift_assignments(self):
        """Get all draft shift assignment records (docstatus = 0)"""
        logger.info("📋 Fetching all draft shift assignment records...")

        try:
            # Get all draft shift assignment records for the company
            draft_assignments = self.api.get_list("Shift Assignment",
                                                  filters={
                                                      "docstatus": 0,  # Draft status
                                                      "company": COMPANY_NAME
                                                  },
                                                  fields=["name", "employee", "shift_type", "shift_location", "status", "start_date", "end_date"])

            logger.info(
                f"✅ Found {len(draft_assignments)} draft shift assignment records")
            return draft_assignments

        except Exception as e:
            logger.error(
                f"Error fetching draft shift assignment records: {str(e)}")
            return []

    def submit_shift_assignments(self, shift_assignments):
        """Submit all shift assignment records"""
        if not shift_assignments:
            logger.info("No draft shift assignment records found to submit.")
            return

        total_records = len(shift_assignments)
        logger.info(
            f"🚀 Starting submission of {total_records} shift assignment records...")

        print("\n" + "="*80)
        print("📝 SUBMITTING SHIFT ASSIGNMENT RECORDS")
        print("="*80)
        print(f"📊 Total Records to Submit: {total_records}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print("="*80)

        for i, assignment in enumerate(shift_assignments):
            assignment_name = assignment.get("name")
            employee_name = assignment.get("employee", "Unknown")
            shift_type = assignment.get("shift_type", "Unknown")
            shift_location = assignment.get("shift_location", "Unknown")
            status = assignment.get("status", "Unknown")
            start_date = assignment.get("start_date", "Unknown")
            end_date = assignment.get("end_date", "Unknown")

            try:
                # Submit the shift assignment record
                self.api.submit_doc("Shift Assignment", assignment_name)
                self.submitted_count += 1

                # Progress logging every 25 records
                if self.submitted_count % 25 == 0 or self.submitted_count == total_records:
                    progress_percent = (
                        self.submitted_count / total_records) * 100
                    print(
                        f"✅ Progress: {self.submitted_count}/{total_records} ({progress_percent:.1f}%) submitted")

                status_icon = "🟢" if status == "Active" else "🔴"
                logger.debug(
                    f"✅ Submitted: {assignment_name} - {employee_name} - {shift_type} ({status})")

                # Small delay to avoid overwhelming the server
                time.sleep(0.1)

            except requests.exceptions.HTTPError as e:
                self.failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)

                # Store failed submission details
                self.failed_submissions.append({
                    "name": assignment_name,
                    "employee": employee_name,
                    "shift_type": shift_type,
                    "shift_location": shift_location,
                    "status": status,
                    "start_date": start_date,
                    "end_date": end_date,
                    "error": error_msg
                })

                if e.response and e.response.status_code == 403:
                    logger.warning(
                        f"⚠️ Permission denied for {assignment_name}")
                elif e.response and e.response.status_code == 409:
                    logger.warning(
                        f"⚠️ Conflict for {assignment_name} (may already be submitted)")
                else:
                    logger.error(
                        f"❌ Failed to submit {assignment_name}: {error_msg}")

            except Exception as e:
                self.failed_count += 1
                error_msg = str(e)

                self.failed_submissions.append({
                    "name": assignment_name,
                    "employee": employee_name,
                    "shift_type": shift_type,
                    "shift_location": shift_location,
                    "status": status,
                    "start_date": start_date,
                    "end_date": end_date,
                    "error": error_msg
                })

                logger.error(
                    f"❌ Failed to submit {assignment_name}: {error_msg}")

        # Final summary
        print("\n" + "="*80)
        print("📊 SHIFT ASSIGNMENT SUBMISSION SUMMARY")
        print("="*80)
        print(f"✅ Successfully Submitted: {self.submitted_count}")
        print(f"❌ Failed: {self.failed_count}")
        print(
            f"📈 Success Rate: {(self.submitted_count / total_records * 100):.1f}%")

        if self.failed_submissions:
            print(f"\n❌ Failed Submissions ({len(self.failed_submissions)}):")
            # Show first 10 failures
            for i, failure in enumerate(self.failed_submissions[:10]):
                print(f"   {i+1}. {failure['name']} - {failure['employee']}")
                print(
                    f"      Shift: {failure['shift_type']} at {failure['shift_location']}")
                print(
                    f"      Status: {failure['status']} | {failure['start_date']} → {failure['end_date']}")
                print(f"      Error: {failure['error']}")
                print()

            if len(self.failed_submissions) > 10:
                print(
                    f"   ... and {len(self.failed_submissions) - 10} more failures")

        print("="*80)

        logger.info(
            f"Submission completed: {self.submitted_count} submitted, {self.failed_count} failed")

    def get_status_summary(self):
        """Get summary of shift assignment statuses after submission"""
        try:
            all_assignments = self.api.get_list("Shift Assignment",
                                                filters={
                                                    "company": COMPANY_NAME},
                                                fields=["name", "status", "docstatus"])

            draft_count = 0
            submitted_count = 0
            active_count = 0
            inactive_count = 0

            for assignment in all_assignments:
                docstatus = assignment.get("docstatus", 0)
                status = assignment.get("status", "")

                if docstatus == 0:
                    draft_count += 1
                elif docstatus == 1:
                    submitted_count += 1

                if status == "Active":
                    active_count += 1
                elif status == "Inactive":
                    inactive_count += 1

            print(f"\n📊 FINAL STATUS SUMMARY")
            print("="*50)
            print(f"📋 Document Status:")
            print(f"   📝 Draft (docstatus=0): {draft_count}")
            print(f"   ✅ Submitted (docstatus=1): {submitted_count}")
            print(f"📊 Assignment Status:")
            print(f"   🟢 Active: {active_count}")
            print(f"   🔴 Inactive: {inactive_count}")
            print(f"📈 Total: {len(all_assignments)} shift assignments")
            print("="*50)

        except Exception as e:
            logger.error(f"Error getting status summary: {e}")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("📝 ERPNext Shift Assignment Records Submitter")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🎯 Action: Submit all draft shift assignment records")
        print("=" * 80)

        try:
            # Step 1: Get all draft shift assignment records
            draft_records = self.get_draft_shift_assignments()

            if not draft_records:
                print(
                    "✅ No draft shift assignment records found. All records may already be submitted.")
                self.get_status_summary()
                return

            # Show sample of what will be submitted
            print(f"\n📋 Sample of records to be submitted:")
            for i, record in enumerate(draft_records[:5]):
                employee = record.get("employee", "Unknown")
                shift_type = record.get("shift_type", "Unknown")
                shift_location = record.get("shift_location", "Unknown")
                status = record.get("status", "Unknown")
                start_date = record.get("start_date", "Unknown")
                end_date = record.get("end_date", "Unknown")
                status_icon = "🟢" if status == "Active" else "🔴"

                print(f"   {i+1}. {employee} - {shift_type} at {shift_location}")
                print(
                    f"      📅 {start_date} → {end_date} {status_icon} {status}")

            if len(draft_records) > 5:
                print(f"   ... and {len(draft_records) - 5} more records")

            # Step 2: Confirm submission
            response = input(
                f"\nFound {len(draft_records)} draft shift assignment records. Submit all? (yes/no): ")
            if response.lower() != 'yes':
                print("Operation cancelled.")
                return

            # Step 3: Submit all records
            self.submit_shift_assignments(draft_records)

            # Step 4: Show final status summary
            self.get_status_summary()

            print(f"\n🎉 SHIFT ASSIGNMENT SUBMISSION COMPLETED!")
            if self.submitted_count > 0:
                print(
                    f"✅ {self.submitted_count} shift assignment records have been submitted successfully!")
            if self.failed_count > 0:
                print(
                    f"⚠️ {self.failed_count} records failed to submit - check logs for details")

        except Exception as e:
            logger.error(
                f"Fatal error during shift assignment submission: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Shift Assignment Records Submission...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        return

    print(f"\n📝 This script will submit ALL draft shift assignment records")
    print(f"🏢 Company: {COMPANY_NAME}")
    print(f"⚠️ This action will change docstatus from 0 (Draft) to 1 (Submitted)")
    print(f"🔄 This makes the shift assignments official and active")

    try:
        submitter = ShiftAssignmentSubmitter()
        submitter.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

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
    format='%(message)s',
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

        logger.info(f"Connecting to {self.base_url}")

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
        logger.info("Fetching draft shift assignment records...")

        try:
            draft_assignments = self.api.get_list("Shift Assignment",
                                                  filters={
                                                      "docstatus": 0,
                                                      "company": COMPANY_NAME
                                                  },
                                                  fields=["name", "employee", "shift_type", "shift_location", "status", "start_date", "end_date"])

            logger.info(f"Found {len(draft_assignments)} draft records")
            return draft_assignments

        except Exception as e:
            logger.error(f"Error fetching records: {str(e)}")
            return []

    def submit_shift_assignments(self, shift_assignments):
        """Submit all shift assignment records"""
        if not shift_assignments:
            logger.info("No records to submit")
            return

        total_records = len(shift_assignments)
        logger.info(f"Submitting {total_records} records...")

        for i, assignment in enumerate(shift_assignments):
            assignment_name = assignment.get("name")
            employee_name = assignment.get("employee", "Unknown")

            try:
                self.api.submit_doc("Shift Assignment", assignment_name)
                self.submitted_count += 1
                logger.info(
                    f"[{i+1}/{total_records}] Submitted: {assignment_name}")
                time.sleep(0.05)

            except requests.exceptions.HTTPError as e:
                self.failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)
                self.failed_submissions.append({
                    "name": assignment_name,
                    "employee": employee_name,
                    "error": error_msg
                })
                logger.error(
                    f"[{i+1}/{total_records}] Failed: {assignment_name} - {error_msg}")

            except Exception as e:
                self.failed_count += 1
                self.failed_submissions.append({
                    "name": assignment_name,
                    "employee": employee_name,
                    "error": str(e)
                })
                logger.error(
                    f"[{i+1}/{total_records}] Failed: {assignment_name}")

    def get_status_summary(self):
        """Get summary of shift assignment statuses after submission"""
        try:
            all_assignments = self.api.get_list("Shift Assignment",
                                                filters={
                                                    "company": COMPANY_NAME},
                                                fields=["name", "status", "docstatus"])

            draft_count = sum(
                1 for a in all_assignments if a.get("docstatus", 0) == 0)
            submitted_count = sum(
                1 for a in all_assignments if a.get("docstatus", 0) == 1)

            print(f"\nStatus Summary:")
            print(
                f"Draft: {draft_count}, Submitted: {submitted_count}, Total: {len(all_assignments)}")

        except Exception as e:
            logger.error(f"Error getting status: {e}")

    def run(self):
        """Main execution method"""
        print("Shift Assignment Submission")
        print("="*60)

        try:
            draft_records = self.get_draft_shift_assignments()

            if not draft_records:
                print("No draft records found.")
                return

            print(f"\nFound {len(draft_records)} draft records")
            response = input("Submit all? (yes/no): ")
            if response.lower() != 'yes':
                print("Cancelled.")
                return

            self.submit_shift_assignments(draft_records)
            self.get_status_summary()

            print(
                f"\nCompleted: {self.submitted_count} submitted, {self.failed_count} failed")

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            print(f"Error: {e}")


def main():
    """Main entry point"""
    print("Starting Shift Assignment Submission...")

    if not API_KEY or not API_SECRET:
        print("Error: API_KEY and API_SECRET must be set in .env file")
        return

    try:
        submitter = ShiftAssignmentSubmitter()
        submitter.run()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()

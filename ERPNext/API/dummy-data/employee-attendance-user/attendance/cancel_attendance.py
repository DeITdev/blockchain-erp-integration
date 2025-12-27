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
    format='%(message)s',
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

        logger.info(f"API: {self.base_url}")
        logger.info(f"Company: {COMPANY}")
        logger.info(f"Key: {API_KEY[:8] if API_KEY else 'None'}...")

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
        """Get list of documents with pagination to fetch all records"""
        all_data = []
        page_length = 500
        page_start = 0

        while True:
            params = {
                "limit_page_length": page_length,
                "limit_start": page_start
            }
            if filters:
                params["filters"] = json.dumps(filters)
            if fields:
                params["fields"] = json.dumps(fields)

            response = self._make_request(
                "GET", "resource/" + doctype, params).get("data", [])

            if not response:
                break

            all_data.extend(response)

            if len(response) < page_length:
                break

            page_start += page_length

        return all_data

    def cancel_doc(self, doctype: str, name: str) -> Dict:
        """Cancel a document (set docstatus to 2)"""
        data = {
            "docstatus": 2  # 2 = Cancelled in ERPNext
        }
        return self._make_request("PUT", f"resource/{doctype}/{name}", data)

    def get_all_submitted_attendance_records(self):
        """Fetch all submitted attendance records"""
        logger.info("Fetching submitted attendance records...")

        try:
            attendance_records = self.get_list("Attendance",
                                               filters={
                                                   "company": COMPANY, "docstatus": 1},
                                               fields=["name", "employee", "employee_name", "attendance_date", "status", "docstatus"])

            logger.info(f"Found {len(attendance_records)} submitted records")
            return attendance_records

        except Exception as e:
            logger.error(f"Error fetching records: {str(e)}")
            return []

    def confirm_cancellation(self, attendance_records):
        """Ask for user confirmation before cancellation"""
        if not attendance_records:
            print("No submitted records found.")
            return False

        print(
            f"\nWARNING: This will CANCEL ALL {len(attendance_records)} submitted records")
        print(f"Company: {COMPANY}")
        response = input("Type 'CANCEL ALL' to confirm: ")

        return response == "CANCEL ALL"

    def cancel_attendance_records(self, records_to_cancel):
        """Cancel all submitted attendance records"""
        logger.info(f"Cancelling {len(records_to_cancel)} records...")

        for i, record in enumerate(records_to_cancel, 1):
            try:
                record_name = record.get("name")
                employee = record.get("employee", "Unknown")
                employee_name = record.get("employee_name", "Unknown")
                attendance_date = record.get("attendance_date", "Unknown")

                self.cancel_doc("Attendance", record_name)
                self.cancelled_count += 1
                progress_pct = (i / len(records_to_cancel)) * 100
                logger.info(
                    f"[{i}/{len(records_to_cancel)}] ({progress_pct:.0f}%) Cancelled: {employee_name} - {attendance_date}")

            except Exception as e:
                self.failed_count += 1
                logger.error(
                    f"Failed to cancel {record.get('name')}: {str(e)}")

        return self.cancelled_count, self.failed_count

    def run(self):
        """Main execution method"""
        print("=== ERPNext Attendance Cancellation ===")
        print(f"Endpoint: {BASE_URL}")
        print(f"Company: {COMPANY}")
        print()

        try:
            attendance_records = self.get_all_submitted_attendance_records()

            if not self.confirm_cancellation(attendance_records):
                print("Operation cancelled")
                return

            cancelled_count, failed_count = self.cancel_attendance_records(
                attendance_records)

            print("\n=== Summary ===")
            print(f"Cancelled: {cancelled_count}")
            print(f"Failed: {failed_count}")

        except Exception as e:
            logger.error(f"Error: {str(e)}")


if __name__ == "__main__":
    try:
        if not API_KEY or not API_SECRET:
            print("Error: API_KEY and API_SECRET required in .env")
            sys.exit(1)

        canceller = AttendanceCanceller()
        canceller.run()
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

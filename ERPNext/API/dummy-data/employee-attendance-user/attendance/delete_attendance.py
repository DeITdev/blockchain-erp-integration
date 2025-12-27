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
    format='%(message)s',
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

        logger.info(f"Connecting to {self.base_url}")

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
        """Get list of documents with pagination to fetch all records"""
        all_data = []
        page_length = 500  # Fetch in chunks of 500 to be safe
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
            logger.debug(
                f"Fetched {len(response)} records. Total so far: {len(all_data)}")

            # If we got fewer records than requested, we've reached the end
            if len(response) < page_length:
                break

            page_start += page_length

        return all_data

    def delete_doc(self, doctype: str, name: str) -> Dict:
        """Delete a document"""
        return self._make_request("DELETE", f"resource/{doctype}/{name}")

    def get_all_attendance_records(self):
        """Fetch all attendance records with Draft and Cancelled status"""
        logger.info("Fetching attendance records...")

        try:
            attendance_records = self.get_list("Attendance",
                                               filters=[["Attendance", "company", "=", COMPANY], [
                                                   "Attendance", "docstatus", "in", [0, 2]]],
                                               fields=["name", "employee", "attendance_date", "status", "docstatus"])

            logger.info(
                f"Found {len(attendance_records)} draft/cancelled records")
            return attendance_records

        except Exception as e:
            logger.error(f"Error fetching records: {str(e)}")
            return []

    def confirm_deletion(self, attendance_records):
        """Ask for user confirmation before deletion"""
        if not attendance_records:
            print("No records found to delete.")
            return False

        print(f"\nWARNING: This will delete {len(attendance_records)} records")
        response = input("Type 'DELETE ALL' to confirm: ")
        return response == "DELETE ALL"

    def delete_attendance_records(self, records_to_delete):
        """Delete all attendance records"""
        print("\n" + "="*60)
        print("Deleting Records")
        print("="*60)

        logger.info(
            f"Starting deletion of {len(records_to_delete)} records...")

        for i, record in enumerate(records_to_delete):
            try:
                record_name = record.get("name")
                employee = record.get("employee", "Unknown")

                self.delete_doc("Attendance", record_name)
                self.deleted_count += 1
                logger.info(
                    f"[{i+1}/{len(records_to_delete)}] Deleted: {record_name}")
                time.sleep(0.05)

            except Exception as e:
                self.failed_count += 1
                logger.error(
                    f"[{i+1}/{len(records_to_delete)}] Failed: {record.get('name', 'Unknown')}")

        return self.deleted_count, self.failed_count

    def run(self):
        """Main execution method"""
        print("="*80)
        print("Delete Attendance Records")
        print("="*80)

        try:
            attendance_records = self.get_all_attendance_records()

            if not self.confirm_deletion(attendance_records):
                print("Cancelled.")
                return

            deleted_count, failed_count = self.delete_attendance_records(
                attendance_records)

            print("\n" + "="*60)
            print("Summary")
            print("="*60)
            print(f"Deleted: {deleted_count}")
            print(f"Failed: {failed_count}")
            print(f"Total: {len(attendance_records)}")

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            print(f"Error: {e}")


def main():
    """Main entry point"""
    print("Starting Attendance Deletion...")

    if not API_KEY or not API_SECRET:
        print("Error: API_KEY and API_SECRET must be set in .env file")
        return

    try:
        deleter = AttendanceDeleter()
        deleter.run()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()

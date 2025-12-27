#!/usr/bin/env python3
"""
ERPNext Shift Assignment Deletion Script
Deletes all shift assignments with draft and cancelled status.
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional
import sys


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


load_env_file()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY = "PT Fiyansa Mulya"

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class ShiftAssignmentDeleter:
    """Deletes shift assignments with draft and cancelled status"""

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

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT", "DELETE"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            if method == "DELETE":
                return {"success": True}
            else:
                return response.json()
        except requests.exceptions.RequestException as e:
            if retry_count < 3:
                time.sleep(2)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        """Get list of documents with pagination"""
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

    def delete_doc(self, doctype: str, name: str) -> Dict:
        """Delete a document"""
        return self._make_request("DELETE", f"resource/{doctype}/{name}")

    def get_shift_assignments(self):
        """Fetch shift assignments with draft and cancelled status"""
        logger.info(
            "Fetching shift assignments with draft and cancelled status...")
        try:
            shift_assignments = self.get_list("Shift Assignment",
                                              filters=[["Shift Assignment", "company", "=", COMPANY],
                                                       ["Shift Assignment", "docstatus", "in", [0, 2]]],
                                              fields=["name", "employee", "start_date"])
            logger.info(f"Found {len(shift_assignments)} records")
            return shift_assignments
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return []

    def delete_shift_assignments(self, records_to_delete):
        """Delete all shift assignments"""
        logger.info(f"Deleting {len(records_to_delete)} records...")

        for i, record in enumerate(records_to_delete, 1):
            try:
                record_name = record.get("name")
                employee = record.get("employee", "Unknown")
                self.delete_doc("Shift Assignment", record_name)
                self.deleted_count += 1
                logger.info(
                    f"[{i}/{len(records_to_delete)}] Deleted: {record_name} ({employee})")

            except Exception as e:
                self.failed_count += 1
                logger.error(f"Failed: {record.get('name')}: {str(e)}")

        return self.deleted_count, self.failed_count

    def run(self):
        """Main execution"""
        shift_assignments = self.get_shift_assignments()

        if not shift_assignments:
            print("No records found")
            return

        print(
            f"\nWARNING: This will DELETE ALL {len(shift_assignments)} records")
        response = input("Type 'DELETE ALL' to confirm: ")

        if response != "DELETE ALL":
            print("Operation cancelled")
            return

        deleted_count, failed_count = self.delete_shift_assignments(
            shift_assignments)

        print(f"\nDeleted: {deleted_count}")
        print(f"Failed: {failed_count}")


if __name__ == "__main__":
    try:
        if not API_KEY or not API_SECRET:
            print("Error: API_KEY and API_SECRET required in .env")
            sys.exit(1)

        deleter = ShiftAssignmentDeleter()
        deleter.run()
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

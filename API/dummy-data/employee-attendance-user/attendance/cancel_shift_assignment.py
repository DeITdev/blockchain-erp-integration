#!/usr/bin/env python3
"""
ERPNext Shift Assignment Cancellation Script
Cancels all submitted shift assignments.
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


class ShiftAssignmentCanceller:
    """Cancels submitted shift assignments"""

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

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT", "DELETE"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            if method in ["DELETE", "PUT"]:
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

    def cancel_doc(self, doctype: str, name: str) -> Dict:
        """Cancel a document"""
        data = {"docstatus": 2}
        return self._make_request("PUT", f"resource/{doctype}/{name}", data)

    def get_submitted_shift_assignments(self):
        """Fetch all submitted shift assignments"""
        logger.info("Fetching submitted shift assignments...")
        try:
            shift_assignments = self.get_list("Shift Assignment",
                                              filters=[["Shift Assignment", "company", "=", COMPANY],
                                                       ["Shift Assignment", "docstatus", "=", 1]],
                                              fields=["name", "employee", "start_date"])
            logger.info(f"Found {len(shift_assignments)} records")
            return shift_assignments
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return []

    def cancel_shift_assignments(self, records_to_cancel):
        """Cancel all submitted shift assignments"""
        logger.info(f"Cancelling {len(records_to_cancel)} records...")

        for i, record in enumerate(records_to_cancel, 1):
            try:
                record_name = record.get("name")
                employee = record.get("employee", "Unknown")
                self.cancel_doc("Shift Assignment", record_name)
                self.cancelled_count += 1
                logger.info(
                    f"[{i}/{len(records_to_cancel)}] Cancelled: {record_name} ({employee})")

            except Exception as e:
                self.failed_count += 1
                logger.error(f"Failed: {record.get('name')}: {str(e)}")

        return self.cancelled_count, self.failed_count

    def run(self):
        """Main execution"""
        shift_assignments = self.get_submitted_shift_assignments()

        if not shift_assignments:
            print("No records found")
            return

        print(
            f"\nWARNING: This will CANCEL ALL {len(shift_assignments)} submitted records")
        response = input("Type 'CANCEL ALL' to confirm: ")

        if response != "CANCEL ALL":
            print("Operation cancelled")
            return

        cancelled_count, failed_count = self.cancel_shift_assignments(
            shift_assignments)

        print(f"\nCancelled: {cancelled_count}")
        print(f"Failed: {failed_count}")


if __name__ == "__main__":
    try:
        if not API_KEY or not API_SECRET:
            print("Error: API_KEY and API_SECRET required in .env")
            sys.exit(1)

        canceller = ShiftAssignmentCanceller()
        canceller.run()
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

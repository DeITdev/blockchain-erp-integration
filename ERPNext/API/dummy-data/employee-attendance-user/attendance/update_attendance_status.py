#!/usr/bin/env python3
"""
ERPNext Attendance Status Updater
Updates attendance status for draft or cancelled records.
Uses environment variables from .env.local file for configuration.
"""

import requests
import json
import logging
import random
import time
import os
from pathlib import Path
from typing import Dict, List, Optional
import sys


def load_env_file():
    """Load environment variables from .env.local file"""
    env_path = Path(__file__).parent.parent.parent.parent / '.env.local'
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print(f"✅ Loaded environment variables from {env_path}")
    else:
        print(f"⚠️ .env.local file not found at {env_path}")


load_env_file()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")

COMPANY = "PT Fiyansa Mulya"

# All possible attendance statuses
ATTENDANCE_STATUSES = ["Present", "Absent", "On Leave", "Half Day", "Work From Home"]

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class ERPNextAPI:
    """API client for ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

    def request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry: int = 0) -> Dict:
        try:
            url = f"{self.base_url}/api/{endpoint}"
            response = self.session.request(
                method, url,
                json=data if method in ["POST", "PUT"] else None,
                params=data if method == "GET" else None
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if retry < 3:
                time.sleep(1)
                return self.request(method, endpoint, data, retry + 1)
            raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None, limit: int = 500) -> List[Dict]:
        params = {"limit_page_length": limit}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self.request("GET", f"resource/{doctype}", params).get("data", [])

    def update_doc(self, doctype: str, name: str, data: Dict) -> Dict:
        """Update document fields"""
        return self.request("PUT", f"resource/{doctype}/{name}", data)


class AttendanceStatusUpdater:
    """Updates attendance status for draft or cancelled records"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.updated_count = 0
        self.failed_count = 0
        self.start_time = None

    def get_editable_attendance(self) -> List[Dict]:
        """Fetch draft (docstatus=0) or cancelled (docstatus=2) attendance records"""
        logger.info("Fetching draft and cancelled attendance records...")
        
        # Get draft records (docstatus=0)
        draft_records = self.api.get_list(
            "Attendance",
            filters={"company": COMPANY, "docstatus": 0},
            fields=["name", "employee", "employee_name", "attendance_date", "status", "docstatus"]
        )
        
        # Get cancelled records (docstatus=2)
        cancelled_records = self.api.get_list(
            "Attendance",
            filters={"company": COMPANY, "docstatus": 2},
            fields=["name", "employee", "employee_name", "attendance_date", "status", "docstatus"]
        )
        
        all_records = draft_records + cancelled_records
        
        logger.info(f"Found {len(draft_records)} draft, {len(cancelled_records)} cancelled records")
        logger.info(f"Total editable: {len(all_records)} records for {COMPANY}")
        
        return all_records

    def get_new_status(self, current_status: str) -> str:
        """Get a random status different from current"""
        available = [s for s in ATTENDANCE_STATUSES if s != current_status]
        return random.choice(available)

    def update_attendance(self, records_to_update: List[Dict]):
        """Update attendance status directly"""
        total = len(records_to_update)
        logger.info(f"\nUpdating {total} attendance record(s)...")
        logger.info("-" * 60)

        for i, record in enumerate(records_to_update, 1):
            record_name = record.get("name")
            employee_name = record.get("employee_name", "Unknown")
            attendance_date = record.get("attendance_date", "Unknown")
            current_status = record.get("status", "Unknown")
            docstatus = record.get("docstatus", 0)
            status_label = "Draft" if docstatus == 0 else "Cancelled"
            new_status = self.get_new_status(current_status)
            progress_pct = (i / total) * 100

            try:
                # Start timer on first API call
                if self.start_time is None:
                    self.start_time = time.time()
                    logger.info("[TIMER] Started\n")

                # Update the status directly
                self.api.update_doc("Attendance", record_name, {"status": new_status})

                self.updated_count += 1
                logger.info(
                    f"[{i}/{total}] ({progress_pct:.0f}%) [{status_label}] {employee_name} - {attendance_date}: "
                    f"{current_status} -> {new_status}"
                )

            except Exception as e:
                self.failed_count += 1
                logger.error(f"[{i}/{total}] Failed {record_name}: {str(e)[:80]}")

        logger.info("-" * 60)

    def run(self, records_to_update: List[Dict]):
        """Main execution"""
        self.update_attendance(records_to_update)

        print("\n=== Summary ===")
        print(f"Updated: {self.updated_count}")
        print(f"Failed: {self.failed_count}")
        if self.start_time:
            elapsed = time.time() - self.start_time
            print(f"Elapsed Time: {elapsed:.2f} seconds")
        print(f"\nThis should generate {self.updated_count} CDC event(s)")


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env.local")
        return

    print("=== ERPNext Attendance Status Updater ===")
    print(f"Endpoint: {BASE_URL}")
    print(f"Company: {COMPANY}")
    print()

    try:
        updater = AttendanceStatusUpdater()
        records = updater.get_editable_attendance()

        if not records:
            print("\n⚠️  No draft or cancelled attendance records found.")
            print("    (Draft: docstatus=0, Cancelled: docstatus=2)")
            print("    Try running generate_attendance.py or cancel_attendance.py first.")
            return

        # Ask user how many to update
        total = len(records)
        while True:
            try:
                count_input = input(f"How many records to update? (1-{total}, or 'all'): ").strip().lower()
                if count_input == 'all':
                    num_to_update = total
                    break
                num_to_update = int(count_input)
                if 1 <= num_to_update <= total:
                    break
                else:
                    print(f"Please enter a number between 1 and {total}")
            except ValueError:
                print("Invalid input. Enter a number or 'all'")

        # Select random records to update
        records_to_update = random.sample(records, num_to_update)

        print(f"\nThis will update the status of {num_to_update} record(s).")
        confirm = input("Type 'UPDATE' to confirm: ")

        if confirm != 'UPDATE':
            logger.info("Cancelled")
            return

        updater.run(records_to_update)

    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

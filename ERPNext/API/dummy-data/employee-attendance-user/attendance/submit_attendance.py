#!/usr/bin/env python3
"""ERPNext Attendance Submission Script - Minimalist Version"""

import requests
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional
import sys


def load_env():
    env_path = Path(__file__).parent.parent.parent.parent / '.env.local'
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value


load_env()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")
COMPANY = "PT Fiyansa Mulya"  # Default company

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class API:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Content-Type': 'application/json'
        })

    def request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry: int = 0) -> Dict:
        try:
            url = f"{BASE_URL}/api/{endpoint}"
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if retry < 3:
                return self.request(method, endpoint, data, retry + 1)
            raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None) -> List[Dict]:
        params = {"limit_page_length": 500}
        if filters:
            params["filters"] = json.dumps(filters)
        return self.request("GET", f"resource/{doctype}", params).get("data", [])

    def get_doc(self, doctype: str, name: str) -> Dict:
        return self.request("GET", f"resource/{doctype}/{name}")

    def submit(self, doctype: str, name: str):
        # First, fetch the latest version to ensure we have current timestamp
        doc = self.get_doc(doctype, name)
        data = {"doctype": doctype, "docstatus": 1}
        return self.request("PUT", f"resource/{doctype}/{name}", data)


class AttendanceSubmitter:
    def __init__(self):
        self.api = API()
        self.submitted = 0
        self.failed = 0
        self.start_time = None

    def get_draft_records(self):
        """Fetch draft attendance records"""
        logger.info("Fetching draft attendance records...")
        attendance_list = self.api.get_list(
            "Attendance", {"docstatus": 0, "company": COMPANY})
        logger.info(
            f"Found {len(attendance_list)} draft records for {COMPANY}")
        return attendance_list

    def submit_attendance(self, records_to_submit):
        """Submit specified attendance records"""
        try:
            for i, record in enumerate(records_to_submit):
                att_name = record.get("name")
                emp = record.get("employee", "Unknown")
                date = record.get("attendance_date", "Unknown")
                progress_pct = ((i + 1) / len(records_to_submit)) * 100

                try:
                    # Start timer on first API call
                    if self.start_time is None:
                        self.start_time = time.time()
                        logger.info("[TIMER] Started")
                    self.api.submit("Attendance", att_name)
                    self.submitted += 1
                    logger.info(
                        f"[{i+1}/{len(records_to_submit)}] ({progress_pct:.0f}%) Submitted: {emp} - {date}")
                except Exception as e:
                    error_msg = str(e)
                    if "TimestampMismatchError" in error_msg or "has been modified" in error_msg:
                        logger.warning(
                            f"Timestamp mismatch for {att_name}, retrying in 1s...")
                        time.sleep(1)
                        try:
                            self.api.submit("Attendance", att_name)
                            self.submitted += 1
                            logger.info(
                                f"[{i+1}/{len(records_to_submit)}] ({progress_pct:.0f}%) Submitted (retry): {emp} - {date}")
                        except Exception as retry_e:
                            self.failed += 1
                            logger.error(
                                f"Failed {att_name}: {str(retry_e)[:80]}")
                    else:
                        self.failed += 1
                        logger.error(f"Failed {att_name}: {error_msg[:80]}")

            elapsed_str = ""
            if self.start_time:
                elapsed = time.time() - self.start_time
                elapsed_str = f", Elapsed: {elapsed:.2f}s"
            logger.info(
                f"Summary: Submitted {self.submitted}, Failed {self.failed}{elapsed_str}")
        except Exception as e:
            logger.error(f"Error: {e}")

    def run(self, records_to_submit):
        self.submit_attendance(records_to_submit)


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env")
        return

    try:
        submitter = AttendanceSubmitter()
        attendance_list = submitter.get_draft_records()

        if not attendance_list:
            print("\n⚠️  No draft records found to submit.")
            print("    (Draft records have docstatus=0)")
            print("    Try running generate_attendance.py first.")
            return

        # Ask user how many to submit
        total = len(attendance_list)
        while True:
            try:
                count_input = input(f"How many records to submit? (1-{total}, or 'all'): ").strip().lower()
                if count_input == 'all':
                    num_to_submit = total
                    break
                num_to_submit = int(count_input)
                if 1 <= num_to_submit <= total:
                    break
                else:
                    print(f"Please enter a number between 1 and {total}")
            except ValueError:
                print("Invalid input. Enter a number or 'all'")

        # Select records to submit
        records_to_submit = attendance_list[:num_to_submit]

        confirm = input(f"Type 'SUBMIT' to confirm submitting {num_to_submit} record(s): ")
        if confirm != 'SUBMIT':
            logger.info("Cancelled")
            return

        submitter.run(records_to_submit)
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

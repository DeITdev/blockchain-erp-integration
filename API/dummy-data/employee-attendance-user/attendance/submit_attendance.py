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
    env_path = Path(__file__).parent.parent.parent / '.env'
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

    def submit_attendance(self):
        logger.info("Fetching draft attendance records...")
        try:
            # Filter by company to avoid submitting records from other companies
            attendance_list = self.api.get_list(
                "Attendance", {"docstatus": 0, "company": COMPANY})
            logger.info(
                f"Found {len(attendance_list)} draft records for {COMPANY}")

            if not attendance_list:
                logger.info("No draft records to submit")
                return

            for i, record in enumerate(attendance_list):
                att_name = record.get("name")
                emp = record.get("employee", "Unknown")
                date = record.get("attendance_date", "Unknown")
                progress_pct = ((i + 1) / len(attendance_list)) * 100

                try:
                    self.api.submit("Attendance", att_name)
                    self.submitted += 1
                    logger.info(
                        f"[{i+1}/{len(attendance_list)}] ({progress_pct:.0f}%) Submitted: {emp} - {date}")
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
                                f"[{i+1}/{len(attendance_list)}] ({progress_pct:.0f}%) Submitted (retry): {emp} - {date}")
                        except Exception as retry_e:
                            self.failed += 1
                            logger.error(
                                f"Failed {att_name}: {str(retry_e)[:80]}")
                    else:
                        self.failed += 1
                        logger.error(f"Failed {att_name}: {error_msg[:80]}")

            logger.info(
                f"Summary: Submitted {self.submitted}, Failed {self.failed}")
        except Exception as e:
            logger.error(f"Error: {e}")

    def run(self):
        self.submit_attendance()


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env")
        return

    confirm = input("Submit all draft attendance records? (yes/no): ")
    if confirm.lower() != 'yes':
        logger.info("Cancelled")
        return

    try:
        submitter = AttendanceSubmitter()
        submitter.run()
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

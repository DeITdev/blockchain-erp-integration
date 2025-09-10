#!/usr/bin/env python3
"""
ERPNext Attendance Deletion Script (Deletes All Records with Pagination)
Cancels and deletes all attendance records from ERPNext system.
Uses environment variables from .env file for configuration.
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional
import sys
from logging import StreamHandler


def load_env_file():
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value


load_env_file()


class Config:
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL")
    COMPANY_NAME = os.getenv("COMPANY_NAME")

    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2
    PAGE_LIMIT = 500  # ambil 500 per batch (aman untuk API)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

for handler in logger.handlers:
    if isinstance(handler, StreamHandler):
        handler.stream.reconfigure(encoding='utf-8', errors='replace')


class ERPNextAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Expect': ''  # fix untuk 417 error
        })
        self.base_url = Config.BASE_URL

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None,
                      retry_count: int = 0) -> Dict:
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(
                method,
                url,
                json=data if method in ["POST", "PUT"] else None,
                params=data if method == "GET" else None
            )
            response.raise_for_status()
            return response.json() if response.text else {}
        except requests.exceptions.RequestException:
            if retry_count < Config.RETRY_ATTEMPTS:
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None,
                 fields: Optional[List[str]] = None, start: int = 0, page_limit: int = 500) -> List[Dict]:
        params = {
            "limit_start": start,
            "limit_page_length": page_limit
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def cancel_doc(self, doctype: str, name: str) -> Dict:
        return self._make_request("PUT", f"resource/{doctype}/{name}", {"docstatus": 2})

    def delete_doc(self, doctype: str, name: str) -> Dict:
        return self._make_request("DELETE", f"resource/{doctype}/{name}")


class AttendanceDeletor:
    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_count = 0
        self.failed_count = 0

    def get_all_attendance(self) -> List[Dict]:
        """Fetch all attendance records using pagination"""
        all_records = []
        start = 0
        while True:
            records = self.api.get_list(
                "Attendance",
                filters={"company": Config.COMPANY_NAME},
                fields=["name", "employee_name",
                        "attendance_date", "docstatus"],
                start=start,
                page_limit=Config.PAGE_LIMIT
            )
            if not records:
                break
            all_records.extend(records)
            start += Config.PAGE_LIMIT
        return all_records

    def delete_attendance(self, attendance_records: List[Dict]):
        total = len(attendance_records)
        for idx, record in enumerate(attendance_records, 1):
            att_id = record.get("name")
            emp_name = record.get("employee_name", "Unknown")
            att_date = record.get("attendance_date", "Unknown")

            try:
                if record.get("docstatus") == 1:
                    self.api.cancel_doc("Attendance", att_id)
                self.api.delete_doc("Attendance", att_id)
                self.deleted_count += 1
                print(f"[{idx}/{total}] Deleted: {emp_name} on {att_date}")
            except Exception as e:
                self.failed_count += 1
                print(
                    f"[{idx}/{total}] Failed: {emp_name} on {att_date} ({str(e)[:50]})")

        return self.deleted_count, self.failed_count

    def run(self):
        records = self.get_all_attendance()
        if not records:
            print("Attendance Deleted: 0")
            print("Attendance Failed: 0")
            return

        deleted, failed = self.delete_attendance(records)

        print("\nSummary:")
        print("Attendance Deleted:", deleted)
        print("Attendance Failed:", failed)
        print("Total Processed:", len(records))


def main():
    print("ERPNext Attendance Deletion Script")
    print(f"API Endpoint: {Config.BASE_URL}")
    print(f"Company: {Config.COMPANY_NAME}")

    if not Config.API_KEY or not Config.API_SECRET:
        print("Error: API credentials not set in .env file")
        return

    response = input("Proceed with attendance deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = AttendanceDeletor()
        deletor.run()
        print("\nATTENDANCE DELETION COMPLETED!")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")


if __name__ == "__main__":
    main()

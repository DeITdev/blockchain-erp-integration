#!/usr/bin/env python3
"""
ERPNext Employee Deletion Script (Minimal Output with Progress)
Deletes all employee records from ERPNext system.
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
    COMPANY_ABBR = os.getenv("COMPANY_ABBR")

    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2


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
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(
                method,
                url,
                json=data if method in ["POST", "PUT", "DELETE"] else None,
                params=data if method == "GET" else None
            )
            response.raise_for_status()
            if method == "DELETE":
                return {"success": True}
            return response.json()
        except requests.exceptions.RequestException as e:
            if retry_count < Config.RETRY_ATTEMPTS:
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def delete_doc(self, doctype: str, name: str) -> Dict:
        return self._make_request("DELETE", f"resource/{doctype}/{name}")


class EmployeeDeletor:
    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_count = 0
        self.failed_count = 0
        self.related_data_deleted = {
            "attendance": 0, "employee_checkins": 0, "leave_applications": 0}

    def get_all_employees(self):
        try:
            all_employees = self.api.get_list(
                "Employee", fields=["name", "employee_name", "company"])
            return [emp for emp in all_employees if emp.get("company") == Config.COMPANY_NAME]
        except Exception:
            return []

    def delete_basic_related_data(self, employee_id: str):
        basic_doctypes = [
            ("Attendance", "attendance"),
            ("Employee Checkin", "employee_checkins"),
            ("Leave Application", "leave_applications")
        ]
        for doctype, key in basic_doctypes:
            try:
                records = self.api.get_list(
                    doctype, filters={"employee": employee_id}, fields=["name"])
                for record in records:
                    try:
                        self.api.delete_doc(doctype, record["name"])
                        self.related_data_deleted[key] += 1
                    except Exception:
                        pass
            except Exception:
                pass

    def delete_employees(self, employees_to_delete):
        for idx, emp in enumerate(employees_to_delete, 1):
            emp_id = emp.get("name")
            emp_name = emp.get("employee_name", "Unknown")
            try:
                self.delete_basic_related_data(emp_id)
                self.api.delete_doc("Employee", emp_id)
                self.deleted_count += 1
                print(f"[{idx}/{len(employees_to_delete)}] Deleted: {emp_name}")
            except Exception as e:
                self.failed_count += 1
                print(
                    f"[{idx}/{len(employees_to_delete)}] Failed: {emp_name} ({str(e)[:50]})")
        return self.deleted_count, self.failed_count

    def run(self):
        employees = self.get_all_employees()
        if not employees:
            print("Employees Deleted: 0")
            print("Employees Failed: 0")
            print("Attendance Deleted:",
                  self.related_data_deleted["attendance"])
            print("Checkins Deleted:",
                  self.related_data_deleted["employee_checkins"])
            print("Leave Applications Deleted:",
                  self.related_data_deleted["leave_applications"])
            return

        deleted_count, failed_count = self.delete_employees(employees)

        print("\nSummary:")
        print("Employees Deleted:", deleted_count)
        print("Employees Failed:", failed_count)
        print("Attendance Deleted:", self.related_data_deleted["attendance"])
        print("Checkins Deleted:",
              self.related_data_deleted["employee_checkins"])
        print("Leave Applications Deleted:",
              self.related_data_deleted["leave_applications"])


def main():
    print("ERPNext Employee Deletion Script")
    print(f"API Endpoint: {Config.BASE_URL}")
    print(f"Company: {Config.COMPANY_NAME}")

    if not Config.API_KEY or not Config.API_SECRET:
        print("Error: API credentials not set in .env file")
        return

    response = input("Proceed with employee deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = EmployeeDeletor()
        deletor.run()
        print("\nEMPLOYEE DELETION COMPLETED!")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")


if __name__ == "__main__":
    main()

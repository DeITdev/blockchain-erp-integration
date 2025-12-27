#!/usr/bin/env python3
"""
ERPNext Employee Deletion Script
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


class EmployeeDeletor:
    """Deletes employees"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_count = 0
        self.failed_count = 0

    def get_all_employees(self):
        """Get all employees"""
        logger.info("Fetching employees...")
        try:
            employees = self.api.get_list("Employee",
                                          filters={"company": COMPANY},
                                          fields=["name", "employee_name"])
            logger.info(f"Found {len(employees)} employees")
            return employees
        except Exception as e:
            logger.error(f"Error fetching employees: {str(e)}")
            return []

    def delete_basic_related_data(self, employee_id: str):
        """Delete basic related data"""
        basic_doctypes = [
            ("Shift Assignment", "Shift Assignment"),
            ("Attendance", "Attendance"),
            ("Employee Checkin", "Employee Checkin"),
            ("Leave Application", "Leave Application")
        ]

        for doctype, _ in basic_doctypes:
            try:
                records = self.api.get_list(
                    doctype, filters={"employee": employee_id}, fields=["name"])
                for record in records:
                    try:
                        self.api.delete_doc(doctype, record["name"])
                    except Exception:
                        pass
            except Exception:
                pass

    def delete_employees(self, employees_to_delete):
        """Delete employees"""
        logger.info(f"Deleting {len(employees_to_delete)} employees...")

        for i, employee in enumerate(employees_to_delete, 1):
            try:
                employee_name = employee.get("employee_name", "Unknown")
                employee_id = employee.get("name", "Unknown")

                self.delete_basic_related_data(employee_id)
                self.api.delete_doc("Employee", employee_id)

                self.deleted_count += 1
                logger.info(
                    f"Deleted {i}/{len(employees_to_delete)}: {employee_name}")

            except Exception as e:
                self.failed_count += 1
                logger.error(
                    f"Failed to delete {employee.get('employee_name', 'Unknown')}: {str(e)}")

        return self.deleted_count, self.failed_count

    def run(self):
        """Main execution"""
        employees = self.get_all_employees()

        if not employees:
            logger.info("No employees found")
            return

        logger.info(f"Confirming deletion of {len(employees)} employees")
        response = input("Type 'DELETE ALL' to confirm: ")

        if response != "DELETE ALL":
            logger.info("Operation cancelled")
            return

        deleted_count, failed_count = self.delete_employees(employees)

        logger.info(f"Deleted: {deleted_count}")
        logger.info(f"Failed: {failed_count}")


if __name__ == "__main__":
    try:
        deletor = EmployeeDeletor()
        deletor.run()
    except KeyboardInterrupt:
        logger.info("Operation interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

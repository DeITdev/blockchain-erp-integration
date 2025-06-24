#!/usr/bin/env python3
"""
ERPNext Employee Deletion Script (Clean & Simple)
Deletes all employee records from ERPNext system.
NOTE: Run the user permissions deletion script FIRST to clear dependencies.
Uses environment variables from .env file for configuration.
Author: ERPNext Employee Deletion Script
Version: 3.0.0 (Clean)
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler


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

# Configuration


class Config:
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL")
    COMPANY_NAME = os.getenv("COMPANY_NAME")
    COMPANY_ABBR = os.getenv("COMPANY_ABBR")

    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('erpnext_employee_deletion.log', encoding='utf-8'),
        StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set console handler encoding
for handler in logger.handlers:
    if isinstance(handler, StreamHandler):
        handler.stream.reconfigure(encoding='utf-8', errors='replace')


class ERPNextAPI:
    """Simple API client for ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

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
            if retry_count < Config.RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed, retrying... ({retry_count + 1}/{Config.RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {Config.RETRY_ATTEMPTS} attempts: {str(e)}")
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        """Get list of documents"""
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)

        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def delete_doc(self, doctype: str, name: str) -> Dict:
        """Delete a document"""
        return self._make_request("DELETE", f"resource/{doctype}/{name}")


class EmployeeDeletor:
    """Simple employee deletion (assumes user permissions already cleared)"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_employees = []
        self.failed_deletions = []
        self.related_data_deleted = {
            "attendance": 0,
            "employee_checkins": 0,
            "leave_applications": 0
        }

    def get_all_employees(self):
        """Get all employees from ERPNext"""
        logger.info("Fetching all employees...")

        try:
            all_employees = self.api.get_list("Employee",
                                              fields=["name", "employee_name", "user_id", "status", "company"])

            # Filter by company
            company_employees = [emp for emp in all_employees if emp.get(
                "company") == Config.COMPANY_NAME]

            logger.info(
                f"Found {len(company_employees)} employees for company '{Config.COMPANY_NAME}'")
            return company_employees

        except Exception as e:
            logger.error(f"Error fetching employees: {str(e)}")
            return []

    def delete_basic_related_data(self, employee_id: str, employee_name: str):
        """Delete only basic related data (no complex doctypes)"""
        deleted_counts = {
            "attendance": 0,
            "employee_checkins": 0,
            "leave_applications": 0
        }

        # Only try to delete the most basic, common records
        basic_doctypes = [
            ("Attendance", "attendance"),
            ("Employee Checkin", "employee_checkins"),
            ("Leave Application", "leave_applications")
        ]

        for doctype, count_key in basic_doctypes:
            try:
                records = self.api.get_list(
                    doctype, filters={"employee": employee_id}, fields=["name"])
                for record in records:
                    try:
                        self.api.delete_doc(doctype, record["name"])
                        deleted_counts[count_key] += 1
                    except Exception as e:
                        logger.debug(
                            f"Failed to delete {doctype} {record['name']}: {e}")

                # Update totals
                self.related_data_deleted[count_key] += deleted_counts[count_key]

            except Exception as e:
                logger.debug(
                    f"Could not access {doctype} for {employee_name}: {e}")

        total_deleted = sum(deleted_counts.values())
        if total_deleted > 0:
            logger.debug(
                f"Deleted {total_deleted} related records for {employee_name}")

    def display_employees_summary(self, employees):
        """Display summary of employees to be deleted"""
        if not employees:
            print("\n✅ No employees found to delete.")
            return False

        print(f"\n⚠️  WARNING: This will DELETE {len(employees)} employees!")
        print(f"\n📊 Employee Summary for company '{Config.COMPANY_NAME}':")

        # Count by status
        status_counts = {}
        for emp in employees:
            status = emp.get("status", "Unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        for status, count in status_counts.items():
            print(f"   - {status}: {count} employees")

        print(f"\n🗑️ Employees to be DELETED (showing first 10):")
        for i, employee in enumerate(employees[:10]):
            emp_name = employee.get("employee_name", "Unknown")
            emp_id = employee.get("name", "Unknown")
            status = employee.get("status", "Unknown")
            print(f"   {i+1}. {emp_name} (ID: {emp_id}, Status: {status})")

        if len(employees) > 10:
            print(f"   ... and {len(employees) - 10} more employees")

        print(f"\n📋 Basic related data that will be deleted:")
        print(f"   - Attendance records")
        print(f"   - Employee checkins")
        print(f"   - Leave applications")
        print(f"   - NOTE: User permissions should be deleted separately first!")

        return True

    def confirm_deletion(self, employees):
        """Ask for user confirmation"""
        if not self.display_employees_summary(employees):
            return False

        print(f"\n🚨 IMPORTANT:")
        print(f"   - Make sure you ran the user permissions deletion script FIRST!")
        print(f"   - This action CANNOT be undone!")

        response = input(f"\nType 'DELETE ALL EMPLOYEES' to confirm: ")
        return response == "DELETE ALL EMPLOYEES"

    def delete_employees(self, employees_to_delete):
        """Delete employees and their basic related data"""
        logger.info(
            f"Starting deletion of {len(employees_to_delete)} employees...")

        deleted_count = 0
        failed_count = 0

        for i, employee in enumerate(employees_to_delete):
            employee_name = employee.get("employee_name", "Unknown")
            employee_id = employee.get("name", "Unknown")

            try:
                logger.info(
                    f"🗑️ Deleting employee {i+1}/{len(employees_to_delete)}: {employee_name}")

                # Delete basic related data first
                self.delete_basic_related_data(employee_id, employee_name)

                # Delete the employee record
                self.api.delete_doc("Employee", employee_id)

                self.deleted_employees.append(employee)
                deleted_count += 1

                logger.info(f"✅ Successfully deleted: {employee_name}")

                # Progress update every 10 deletions
                if deleted_count % 10 == 0:
                    logger.info(
                        f"📊 Progress: {deleted_count}/{len(employees_to_delete)} completed")

                time.sleep(0.2)  # Small delay

            except Exception as e:
                failed_count += 1
                error_msg = str(e)

                if "409" in error_msg or "dependencies" in error_msg.lower():
                    logger.error(
                        f"❌ Cannot delete {employee_name} - has dependencies!")
                    logger.error(
                        f"💡 Hint: Run user permissions deletion script first!")
                elif "403" in error_msg or "permission" in error_msg.lower():
                    logger.error(f"❌ Permission denied for {employee_name}")
                else:
                    logger.error(
                        f"❌ Failed to delete {employee_name}: {error_msg}")

                self.failed_deletions.append(
                    {"employee": employee, "error": error_msg})

        logger.info(
            f"Deletion completed: {deleted_count} deleted, {failed_count} failed")
        return deleted_count, failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🗑️ ERPNext Employee Deletion Script (Clean & Simple)")
        print("=" * 80)
        print(f"📡 API Endpoint: {Config.BASE_URL}")
        print(f"🏢 Company: {Config.COMPANY_NAME}")
        print("💡 IMPORTANT: Run user permissions deletion script FIRST!")
        print("=" * 80)

        try:
            # Get all employees
            all_employees = self.get_all_employees()
            if not all_employees:
                print("✅ No employees found to delete")
                return

            # Confirm deletion
            if not self.confirm_deletion(all_employees):
                print("Operation cancelled.")
                return

            # Delete employees
            deleted_count, failed_count = self.delete_employees(all_employees)

            # Final summary
            print("\n" + "=" * 80)
            print("✅ EMPLOYEE DELETION COMPLETED!")
            print("=" * 80)
            print(f"📊 Summary:")
            print(f"   - Employees deleted: {deleted_count}")
            print(f"   - Employees failed: {failed_count}")

            print(f"\n📋 Related data deleted:")
            print(
                f"   - Attendance records: {self.related_data_deleted['attendance']}")
            print(
                f"   - Employee checkins: {self.related_data_deleted['employee_checkins']}")
            print(
                f"   - Leave applications: {self.related_data_deleted['leave_applications']}")

            if self.failed_deletions:
                print(f"\n❌ Failed deletions ({len(self.failed_deletions)}):")
                for failure in self.failed_deletions[:5]:
                    employee = failure["employee"]
                    error = failure["error"]
                    emp_name = employee.get("employee_name", "Unknown")
                    print(f"   - {emp_name}: {error}")
                if len(self.failed_deletions) > 5:
                    print(
                        f"   ... and {len(self.failed_deletions) - 5} more failures")

                if any("dependencies" in str(f["error"]).lower() for f in self.failed_deletions):
                    print(f"\n💡 Troubleshooting:")
                    print(f"   Run the user permissions deletion script first!")

            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            print(f"\n💥 ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Employee Deletion...")

    if not Config.API_KEY or not Config.API_SECRET:
        print("❌ Error: API credentials not set in .env file")
        return

    print(f"\n⚠️ REMINDERS:")
    print(f"   1. Run 'delete_user_permissions.py' FIRST")
    print(f"   2. This will delete ALL employees for '{Config.COMPANY_NAME}'")
    print(f"   3. This action CANNOT be undone!")

    response = input(
        f"\nHave you run the user permissions deletion script? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ Please run 'delete_user_permissions.py' first!")
        return

    response = input(f"\nProceed with employee deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = EmployeeDeletor()
        deletor.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

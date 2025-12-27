#!/usr/bin/env python3
"""
ERPNext Attendance Generator
Creates 10 attendance records per employee for the current month.
Uses environment variables from .env file for configuration.
Author: ERPNext Attendance Generator
Version: 1.0.0
"""

import requests
import json
import random
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys
import calendar

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

# Attendance Configuration
COMPANY = "PT Fiyansa Mulya"
ATTENDANCE_PER_EMPLOYEE = 10  # Default value, can be overridden by user

# Get current month details
current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
current_month_name = current_date.strftime("%B")

# Simple logging - console only, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class AttendanceGenerator:
    """Attendance Generator with current month focus"""

    def __init__(self, attendance_count=ATTENDANCE_PER_EMPLOYEE):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL
        self.attendance_count = attendance_count

        # Master data collections
        self.employees = []
        self.shift_types = []
        self.attendance_statuses = [
            "Present",
            "Absent",
            "Half Day",
            "Work From Home",
            "On Leave"
        ]

        logger.info(f"Connecting to {self.base_url}")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < 3:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/3) - Error: {e}")
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after 3 attempts for {url}: {str(e)}")
                if hasattr(e, 'response') and e.response:
                    logger.error(f"Response content: {e.response.text}")
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        """Get list of documents"""
        params = {
            "limit_page_length": 1000
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)

        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def fetch_employees(self):
        """Fetch active employees from the company"""
        logger.info("Fetching employees...")

        try:
            employees = self.get_list("Employee",
                                      filters={"company": COMPANY,
                                               "status": "Active"},
                                      fields=["name", "employee_name"])

            self.employees = employees
            logger.info(f"Found {len(self.employees)} employees")
            return bool(self.employees)

        except Exception as e:
            logger.error(f"Error fetching employees: {str(e)}")
            return False

    def fetch_shift_types(self):
        """Fetch existing shift types"""
        logger.info("Fetching shift types...")

        try:
            shift_types = self.get_list("Shift Type", fields=["name"])
            self.shift_types = [shift.get("name")
                                for shift in shift_types if shift.get("name")]

            logger.info(f"Found {len(self.shift_types)} shift types")
            return True

        except Exception as e:
            logger.warning(f"Could not fetch shift types: {e}")
            return False

    def get_current_month_dates(self):
        """Get all weekdays (Monday-Friday) in the current month"""
        logger.info(
            f"Generating dates for {current_month_name} {current_year}...")

        first_day = datetime(current_year, current_month, 1)
        last_day = datetime(current_year, current_month,
                            calendar.monthrange(current_year, current_month)[1])

        weekdays = []
        current_day = first_day

        while current_day <= last_day:
            if current_day.weekday() < 5:
                weekdays.append(current_day.strftime("%Y-%m-%d"))
            current_day += timedelta(days=1)

        logger.info(f"Found {len(weekdays)} weekdays")
        return weekdays

    def generate_random_time(self, base_time: str, variation_minutes: int = 30):
        """Generate random time based on base time with variation"""
        try:
            base = datetime.strptime(base_time, "%H:%M:%S")
            # Add random variation (can be negative or positive)
            variation = random.randint(-variation_minutes, variation_minutes)
            new_time = base + timedelta(minutes=variation)
            return new_time.strftime("%H:%M:%S")
        except:
            return base_time

    def create_attendance_records(self):
        """Create attendance records for all employees"""
        print("\nCreating Attendance Records")
        print("="*60)

        if not self.employees:
            logger.error("No employees available")
            return 0, 0

        available_dates = self.get_current_month_dates()

        if len(available_dates) < self.attendance_count:
            logger.warning(
                f"Only {len(available_dates)} weekdays available, using all")
            records_per_employee = len(available_dates)
        else:
            records_per_employee = self.attendance_count

        total_records_to_create = len(self.employees) * records_per_employee
        logger.info(
            f"Target: {total_records_to_create} total records ({records_per_employee} per employee)")

        created_count = 0
        failed_count = 0

        for emp_index, employee in enumerate(self.employees):
            employee_name = employee.get("employee_name", "Unknown")
            employee_id = employee.get("name")
            progress_pct = ((emp_index + 1) / len(self.employees)) * 100

            employee_dates = random.sample(available_dates, min(
                records_per_employee, len(available_dates)))
            employee_dates.sort()

            for date_index, attendance_date in enumerate(employee_dates):
                try:
                    status = random.choice(self.attendance_statuses)
                    shift = random.choice(
                        self.shift_types) if self.shift_types else None
                    late_entry = random.choice([0, 1])
                    early_exit = random.choice([0, 1])

                    attendance_data = {
                        "employee": employee_id,
                        "attendance_date": attendance_date,
                        "status": status,
                        "company": COMPANY,
                        "late_entry": late_entry,
                        "early_exit": early_exit
                    }

                    if shift:
                        attendance_data["shift"] = shift

                    if status in ["Present", "Half Day", "Work From Home"]:
                        check_in_time = self.generate_random_time(
                            "08:00:00", 60)
                        check_out_time = self.generate_random_time(
                            "17:00:00", 60)
                        attendance_data["in_time"] = f"{attendance_date} {check_in_time}"
                        attendance_data["out_time"] = f"{attendance_date} {check_out_time}"

                    self.create_doc("Attendance", attendance_data)
                    created_count += 1
                    logger.info(
                        f"[{emp_index+1}/{len(self.employees)}] ({progress_pct:.0f}%) {employee_name} - {attendance_date}")

                except Exception as e:
                    failed_count += 1
                    logger.error(f"Failed to create attendance: {str(e)}")

        return created_count, failed_count

    def run(self):
        """Main execution method"""
        print("="*80)
        print("Attendance Generator")
        print("="*80)
        print(f"Records per employee: {self.attendance_count}")

        try:
            if not self.fetch_employees():
                print("Cannot proceed without employees")
                return

            self.fetch_shift_types()

            created_count, failed_count = self.create_attendance_records()

            print("\n" + "="*60)
            print("Summary")
            print("="*60)
            print(f"Created: {created_count}")
            print(f"Failed: {failed_count}")
            print(f"Employees: {len(self.employees)}")
            print(f"Per employee: {self.attendance_count}")
            print(f"Month: {current_month_name} {current_year}")

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            print(f"Error: {e}")


def main():
    """Main entry point"""
    print("Starting Attendance Generation...")

    if not API_KEY or not API_SECRET:
        print("Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"\nGenerating attendance for {current_month_name} {current_year}")

    # Ask user for attendance count per employee
    while True:
        try:
            count_input = input(
                "How many attendance records per employee? (default 10): ").strip()
            if count_input == "":
                attendance_count = ATTENDANCE_PER_EMPLOYEE
                break
            attendance_count = int(count_input)
            if attendance_count <= 0:
                print("Please enter a positive number")
                continue
            break
        except ValueError:
            print("Please enter a valid number")

    response = input(
        f"Proceed with {attendance_count} records per employee? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return

    try:
        generator = AttendanceGenerator(attendance_count)
        generator.run()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()

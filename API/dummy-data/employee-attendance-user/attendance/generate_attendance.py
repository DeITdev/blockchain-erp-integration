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
ATTENDANCE_PER_EMPLOYEE = 10
COMPANY = "PT Fiyansa Mulya"

# Get current month details
current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
current_month_name = current_date.strftime("%B")

# Simple logging - console only, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class AttendanceGenerator:
    """Attendance Generator with current month focus"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

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

        logger.info(f"🔗 API: {self.base_url}")
        logger.info(f"🏢 Company: {COMPANY}")
        logger.info(f"📅 Target Month: {current_month_name} {current_year}")
        logger.info(f"🔑 Key: {API_KEY[:8] if API_KEY else 'None'}...")

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
        logger.info("👥 Fetching active employees...")

        try:
            employees = self.get_list("Employee",
                                      filters={"company": COMPANY,
                                               "status": "Active"},
                                      fields=["name", "employee_name"])

            self.employees = employees
            logger.info(f"✅ Found {len(self.employees)} active employees")

            if not self.employees:
                logger.error("❌ No active employees found!")
                return False

            # Show sample employees
            logger.info("Sample employees:")
            for i, emp in enumerate(self.employees[:3]):
                logger.info(
                    f"   {i+1}. {emp.get('employee_name')} ({emp.get('name')})")

            if len(self.employees) > 3:
                logger.info(f"   ... and {len(self.employees) - 3} more")

            return True

        except Exception as e:
            logger.error(f"Error fetching employees: {str(e)}")
            return False

    def fetch_shift_types(self):
        """Fetch existing shift types"""
        logger.info("⏰ Fetching shift types...")

        try:
            shift_types = self.get_list("Shift Type", fields=["name"])
            self.shift_types = [shift.get("name")
                                for shift in shift_types if shift.get("name")]

            logger.info(f"✅ Found {len(self.shift_types)} shift types")

            if self.shift_types:
                logger.info(f"Available shifts: {self.shift_types}")
            else:
                logger.warning(
                    "⚠️ No shift types found, attendance will be created without shifts")

            return True

        except Exception as e:
            logger.warning(f"⚠️ Could not fetch shift types: {e}")
            return False

    def get_current_month_dates(self):
        """Get all weekdays (Monday-Friday) in the current month"""
        logger.info(
            f"📅 Generating dates for {current_month_name} {current_year}...")

        # Get the first and last day of current month
        first_day = datetime(current_year, current_month, 1)
        last_day = datetime(current_year, current_month,
                            calendar.monthrange(current_year, current_month)[1])

        # Get all weekdays in the current month
        weekdays = []
        current_day = first_day

        while current_day <= last_day:
            # Monday = 0, Sunday = 6, so weekdays are 0-4
            if current_day.weekday() < 5:  # Monday to Friday
                weekdays.append(current_day.strftime("%Y-%m-%d"))
            current_day += timedelta(days=1)

        logger.info(
            f"✅ Found {len(weekdays)} weekdays in {current_month_name}")
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
        print("\n" + "="*60)
        print("📝 Creating Attendance Records")
        print("="*60)

        if not self.employees:
            logger.error("❌ No employees available for attendance creation")
            return 0

        # Get available dates for current month
        available_dates = self.get_current_month_dates()

        if len(available_dates) < ATTENDANCE_PER_EMPLOYEE:
            logger.warning(
                f"⚠️ Only {len(available_dates)} weekdays available in {current_month_name}, but need {ATTENDANCE_PER_EMPLOYEE} records per employee")
            logger.warning("Will create attendance for all available dates")
            records_per_employee = len(available_dates)
        else:
            records_per_employee = ATTENDANCE_PER_EMPLOYEE

        total_records_to_create = len(self.employees) * records_per_employee
        logger.info(
            f"🎯 Target: {records_per_employee} records per employee = {total_records_to_create} total records")

        created_count = 0
        failed_count = 0

        for emp_index, employee in enumerate(self.employees):
            employee_name = employee.get("employee_name", "Unknown")
            employee_id = employee.get("name")

            print(
                f"\n👤 Employee {emp_index + 1}/{len(self.employees)}: {employee_name}")

            # Randomly select dates for this employee
            employee_dates = random.sample(available_dates, min(
                records_per_employee, len(available_dates)))
            employee_dates.sort()  # Sort dates chronologically

            employee_records_created = 0

            for date_index, attendance_date in enumerate(employee_dates):
                try:
                    # Random attendance status
                    status = random.choice(self.attendance_statuses)

                    # Random shift (if available)
                    shift = random.choice(
                        self.shift_types) if self.shift_types else None

                    # Random late entry and early exit
                    # 0 = unchecked, 1 = checked
                    late_entry = random.choice([0, 1])
                    early_exit = random.choice([0, 1])

                    # Prepare attendance data
                    attendance_data = {
                        "employee": employee_id,
                        "attendance_date": attendance_date,
                        "status": status,
                        "company": COMPANY,
                        "late_entry": late_entry,
                        "early_exit": early_exit
                    }

                    # Add shift if available
                    if shift:
                        attendance_data["shift"] = shift

                    # Add realistic time data for present/working statuses
                    if status in ["Present", "Half Day", "Work From Home"]:
                        # Generate realistic check-in and check-out times
                        check_in_time = self.generate_random_time(
                            "08:00:00", 60)  # 8 AM ± 1 hour
                        check_out_time = self.generate_random_time(
                            "17:00:00", 60)  # 5 PM ± 1 hour

                        attendance_data["in_time"] = f"{attendance_date} {check_in_time}"
                        attendance_data["out_time"] = f"{attendance_date} {check_out_time}"

                    # Create attendance record
                    result = self.create_doc("Attendance", attendance_data)

                    created_count += 1
                    employee_records_created += 1

                    # Status indicators
                    late_indicator = "🔴" if late_entry else "⚪"
                    early_indicator = "🔴" if early_exit else "⚪"
                    shift_indicator = f"⏰ {shift}" if shift else "⏰ No shift"

                    print(
                        f"   ✅ {date_index + 1}/{len(employee_dates)}: {attendance_date} - {status}")
                    print(
                        f"      {shift_indicator} | Late: {late_indicator} | Early: {early_indicator}")

                except Exception as e:
                    failed_count += 1
                    logger.error(
                        f"❌ Failed to create attendance for {employee_name} on {attendance_date}: {str(e)}")
                    print(
                        f"   ❌ {date_index + 1}/{len(employee_dates)}: {attendance_date} - FAILED")

            print(
                f"   📊 Created {employee_records_created} records for {employee_name}")

        return created_count, failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("📝 ERPNext Attendance Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY}")
        print(f"📅 Target Month: {current_month_name} {current_year}")
        print(f"📊 Records per Employee: {ATTENDANCE_PER_EMPLOYEE}")
        print("=" * 80)

        try:
            # Fetch required data
            if not self.fetch_employees():
                print("❌ Cannot proceed without employees")
                return

            self.fetch_shift_types()

            # Create attendance records
            created_count, failed_count = self.create_attendance_records()

            # Summary
            print("\n" + "="*60)
            print("📊 SUMMARY")
            print("="*60)
            print(f"✅ Attendance Records Created: {created_count}")
            print(f"❌ Failed Records: {failed_count}")
            print(f"👥 Employees Processed: {len(self.employees)}")
            print(f"📅 Month: {current_month_name} {current_year}")
            print(f"⏰ Shifts Used: {len(self.shift_types)} different shifts")
            print(f"📊 Statuses Used: {', '.join(self.attendance_statuses)}")

            if created_count > 0:
                print(f"\n💡 Attendance features:")
                print(
                    f"   - Random dates within {current_month_name} {current_year}")
                print(f"   - Random attendance statuses")
                print(f"   - Random shift assignments")
                print(f"   - Random late entry/early exit flags")
                print(f"   - Realistic check-in/out times for working days")

        except Exception as e:
            logger.error(f"Fatal error during attendance generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Attendance Generation...")

    # Check API credentials
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(
        f"\n📝 This will create attendance records for {current_month_name} {current_year}:")
    print(f"   📊 {ATTENDANCE_PER_EMPLOYEE} records per employee")
    print(f"   📅 Random dates within current month")
    print(f"   ⏰ Random shifts and statuses")
    print(f"   🔴 Random late entry/early exit flags")
    print(f"   🏢 Company: {COMPANY}")

    response = input(f"\nProceed with attendance generation? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = AttendanceGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

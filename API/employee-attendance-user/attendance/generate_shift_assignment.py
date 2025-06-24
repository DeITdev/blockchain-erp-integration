#!/usr/bin/env python3
"""
ERPNext Shift Assignment Generator
Creates shift assignment records with existing employee, shift type, and shift location data.
Each employee gets 2 shift assignments (total ~100 records).
Uses environment variables from .env file for configuration.
Author: ERPNext Shift Assignment Generator
Version: 1.0.0
"""

import requests
import json
import logging
import time
import os
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys

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


# Load environment variables
load_env_file()

# Configuration from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")

# Shift Assignment Configuration
ASSIGNMENTS_PER_EMPLOYEE = 2
TARGET_YEAR = 2025

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class ERPNextAPI:
    """Handles all API interactions with ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        logger.info(f"🔗 API: {self.base_url}")
        logger.info(f"🏢 Company: {COMPANY_NAME}")
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
            if retry_count < RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
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
        """Create new document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)


class ShiftAssignmentGenerator:
    """Generates shift assignment records using existing master data"""

    def __init__(self):
        self.api = ERPNextAPI()

        # Master data collections
        self.employees = []
        self.shift_types = []
        self.shift_locations = []
        self.shift_assignments = []

        # Fetch all required master data
        self._fetch_master_data()

    def _fetch_master_data(self):
        """Fetch all required master data from ERPNext"""
        logger.info("📊 Fetching master data for shift assignments...")

        # Fetch Employees
        try:
            employees = self.api.get_list("Employee",
                                          filters={
                                              "company": COMPANY_NAME,
                                              "status": "Active"
                                          },
                                          fields=["name", "employee_name"])
            self.employees = employees
            logger.info(f"✅ Employees: {len(self.employees)} found")
            if self.employees:
                logger.info(
                    f"   Sample: {[emp.get('employee_name', emp.get('name')) for emp in self.employees[:3]]}")
        except Exception as e:
            logger.error(f"❌ Could not fetch employees: {e}")
            return False

        # Fetch Shift Types - Fixed approach
        try:
            logger.info("🔄 Fetching shift types...")

            # Try multiple methods to fetch shift types
            shift_types = None

            # Method 1: Try with all fields
            try:
                shift_types = self.api.get_list("Shift Type")
                logger.info(
                    f"✅ Method 1 - Found {len(shift_types)} shift types")
            except Exception as e1:
                logger.warning(f"Method 1 failed: {e1}")

                # Method 2: Try with specific fields
                try:
                    shift_types = self.api.get_list(
                        "Shift Type", fields=["name", "shift_type"])
                    logger.info(
                        f"✅ Method 2 - Found {len(shift_types)} shift types")
                except Exception as e2:
                    logger.warning(f"Method 2 failed: {e2}")

                    # Method 3: Try with just name field
                    try:
                        shift_types = self.api.get_list(
                            "Shift Type", fields=["name"])
                        logger.info(
                            f"✅ Method 3 - Found {len(shift_types)} shift types")
                    except Exception as e3:
                        logger.error(f"All methods failed: {e3}")
                        shift_types = []

            self.shift_types = shift_types or []

            if self.shift_types:
                logger.info(
                    f"✅ Successfully fetched {len(self.shift_types)} shift types:")
                for i, st in enumerate(self.shift_types):
                    # Handle different possible field names
                    shift_name = st.get('name', 'Unknown')
                    shift_type_name = st.get(
                        'shift_type', st.get('name', 'Unknown'))
                    logger.info(
                        f"   {i+1}. ID: '{shift_name}', Name: '{shift_type_name}'")

                # Verify we have the expected shift types
                expected_types = ["Afternoon Shift", "Day Shift",
                                  "Extended Day Shift", "Morning Shift", "Night Shift"]
                found_types = []
                for st in self.shift_types:
                    shift_type_name = st.get('shift_type', st.get('name', ''))
                    if shift_type_name in expected_types:
                        found_types.append(shift_type_name)

                logger.info(f"✅ Found expected shift types: {found_types}")
                missing_types = [
                    t for t in expected_types if t not in found_types]
                if missing_types:
                    logger.warning(
                        f"⚠️ Missing expected shift types: {missing_types}")

            else:
                logger.error(
                    "❌ No shift types found despite multiple attempts")
                return False

        except Exception as e:
            logger.error(f"❌ Could not fetch shift types: {e}")
            logger.error(f"   Debugging info:")
            logger.error(f"   - API URL: {self.api.base_url}")
            logger.error(
                f"   - This suggests the Shift Type doctype might not be accessible")
            return False

        # Fetch Shift Locations
        try:
            shift_locations = self.api.get_list(
                "Shift Location", fields=["name", "location_name"])
            self.shift_locations = shift_locations
            logger.info(
                f"✅ Shift Locations: {len(self.shift_locations)} found")
            if self.shift_locations:
                logger.info(
                    f"   Sample: {[sl.get('location_name', sl.get('name')) for sl in self.shift_locations[:3]]}")
        except Exception as e:
            logger.error(f"❌ Could not fetch shift locations: {e}")
            return False

        # Check if we have enough master data
        missing_data = []
        if not self.employees:
            missing_data.append("Employees")
        if not self.shift_types:
            missing_data.append("Shift Types")
        if not self.shift_locations:
            missing_data.append("Shift Locations")

        if missing_data:
            logger.error(
                f"❌ Missing required master data: {', '.join(missing_data)}")
            return False

        logger.info("✅ All required master data available!")
        return True

    def generate_random_date_range_2025(self):
        """Generate random start and end dates within 2025"""
        # Define 2025 date range
        year_start = datetime(TARGET_YEAR, 1, 1)
        year_end = datetime(TARGET_YEAR, 12, 31)

        # Generate random start date (first 10 months to allow for end date)
        start_range_end = datetime(TARGET_YEAR, 10, 31)
        days_for_start = (start_range_end - year_start).days
        start_random_days = random.randint(0, days_for_start)
        start_date = year_start + timedelta(days=start_random_days)

        # Generate end date (1-3 months after start date, but within 2025)
        min_duration = 30   # Minimum 1 month
        max_duration = 90   # Maximum 3 months
        duration_days = random.randint(min_duration, max_duration)
        end_date = start_date + timedelta(days=duration_days)

        # Ensure end date doesn't exceed 2025
        if end_date > year_end:
            end_date = year_end

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def check_existing_shift_assignments(self):
        """Check existing shift assignments for the company"""
        logger.info("📊 Checking existing shift assignments...")

        try:
            existing_assignments = self.api.get_list("Shift Assignment",
                                                     filters={
                                                         "company": COMPANY_NAME},
                                                     fields=["name", "employee", "shift_type", "status"])

            current_count = len(existing_assignments)
            self.shift_assignments = existing_assignments

            target_count = len(self.employees) * ASSIGNMENTS_PER_EMPLOYEE

            logger.info(f"Current shift assignments: {current_count}")
            logger.info(
                f"Target shift assignments: {target_count} ({len(self.employees)} employees × {ASSIGNMENTS_PER_EMPLOYEE} assignments each)")

            if current_count >= target_count:
                logger.info(
                    f"Already have {current_count} shift assignments (>= target {target_count}). Skipping new creation.")
                return 0

            assignments_to_create = target_count - current_count
            logger.info(
                f"Need to create {assignments_to_create} shift assignments to reach target {target_count}")

            return assignments_to_create

        except Exception as e:
            logger.error(
                f"Error checking existing shift assignments: {str(e)}")
            return len(self.employees) * ASSIGNMENTS_PER_EMPLOYEE

    def create_shift_assignments(self):
        """Create shift assignment records for all employees"""
        logger.info("🚀 Starting shift assignment creation...")

        # Check how many assignments we need to create
        assignments_to_create = self.check_existing_shift_assignments()

        if assignments_to_create <= 0:
            logger.info("No new shift assignments need to be created.")
            return

        target_total = len(self.employees) * ASSIGNMENTS_PER_EMPLOYEE

        print("\n" + "="*80)
        print("🔄 Creating Shift Assignments")
        print("="*80)
        print(f"👥 Employees: {len(self.employees)}")
        print(f"📊 Assignments per Employee: {ASSIGNMENTS_PER_EMPLOYEE}")
        print(f"🎯 Target Total: {target_total} shift assignments")
        print(f"📅 Date Range: {TARGET_YEAR}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print("="*80)

        assignments_created_count = 0
        assignments_failed_count = 0

        # Get existing assignments to track which employees already have some
        existing_employee_assignments = {}
        for assignment in self.shift_assignments:
            employee = assignment.get("employee")
            if employee:
                existing_employee_assignments[employee] = existing_employee_assignments.get(
                    employee, 0) + 1

        for employee in self.employees:
            employee_name = employee.get("name")
            employee_display_name = employee.get(
                "employee_name", employee_name)

            # Check how many assignments this employee already has
            existing_count = existing_employee_assignments.get(
                employee_name, 0)
            assignments_needed = ASSIGNMENTS_PER_EMPLOYEE - existing_count

            if assignments_needed <= 0:
                logger.debug(
                    f"Employee {employee_display_name} already has {existing_count} assignments, skipping")
                continue

            print(
                f"\n👤 Creating {assignments_needed} assignments for: {employee_display_name}")

            for assignment_num in range(assignments_needed):
                # Generate random assignment data
                shift_type = random.choice(self.shift_types)
                shift_location = random.choice(self.shift_locations)
                status = random.choice(["Active", "Inactive"])
                start_date, end_date = self.generate_random_date_range_2025()

                # Get proper field values - handle different possible field structures
                shift_type_name = shift_type.get(
                    "name")  # Use the document ID/name
                shift_type_display = shift_type.get(
                    "shift_type", shift_type.get("name", "Unknown"))

                shift_location_name = shift_location.get(
                    "name")  # Use the document ID/name
                shift_location_display = shift_location.get(
                    "location_name", shift_location.get("name", "Unknown"))

                # Prepare shift assignment data
                assignment_data = {
                    "employee": employee_name,
                    "shift_type": shift_type_name,  # Use the document name/ID
                    "shift_location": shift_location_name,  # Use the document name/ID
                    "status": status,
                    "start_date": start_date,
                    "end_date": end_date,
                    "company": COMPANY_NAME
                }

                try:
                    assignment = self.api.create_doc(
                        "Shift Assignment", assignment_data)
                    assignments_created_count += 1

                    status_icon = "🟢" if status == "Active" else "🔴"

                    print(
                        f"   ✅ Assignment {assignment_num + 1}: {shift_type_display} at {shift_location_display}")
                    print(f"      📅 {start_date} → {end_date}")
                    print(f"      {status_icon} Status: {status}")
                    print(
                        f"      🔧 Data: shift_type='{shift_type_name}', shift_location='{shift_location_name}'")

                    logger.debug(
                        f"Created assignment: {employee_display_name} - {shift_type_display} - {shift_location_display} ({status})")

                    # Small delay to avoid overwhelming the server
                    time.sleep(0.1)

                except Exception as e:
                    assignments_failed_count += 1
                    logger.error(
                        f"❌ Failed to create assignment for {employee_display_name}: {str(e)}")
                    print(
                        f"   ❌ Assignment {assignment_num + 1}: FAILED - {str(e)[:50]}...")

            # Small delay between employees
            time.sleep(0.2)

        # Final Summary
        print("\n" + "="*80)
        print("📊 SHIFT ASSIGNMENT CREATION SUMMARY")
        print("="*80)
        print(f"✅ Assignments Created: {assignments_created_count}")
        print(f"❌ Assignments Failed: {assignments_failed_count}")
        print(f"👥 Employees Processed: {len(self.employees)}")
        print(
            f"📊 Average per Employee: {assignments_created_count / len(self.employees) if self.employees else 0:.1f}")

        # Show status distribution
        active_count = 0
        inactive_count = 0
        try:
            all_assignments = self.api.get_list("Shift Assignment",
                                                filters={
                                                    "company": COMPANY_NAME},
                                                fields=["status"])
            for assignment in all_assignments:
                if assignment.get("status") == "Active":
                    active_count += 1
                else:
                    inactive_count += 1

            print(f"\n📈 Total Assignment Status Distribution:")
            print(f"   🟢 Active: {active_count}")
            print(f"   🔴 Inactive: {inactive_count}")
        except Exception as e:
            logger.warning(f"Could not fetch status distribution: {e}")

        print("="*80)

        logger.info(
            f"Successfully created {assignments_created_count} shift assignments")

    def display_summary(self):
        """Display a summary of all master data and assignments"""
        print(f"\n📋 MASTER DATA SUMMARY")
        print("="*50)
        print(f"👥 Employees: {len(self.employees)}")
        print(f"🔄 Shift Types: {len(self.shift_types)}")
        print(f"📍 Shift Locations: {len(self.shift_locations)}")

        try:
            total_assignments = self.api.get_list("Shift Assignment",
                                                  filters={
                                                      "company": COMPANY_NAME},
                                                  fields=["name"])
            print(f"📊 Total Shift Assignments: {len(total_assignments)}")
        except Exception as e:
            logger.warning(f"Could not fetch total assignments: {e}")

        print("="*50)

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🔄 ERPNext Shift Assignment Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"📊 Assignments per Employee: {ASSIGNMENTS_PER_EMPLOYEE}")
        print(f"📅 Year Range: {TARGET_YEAR}")
        print("=" * 80)

        try:
            # Check if we have required master data
            if not self.employees:
                print("❌ No employees found! Please create employees first.")
                return
            if not self.shift_types:
                print("❌ No shift types found! Please create shift types first.")
                return
            if not self.shift_locations:
                print("❌ No shift locations found! Please create shift locations first.")
                return

            # Display master data summary
            self.display_summary()

            # Create shift assignments
            self.create_shift_assignments()

            print(f"\n🎉 SHIFT ASSIGNMENT GENERATION COMPLETED!")

        except Exception as e:
            logger.error(
                f"Fatal error during shift assignment generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Shift Assignment Generation...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        return

    print(f"\n🔄 This script will create shift assignments:")
    print(f"👥 Each employee gets {ASSIGNMENTS_PER_EMPLOYEE} shift assignments")
    print(f"📅 All dates will be within {TARGET_YEAR}")
    print(f"🏢 Company: {COMPANY_NAME}")
    print(f"🔄 Uses existing: Employees, Shift Types, Shift Locations")

    response = input(f"\nContinue with shift assignment creation? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = ShiftAssignmentGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

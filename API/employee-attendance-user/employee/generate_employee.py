#!/usr/bin/env python3
"""
ERPNext Employee Generator with Master Data
Generates 50 employees using existing Branch, Employee Grade, Department, etc.
Uses environment variables from .env file for configuration.
Author: ERPNext Employee Generator
Version: 2.0.0
"""

import requests
import json
import random
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    # Look for .env file in the API directory (parent of current directory)
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

# Initialize Faker for generating random data
fake = Faker('id_ID')  # Indonesian locale for realistic data

# Configuration from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")
COMPANY_ABBR = os.getenv("COMPANY_ABBR")

# Employee Configuration
TARGET_EMPLOYEES = 50

# Date ranges
BIRTH_YEAR_START = 1990
BIRTH_YEAR_END = 2005
JOIN_YEAR_START = 2020
JOIN_YEAR_END = 2025

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# Simple logging - console only, no log files
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

    def check_exists(self, doctype: str, name: str) -> bool:
        """Check if document exists"""
        try:
            if doctype == "Employee":
                result = self.get_list(doctype, filters={
                                       "employee_name": name, "company": COMPANY_NAME}, fields=["name"])
                return len(result) > 0
            else:
                self._make_request("GET", f"resource/{doctype}/{name}")
                return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                logger.error(
                    f"Error checking existence of {doctype} {name}: {e}")
                return False
        except Exception as e:
            logger.error(
                f"Unexpected error checking existence of {doctype} {name}: {e}")
            raise


class EmployeeGenerator:
    """Generates employee records using existing master data"""

    def __init__(self):
        self.fake = Faker('id_ID')
        self.api = ERPNextAPI()
        self.users = []
        self.employees = []

        # Master data collections
        self.master_data = {
            'branches': [],
            'employee_grades': [],
            'departments': [],
            'employment_types': [],
            'designations': []
        }

        # Fetch all required data during initialization
        self._fetch_users()
        self._fetch_master_data()

    def _fetch_users(self):
        """Fetch existing users to base employee data on"""
        logger.info("📋 Fetching existing users...")

        try:
            all_users = self.api.get_list("User",
                                          filters=[
                                              ["name", "not in", [
                                                  "Administrator", "Guest"]],
                                              ["enabled", "=", 1]
                                          ],
                                          fields=["name", "email", "first_name", "last_name", "mobile_no", "phone"])

            self.users = all_users
            logger.info(
                f"✅ Found {len(self.users)} users available for employee creation")

            if not self.users:
                logger.error("❌ No users found! Please create users first.")
                return False

            return True

        except Exception as e:
            logger.error(f"Error fetching users: {str(e)}")
            return False

    def _fetch_master_data(self):
        """Fetch all required master data from ERPNext"""
        logger.info("📊 Fetching master data...")

        # Fetch Branches
        try:
            branches = self.api.get_list("Branch", fields=["name", "branch"])
            self.master_data['branches'] = [branch.get(
                "name") for branch in branches if branch.get("name")]
            logger.info(
                f"✅ Branches: {len(self.master_data['branches'])} found")
            if self.master_data['branches']:
                logger.info(f"   Sample: {self.master_data['branches'][:3]}")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch branches: {e}")

        # Fetch Employee Grades
        try:
            grades = self.api.get_list("Employee Grade", fields=["name"])
            self.master_data['employee_grades'] = [
                grade.get("name") for grade in grades if grade.get("name")]
            logger.info(
                f"✅ Employee Grades: {len(self.master_data['employee_grades'])} found")
            if self.master_data['employee_grades']:
                logger.info(
                    f"   Sample: {self.master_data['employee_grades'][:3]}")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch employee grades: {e}")

        # Fetch Departments
        try:
            departments = self.api.get_list("Department",
                                            filters={"company": COMPANY_NAME},
                                            fields=["name", "department_name"])
            self.master_data['departments'] = [
                dept.get("name") for dept in departments if dept.get("name")]
            logger.info(
                f"✅ Departments: {len(self.master_data['departments'])} found")
            if self.master_data['departments']:
                logger.info(
                    f"   Sample: {self.master_data['departments'][:3]}")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch departments: {e}")

        # Fetch Employment Types
        try:
            emp_types = self.api.get_list("Employment Type", fields=["name"])
            self.master_data['employment_types'] = [emp_type.get(
                "name") for emp_type in emp_types if emp_type.get("name")]
            logger.info(
                f"✅ Employment Types: {len(self.master_data['employment_types'])} found")
            if self.master_data['employment_types']:
                logger.info(
                    f"   Sample: {self.master_data['employment_types'][:3]}")
        except Exception as e:
            logger.warning(
                f"⚠️ Could not fetch employment types from API: {e}")
            # Use hardcoded list as fallback
            logger.info("📋 Using hardcoded employment types as fallback...")
            self.master_data['employment_types'] = [
                "Apprentice",
                "Intern",
                "Piecework Commission",
                "Contract",
                "Probation",
                "Part-time",
                "Full-time"
            ]
            logger.info(
                f"✅ Employment Types (fallback): {len(self.master_data['employment_types'])} available")

        # Fetch Designations
        try:
            designations = self.api.get_list("Designation", fields=["name"])
            self.master_data['designations'] = [
                des.get("name") for des in designations if des.get("name")]
            logger.info(
                f"✅ Designations: {len(self.master_data['designations'])} found")
            if self.master_data['designations']:
                logger.info(
                    f"   Sample: {self.master_data['designations'][:3]}")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch designations: {e}")
            # Provide fallback designations if API fails
            logger.info("📋 Using common designations as fallback...")
            self.master_data['designations'] = [
                "Manager",
                "Assistant Manager",
                "Senior Executive",
                "Executive",
                "Junior Executive",
                "Analyst",
                "Senior Analyst",
                "Specialist"
            ]
            logger.info(
                f"✅ Designations (fallback): {len(self.master_data['designations'])} available")

        # Check if we have enough master data
        missing_data = []
        if not self.master_data['branches']:
            missing_data.append("Branches")
        if not self.master_data['employee_grades']:
            missing_data.append("Employee Grades")
        if not self.master_data['departments']:
            missing_data.append("Departments")
        # Employment types and designations now have fallbacks, so they're always available

        if missing_data:
            logger.warning(
                f"⚠️ Missing master data: {', '.join(missing_data)}")
            logger.warning(
                "Employees will be created with available data only")
        else:
            logger.info(
                "✅ All required master data available (including fallbacks)!")

    def generate_random_date_in_range(self, start_year: int, end_year: int) -> str:
        """Generate random date within year range"""
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)

        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)

        return random_date.strftime("%Y-%m-%d")

    def generate_phone_number(self) -> str:
        """Generate valid Indonesian phone number"""
        return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"

    def check_existing_employees(self):
        """Check existing employees and determine how many to create"""
        logger.info("📊 Checking existing employees...")

        try:
            existing_employees = self.api.get_list("Employee",
                                                   filters={
                                                       "company": COMPANY_NAME, "status": "Active"},
                                                   fields=["name", "employee_name", "user_id"])

            current_employee_count = len(existing_employees)
            self.employees = existing_employees

            logger.info(f"Current employees: {current_employee_count}")
            logger.info(f"Target employees: {TARGET_EMPLOYEES}")

            if current_employee_count >= TARGET_EMPLOYEES:
                logger.info(
                    f"Already have {current_employee_count} employees (>= target {TARGET_EMPLOYEES}). Skipping new employee creation.")
                return 0

            employees_to_create = TARGET_EMPLOYEES - current_employee_count
            logger.info(
                f"Need to create {employees_to_create} employees to reach target {TARGET_EMPLOYEES}")

            return employees_to_create

        except Exception as e:
            logger.error(f"Error checking existing employees: {str(e)}")
            return TARGET_EMPLOYEES

    def get_available_users_for_employees(self):
        """Get users that are not already linked to employees"""
        logger.info("👥 Finding users available for employee creation...")

        # Get list of user IDs already linked to employees
        linked_user_ids = set()
        for employee in self.employees:
            if employee.get("user_id"):
                linked_user_ids.add(employee["user_id"])

        # Filter users that are not yet linked to employees
        available_users = [
            user for user in self.users if user["name"] not in linked_user_ids]

        logger.info(
            f"Found {len(available_users)} users available for employee creation")
        logger.info(
            f"(out of {len(self.users)} total users, {len(linked_user_ids)} already linked)")

        return available_users

    def create_employees(self):
        """Create employee records with master data"""
        logger.info("🚀 Starting employee creation with master data...")

        # Check how many employees we need to create
        employees_to_create = self.check_existing_employees()

        if employees_to_create <= 0:
            logger.info("No new employees need to be created.")
            return

        # Get available users
        available_users = self.get_available_users_for_employees()

        if not available_users:
            logger.error("❌ No available users found for employee creation!")
            return

        if len(available_users) < employees_to_create:
            logger.warning(
                f"⚠️ Only {len(available_users)} users available, but need {employees_to_create} employees")
            employees_to_create = len(available_users)

        print("\n" + "="*60)
        print("👥 Creating Employees with Master Data")
        print("="*60)

        employees_created_count = 0
        random.shuffle(available_users)  # Randomize user selection

        for i in range(employees_to_create):
            user = available_users[i]

            # Extract user data
            first_name = user.get("first_name", "")
            last_name = user.get("last_name", "")
            full_name = f"{first_name} {last_name}".strip()
            email = user.get("email", "")
            mobile_no = user.get("mobile_no") or user.get(
                "phone") or self.generate_phone_number()

            # Generate employee-specific data
            employee_name = full_name if full_name else f"Employee {i+1:03d}"
            gender = random.choice(["Male", "Female"])
            date_of_birth = self.generate_random_date_in_range(
                BIRTH_YEAR_START, BIRTH_YEAR_END)
            date_of_joining = self.generate_random_date_in_range(
                JOIN_YEAR_START, JOIN_YEAR_END)

            # Check if employee already exists
            if self.api.check_exists("Employee", employee_name):
                logger.debug(
                    f"Employee '{employee_name}' already exists, skipping...")
                continue

            # Prepare employee data with master data
            employee_data = {
                "employee_name": employee_name,
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "date_of_birth": date_of_birth,
                "date_of_joining": date_of_joining,
                "company": COMPANY_NAME,
                "status": "Active",
                "user_id": user["name"],  # Link to the user
                "cell_number": mobile_no,
                "personal_email": email,
                "prefered_contact_email": "Personal Email"
            }

            # Add master data fields (randomly selected from available options)
            master_data_status = {}

            if self.master_data['departments']:
                employee_data["department"] = random.choice(
                    self.master_data['departments'])
                master_data_status["Department"] = "✅"
            else:
                master_data_status["Department"] = "❌"

            if self.master_data['employment_types']:
                employee_data["employment_type"] = random.choice(
                    self.master_data['employment_types'])
                master_data_status["Employment Type"] = "✅"
            else:
                master_data_status["Employment Type"] = "❌"

            if self.master_data['designations']:
                employee_data["designation"] = random.choice(
                    self.master_data['designations'])
                master_data_status["Designation"] = "✅"
            else:
                master_data_status["Designation"] = "❌"

            if self.master_data['branches']:
                employee_data["branch"] = random.choice(
                    self.master_data['branches'])
                master_data_status["Branch"] = "✅"
            else:
                master_data_status["Branch"] = "❌"

            if self.master_data['employee_grades']:
                employee_data["grade"] = random.choice(
                    self.master_data['employee_grades'])
                master_data_status["Grade"] = "✅"
            else:
                master_data_status["Grade"] = "❌"

            # Reports to is left null as requested

            try:
                employee = self.api.create_doc("Employee", employee_data)
                employees_created_count += 1

                print(
                    f"✅ {employees_created_count}/{employees_to_create}: {employee_name}")
                print(f"   👤 User: {user['name']}")
                print(f"   📧 Email: {email}")
                print(f"   🎂 Birth: {date_of_birth}")
                print(f"   📅 Join: {date_of_joining}")

                # Show assigned master data
                print(f"   📊 Master Data:")
                for field, status in master_data_status.items():
                    if status == "✅":
                        field_value = employee_data.get(
                            field.lower().replace(" ", "_"), "N/A")
                        print(f"      {status} {field}: {field_value}")
                    else:
                        print(f"      {status} {field}: Not available")
                print()

                # Small delay
                time.sleep(0.3)

            except Exception as e:
                logger.error(
                    f"❌ Failed to create employee '{employee_name}': {str(e)}")
                print(
                    f"❌ {employees_created_count+1}/{employees_to_create}: {employee_name} - FAILED")
                print(f"   Error: {str(e)[:100]}...")
                print()

        # Summary
        print("="*60)
        print("📊 SUMMARY")
        print("="*60)
        print(
            f"✅ Employees Created: {employees_created_count}/{employees_to_create}")
        print(f"📊 Master Data Used:")
        print(f"   🏢 Branches: {len(self.master_data['branches'])}")
        print(
            f"   📊 Employee Grades: {len(self.master_data['employee_grades'])}")
        print(f"   🏛️ Departments: {len(self.master_data['departments'])}")
        print(
            f"   💼 Employment Types: {len(self.master_data['employment_types'])}")
        print(f"   🎯 Designations: {len(self.master_data['designations'])}")

        logger.info(
            f"Successfully created {employees_created_count} employees with master data")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("👥 ERPNext Employee Generator with Master Data")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🎯 Target Employees: {TARGET_EMPLOYEES}")
        print(f"📅 Birth Year Range: {BIRTH_YEAR_START} - {BIRTH_YEAR_END}")
        print(f"📅 Join Year Range: {JOIN_YEAR_START} - {JOIN_YEAR_END}")
        print("=" * 80)

        try:
            # Check if we have users to work with
            if not self.users:
                print("❌ No users found! Please create users first.")
                return

            # Create employees
            self.create_employees()

            print(f"\n🎉 EMPLOYEE GENERATION COMPLETED!")

        except Exception as e:
            logger.error(f"Fatal error during employee generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Employee Generation with Master Data...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    response = input(
        f"\nThis will create up to {TARGET_EMPLOYEES} employees using existing master data. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = EmployeeGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Employee Generator with Master Data (Independent of Users)
Generates employees without requiring existing users - creates standalone employee records.
FIXED: Ensures joining dates are before June 2025 to allow attendance creation.
Uses environment variables from .env file for configuration.
Author: ERPNext Employee Generator
Version: 3.0.0 (Independent Employee Creation)
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
TARGET_EMPLOYEES = 1000

# Date ranges - FIXED FOR ATTENDANCE COMPATIBILITY
BIRTH_YEAR_START = 1990
BIRTH_YEAR_END = 2005
# CRITICAL FIX: All employees must join before June 2025 for attendance to work
JOIN_YEAR_START = 2020
JOIN_YEAR_END = 2024  # Changed from 2025 to 2024
# Alternative: Join early 2025 but definitely before June
JOIN_EARLY_2025_START = datetime(2024, 6, 1)  # Start from mid-2024
JOIN_EARLY_2025_END = datetime(2025, 5, 31)   # End before June 2025

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
    """Generates employee records using existing master data - independent of users"""

    def __init__(self):
        self.fake = Faker('id_ID')
        self.api = ERPNextAPI()
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
        self._fetch_master_data()

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
                "Specialist",
                "Team Lead",
                "Senior Developer",
                "Developer",
                "Junior Developer",
                "Coordinator",
                "Supervisor",
                "Officer"
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

    def generate_early_joining_date(self) -> str:
        """
        Generate joining date that's definitely before June 2025
        This ensures attendance can be created for June 2025 onwards
        """
        # Calculate days between start and end dates
        days_between = (JOIN_EARLY_2025_END - JOIN_EARLY_2025_START).days
        random_days = random.randint(0, days_between)
        random_date = JOIN_EARLY_2025_START + timedelta(days=random_days)

        # Double-check: ensure date is before June 2025
        june_2025 = datetime(2025, 6, 1)
        if random_date >= june_2025:
            # Force date to be in early 2025
            random_date = datetime(
                2025, random.randint(1, 5), random.randint(1, 28))

        return random_date.strftime("%Y-%m-%d")

    def generate_phone_number(self) -> str:
        """Generate valid Indonesian phone number"""
        return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"

    def generate_employee_id(self, index: int) -> str:
        """Generate unique employee ID"""
        return f"{COMPANY_ABBR}-EMP-{index:05d}"

    def generate_email(self, first_name: str, last_name: str, index: int) -> str:
        """Generate email address for employee"""
        # Clean names for email
        first_clean = first_name.lower().replace(" ", "")
        last_clean = last_name.lower().replace(" ", "")

        # Try different email formats
        email_formats = [
            f"{first_clean}.{last_clean}@company.com",
            f"{first_clean}{last_clean}@company.com",
            f"{first_clean}.{last_clean}{index}@company.com",
            f"{first_clean[0]}{last_clean}@company.com",
            f"{first_clean}{last_clean[0]}@company.com"
        ]

        return random.choice(email_formats)

    def check_existing_employees(self):
        """Check existing employees and determine how many to create"""
        logger.info("📊 Checking existing employees...")

        try:
            existing_employees = self.api.get_list("Employee",
                                                   filters={
                                                       "company": COMPANY_NAME, "status": "Active"},
                                                   fields=["name", "employee_name"])

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

    def create_employees(self):
        """Create employee records with generated data"""
        logger.info(
            "🚀 Starting independent employee creation with master data...")
        logger.info(
            "🎯 FIXED: All joining dates will be before June 2025 for attendance compatibility")

        # Check how many employees we need to create
        employees_to_create = self.check_existing_employees()

        if employees_to_create <= 0:
            logger.info("No new employees need to be created.")
            return

        print("\n" + "="*60)
        print("👥 Creating Independent Employees with Master Data (ATTENDANCE COMPATIBLE)")
        print("="*60)
        print("🎯 All joining dates will be before June 2025")
        print("✅ This ensures attendance can be created for June 2025 onwards")
        print("🔄 Employees created independently without requiring existing users")
        print("="*60)

        employees_created_count = 0
        # Start from current employee count + 1
        start_index = len(self.employees) + 1

        for i in range(employees_to_create):
            current_index = start_index + i

            # Generate employee data using Faker
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            full_name = f"{first_name} {last_name}"

            # Generate other personal data
            gender = random.choice(["Male", "Female"])
            date_of_birth = self.generate_random_date_in_range(
                BIRTH_YEAR_START, BIRTH_YEAR_END)

            # CRITICAL FIX: Use the new method to ensure early joining dates
            date_of_joining = self.generate_early_joining_date()

            # Generate contact information
            mobile_no = self.generate_phone_number()
            email = self.generate_email(first_name, last_name, current_index)
            employee_id = self.generate_employee_id(current_index)

            # Verify the joining date is indeed before June 2025
            join_date_obj = datetime.strptime(date_of_joining, "%Y-%m-%d")
            june_2025 = datetime(2025, 6, 1)
            if join_date_obj >= june_2025:
                logger.warning(
                    f"⚠️ Generated join date {date_of_joining} is not before June 2025, fixing...")
                # Force to early 2025
                date_of_joining = f"2025-{random.randint(1, 5):02d}-{random.randint(1, 28):02d}"
                logger.info(f"✅ Fixed join date to: {date_of_joining}")

            # Check if employee already exists
            if self.api.check_exists("Employee", full_name):
                logger.debug(
                    f"Employee '{full_name}' already exists, trying with different name...")
                # Try with employee ID instead
                full_name = f"{first_name} {last_name} {current_index}"

            # Prepare employee data with master data
            employee_data = {
                "employee_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "date_of_birth": date_of_birth,
                "date_of_joining": date_of_joining,
                "company": COMPANY_NAME,
                "status": "Active",
                "cell_number": mobile_no,
                "personal_email": email,
                "prefered_contact_email": "Personal Email",
                "employee_number": employee_id
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

            try:
                employee = self.api.create_doc("Employee", employee_data)
                employees_created_count += 1

                print(
                    f"✅ {employees_created_count}/{employees_to_create}: {full_name}")
                print(f"   🆔 ID: {employee_id}")
                print(f"   📧 Email: {email}")
                print(f"   📱 Mobile: {mobile_no}")
                print(f"   🎂 Birth: {date_of_birth}")
                print(f"   📅 Join: {date_of_joining} ✅ (Before June 2025)")

                # Show assigned master data
                print(f"   📊 Master Data:")
                for field, status in master_data_status.items():
                    if status == "✅":
                        field_key = field.lower().replace(" ", "_")
                        # Map field names to actual data keys
                        field_mapping = {
                            "employment_type": "employment_type",
                            "grade": "grade",
                            "branch": "branch",
                            "department": "department",
                            "designation": "designation"
                        }
                        field_value = employee_data.get(
                            field_mapping.get(field_key, field_key), "N/A")
                        print(f"      {status} {field}: {field_value}")
                    else:
                        print(f"      {status} {field}: Not available")
                print()

                # No delay for faster processing
                # time.sleep(0.3)  # Removed for maximum speed

            except Exception as e:
                logger.error(
                    f"❌ Failed to create employee '{full_name}': {str(e)}")
                print(
                    f"❌ {employees_created_count+1}/{employees_to_create}: {full_name} - FAILED")
                print(f"   Error: {str(e)[:100]}...")
                print()

        # Summary
        print("="*60)
        print("📊 SUMMARY")
        print("="*60)
        print(
            f"✅ Employees Created: {employees_created_count}/{employees_to_create}")
        print(f"🎯 All joining dates are before June 2025 ✅")
        print(f"📅 This allows attendance creation for June 2025 onwards")
        print(f"🔄 All employees created independently without requiring users")
        print(f"📊 Master Data Used:")
        print(f"   🏢 Branches: {len(self.master_data['branches'])}")
        print(
            f"   📊 Employee Grades: {len(self.master_data['employee_grades'])}")
        print(f"   🏛️ Departments: {len(self.master_data['departments'])}")
        print(
            f"   💼 Employment Types: {len(self.master_data['employment_types'])}")
        print(f"   🎯 Designations: {len(self.master_data['designations'])}")

        logger.info(
            f"Successfully created {employees_created_count} independent employees with master data")
        logger.info(
            "🎯 All employees have joining dates before June 2025 - attendance creation will work!")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print(
            "👥 ERPNext Independent Employee Generator with Master Data (ATTENDANCE FIXED)")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🎯 Target Employees: {TARGET_EMPLOYEES}")
        print(f"📅 Birth Year Range: {BIRTH_YEAR_START} - {BIRTH_YEAR_END}")
        print(
            f"📅 Join Date Range: {JOIN_EARLY_2025_START.strftime('%Y-%m-%d')} to {JOIN_EARLY_2025_END.strftime('%Y-%m-%d')}")
        print("🎯 FIXED: All joining dates will be before June 2025")
        print("✅ This ensures attendance can be created without validation errors")
        print("🔄 No dependency on existing users - creates standalone employees")
        print("=" * 80)

        try:
            # Create employees
            self.create_employees()

            print(f"\n🎉 EMPLOYEE GENERATION COMPLETED!")
            print("✅ All employees now have joining dates before June 2025")
            print("📅 Attendance creation for June 2025 should work without errors")
            print("🔄 All employees created independently without user dependencies")

        except Exception as e:
            logger.error(f"Fatal error during employee generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Independent Employee Generation with Master Data (ATTENDANCE FIXED)...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"\n🎯 This script fixes the attendance date validation error!")
    print(f"✅ All employees will have joining dates before June 2025")
    print(f"📅 This allows attendance creation for June 2025 onwards")
    print(f"🔄 Creates employees independently without requiring existing users")

    response = input(
        f"\nThis will create up to {TARGET_EMPLOYEES} independent employees using existing master data. Continue? (yes/no): ")
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

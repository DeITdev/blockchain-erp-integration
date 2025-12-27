#!/usr/bin/env python3
"""
ERPNext Employee Generator
Generates employees with master data.
"""

import requests
import json
import random
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, List, Any, Optional
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

fake = Faker('id_ID')

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")
COMPANY_ABBR = os.getenv("COMPANY_ABBR")

BIRTH_YEAR_START = 1945  # Age 80 (2025)
BIRTH_YEAR_END = 2010    # Age 15 (2025)
JOIN_EARLY_2025_START = datetime(2024, 6, 1)
JOIN_EARLY_2025_END = datetime(2025, 5, 31)

EMPLOYMENT_TYPES = [
    "Apprentice",
    "Intern",
    "Piecework",
    "Commission",
    "Contract",
    "Probation",
    "Part-time",
    "Full-time"
]

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
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if retry_count < 3:
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        """Get list of documents"""
        params = {"limit_page_length": 500}
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
            self._make_request("GET", f"resource/{doctype}/{name}")
            return True
        except:
            return False


class EmployeeGenerator:
    """Generates employee records"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.master_data = {'branches': [], 'employee_grades': [
        ], 'departments': [], 'designations': []}
        self.created_count = 0
        self.failed_count = 0
        self.age_count = {'75_79': 0, '80_plus': 0}  # Track older employees
        self._fetch_master_data()

    def _fetch_master_data(self):
        """Fetch all required master data"""
        logger.info("Fetching master data...")
        try:
            branches = self.api.get_list("Branch", fields=["name"])
            self.master_data['branches'] = [
                b.get("name") for b in branches if b.get("name")]
        except:
            pass
        try:
            grades = self.api.get_list("Employee Grade", fields=["name"])
            self.master_data['employee_grades'] = [
                g.get("name") for g in grades if g.get("name")]
        except:
            pass
        try:
            depts = self.api.get_list("Department", filters={
                                      "company": COMPANY_NAME}, fields=["name"])
            self.master_data['departments'] = [
                d.get("name") for d in depts if d.get("name")]
        except:
            pass
        try:
            desigs = self.api.get_list("Designation", fields=["name"])
            self.master_data['designations'] = [
                d.get("name") for d in desigs if d.get("name")]
        except:
            self.master_data['designations'] = [
                "Manager", "Executive", "Developer", "Analyst", "Coordinator"]

    def generate_random_date_in_range(self, start_year: int, end_year: int) -> str:
        """Generate random date within year range with weighted distribution
        Rules:
        - 15-54 years old: Dominant (most employees)
        - 55-74 years old: Moderate (fewer employees)
        - 75-79 years old: Max 10 people
        - 80+ years old: Max 10 people
        """
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        days_between = (end_date - start_date).days

        # Use cubic power for much stronger bias to younger ages (15-54)
        # This heavily skews toward recent birth years
        random_factor = random.random() ** 3.5
        random_days = int(random_factor * days_between)

        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime("%Y-%m-%d")

    def calculate_age(self, dob_str: str) -> int:
        """Calculate age from date of birth string"""
        dob = datetime.strptime(dob_str, "%Y-%m-%d")
        today = datetime(2025, 10, 29)
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

    def is_age_allowed(self, dob_str: str) -> bool:
        """Check if age meets the requirements:
        - 75-79: Max 10 people
        - 80+: Max 10 people
        """
        age = self.calculate_age(dob_str)

        if 75 <= age <= 79:
            if self.age_count['75_79'] >= 10:
                return False
            self.age_count['75_79'] += 1
            return True
        elif age >= 80:
            if self.age_count['80_plus'] >= 10:
                return False
            self.age_count['80_plus'] += 1
            return True

        return True

    def generate_joining_date(self) -> str:
        """Generate joining date before June 2025"""
        days_between = (JOIN_EARLY_2025_END - JOIN_EARLY_2025_START).days
        random_days = random.randint(0, days_between)
        random_date = JOIN_EARLY_2025_START + timedelta(days=random_days)
        june_2025 = datetime(2025, 6, 1)
        if random_date >= june_2025:
            random_date = datetime(
                2025, random.randint(1, 5), random.randint(1, 28))
        return random_date.strftime("%Y-%m-%d")

    def generate_phone_number(self) -> str:
        """Generate Indonesian phone number"""
        return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"

    def generate_employee_id(self, index: int) -> str:
        """Generate unique employee ID"""
        return f"{COMPANY_ABBR}-EMP-{index:05d}"

    def generate_email(self, first_name: str, last_name: str, index: int) -> str:
        """Generate email address"""
        first_clean = first_name.lower().replace(" ", "")
        last_clean = last_name.lower().replace(" ", "")
        email_formats = [
            f"{first_clean}.{last_clean}@company.com",
            f"{first_clean}{last_clean}@company.com",
            f"{first_clean}.{last_clean}{index}@company.com",
            f"{first_clean[0]}{last_clean}@company.com",
        ]
        return random.choice(email_formats)

    def create_employees(self, num_to_create: int = 10):
        """Create employee records"""
        employees_to_create = max(0, num_to_create)

        if employees_to_create <= 0:
            logger.info("No employees to create")
            return

        logger.info(f"Creating {employees_to_create} employees...")

        for i in range(employees_to_create):
            try:
                emp_index = i + 1
                first_name = fake.first_name()
                last_name = fake.last_name()

                # Generate DOB and validate age constraints
                dob = self.generate_random_date_in_range(
                    BIRTH_YEAR_START, BIRTH_YEAR_END)
                retry_count = 0
                while not self.is_age_allowed(dob) and retry_count < 10:
                    dob = self.generate_random_date_in_range(
                        BIRTH_YEAR_START, BIRTH_YEAR_END)
                    retry_count += 1

                if retry_count >= 10:
                    # Skip this employee if we can't find a valid age (old age limits reached)
                    logger.info(
                        f"Skipping {first_name} {last_name} - age limits reached for 75+")
                    continue

                employee_data = {
                    "employee": self.generate_employee_id(emp_index),
                    "first_name": first_name,
                    "last_name": last_name,
                    "employee_name": f"{first_name} {last_name}",
                    "gender": random.choice(["Male", "Female"]),
                    "date_of_birth": dob,
                    "date_of_joining": self.generate_joining_date(),
                    "company": COMPANY_NAME,
                    "email": self.generate_email(first_name, last_name, emp_index),
                    "phone_number": self.generate_phone_number(),
                    "employment_type": random.choice(EMPLOYMENT_TYPES),
                    "status": "Active"
                }

                if self.master_data['departments']:
                    employee_data["department"] = random.choice(
                        self.master_data['departments'])
                if self.master_data['employee_grades']:
                    employee_data["grade"] = random.choice(
                        self.master_data['employee_grades'])
                if self.master_data['branches']:
                    employee_data["branch"] = random.choice(
                        self.master_data['branches'])
                if self.master_data['designations']:
                    employee_data["designation"] = random.choice(
                        self.master_data['designations'])

                self.api.create_doc("Employee", employee_data)
                self.created_count += 1
                logger.info(
                    f"Created {i+1}/{employees_to_create}: {first_name} {last_name}")

            except Exception as e:
                self.failed_count += 1
                logger.error(f"Failed to create employee: {str(e)[:100]}")

    def run(self, num_to_create: int = 10):
        """Main execution"""
        logger.info("Starting employee creation...")
        self.create_employees(num_to_create)
        logger.info(f"Created: {self.created_count}")
        logger.info(f"Failed: {self.failed_count}")


if __name__ == "__main__":
    try:
        while True:
            try:
                num_employees = int(
                    input("How many employees to create? (1-1000): "))
                if 1 <= num_employees <= 1000:
                    break
                else:
                    logger.error("Please enter a number between 1 and 1000")
            except ValueError:
                logger.error("Invalid input. Please enter a number")

        response = input(
            f"Confirming creation of {num_employees} employees. Type 'CREATE' to confirm: ")

        if response != "CREATE":
            logger.info("Operation cancelled")
            sys.exit(0)

        generator = EmployeeGenerator()
        generator.run(num_employees)
    except KeyboardInterrupt:
        logger.info("Operation interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

#!/usr/bin/env python3
"""
ERPNext Employee Creator from API Data (FAST VERSION - NO DELAYS)
Fetches user data from external API and creates employee records in ERPNext.
Uses environment variables from .env file for configuration.
Author: ERPNext Employee API Creator
Version: 2.0.0 (Fast)
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
from logging import StreamHandler

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    # File is at: API/fiyansa-data/employee-attendance/send_employee.py
    # .env is at: API/.env
    # Need to go up 2 directories from the file's directory (3 from the file itself)
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

# External API Configuration
EXTERNAL_API_URL = "https://pre.fiyansa.com/api/user-get?limit=100"

# Date ranges
BIRTH_YEAR_START = 1990
BIRTH_YEAR_END = 2005
# All employees must join before June 2025 for attendance compatibility
JOIN_EARLY_2025_START = datetime(2024, 6, 1)  # Start from mid-2024
JOIN_EARLY_2025_END = datetime(2025, 5, 31)   # End before June 2025

# Retry settings - REDUCED FOR SPEED
RETRY_ATTEMPTS = 1  # Reduced from 3
RETRY_DELAY = 0.5  # Reduced from 2 seconds

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set console handler encoding to handle unicode characters
for handler in logger.handlers:
    if isinstance(handler, StreamHandler):
        handler.stream.reconfigure(encoding='utf-8', errors='replace')


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

        logger.info(f"🔗 ERPNext API: {self.base_url}")
        logger.info(f"🏢 Company: {COMPANY_NAME}")
        logger.info(f"🔑 Key: {API_KEY[:8] if API_KEY else 'None'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with minimal retry logic for speed"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed, retrying... ({retry_count + 1}/{RETRY_ATTEMPTS})")
                # NO SLEEP for maximum speed
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(f"Request failed: {str(e)[:100]}")
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

    def check_exists(self, doctype: str, email: str) -> bool:
        """Check if employee with email already exists"""
        try:
            result = self.get_list(doctype, filters={
                                   "personal_email": email, "company": COMPANY_NAME}, fields=["name"])
            return len(result) > 0
        except Exception:
            return False


class ExternalAPIClient:
    """Handles external API calls to fetch user data"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

    def fetch_users(self) -> List[Dict]:
        """Fetch users from external API"""
        logger.info(f"🌐 Fetching users from external API: {EXTERNAL_API_URL}")

        try:
            response = self.session.get(EXTERNAL_API_URL)
            response.raise_for_status()

            data = response.json()

            # Handle different response structures
            if isinstance(data, list):
                users = data
            elif isinstance(data, dict):
                if 'data' in data:
                    users = data['data']
                elif 'users' in data:
                    users = data['users']
                elif 'results' in data:
                    users = data['results']
                else:
                    users = [data] if data else []
            else:
                logger.error("❌ Unexpected API response format")
                return []

            logger.info(f"✅ Found {len(users)} users from external API")
            return users

        except Exception as e:
            logger.error(f"❌ Error fetching from external API: {str(e)}")
            return []


class EmployeeCreator:
    """Creates employees in ERPNext from external API data"""

    def __init__(self):
        self.fake = Faker('id_ID')
        self.erpnext_api = ERPNextAPI()
        self.external_api = ExternalAPIClient()

    def generate_random_date_in_range(self, start_year: int, end_year: int) -> str:
        """Generate random date within year range"""
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)

        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)

        return random_date.strftime("%Y-%m-%d")

    def generate_early_joining_date(self) -> str:
        """Generate joining date that's definitely before June 2025"""
        days_between = (JOIN_EARLY_2025_END - JOIN_EARLY_2025_START).days
        random_days = random.randint(0, days_between)
        random_date = JOIN_EARLY_2025_START + timedelta(days=random_days)

        # Ensure date is before June 2025
        june_2025 = datetime(2025, 6, 1)
        if random_date >= june_2025:
            random_date = datetime(
                2025, random.randint(1, 5), random.randint(1, 28))

        return random_date.strftime("%Y-%m-%d")

    def generate_phone_number(self) -> str:
        """Generate Indonesian phone number"""
        return self.fake.phone_number()

    def create_employees_from_api(self):
        """Create employees from external API data - FAST VERSION"""
        # Fetch users from external API
        users = self.external_api.fetch_users()

        if not users:
            logger.error("❌ No users fetched from external API")
            return

        # Get existing employees in one batch for speed
        existing_employees = self.erpnext_api.get_list(
            "Employee",
            filters={"company": COMPANY_NAME},
            fields=["personal_email"]
        )
        existing_emails = {emp.get("personal_email")
                           for emp in existing_employees if emp.get("personal_email")}

        print("="*80)
        print(f"📝 Creating {len(users)} Employees (FAST MODE - NO DELAYS)")
        print("="*80)

        employees_created_count = 0
        employees_skipped_count = 0
        employees_failed_count = 0

        # Process counter for progress display
        process_counter = 0

        for i, user in enumerate(users):
            try:
                # Extract user data
                name = user.get("name", "").strip()
                email = user.get("email", "").strip()

                if not name or not email:
                    employees_skipped_count += 1
                    continue

                # Check if employee already exists
                if email in existing_emails or self.erpnext_api.check_exists("Employee", email):
                    employees_skipped_count += 1
                    continue

                # Generate random employee data
                gender = random.choice(["Male", "Female"])
                date_of_birth = self.generate_random_date_in_range(
                    BIRTH_YEAR_START, BIRTH_YEAR_END)
                date_of_joining = self.generate_early_joining_date()
                mobile_no = self.generate_phone_number()

                # Prepare employee data
                employee_data = {
                    "employee_name": name,
                    "first_name": name.split()[0] if name.split() else name,
                    "last_name": " ".join(name.split()[1:]) if len(name.split()) > 1 else "",
                    "gender": gender,
                    "date_of_birth": date_of_birth,
                    "date_of_joining": date_of_joining,
                    "company": COMPANY_NAME,
                    "status": "Active",
                    "personal_email": email,
                    "cell_number": mobile_no,
                    "prefered_contact_email": "Personal Email"
                }

                # Create employee - NO DELAY
                employee = self.erpnext_api.create_doc(
                    "Employee", employee_data)
                employees_created_count += 1

                # Progress indicator - show every 10 employees for speed
                process_counter += 1
                if process_counter % 10 == 0 or process_counter == 1:
                    print(
                        f"⚡ Progress: {process_counter}/{len(users)} - Created: {employees_created_count}, Skipped: {employees_skipped_count}")

                # NO SLEEP - Maximum speed!

            except Exception as e:
                employees_failed_count += 1
                logger.error(
                    f"Failed: {user.get('name', 'Unknown')}: {str(e)[:50]}")

        # Final progress update
        print(
            f"⚡ Progress: {len(users)}/{len(users)} - Created: {employees_created_count}, Skipped: {employees_skipped_count}")

        # Summary
        print("="*80)
        print("📊 SUMMARY")
        print("="*80)
        print(f"✅ Employees Created: {employees_created_count}")
        print(f"⏭️ Employees Skipped: {employees_skipped_count}")
        print(f"❌ Employees Failed: {employees_failed_count}")
        print(f"📊 Total Processed: {len(users)}")
        print(f"⚡ FAST MODE: No delays used!")
        print(f"🎯 All joining dates are before June 2025 ✅")
        print("="*80)

        logger.info(
            f"Successfully created {employees_created_count} employees")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("👥 ERPNext Employee Creator - FAST MODE ⚡")
        print("=" * 80)
        print(f"📡 ERPNext API: {BASE_URL}")
        print(f"🌐 External API: {EXTERNAL_API_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"⚡ FAST MODE: No delays between records!")
        print("=" * 80)

        try:
            # Create employees from API data
            self.create_employees_from_api()

            print(f"\n🎉 EMPLOYEE CREATION COMPLETED!")
            print("✅ All employees have joining dates before June 2025")
            print("📅 You can now run send_attendance.py to sync attendance")

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Employee Creation (FAST MODE)...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        print("\n📋 Required .env file location: API/.env")
        return

    print(f"\n🌐 This script will fetch users from: {EXTERNAL_API_URL}")
    print(f"⚡ FAST MODE: No delays between employee creation")
    print(f"📊 Progress will be shown every 10 records")

    response = input(f"\nProceed with FAST employee creation? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        creator = EmployeeCreator()
        creator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

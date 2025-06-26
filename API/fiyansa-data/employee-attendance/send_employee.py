#!/usr/bin/env python3
"""
ERPNext Employee Creator from API Data
Fetches user data from external API and creates employee records in ERPNext.
Uses environment variables from .env file for configuration.
Author: ERPNext Employee API Creator
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
from faker import Faker
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent.parent / '.env'

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

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

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

    def check_exists(self, doctype: str, email: str) -> bool:
        """Check if employee with email already exists"""
        try:
            result = self.get_list(doctype, filters={
                                   "personal_email": email, "company": COMPANY_NAME}, fields=["name"])
            return len(result) > 0
        except Exception as e:
            logger.warning(f"Error checking if employee exists: {e}")
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
            logger.info(
                f"📊 API Response structure: {list(data.keys()) if isinstance(data, dict) else 'List response'}")

            # Handle different response structures
            if isinstance(data, list):
                users = data
            elif isinstance(data, dict):
                # Try common keys for user data
                if 'data' in data:
                    users = data['data']
                elif 'users' in data:
                    users = data['users']
                elif 'results' in data:
                    users = data['results']
                else:
                    # If no common key, assume the entire response is user data
                    users = [data] if data else []
            else:
                logger.error("❌ Unexpected API response format")
                return []

            logger.info(f"✅ Found {len(users)} users from external API")

            # Show sample data
            if users:
                logger.info("📋 Sample user data:")
                sample_user = users[0]
                logger.info(
                    f"   - Keys available: {list(sample_user.keys()) if isinstance(sample_user, dict) else 'Not a dict'}")
                logger.info(
                    f"   - Sample name: {sample_user.get('name', 'N/A')}")
                logger.info(
                    f"   - Sample email: {sample_user.get('email', 'N/A')}")

            return users

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching from external API: {str(e)}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parsing JSON response: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            return []


class EmployeeCreator:
    """Creates employees in ERPNext from external API data"""

    def __init__(self):
        self.fake = Faker('id_ID')
        self.erpnext_api = ERPNextAPI()
        self.external_api = ExternalAPIClient()
        self.employees = []

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

    def check_existing_employees(self):
        """Check existing employees in the company"""
        logger.info("📊 Checking existing employees in ERPNext...")

        try:
            existing_employees = self.erpnext_api.get_list("Employee",
                                                           filters={
                                                               "company": COMPANY_NAME, "status": "Active"},
                                                           fields=["name", "employee_name", "personal_email"])

            current_employee_count = len(existing_employees)
            self.employees = existing_employees

            logger.info(f"Current employees: {current_employee_count}")
            return existing_employees

        except Exception as e:
            logger.error(f"Error checking existing employees: {str(e)}")
            return []

    def create_employees_from_api(self):
        """Fetch users from API and create employees in ERPNext"""
        logger.info("🚀 Starting employee creation from external API...")

        # Step 1: Fetch users from external API
        users = self.external_api.fetch_users()

        if not users:
            logger.error("❌ No users fetched from external API")
            return

        logger.info(f"📊 Processing {len(users)} users from API")

        # Step 2: Check existing employees
        existing_employees = self.check_existing_employees()
        existing_emails = {emp.get("personal_email")
                           for emp in existing_employees if emp.get("personal_email")}

        print("\n" + "="*80)
        print("👥 Creating Employees from External API")
        print("="*80)
        print(f"🌐 Source API: {EXTERNAL_API_URL}")
        print(f"📊 Users to process: {len(users)}")
        print(f"👥 Existing employees: {len(existing_employees)}")
        print(f"🎯 All joining dates will be before June 2025")
        print("="*80)

        employees_created_count = 0
        employees_skipped_count = 0
        employees_failed_count = 0

        for i, user in enumerate(users):
            try:
                # Extract user data
                name = user.get("name", "").strip()
                email = user.get("email", "").strip()

                if not name or not email:
                    logger.warning(
                        f"⚠️ Skipping user {i+1}: Missing name or email")
                    employees_skipped_count += 1
                    continue

                # Check if employee already exists
                if email in existing_emails or self.erpnext_api.check_exists("Employee", email):
                    logger.debug(
                        f"⏭️ Employee with email {email} already exists, skipping...")
                    employees_skipped_count += 1
                    continue

                # Generate random employee data
                gender = random.choice(["Male", "Female"])
                date_of_birth = self.generate_random_date_in_range(
                    BIRTH_YEAR_START, BIRTH_YEAR_END)
                date_of_joining = self.generate_early_joining_date()
                mobile_no = self.generate_phone_number()

                # Verify the joining date is indeed before June 2025
                join_date_obj = datetime.strptime(date_of_joining, "%Y-%m-%d")
                june_2025 = datetime(2025, 6, 1)
                if join_date_obj >= june_2025:
                    logger.warning(
                        f"⚠️ Generated join date {date_of_joining} is not before June 2025, fixing...")
                    # Force to early 2025
                    date_of_joining = f"2025-{random.randint(1, 5):02d}-{random.randint(1, 28):02d}"
                    logger.info(f"✅ Fixed join date to: {date_of_joining}")

                # Prepare employee data with minimal required fields
                employee_data = {
                    "employee_name": name,
                    # First word as first name
                    "first_name": name.split()[0] if name.split() else name,
                    # Rest as last name
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

                # Create employee
                employee = self.erpnext_api.create_doc(
                    "Employee", employee_data)
                employees_created_count += 1

                print(f"✅ {employees_created_count}/{len(users)}: {name}")
                print(f"   📧 Email: {email}")
                print(f"   👤 Gender: {gender}")
                print(f"   🎂 Birth: {date_of_birth}")
                print(f"   📅 Join: {date_of_joining} ✅ (Before June 2025)")
                print(f"   📱 Mobile: {mobile_no}")
                print()

                # Small delay to avoid overwhelming the server
                time.sleep(0.3)

            except Exception as e:
                employees_failed_count += 1
                logger.error(
                    f"❌ Failed to create employee for {user.get('name', 'Unknown')}: {str(e)}")
                print(
                    f"❌ {i+1}/{len(users)}: {user.get('name', 'Unknown')} - FAILED")
                print(f"   Error: {str(e)[:100]}...")
                print()

        # Summary
        print("="*80)
        print("📊 SUMMARY")
        print("="*80)
        print(f"✅ Employees Created: {employees_created_count}")
        print(f"⏭️ Employees Skipped: {employees_skipped_count}")
        print(f"❌ Employees Failed: {employees_failed_count}")
        print(f"📊 Total Processed: {len(users)}")
        print(f"🎯 All joining dates are before June 2025 ✅")
        print(f"📅 This allows attendance creation for June 2025 onwards")

        logger.info(
            f"Successfully created {employees_created_count} employees from external API")
        logger.info(
            "🎯 All employees have joining dates before June 2025 - attendance creation will work!")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("👥 ERPNext Employee Creator from External API")
        print("=" * 80)
        print(f"📡 ERPNext API: {BASE_URL}")
        print(f"🌐 External API: {EXTERNAL_API_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"📅 Birth Year Range: {BIRTH_YEAR_START} - {BIRTH_YEAR_END}")
        print(
            f"📅 Join Date Range: {JOIN_EARLY_2025_START.strftime('%Y-%m-%d')} to {JOIN_EARLY_2025_END.strftime('%Y-%m-%d')}")
        print("🎯 All joining dates will be before June 2025")
        print("✅ This ensures attendance can be created without validation errors")
        print("=" * 80)

        try:
            # Create employees from API data
            self.create_employees_from_api()

            print(f"\n🎉 EMPLOYEE CREATION FROM API COMPLETED!")
            print("✅ All employees now have joining dates before June 2025")
            print("📅 Attendance creation for June 2025 should work without errors")

        except Exception as e:
            logger.error(f"Fatal error during employee creation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Employee Creation from External API...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    print(f"\n🌐 This script will fetch users from: {EXTERNAL_API_URL}")
    print(f"✅ All employees will have joining dates before June 2025")
    print(f"📅 This allows attendance creation for June 2025 onwards")
    print(f"📊 Only name and email will be taken from API, rest will be randomized")

    response = input(
        f"\nContinue with employee creation from external API? (yes/no): ")
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

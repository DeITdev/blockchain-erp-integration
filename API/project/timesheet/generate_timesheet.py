#!/usr/bin/env python3
"""
ERPNext Timesheet Generator
Creates 20 timesheets (one for each project) with realistic data.
Uses environment variables from .env file for configuration.
Author: ERPNext Timesheet Generator
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

# Timesheet Configuration
TARGET_TIMESHEETS = 20

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

        logger.info(f"Using API configuration:")
        logger.info(f"  Base URL: {self.base_url}")
        logger.info(f"  Company: {COMPANY_NAME}")
        logger.info(f"  API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")

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


class TimesheetGenerator:
    """Generates timesheet records with realistic data"""

    def __init__(self):
        self.fake = Faker()
        self.api = ERPNextAPI()
        self.timesheets = []
        self.projects = []
        self.employees = []
        self.activity_types = []

        # Fetch required data
        self._fetch_projects()
        self._fetch_employees()
        self._fetch_activity_types()

    def _fetch_projects(self):
        """Fetch existing projects from ERPNext"""
        logger.info("Fetching existing projects...")

        try:
            projects = self.api.get_list("Project",
                                         filters={"company": COMPANY_NAME},
                                         fields=["name", "project_name"])

            self.projects = projects
            logger.info(
                f"Found {len(self.projects)} projects for timesheet creation")

            if not self.projects:
                logger.error(
                    "❌ No projects found! Please create projects first.")
                return False

            # Show sample projects
            logger.info("Sample projects:")
            for i, proj in enumerate(self.projects[:3]):
                logger.info(
                    f"   {i+1}. {proj.get('project_name')} ({proj.get('name')})")

            if len(self.projects) > 3:
                logger.info(f"   ... and {len(self.projects) - 3} more")

            return True

        except Exception as e:
            logger.error(f"Error fetching projects: {str(e)}")
            return False

    def _fetch_employees(self):
        """Fetch existing employees from ERPNext"""
        logger.info("Fetching existing employees...")

        try:
            employees = self.api.get_list("Employee",
                                          filters={
                                              "company": COMPANY_NAME, "status": "Active"},
                                          fields=["name", "employee_name"])

            self.employees = employees
            logger.info(f"Found {len(self.employees)} active employees")

            if not self.employees:
                logger.error(
                    "❌ No active employees found! Please create employees first.")
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

    def _fetch_activity_types(self):
        """Fetch existing activity types from ERPNext"""
        logger.info("Fetching existing activity types...")

        try:
            activity_types = self.api.get_list("Activity Type",
                                               fields=["name", "activity_type"])

            self.activity_types = activity_types
            logger.info(f"Found {len(self.activity_types)} activity types")

            if not self.activity_types:
                logger.warning(
                    "⚠️ No activity types found! Will create timesheets without activity types.")

            # Show sample activity types
            if self.activity_types:
                logger.info("Sample activity types:")
                for i, act in enumerate(self.activity_types[:5]):
                    activity_name = act.get('activity_type') or act.get('name')
                    logger.info(f"   {i+1}. {activity_name}")

                if len(self.activity_types) > 5:
                    logger.info(
                        f"   ... and {len(self.activity_types) - 5} more")

            return True

        except Exception as e:
            logger.error(f"Error fetching activity types: {str(e)}")
            return False

    def generate_random_time(self) -> str:
        """Generate random datetime in YYYY-MM-DD HH:MM:SS format"""
        # Generate a recent date (within last 30 days)
        base_date = datetime.now() - timedelta(days=random.randint(1, 30))

        # Generate work hours (8 AM to 5 PM)
        hour = random.randint(8, 17)
        minute = random.choice([0, 15, 30, 45])  # Quarter hour intervals

        # Combine date and time
        datetime_obj = base_date.replace(
            hour=hour, minute=minute, second=0, microsecond=0)
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    def generate_to_time(self, from_time_str: str, hours: float) -> str:
        """Generate to_time based on from_time and hours"""
        try:
            from_time = datetime.strptime(from_time_str, "%Y-%m-%d %H:%M:%S")
            to_time = from_time + timedelta(hours=hours)
            return to_time.strftime("%Y-%m-%d %H:%M:%S")
        except:
            # Fallback if parsing fails
            return from_time_str

    def generate_random_hours(self) -> float:
        """Generate random working hours (0.5 to 8.0 hours)"""
        hours_options = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5,
                         4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0]
        return random.choice(hours_options)

    def generate_timesheet_details(self, project_list: List[Dict]) -> List[Dict]:
        """Generate timesheet detail rows"""
        # Random number of rows (1-5 activities per timesheet)
        num_rows = random.randint(1, 5)
        timesheet_details = []

        for i in range(num_rows):
            from_time = self.generate_random_time()
            hours = self.generate_random_hours()
            to_time = self.generate_to_time(from_time, hours)

            detail = {
                "from_time": from_time,
                "to_time": to_time,
                "hours": hours,
                # Random billable/non-billable
                "is_billable": random.choice([0, 1]),
                # Random project
                "project": random.choice(project_list)["name"]
            }

            # Add activity type if available
            if self.activity_types:
                activity = random.choice(self.activity_types)
                detail["activity_type"] = activity.get("name")

            timesheet_details.append(detail)

        return timesheet_details

    def check_existing_timesheets(self):
        """Check existing timesheets for the company"""
        logger.info("Checking existing timesheets...")

        try:
            existing_timesheets = self.api.get_list("Timesheet",
                                                    filters={
                                                        "company": COMPANY_NAME},
                                                    fields=["name", "employee"])

            current_timesheet_count = len(existing_timesheets)

            logger.info(f"Current timesheets: {current_timesheet_count}")
            logger.info(f"Target timesheets: {TARGET_TIMESHEETS}")

            if current_timesheet_count >= TARGET_TIMESHEETS:
                logger.info(
                    f"Already have {current_timesheet_count} timesheets (>= target {TARGET_TIMESHEETS}). Skipping new timesheet creation.")
                return 0

            timesheets_to_create = TARGET_TIMESHEETS - current_timesheet_count
            logger.info(
                f"Need to create {timesheets_to_create} timesheets to reach target {TARGET_TIMESHEETS}")

            return timesheets_to_create

        except Exception as e:
            logger.error(f"Error checking existing timesheets: {str(e)}")
            return TARGET_TIMESHEETS

    def create_timesheets(self):
        """Create timesheet records"""
        logger.info(f"Creating {TARGET_TIMESHEETS} timesheets...")

        # Check how many timesheets we need to create
        timesheets_to_create = self.check_existing_timesheets()

        if timesheets_to_create <= 0:
            logger.info("No new timesheets need to be created.")
            return

        print("\n" + "=" * 80)
        print("⏰ Creating Timesheets")
        print("=" * 80)
        print(f"📊 Target Timesheets: {TARGET_TIMESHEETS}")
        print(f"📁 Available Projects: {len(self.projects)}")
        print(f"👥 Available Employees: {len(self.employees)}")
        print(f"📋 Available Activity Types: {len(self.activity_types)}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print("=" * 80)

        created_timesheets = []

        # Ensure we have enough projects (limit to available projects)
        actual_timesheets_to_create = min(
            timesheets_to_create, len(self.projects))

        if actual_timesheets_to_create < timesheets_to_create:
            logger.warning(
                f"⚠️ Only {len(self.projects)} projects available, creating {actual_timesheets_to_create} timesheets instead of {timesheets_to_create}")

        for i in range(actual_timesheets_to_create):
            try:
                # Select random employee and project
                employee = random.choice(self.employees)
                project = self.projects[i] if i < len(
                    self.projects) else random.choice(self.projects)

                # Generate timesheet details (table rows)
                timesheet_details = self.generate_timesheet_details(
                    self.projects)

                # Create timesheet data
                timesheet_data = {
                    # Company - must be PT Fiyansa Mulya
                    "company": COMPANY_NAME,

                    # Employee - choose randomly
                    "employee": employee["name"],

                    # Project - choose randomly (one timesheet per project)
                    "project": project["name"],

                    # Customer - leave blank as requested

                    # Timesheet details (table)
                    "time_logs": timesheet_details
                }

                # Create timesheet
                timesheet_result = self.api.create_doc(
                    "Timesheet", timesheet_data)

                # Handle response
                timesheet_doc_id = None
                if isinstance(timesheet_result, dict):
                    timesheet_doc_id = (timesheet_result.get("name") or
                                        timesheet_result.get("data", {}).get("name") if isinstance(timesheet_result.get("data"), dict) else None or
                                        timesheet_result.get("message", {}).get("name") if isinstance(timesheet_result.get("message"), dict) else None)

                if timesheet_doc_id or timesheet_result:  # Count as created if we got any response
                    timesheet_info = {
                        "name": timesheet_doc_id or f"Timesheet-{i+1}",
                        "employee": employee.get("employee_name"),
                        "project": project.get("project_name"),
                        "activities": len(timesheet_details)
                    }
                    created_timesheets.append(timesheet_info)

                    # Calculate total hours
                    total_hours = sum(detail["hours"]
                                      for detail in timesheet_details)
                    billable_count = sum(
                        1 for detail in timesheet_details if detail["is_billable"])

                    logger.info(
                        f"✅ Created timesheet {i+1}/{actual_timesheets_to_create}")
                    logger.info(
                        f"   - Employee: {employee.get('employee_name')}")
                    logger.info(f"   - Project: {project.get('project_name')}")
                    logger.info(
                        f"   - Activities: {len(timesheet_details)} rows")
                    logger.info(f"   - Total Hours: {total_hours}")
                    logger.info(
                        f"   - Billable Activities: {billable_count}/{len(timesheet_details)}")

                    # Show activity details
                    for j, detail in enumerate(timesheet_details):
                        billable_indicator = "💰" if detail["is_billable"] else "⚪"
                        activity_name = detail.get(
                            "activity_type", "No Activity")
                        logger.info(
                            f"     {j+1}. {activity_name} - {detail['hours']}hrs {billable_indicator}")

                else:
                    logger.warning(
                        f"⚠️ Timesheet {i+1} created but no document ID returned")

            except Exception as e:
                logger.error(f"❌ Failed to create timesheet {i+1}: {str(e)}")

        # Final summary
        print("\n" + "=" * 80)
        print("📊 TIMESHEET CREATION SUMMARY")
        print("=" * 80)
        print(f"✅ Timesheets Created: {len(created_timesheets)}")
        print(
            f"📁 Projects Used: {len(set(ts['project'] for ts in created_timesheets))}")
        print(
            f"👥 Employees Used: {len(set(ts['employee'] for ts in created_timesheets))}")

        # Activity summary
        total_activities = sum(ts["activities"] for ts in created_timesheets)
        print(f"📋 Total Activities: {total_activities}")
        print(
            f"📊 Average Activities per Timesheet: {total_activities / len(created_timesheets) if created_timesheets else 0:.1f}")

        print("\n📋 Created Timesheets:")
        for i, ts in enumerate(created_timesheets, 1):
            print(
                f"   {i}. {ts['employee']} → {ts['project']} ({ts['activities']} activities)")

        print("=" * 80)

        logger.info(
            f"Successfully created {len(created_timesheets)} timesheets")
        return created_timesheets

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("⏰ ERPNext Timesheet Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🔑 Using API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")
        print(f"📊 Target Timesheets: {TARGET_TIMESHEETS}")
        print("=" * 80)

        try:
            # Check if we have required data
            if not self.projects:
                print("❌ No projects found! Please create projects first.")
                return
            if not self.employees:
                print("❌ No active employees found! Please create employees first.")
                return

            # Create timesheets
            timesheets = self.create_timesheets()

            print(f"\n🎉 TIMESHEET GENERATION COMPLETED!")
            print(
                f"📊 Total Timesheets: {len(timesheets) if timesheets else 0}")

        except Exception as e:
            logger.error(f"Fatal error during timesheet generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Timesheet creation permissions")
            print(f"2. Verify ERPNext is running at {BASE_URL}")
            print("3. Make sure projects and employees exist")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Timesheet Generation...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in API/.env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    response = input(
        f"\nThis will create {TARGET_TIMESHEETS} timesheets in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = TimesheetGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

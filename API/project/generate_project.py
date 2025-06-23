#!/usr/bin/env python3
"""
ERPNext Project Only Generator - Updated for 20 Projects
Creates 20 projects with random data following the specified flow.
Ensures there are projects with "Open" status.
Uses environment variables from .env file for configuration.
Author: ERPNext Project Generator
Version: 1.1.0
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

# Configuration


class Config:
    # API Configuration (from .env file)
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
    COMPANY_NAME = os.getenv("COMPANY_NAME", "PT Fiyansa Mulya")
    COMPANY_ABBR = os.getenv("COMPANY_ABBR", "PFM")

    # Data Volumes - UPDATED TO 20 PROJECTS
    PROJECT_COUNT = 20

    # Date Range for 2025
    START_DATE = datetime(2025, 1, 1)
    END_DATE = datetime(2025, 12, 31)

    # Retry settings
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # seconds


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            'erpnext_project_only_generation.log', encoding='utf-8'),
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
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

        # Log the configuration being used
        logger.info(f"Using API configuration:")
        logger.info(f"  Base URL: {self.base_url}")
        logger.info(f"  Company: {Config.COMPANY_NAME}")
        logger.info(
            f"  API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in [
                                            "POST", "PUT"] else None, params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < Config.RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{Config.RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {Config.RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
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


class ProjectOnlyGenerator:
    """Generates only project records with random data"""

    def __init__(self):
        self.fake = Faker()
        self.api = ERPNextAPI()
        self.projects = []
        self.master_data_options = {}

        # Initialize master data
        self._ensure_company_exists()
        self._fetch_master_data_options()

    def _ensure_company_exists(self):
        """Ensure the company exists, create if not found"""
        logger.info(f"Checking if company '{Config.COMPANY_NAME}' exists...")

        try:
            companies = self.api.get_list("Company",
                                          filters={
                                              "company_name": Config.COMPANY_NAME},
                                          fields=["name", "company_name"])

            if not companies:
                logger.info(
                    f"Company '{Config.COMPANY_NAME}' not found. Creating it...")

                company_data = {
                    "company_name": Config.COMPANY_NAME,
                    "abbr": Config.COMPANY_ABBR,
                    "default_currency": "IDR",
                    "country": "Indonesia"
                }

                try:
                    company_result = self.api.create_doc(
                        "Company", company_data)
                    logger.info(f"✅ Created company: {Config.COMPANY_NAME}")
                    time.sleep(2)  # Wait for company creation to complete
                    return True
                except Exception as e:
                    logger.error(f"❌ Failed to create company: {str(e)}")
                    return False
            else:
                logger.info(
                    f"✅ Company '{Config.COMPANY_NAME}' already exists")
                return True

        except Exception as e:
            logger.error(f"Error checking/creating company: {str(e)}")
            return False

    def _fetch_master_data_options(self):
        """Fetch existing options for all project fields using standard ERPNext values"""
        logger.info("Setting up master data options for Project fields...")

        try:
            # Use standard ERPNext field options (as you specified)
            self.master_data_options["Status"] = [
                "Open", "Completed", "Cancelled"]
            self.master_data_options["Project Type"] = [
                "Internal", "External", "Other"]
            self.master_data_options["Is Active"] = ["Yes", "No"]
            self.master_data_options["Priority"] = ["Low", "Medium", "High"]
            self.master_data_options["Percent Complete Method"] = [
                "Manual", "Task Completion", "Task Progress", "Task Weight"]

            # Fetch existing departments for the company
            try:
                departments = self.api.get_list("Department",
                                                filters={
                                                    "company": Config.COMPANY_NAME},
                                                fields=["name", "department_name"])
                self.master_data_options["Department"] = [
                    dept["name"] for dept in departments] if departments else []
                logger.info(
                    f"Found {len(self.master_data_options['Department'])} departments")
            except Exception as e:
                logger.warning(f"Could not fetch departments: {str(e)}")
                self.master_data_options["Department"] = []

            logger.info(
                f"✅ Status options: {self.master_data_options['Status']}")
            logger.info(
                f"✅ Project Type options: {self.master_data_options['Project Type']}")
            logger.info(
                f"✅ Priority options: {self.master_data_options['Priority']}")
            logger.info(
                f"✅ Complete Method options: {self.master_data_options['Percent Complete Method']}")
            logger.info(
                f"✅ Departments: {len(self.master_data_options['Department'])} found")

        except Exception as e:
            logger.warning(
                f"Error setting up master data options: {str(e)}. Using defaults.")
            # Ensure we always have fallback values
            self.master_data_options["Status"] = [
                "Open", "Completed", "Cancelled"]
            self.master_data_options["Project Type"] = [
                "Internal", "External", "Other"]
            self.master_data_options["Is Active"] = ["Yes", "No"]
            self.master_data_options["Priority"] = ["Low", "Medium", "High"]
            self.master_data_options["Percent Complete Method"] = [
                "Manual", "Task Completion", "Task Progress", "Task Weight"]
            self.master_data_options["Department"] = []

    def generate_date_in_range(self, start_date: datetime, end_date: datetime) -> str:
        """Generate random date within range"""
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        days_between = (end_date - start_date).days
        if days_between < 0:
            return start_date.strftime("%Y-%m-%d")

        random_days = random.randint(0, days_between)
        date = start_date + timedelta(days=random_days)
        return date.strftime("%Y-%m-%d")

    def generate_random_project_name(self):
        """Generate random project name"""
        project_types = ["System", "Platform", "Application",
                         "Service", "Solution", "Framework"]
        technologies = ["AI", "Blockchain", "Cloud",
                        "Mobile", "Web", "IoT", "Analytics", "Security"]
        purposes = ["Development", "Integration", "Migration",
                    "Enhancement", "Implementation", "Optimization"]

        return f"{random.choice(technologies)} {random.choice(project_types)} {random.choice(purposes)} {random.randint(1000, 9999)}"

    def determine_project_status(self, project_index: int, total_projects: int) -> str:
        """
        Determine project status with guaranteed distribution:
        - At least 50% will be "Open" 
        - 30% will be "Completed"
        - 20% will be "Cancelled"
        """
        # Calculate how many projects should have each status
        open_count = max(1, int(total_projects * 0.5))  # At least 50% Open
        completed_count = int(total_projects * 0.3)     # 30% Completed
        cancelled_count = total_projects - open_count - \
            completed_count  # Remaining are Cancelled

        # Create a list with the exact distribution we want
        status_distribution = (["Open"] * open_count +
                               ["Completed"] * completed_count +
                               ["Cancelled"] * cancelled_count)

        # Shuffle to randomize but maintain distribution
        random.shuffle(status_distribution)

        # Return the status for this project index
        return status_distribution[project_index % len(status_distribution)]

    def check_existing_projects_status(self):
        """Check status distribution of existing projects"""
        try:
            existing_projects = self.api.get_list("Project",
                                                  filters={
                                                      "company": Config.COMPANY_NAME},
                                                  fields=["name", "project_name", "status"])

            status_count = {"Open": 0, "Completed": 0, "Cancelled": 0}
            for project in existing_projects:
                status = project.get("status", "")
                if status in status_count:
                    status_count[status] += 1

            logger.info(f"📊 Existing project status distribution:")
            logger.info(f"   - Open: {status_count['Open']}")
            logger.info(f"   - Completed: {status_count['Completed']}")
            logger.info(f"   - Cancelled: {status_count['Cancelled']}")

            return existing_projects, status_count

        except Exception as e:
            logger.error(f"Error checking existing projects: {e}")
            return [], {"Open": 0, "Completed": 0, "Cancelled": 0}

    def create_projects(self):
        """Create project records following the specified flow with guaranteed Open status projects"""
        logger.info(f"Creating up to {Config.PROJECT_COUNT} projects...")

        # Check existing projects and their status distribution
        existing_projects, existing_status_count = self.check_existing_projects_status()

        logger.info(
            f"Found {len(existing_projects)} existing projects for '{Config.COMPANY_NAME}'.")

        # Check if we already have enough projects
        if len(existing_projects) >= Config.PROJECT_COUNT:
            logger.info(
                f"Already have {len(existing_projects)} projects (>= target {Config.PROJECT_COUNT}). Skipping new project creation.")

            # If no Open projects exist, let's create at least a few
            if existing_status_count["Open"] == 0:
                logger.warning(
                    "⚠️ No 'Open' projects found! Creating 5 Open projects anyway...")
                projects_to_create = 5
            else:
                return existing_projects
        else:
            projects_to_create = Config.PROJECT_COUNT - len(existing_projects)

        logger.info(
            f"Creating {projects_to_create} new projects to reach target {Config.PROJECT_COUNT}...")

        created_projects = []

        # Calculate how many of each status we should create
        # Ensure at least 60% of new projects are "Open"
        open_projects_needed = max(1, int(projects_to_create * 0.6))
        completed_projects_needed = int(projects_to_create * 0.25)
        cancelled_projects_needed = projects_to_create - \
            open_projects_needed - completed_projects_needed

        logger.info(f"📋 Planned new project distribution:")
        logger.info(f"   - Open: {open_projects_needed}")
        logger.info(f"   - Completed: {completed_projects_needed}")
        logger.info(f"   - Cancelled: {cancelled_projects_needed}")

        # Create status list with guaranteed distribution
        status_list = (["Open"] * open_projects_needed +
                       ["Completed"] * completed_projects_needed +
                       ["Cancelled"] * cancelled_projects_needed)

        # Shuffle to randomize order but maintain counts
        random.shuffle(status_list)

        for i in range(projects_to_create):
            # Generate random project name
            project_name = self.generate_random_project_name()

            # Use predetermined status from our distribution
            project_status = status_list[i] if i < len(status_list) else "Open"

            # Generate random project dates
            expected_start = self.generate_date_in_range(
                Config.START_DATE,
                datetime(2025, 10, 31)  # Start projects throughout 2025
            )

            # Project duration between 1-12 months
            duration_days = random.randint(30, 365)
            expected_end_date = datetime.strptime(
                expected_start, "%Y-%m-%d") + timedelta(days=duration_days)

            # Ensure end date is within reasonable bounds
            if expected_end_date > datetime(2026, 12, 31):
                expected_end_date = datetime(2026, 12, 31)

            # Create project data following your specified flow
            project_data = {
                # Fill the name with random
                "project_name": project_name,

                # Use our predetermined status distribution
                "status": project_status,

                # Project type can be random on these list: internal, external, other
                "project_type": random.choice(self.master_data_options["Project Type"]),

                # IsActive can be Yes or No, random as well
                "is_active": random.choice(self.master_data_options["Is Active"]),

                # Complete method can be random on these list: Manual, Task Completion, Task Progress, Task Weight
                "percent_complete_method": random.choice(self.master_data_options["Percent Complete Method"]),

                # From template set it null (leave it empty)
                # "project_template": None,  # Not setting this field

                # Expected start date can be random date
                "expected_start_date": expected_start,

                # Expected end date can be random date
                "expected_end_date": expected_end_date.strftime("%Y-%m-%d"),

                # Priority can be Low, Medium, High
                "priority": random.choice(self.master_data_options["Priority"]),

                # Company must be PT Fiyansa Mulya
                "company": Config.COMPANY_NAME,

                # Random cost estimate
                "estimated_costing": random.randint(10_000_000, 1_000_000_000),

                # Notes
                "notes": f"Auto-generated project: {project_name} (Status: {project_status})"
            }

            # Department can be random depending on the existing list (can be null)
            if self.master_data_options["Department"]:
                project_data["department"] = random.choice(
                    self.master_data_options["Department"])
                logger.debug(
                    f"Assigned department: {project_data['department']}")

            try:
                project = self.api.create_doc("Project", project_data)

                # Get the actual document name/ID that ERPNext assigned
                project_doc_id = project.get("name")

                if project_doc_id:
                    project_info = {
                        "name": project_doc_id,
                        "project_name": project_name,
                        "status": project_data["status"],
                        "project_type": project_data["project_type"],
                        "priority": project_data["priority"]
                    }
                    created_projects.append(project_info)

                    # Highlight Open projects
                    status_indicator = "🟢" if project_status == "Open" else "🔴" if project_status == "Cancelled" else "🟡"

                    logger.info(
                        f"✅ Created project {i+1}/{projects_to_create}: '{project_name}' {status_indicator}")
                    logger.info(f"   - Document ID: {project_doc_id}")
                    logger.info(f"   - Status: {project_data['status']}")
                    logger.info(f"   - Type: {project_data['project_type']}")
                    logger.info(f"   - Priority: {project_data['priority']}")
                    logger.info(f"   - Is Active: {project_data['is_active']}")
                    logger.info(
                        f"   - Complete Method: {project_data['percent_complete_method']}")
                    logger.info(f"   - Start Date: {expected_start}")
                    logger.info(
                        f"   - End Date: {expected_end_date.strftime('%Y-%m-%d')}")
                    if project_data.get("department"):
                        logger.info(
                            f"   - Department: {project_data['department']}")
                else:
                    logger.warning(
                        f"No document ID returned for project '{project_name}'")

                # Small delay to avoid overwhelming the server
                time.sleep(0.3)

            except Exception as e:
                logger.error(
                    f"❌ Failed to create project '{project_name}': {str(e)}")

        # Final summary with status breakdown
        total_projects = len(existing_projects) + len(created_projects)

        # Count statuses in created projects
        created_status_count = {"Open": 0, "Completed": 0, "Cancelled": 0}
        for proj in created_projects:
            status = proj.get("status", "")
            if status in created_status_count:
                created_status_count[status] += 1

        logger.info(f"\n=== PROJECT CREATION SUMMARY ===")
        logger.info(
            f"✅ Successfully created {len(created_projects)} new projects")
        logger.info(
            f"📊 Total projects for {Config.COMPANY_NAME}: {total_projects}/{Config.PROJECT_COUNT}")

        logger.info(f"\n📋 New Projects Status Distribution:")
        logger.info(f"   🟢 Open: {created_status_count['Open']}")
        logger.info(f"   🟡 Completed: {created_status_count['Completed']}")
        logger.info(f"   🔴 Cancelled: {created_status_count['Cancelled']}")

        if len(created_projects) > 0:
            logger.info(f"\n📋 Created Projects:")
            for i, proj in enumerate(created_projects, 1):
                status_indicator = "🟢" if proj['status'] == "Open" else "🔴" if proj['status'] == "Cancelled" else "🟡"
                logger.info(
                    f"   {i}. {proj['project_name']} (ID: {proj['name']}) - {proj['status']} {status_indicator}")

        # Final check to ensure we have Open projects
        total_open_projects = existing_status_count["Open"] + \
            created_status_count["Open"]
        if total_open_projects > 0:
            logger.info(
                f"\n🎉 SUCCESS: You now have {total_open_projects} 'Open' projects!")
        else:
            logger.warning(f"\n⚠️ WARNING: Still no 'Open' projects found!")

        return existing_projects + created_projects

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🎯 ERPNext Project Only Generator - 20 Projects Edition")
        print("=" * 80)
        print(f"📡 API Endpoint: {Config.BASE_URL}")
        print(f"🏢 Company: {Config.COMPANY_NAME}")
        print(
            f"🔑 Using API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")
        print(f"📊 Target Projects: {Config.PROJECT_COUNT}")
        print(
            f"📅 Date Range: {Config.START_DATE.strftime('%Y-%m-%d')} to {Config.END_DATE.strftime('%Y-%m-%d')}")
        print("🎯 Special Focus: Ensuring projects with 'Open' status exist!")
        print("=" * 80)

        try:
            # Create projects only
            projects = self.create_projects()

            print("\n" + "=" * 80)
            print("✅ PROJECT CREATION COMPLETED!")
            print("=" * 80)
            print(f"📊 Total Projects: {len(projects)}")
            print("📋 Check the log file for detailed information:")
            print("   erpnext_project_only_generation.log")
            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during project creation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Project creation permissions")
            print(f"2. Verify ERPNext is running at {Config.BASE_URL}")
            print("3. Make sure the company name in .env matches ERPNext")
            print("4. Check the log file for detailed error information")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Project Only Generation (20 Projects)...")

    # Check if API credentials are set
    if not Config.API_KEY or not Config.API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in API/.env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    response = input(
        "\nThis will create up to 20 projects (with guaranteed 'Open' status projects) in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = ProjectOnlyGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

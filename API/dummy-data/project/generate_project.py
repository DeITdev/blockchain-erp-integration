#!/usr/bin/env python3
"""
ERPNext Project Generator - Updated Version
Creates 20 projects with realistic data following specified requirements.
Uses environment variables from .env file for configuration.
Author: ERPNext Project Generator
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

# Project Configuration
TARGET_PROJECTS = 20

# Date Range for 2025 (March - July)
START_DATE = datetime(2025, 3, 1)
END_DATE = datetime(2025, 7, 31)

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


class ProjectGenerator:
    """Generates project records with realistic data"""

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
        logger.info(f"Checking if company '{COMPANY_NAME}' exists...")

        try:
            companies = self.api.get_list("Company",
                                          filters={
                                              "company_name": COMPANY_NAME},
                                          fields=["name", "company_name"])

            if not companies:
                logger.info(
                    f"Company '{COMPANY_NAME}' not found. Creating it...")
                company_data = {
                    "company_name": COMPANY_NAME,
                    "abbr": COMPANY_ABBR,
                    "default_currency": "IDR",
                    "country": "Indonesia"
                }

                try:
                    company_result = self.api.create_doc(
                        "Company", company_data)
                    logger.info(f"✅ Created company: {COMPANY_NAME}")
                    time.sleep(2)
                    return True
                except Exception as e:
                    logger.error(f"❌ Failed to create company: {str(e)}")
                    return False
            else:
                logger.info(f"✅ Company '{COMPANY_NAME}' already exists")
                return True

        except Exception as e:
            logger.error(f"Error checking/creating company: {str(e)}")
            return False

    def _fetch_master_data_options(self):
        """Fetch existing options from ERPNext"""
        logger.info("Fetching master data options...")

        # Fetch Departments
        try:
            departments = self.api.get_list("Department",
                                            filters={"company": COMPANY_NAME},
                                            fields=["name", "department_name"])
            self.master_data_options["Department"] = [dept["name"]
                                                      for dept in departments] if departments else []
            logger.info(
                f"Found {len(self.master_data_options['Department'])} departments")
        except Exception as e:
            logger.warning(f"Could not fetch departments: {str(e)}")
            self.master_data_options["Department"] = []

        # For other options, we'll use hardcoded values since these are standard ERPNext options
        self.master_data_options["Status"] = ["Open", "Completed", "Cancelled"]
        self.master_data_options["Project Type"] = [
            "Internal", "External", "Other"]
        self.master_data_options["Is Active"] = ["Yes", "No"]
        self.master_data_options["Priority"] = ["Low", "Medium", "High"]
        self.master_data_options["Percent Complete Method"] = [
            "Manual", "Task Completion", "Task Progress", "Task Weight"]

        logger.info("✅ Master data options loaded:")
        for key, values in self.master_data_options.items():
            logger.info(f"   - {key}: {len(values)} options")

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
        """Generate realistic project name"""
        project_types = [
            "System Development", "Platform Migration", "Application Enhancement",
            "Service Implementation", "Solution Deployment", "Framework Integration",
            "Infrastructure Upgrade", "Database Optimization", "Security Enhancement",
            "Performance Improvement", "User Interface Redesign", "API Development",
            "Mobile Application", "Web Portal", "Analytics Dashboard", "Reporting System",
            "Automation Project", "Digital Transformation", "Process Optimization",
            "Quality Assurance"
        ]

        technologies = [
            "ERP", "CRM", "HR", "Finance", "Inventory", "Sales", "Marketing",
            "Operations", "Supply Chain", "Customer Service", "Business Intelligence",
            "Data Analytics", "Cloud", "Mobile", "Web", "API", "Integration"
        ]

        return f"{random.choice(technologies)} {random.choice(project_types)} {random.randint(2025, 2027)}"

    def check_existing_projects(self):
        """Check existing projects and determine how many to create"""
        logger.info("Checking existing projects...")

        try:
            existing_projects = self.api.get_list("Project",
                                                  filters={
                                                      "company": COMPANY_NAME},
                                                  fields=["name", "project_name", "status"])

            current_project_count = len(existing_projects)

            logger.info(f"Current projects: {current_project_count}")
            logger.info(f"Target projects: {TARGET_PROJECTS}")

            if current_project_count >= TARGET_PROJECTS:
                logger.info(
                    f"Already have {current_project_count} projects (>= target {TARGET_PROJECTS}). Skipping new project creation.")
                return 0

            projects_to_create = TARGET_PROJECTS - current_project_count
            logger.info(
                f"Need to create {projects_to_create} projects to reach target {TARGET_PROJECTS}")

            return projects_to_create

        except Exception as e:
            logger.error(f"Error checking existing projects: {str(e)}")
            return TARGET_PROJECTS

    def create_projects(self):
        """Create project records following the specified requirements"""
        logger.info(f"Creating up to {TARGET_PROJECTS} projects...")

        # Check existing projects
        projects_to_create = self.check_existing_projects()

        if projects_to_create <= 0:
            logger.info("No new projects need to be created.")
            return

        logger.info(f"Creating {projects_to_create} new projects...")

        created_projects = []

        for i in range(projects_to_create):
            # Generate random project name
            project_name = self.generate_random_project_name()

            # Generate random project dates (March - July 2025)
            expected_start = self.generate_date_in_range(
                START_DATE, datetime(2025, 6, 30))

            # Project duration between 1-4 months
            start_date_obj = datetime.strptime(expected_start, "%Y-%m-%d")
            duration_days = random.randint(30, 120)  # 1-4 months
            expected_end_date = start_date_obj + timedelta(days=duration_days)

            # Ensure end date is within July 2025
            if expected_end_date > END_DATE:
                expected_end_date = END_DATE

            # Create project data following requirements
            project_data = {
                # Project name - random but sensible
                "project_name": project_name,

                # Status - use existing options randomly
                "status": random.choice(self.master_data_options["Status"]),

                # Project type - use existing options randomly
                "project_type": random.choice(self.master_data_options["Project Type"]),

                # Is Active - Yes or No randomly
                "is_active": random.choice(self.master_data_options["Is Active"]),

                # Complete method - use existing options randomly
                "percent_complete_method": random.choice(self.master_data_options["Percent Complete Method"]),

                # Expected dates - March to July 2025
                "expected_start_date": expected_start,
                "expected_end_date": expected_end_date.strftime("%Y-%m-%d"),

                # Priority - use existing options randomly
                "priority": random.choice(self.master_data_options["Priority"]),

                # Company - required field
                "company": COMPANY_NAME,

                # Random cost estimate
                "estimated_costing": random.randint(50_000_000, 2_000_000_000),

                # Notes
                "notes": f"Auto-generated project: {project_name}"
            }

            # Department - use existing options randomly if available
            if self.master_data_options["Department"]:
                project_data["department"] = random.choice(
                    self.master_data_options["Department"])

            try:
                project = self.api.create_doc("Project", project_data)

                # Debug: Show the actual response structure
                logger.debug(f"API Response: {project}")

                # Try different ways to get the document ID
                project_doc_id = None
                if isinstance(project, dict):
                    # Try different possible keys
                    project_doc_id = (project.get("name") or
                                      project.get("data", {}).get("name") if isinstance(project.get("data"), dict) else None or
                                      project.get("message", {}).get("name") if isinstance(project.get("message"), dict) else None)

                # If we still don't have an ID, the project was likely created but we can't get the ID
                if project_doc_id:
                    project_info = {
                        "name": project_doc_id,
                        "project_name": project_name,
                        "status": project_data["status"],
                        "project_type": project_data["project_type"],
                        "priority": project_data["priority"]
                    }
                    created_projects.append(project_info)

                    # Status indicators
                    status_indicator = {"Open": "🟢", "Completed": "🔵", "Cancelled": "🔴"}.get(
                        project_data["status"], "❓")
                    priority_indicator = {"Low": "🟢", "Medium": "🟡", "High": "🔴"}.get(
                        project_data["priority"], "⚪")

                    logger.info(
                        f"✅ Created project {i+1}/{projects_to_create}: '{project_name}' {status_indicator}")
                    logger.info(f"   - Document ID: {project_doc_id}")
                    logger.info(f"   - Status: {project_data['status']}")
                    logger.info(f"   - Type: {project_data['project_type']}")
                    logger.info(
                        f"   - Priority: {project_data['priority']} {priority_indicator}")
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
                    # Project was likely created successfully, but we can't get the ID from response
                    # Let's count it as created but without detailed info
                    project_info = {
                        "name": f"Project-{i+1}",  # Fallback ID
                        "project_name": project_name,
                        "status": project_data["status"],
                        "project_type": project_data["project_type"],
                        "priority": project_data["priority"]
                    }
                    created_projects.append(project_info)

                    status_indicator = {"Open": "🟢", "Completed": "🔵", "Cancelled": "🔴"}.get(
                        project_data["status"], "❓")
                    logger.info(
                        f"✅ Created project {i+1}/{projects_to_create}: '{project_name}' {status_indicator}")
                    logger.info(
                        f"   - Project created successfully (ID not returned in response)")
                    logger.info(f"   - Status: {project_data['status']}")
                    logger.info(f"   - Type: {project_data['project_type']}")
                    logger.info(f"   - Priority: {project_data['priority']}")

            except Exception as e:
                logger.error(
                    f"❌ Failed to create project '{project_name}': {str(e)}")

        # Final summary
        total_projects = len(created_projects)

        # Count statuses in created projects
        created_status_count = {"Open": 0, "Completed": 0, "Cancelled": 0}
        for proj in created_projects:
            status = proj.get("status", "")
            if status in created_status_count:
                created_status_count[status] += 1

        logger.info(f"\n=== PROJECT CREATION SUMMARY ===")
        logger.info(
            f"✅ Successfully created {len(created_projects)} new projects")
        logger.info(f"📊 Total projects for {COMPANY_NAME}: {total_projects}")

        logger.info(f"\n📋 New Projects Status Distribution:")
        logger.info(f"   🟢 Open: {created_status_count['Open']}")
        logger.info(f"   🔵 Completed: {created_status_count['Completed']}")
        logger.info(f"   🔴 Cancelled: {created_status_count['Cancelled']}")

        if len(created_projects) > 0:
            logger.info(f"\n📋 Created Projects:")
            for i, proj in enumerate(created_projects, 1):
                status_indicator = {"Open": "🟢", "Completed": "🔵", "Cancelled": "🔴"}.get(
                    proj['status'], "❓")
                logger.info(
                    f"   {i}. {proj['project_name']} (ID: {proj['name']}) - {proj['status']} {status_indicator}")

        return created_projects

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🎯 ERPNext Project Generator - Updated Version")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🔑 Using API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")
        print(f"📊 Target Projects: {TARGET_PROJECTS}")
        print(f"📅 Date Range: March - July 2025")
        print("=" * 80)

        try:
            # Create projects only
            projects = self.create_projects()

            print("\n" + "=" * 80)
            print("✅ PROJECT CREATION COMPLETED!")
            print("=" * 80)
            print(f"📊 Total Projects: {len(projects) if projects else 0}")
            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during project creation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Project creation permissions")
            print(f"2. Verify ERPNext is running at {BASE_URL}")
            print("3. Make sure the company name in .env matches ERPNext")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Project Generation (20 Projects)...")

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
        f"\nThis will create up to {TARGET_PROJECTS} projects in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = ProjectGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

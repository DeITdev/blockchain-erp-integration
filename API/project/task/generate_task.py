#!/usr/bin/env python3
"""
ERPNext Task Generator
Creates 50 tasks for each project (1000 tasks total) with realistic data.
Uses environment variables from .env file for configuration.
Author: ERPNext Task Generator
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

# Task Configuration
TASKS_PER_PROJECT = 50

# Predefined colors
TASK_COLORS = ["#449CF0", "#ECAD4B", "#CB2929", "#29CD42"]

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


class TaskGenerator:
    """Generates task records with realistic data"""

    def __init__(self):
        self.fake = Faker()
        self.api = ERPNextAPI()
        self.tasks = []
        self.projects = []
        self.master_data_options = {}

        # Fetch required data
        self._fetch_projects()
        self._fetch_master_data_options()

    def _fetch_projects(self):
        """Fetch existing projects from ERPNext"""
        logger.info("Fetching existing projects...")

        try:
            projects = self.api.get_list("Project",
                                         filters={"company": COMPANY_NAME},
                                         fields=["name", "project_name", "expected_start_date", "expected_end_date"])

            self.projects = projects
            logger.info(
                f"Found {len(self.projects)} projects for task creation")

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

    def _fetch_master_data_options(self):
        """Fetch existing options from ERPNext dynamically"""
        logger.info("Fetching master data options dynamically...")

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

        # Fetch Task field options dynamically
        try:
            # Get Task doctype metadata to fetch field options
            task_meta = self.api._make_request(
                "GET", "method/frappe.desk.form.meta.get_meta", {"doctype": "Task"})

            if task_meta and "docs" in task_meta:
                # Find Status and Priority field options
                for doc in task_meta["docs"]:
                    if doc.get("doctype") == "DocField":
                        fieldname = doc.get("fieldname")
                        options = doc.get("options", "")

                        if fieldname == "status" and options:
                            self.master_data_options["Status"] = [
                                opt.strip() for opt in options.split('\n') if opt.strip()]
                            logger.info(
                                f"Found Status options: {self.master_data_options['Status']}")

                        elif fieldname == "priority" and options:
                            self.master_data_options["Priority"] = [
                                opt.strip() for opt in options.split('\n') if opt.strip()]
                            logger.info(
                                f"Found Priority options: {self.master_data_options['Priority']}")

            # Fallback to defaults if dynamic fetch failed
            if "Status" not in self.master_data_options or not self.master_data_options["Status"]:
                self.master_data_options["Status"] = [
                    "Open", "Working", "Pending Review", "Overdue", "Template", "Completed", "Cancelled"]
                logger.info("Using fallback Status options")

            if "Priority" not in self.master_data_options or not self.master_data_options["Priority"]:
                self.master_data_options["Priority"] = [
                    "Low", "Medium", "High", "Urgent"]
                logger.info("Using fallback Priority options")

        except Exception as e:
            logger.warning(
                f"Could not fetch field options dynamically: {str(e)}")
            # Use safe fallback values
            self.master_data_options["Status"] = [
                "Open", "Working", "Pending Review", "Overdue", "Template", "Completed", "Cancelled"]
            self.master_data_options["Priority"] = [
                "Low", "Medium", "High", "Urgent"]
            logger.info("Using fallback field options")

        logger.info("✅ Master data options loaded:")
        for key, values in self.master_data_options.items():
            if isinstance(values, list):
                logger.info(f"   - {key}: {values}")
            else:
                logger.info(f"   - {key}: {len(values)} options")

    def generate_task_subject(self, project_name: str, task_index: int) -> str:
        """Generate realistic task subject"""
        task_templates = [
            "Requirements Analysis", "System Design", "Database Schema", "API Development",
            "Frontend Implementation", "Backend Development", "Integration Testing", "Unit Testing",
            "User Interface Design", "User Experience Review", "Code Review", "Documentation",
            "Security Assessment", "Performance Testing", "Data Migration", "Configuration Setup",
            "User Training", "Deployment Planning", "Quality Assurance", "Bug Fixing",
            "System Monitoring", "Performance Optimization", "Feature Implementation", "Module Development",
            "Infrastructure Setup", "Environment Configuration", "Data Validation", "Reporting Setup",
            "Dashboard Creation", "Workflow Design", "Business Logic", "Error Handling",
            "Backup Configuration", "Security Implementation", "User Management", "Access Control",
            "Email Integration", "Notification System", "File Management", "Print Templates",
            "Export Functionality", "Import Tools", "Synchronization", "Automation Rules",
            "Custom Scripts", "Maintenance Tasks", "Support Documentation", "Training Materials",
            "System Testing", "Acceptance Testing", "Go-Live Preparation", "Post-Launch Support"
        ]

        base_subject = random.choice(task_templates)

        # Add project context occasionally
        if random.choice([True, False]):
            project_word = project_name.split(
            )[0] if project_name else "System"
            return f"{project_word} {base_subject}"

        return f"{base_subject} - Phase {random.randint(1, 3)}"

    def generate_task_description(self, subject: str) -> str:
        """Generate realistic task description"""
        descriptions = [
            f"Complete the {subject.lower()} as per project requirements and technical specifications.",
            f"Implement {subject.lower()} following best practices and company standards.",
            f"Execute {subject.lower()} with proper documentation and testing procedures.",
            f"Develop {subject.lower()} ensuring quality, performance, and security standards.",
            f"Perform {subject.lower()} according to the project timeline and deliverables."
        ]
        return random.choice(descriptions)

    def generate_date_within_project(self, project_start: str, project_end: str) -> tuple:
        """Generate start and end dates within project timeline"""
        try:
            # Parse project dates
            if project_start:
                start_date = datetime.strptime(project_start, "%Y-%m-%d")
            else:
                start_date = datetime(2025, 3, 1)

            if project_end:
                end_date = datetime.strptime(project_end, "%Y-%m-%d")
            else:
                end_date = datetime(2025, 7, 31)

            # Generate task start date within project timeline
            project_days = (end_date - start_date).days
            if project_days <= 0:
                project_days = 30  # Default to 30 days

            # Task starts somewhere in first 80% of project
            task_start_offset = random.randint(
                0, max(1, int(project_days * 0.8)))
            task_start = start_date + timedelta(days=task_start_offset)

            # Task duration between 1-14 days
            task_duration = random.randint(1, 14)
            task_end = task_start + timedelta(days=task_duration)

            # Ensure task end doesn't exceed project end
            if task_end > end_date:
                task_end = end_date

            return task_start.strftime("%Y-%m-%d"), task_end.strftime("%Y-%m-%d")

        except Exception as e:
            logger.warning(f"Error generating dates: {e}, using defaults")
            # Fallback dates
            default_start = datetime(2025, 3, 15)
            default_end = default_start + timedelta(days=random.randint(1, 14))
            return default_start.strftime("%Y-%m-%d"), default_end.strftime("%Y-%m-%d")

    def check_existing_tasks(self):
        """Check existing tasks for projects"""
        logger.info("Checking existing tasks...")

        try:
            existing_tasks = self.api.get_list("Task",
                                               filters={
                                                   "company": COMPANY_NAME},
                                               fields=["name", "subject", "project"])

            current_task_count = len(existing_tasks)
            target_task_count = len(self.projects) * TASKS_PER_PROJECT

            logger.info(f"Current tasks: {current_task_count}")
            logger.info(
                f"Target tasks: {target_task_count} ({len(self.projects)} projects × {TASKS_PER_PROJECT} tasks each)")

            if current_task_count >= target_task_count:
                logger.info(
                    f"Already have {current_task_count} tasks (>= target {target_task_count}). Skipping new task creation.")
                return 0

            tasks_to_create = target_task_count - current_task_count
            logger.info(
                f"Need to create {tasks_to_create} tasks to reach target {target_task_count}")

            return tasks_to_create

        except Exception as e:
            logger.error(f"Error checking existing tasks: {str(e)}")
            return len(self.projects) * TASKS_PER_PROJECT

    def create_tasks(self):
        """Create task records for all projects"""
        logger.info(f"Creating {TASKS_PER_PROJECT} tasks for each project...")

        # Check how many tasks we need to create
        tasks_to_create = self.check_existing_tasks()

        if tasks_to_create <= 0:
            logger.info("No new tasks need to be created.")
            return

        print("\n" + "=" * 80)
        print("📋 Creating Tasks for Projects")
        print("=" * 80)
        print(f"📊 Projects: {len(self.projects)}")
        print(f"📋 Tasks per Project: {TASKS_PER_PROJECT}")
        print(
            f"🎯 Total Target Tasks: {len(self.projects) * TASKS_PER_PROJECT}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print("=" * 80)

        created_tasks = []
        total_tasks_created = 0

        for proj_index, project in enumerate(self.projects):
            project_name = project.get("project_name", "Unknown Project")
            project_id = project.get("name")
            project_start = project.get("expected_start_date")
            project_end = project.get("expected_end_date")

            print(
                f"\n📁 Project {proj_index + 1}/{len(self.projects)}: {project_name}")
            print(f"   🎯 Creating {TASKS_PER_PROJECT} tasks...")

            project_tasks_created = 0

            for task_index in range(TASKS_PER_PROJECT):
                try:
                    # Generate task data
                    task_subject = self.generate_task_subject(
                        project_name, task_index + 1)
                    task_description = self.generate_task_description(
                        task_subject)
                    exp_start_date, exp_end_date = self.generate_date_within_project(
                        project_start, project_end)

                    # Create task data following requirements
                    task_data = {
                        # Subject - random but sensible
                        "subject": task_subject,

                        # Project link
                        "project": project_id,

                        # Status - use existing options randomly
                        "status": random.choice(self.master_data_options["Status"]),

                        # Priority - use existing options randomly
                        "priority": random.choice(self.master_data_options["Priority"]),

                        # Weight - random sensible data (1-10)
                        "task_weight": random.randint(1, 10),

                        # Is Group and Is Template - leave unchecked (0)
                        "is_group": 0,
                        "is_template": 0,

                        # Timeline - make sense mock data
                        "exp_start_date": exp_start_date,
                        "exp_end_date": exp_end_date,

                        # More info section
                        "company": COMPANY_NAME,

                        # Color - use predefined colors
                        "color": random.choice(TASK_COLORS),

                        # Description
                        "description": task_description
                    }

                    # Department - use existing data if available
                    if self.master_data_options["Department"]:
                        task_data["department"] = random.choice(
                            self.master_data_options["Department"])

                    # Create task
                    task_result = self.api.create_doc("Task", task_data)

                    # Handle response (similar to project creation)
                    task_doc_id = None
                    if isinstance(task_result, dict):
                        task_doc_id = (task_result.get("name") or
                                       task_result.get("data", {}).get("name") if isinstance(task_result.get("data"), dict) else None or
                                       task_result.get("message", {}).get("name") if isinstance(task_result.get("message"), dict) else None)

                    if task_doc_id or task_result:  # Count as created if we got any response
                        task_info = {
                            "name": task_doc_id or f"Task-{task_index+1}",
                            "subject": task_subject,
                            "project": project_name,
                            "status": task_data["status"],
                            "priority": task_data["priority"]
                        }
                        created_tasks.append(task_info)
                        project_tasks_created += 1
                        total_tasks_created += 1

                        # Progress indicator
                        if project_tasks_created % 10 == 0:
                            print(
                                f"   ✅ Created {project_tasks_created}/{TASKS_PER_PROJECT} tasks for this project")

                except Exception as e:
                    logger.error(
                        f"❌ Failed to create task {task_index + 1} for project {project_name}: {str(e)}")

            print(
                f"   📊 Completed: {project_tasks_created}/{TASKS_PER_PROJECT} tasks for {project_name}")

        # Final summary
        print("\n" + "=" * 80)
        print("📊 TASK CREATION SUMMARY")
        print("=" * 80)
        print(f"✅ Tasks Created: {total_tasks_created}")
        print(f"📁 Projects Processed: {len(self.projects)}")
        print(
            f"📊 Average Tasks per Project: {total_tasks_created / len(self.projects) if self.projects else 0:.1f}")

        # Status distribution
        status_count = {}
        priority_count = {}
        for task in created_tasks:
            status = task.get("status", "Unknown")
            priority = task.get("priority", "Unknown")
            status_count[status] = status_count.get(status, 0) + 1
            priority_count[priority] = priority_count.get(priority, 0) + 1

        print(f"\n📋 Task Status Distribution:")
        for status, count in status_count.items():
            print(f"   - {status}: {count} tasks")

        print(f"\n🔥 Task Priority Distribution:")
        for priority, count in priority_count.items():
            print(f"   - {priority}: {count} tasks")

        print(f"\n🎨 Colors Used: {', '.join(TASK_COLORS)}")
        print("=" * 80)

        logger.info(
            f"Successfully created {total_tasks_created} tasks across {len(self.projects)} projects")
        return created_tasks

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("📋 ERPNext Task Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🔑 Using API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")
        print(f"📋 Tasks per Project: {TASKS_PER_PROJECT}")
        print("=" * 80)

        try:
            # Check if we have projects to work with
            if not self.projects:
                print("❌ No projects found! Please create projects first.")
                return

            # Create tasks
            tasks = self.create_tasks()

            print(f"\n🎉 TASK GENERATION COMPLETED!")
            print(f"📊 Total Tasks: {len(tasks) if tasks else 0}")

        except Exception as e:
            logger.error(f"Fatal error during task generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Task creation permissions")
            print(f"2. Verify ERPNext is running at {BASE_URL}")
            print("3. Make sure projects exist before creating tasks")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Task Generation...")

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
        f"\nThis will create {TASKS_PER_PROJECT} tasks for each project in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = TaskGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

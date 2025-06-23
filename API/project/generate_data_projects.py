#!/usr/bin/env python3
"""
ERPNext Project Module Dummy Data Generator
Generates realistic dummy data for Project and Task modules in ERPNext v16.
Creates 10 projects with 50 tasks each (500 tasks total).
Uses environment variables from .env file for configuration.
Author: ERPNext Project Data Generator
Version: 2.0.0 (Clean)
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

    # Data Volumes
    PROJECT_COUNT = 10
    TASKS_PER_PROJECT = 50
    TOTAL_TASKS = PROJECT_COUNT * TASKS_PER_PROJECT  # 500 tasks

    # Date Range for 2025
    START_DATE = datetime(2025, 1, 1)
    END_DATE = datetime(2025, 12, 31)

    # Batch Processing
    BATCH_SIZE = 50
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # seconds


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            'erpnext_project_data_generation.log', encoding='utf-8'),
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
            "doctype": doctype,
            "limit_page_length": 1000
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)

        if "doctype" in params:
            del params["doctype"]

        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def get_doc(self, doctype: str, name: str) -> Dict:
        """Get single document"""
        return self._make_request("GET", f"resource/{doctype}/{name}")

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create new document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def update_doc(self, doctype: str, name: str, data: Dict) -> Dict:
        """Update existing document"""
        return self._make_request("PUT", f"resource/{doctype}/{name}", data)

    def submit_doc(self, doctype: str, name: str) -> Dict:
        """Submit a document"""
        return self.update_doc(doctype, name, {"docstatus": 1})

    def check_exists(self, doctype: str, name: str) -> bool:
        """Check if document exists"""
        try:
            if doctype == "Project":
                result = self.get_list(doctype, filters={
                                       "project_name": name, "company": Config.COMPANY_NAME}, fields=["name"])
                return len(result) > 0
            elif doctype == "Task":
                result = self.get_list(
                    doctype, filters={"subject": name}, fields=["name"])
                return len(result) > 0
            else:
                self.get_doc(doctype, name)
                return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                logger.error(
                    f"Error checking existence of {doctype} {name} (HTTP {e.response.status_code}): {e.response.text}")
                if e.response.status_code == 500:
                    raise
                return False
        except Exception as e:
            logger.error(
                f"Unexpected error checking existence of {doctype} {name}: {e}")
            raise


class ProjectDataGenerator:
    """Generates realistic project and task dummy data"""

    def __init__(self):
        self.fake = Faker()
        self.api = ERPNextAPI()

        # Cache for generated data
        self.projects = []
        self.tasks = []
        self.employees = []
        self.master_data_options = {}

        # Initialize master data
        self._fetch_employees()
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
            # Use standard ERPNext field options
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

    def _fetch_employees(self):
        """Fetch existing employees for task assignment"""
        try:
            existing_employees = self.api.get_list("Employee",
                                                   filters={
                                                       "company": Config.COMPANY_NAME, "status": "Active"},
                                                   fields=["name", "employee_name"])
            self.employees = existing_employees
            logger.info(
                f"Found {len(self.employees)} active employees for task assignment.")
        except Exception as e:
            logger.warning(
                f"Could not fetch employees: {str(e)}. Tasks will be created without assignment.")
            self.employees = []

    def generate_date_in_range(self, start_date: datetime, end_date: datetime, exclude_weekends: bool = True) -> str:
        """Generate random date within range"""
        if start_date > end_date:
            logger.warning(
                f"Invalid date range: start_date ({start_date}) is after end_date ({end_date}). Swapping dates.")
            start_date, end_date = end_date, start_date

        days_between = (end_date - start_date).days
        if days_between < 0:
            return start_date.strftime("%Y-%m-%d")

        while True:
            random_days = random.randint(0, days_between)
            date = start_date + timedelta(days=random_days)

            if exclude_weekends and date.weekday() >= 5:
                continue
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

    def create_projects(self):
        """Create project records"""
        logger.info(f"Creating {Config.PROJECT_COUNT} projects...")

        # Fetch existing projects
        existing_projects = self.api.get_list("Project",
                                              filters={
                                                  "company": Config.COMPANY_NAME},
                                              fields=["name", "project_name", "status"])
        self.projects = existing_projects
        logger.info(
            f"Found {len(self.projects)} existing projects for '{Config.COMPANY_NAME}'.")

        # Check if we already have enough projects
        if len(self.projects) >= Config.PROJECT_COUNT:
            logger.info(
                f"Already have {len(self.projects)} projects (>= target {Config.PROJECT_COUNT}). Skipping new project creation.")
            return

        projects_to_create = Config.PROJECT_COUNT - len(self.projects)
        logger.info(
            f"Creating {projects_to_create} new projects to reach target {Config.PROJECT_COUNT}...")

        projects_created_count = 0

        for i in range(projects_to_create):
            # Generate random project name
            project_name = self.generate_random_project_name()

            # Check if project already exists (very unlikely with random names)
            if self.api.check_exists("Project", project_name):
                logger.debug(
                    f"Project '{project_name}' already exists, generating new name...")
                project_name = self.generate_random_project_name()

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

            project_data = {
                "project_name": project_name,
                "status": random.choice(self.master_data_options["Status"]),
                "project_type": random.choice(self.master_data_options["Project Type"]) if self.master_data_options["Project Type"] else None,
                "is_active": random.choice(self.master_data_options["Is Active"]),
                "percent_complete_method": random.choice(self.master_data_options["Percent Complete Method"]),
                "priority": random.choice(self.master_data_options["Priority"]),
                "company": Config.COMPANY_NAME,
                "expected_start_date": expected_start,
                "expected_end_date": expected_end_date.strftime("%Y-%m-%d"),
                # 10M - 1B IDR
                "estimated_costing": random.randint(10_000_000, 1_000_000_000),
                "notes": f"Auto-generated project: {project_name}"
            }

            # Add department if available
            if self.master_data_options["Department"]:
                project_data["department"] = random.choice(
                    self.master_data_options["Department"])
                logger.debug(
                    f"Assigned department: {project_data['department']}")

            try:
                project = self.api.create_doc("Project", project_data)
                # Get the actual document name/ID that ERPNext assigned
                project_doc_id = project.get("name")

                if not project_doc_id:
                    logger.warning(
                        f"No document ID returned for project '{project_name}', skipping...")
                    continue

                project_cache_entry = {
                    "name": project_doc_id,  # Use the actual ERPNext document ID
                    "project_name": project_name  # Keep the display name for logging
                }
                self.projects.append(project_cache_entry)
                projects_created_count += 1
                logger.info(
                    f"Created project {projects_created_count}/{projects_to_create}: '{project_name}' (ID: {project_doc_id}) (Status: {project_data['status']})")

                # Small delay to avoid overwhelming the server
                time.sleep(0.3)

            except Exception as e:
                logger.warning(
                    f"Failed to create project '{project_name}': {str(e)}")

        logger.info(
            f"Created {projects_created_count} new projects. Total projects: {len(self.projects)}")

    def create_tasks_for_projects(self):
        """Create tasks for all projects"""
        logger.info(
            f"Creating {Config.TASKS_PER_PROJECT} tasks for each project...")

        if not self.projects:
            logger.error("No projects found. Cannot create tasks.")
            return

        # Debug: List all projects with their actual document IDs
        logger.info("=== DEBUG: Current projects in cache ===")
        for i, project in enumerate(self.projects):
            logger.info(
                f"  Project {i+1}: Name='{project.get('project_name')}', Doc ID='{project.get('name')}'")

        total_tasks_created = 0

        for project in self.projects:
            project_name = project.get('project_name', 'Unknown Project')
            # This is the actual document name/ID
            project_doc_name = project.get('name')

            logger.info(
                f"Creating tasks for project: '{project_name}' (Document ID: {project_doc_name})")

            if not project_doc_name:
                logger.error(
                    f"No document name found for project '{project_name}', skipping...")
                continue

            try:
                # Try to get project details, but handle permission errors gracefully
                try:
                    logger.debug(
                        f"Attempting to fetch project document: {project_doc_name}")
                    project_doc = self.api.get_doc("Project", project_doc_name)
                    logger.debug(
                        f"Successfully fetched project document: {project_doc_name}")

                    project_start = datetime.strptime(project_doc.get(
                        "expected_start_date", Config.START_DATE.strftime("%Y-%m-%d")), "%Y-%m-%d")
                    project_end = datetime.strptime(project_doc.get(
                        "expected_end_date", Config.END_DATE.strftime("%Y-%m-%d")), "%Y-%m-%d")

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403:
                        logger.warning(
                            f"Access denied to project {project_doc_name}. Using default dates for task creation.")
                        # Use default date range if we can't access project details
                        project_start = Config.START_DATE
                        project_end = Config.END_DATE
                    else:
                        raise  # Re-raise other HTTP errors

                # Check existing tasks for this project using the document name
                try:
                    existing_tasks = self.api.get_list("Task",
                                                       filters={
                                                           "project": project_doc_name},  # Use document name
                                                       fields=["name", "subject"])
                except Exception as e:
                    logger.warning(
                        f"Could not fetch existing tasks for project {project_doc_name}: {e}")
                    existing_tasks = []

                existing_task_count = len(existing_tasks)
                tasks_to_create = Config.TASKS_PER_PROJECT - existing_task_count

                if tasks_to_create <= 0:
                    logger.info(
                        f"Project '{project_name}' already has {existing_task_count} tasks. Skipping.")
                    continue

                logger.info(
                    f"Creating {tasks_to_create} tasks for project '{project_name}'")

                # Distribute tasks throughout project timeline
                if tasks_to_create > 0:
                    project_duration = (project_end - project_start).days
                    if project_duration <= 0:
                        project_duration = 30  # Minimum duration

                    task_duration_avg = max(
                        1, project_duration // tasks_to_create)

                # Create tasks
                current_date = project_start
                tasks_created_for_project = 0

                # Use simple task names
                for i in range(tasks_to_create):
                    task_subject = f"Task {i+1:02d} - {project_name}"

                    # Check if task already exists
                    if any(existing_task["subject"] == task_subject for existing_task in existing_tasks):
                        logger.debug(
                            f"Task '{task_subject}' already exists, skipping.")
                        continue

                    # Calculate task dates
                    task_duration = random.randint(
                        max(1, task_duration_avg - 3), task_duration_avg + 7)
                    task_start = current_date
                    task_end = min(
                        task_start + timedelta(days=task_duration), project_end)

                    # Ensure task doesn't exceed project end date
                    if task_start > project_end:
                        task_start = project_end - timedelta(days=1)
                        task_end = project_end

                    task_data = {
                        "subject": task_subject,
                        "project": project_doc_name,  # Use the actual document name
                        "status": random.choice(["Open", "Working", "Completed", "Cancelled"]),
                        "priority": random.choice(["Low", "Medium", "High"]),
                        "expected_start_date": task_start.strftime("%Y-%m-%d"),
                        "expected_end_date": task_end.strftime("%Y-%m-%d"),
                        # 1-10 working days in hours
                        "expected_time": random.randint(8, 80),
                        "description": f"Task {i+1} implementation for project {project_name}",
                        "company": Config.COMPANY_NAME
                    }

                    # Assign to random employee if available
                    if self.employees:
                        assigned_employee = random.choice(self.employees)
                        task_data["_assign"] = json.dumps(
                            [assigned_employee["name"]])

                    try:
                        task = self.api.create_doc("Task", task_data)
                        tasks_created_for_project += 1
                        total_tasks_created += 1

                        # Handle different response formats
                        task_name_for_log = task.get("name") or task.get(
                            "subject") or task_subject
                        logger.debug(
                            f"Created task: '{task_subject}' (ID: {task_name_for_log})")

                        # Move to next task start date
                        current_date = task_end + timedelta(days=1)
                        if current_date > project_end:
                            current_date = project_start + \
                                timedelta(days=random.randint(
                                    0, max(1, project_duration)))

                        # Small delay to avoid overwhelming the server
                        time.sleep(0.1)

                    except Exception as e:
                        logger.warning(
                            f"Failed to create task '{task_subject}': {str(e)}")

                logger.info(
                    f"Created {tasks_created_for_project} tasks for project '{project_name}'")

            except Exception as e:
                logger.error(
                    f"Error creating tasks for project '{project_name}' (ID: {project_doc_name}): {str(e)}")
                continue

        logger.info(f"Total tasks created: {total_tasks_created}")

    def submit_completed_projects_and_tasks(self):
        """Submit projects and tasks that are marked as completed"""
        logger.info("Submitting completed projects and tasks...")
        logger.info(
            "Note: This may fail due to API permissions - this is normal")

        submitted_projects = 0
        submitted_tasks = 0
        permission_errors = 0

        try:
            # Submit completed projects
            completed_projects = self.api.get_list("Project",
                                                   filters={
                                                       "status": "Completed", "docstatus": 0, "company": Config.COMPANY_NAME},
                                                   fields=["name", "project_name"])

            for project in completed_projects:
                try:
                    self.api.submit_doc("Project", project["name"])
                    submitted_projects += 1
                    logger.debug(
                        f"Submitted project: {project.get('project_name', project['name'])}")
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403:
                        permission_errors += 1
                        logger.debug(
                            f"No permission to submit project {project.get('project_name', project['name'])}")
                    else:
                        logger.warning(
                            f"Failed to submit project {project.get('project_name', project['name'])}: {str(e)}")
                except Exception as e:
                    logger.warning(
                        f"Failed to submit project {project.get('project_name', project['name'])}: {str(e)}")

            # Submit completed tasks
            completed_tasks = self.api.get_list("Task",
                                                filters={
                                                    "status": "Completed", "docstatus": 0},
                                                fields=["name", "subject"])

            for task in completed_tasks:
                try:
                    self.api.submit_doc("Task", task["name"])
                    submitted_tasks += 1
                    logger.debug(f"Submitted task: {task['subject']}")
                    time.sleep(0.05)  # Small delay
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403:
                        permission_errors += 1
                        logger.debug(
                            f"No permission to submit task {task['subject']}")
                    else:
                        logger.warning(
                            f"Failed to submit task {task['subject']}: {str(e)}")
                except Exception as e:
                    logger.warning(
                        f"Failed to submit task {task['subject']}: {str(e)}")

        except Exception as e:
            logger.error(f"Error during submission process: {str(e)}")

        logger.info(
            f"Submission summary: {submitted_projects} projects submitted, {submitted_tasks} tasks submitted")
        if permission_errors > 0:
            logger.info(
                f"Permission denied for {permission_errors} items (this is normal if API key has limited permissions)")

    def check_existing_data(self):
        """Check if we already have sufficient data and skip generation if target is reached"""
        logger.info("Checking existing project and task data...")

        try:
            # Get current counts - ONLY for our company
            existing_projects = self.api.get_list(
                "Project", filters={"company": Config.COMPANY_NAME}, fields=["name"])

            # Get tasks that belong to projects of our company
            existing_tasks = []
            if existing_projects:
                project_names = [proj["name"] for proj in existing_projects]
                # Get tasks that belong to our company's projects
                for project_name in project_names:
                    try:
                        project_tasks = self.api.get_list("Task",
                                                          filters={
                                                              "project": project_name},
                                                          fields=["name"])
                        existing_tasks.extend(project_tasks)
                    except Exception as e:
                        logger.warning(
                            f"Could not fetch tasks for project {project_name}: {e}")

            current_project_count = len(existing_projects)
            current_task_count = len(existing_tasks)

            logger.info(
                f"Current data for '{Config.COMPANY_NAME}': {current_project_count} projects, {current_task_count} tasks")
            logger.info(
                f"Target data: {Config.PROJECT_COUNT} projects, {Config.TOTAL_TASKS} tasks")

            # Check if we've already reached the target
            if current_project_count >= Config.PROJECT_COUNT and current_task_count >= Config.TOTAL_TASKS:
                logger.info("TARGET ALREADY REACHED!")
                logger.info(
                    f"Projects: {current_project_count}/{Config.PROJECT_COUNT} (sufficient)")
                logger.info(
                    f"Tasks: {current_task_count}/{Config.TOTAL_TASKS} (sufficient)")
                logger.info("No additional dummy data generation needed.")
                return True

            # Show what still needs to be created
            projects_needed = max(
                0, Config.PROJECT_COUNT - current_project_count)
            tasks_needed = max(0, Config.TOTAL_TASKS - current_task_count)

            logger.info("Generation needed:")
            logger.info(f"   Projects: {projects_needed} more needed")
            logger.info(f"   Tasks: {tasks_needed} more needed")

            return False

        except Exception as e:
            logger.error(f"Error checking existing data: {str(e)}")
            return False

    def generate_all_data(self):
        """Main method to generate all project and task data"""
        logger.info("Starting ERPNext Project module dummy data generation...")
        logger.info(
            f"Target: {Config.PROJECT_COUNT} projects with {Config.TASKS_PER_PROJECT} tasks each ({Config.TOTAL_TASKS} total tasks)")

        try:
            # First ensure company exists
            if not self._ensure_company_exists():
                logger.error("Cannot proceed without valid company")
                return

            # Check if target is already reached
            if self.check_existing_data():
                logger.info("=== SKIPPING DATA GENERATION ===")
                logger.info(
                    "Target already reached. Run again to create additional data.")
                return

            logger.info("=== Fetching Field Options ===")
            # Field options already fetched in __init__

            logger.info("=== Creating Projects ===")
            self.create_projects()

            logger.info("=== Creating Tasks for Projects ===")
            self.create_tasks_for_projects()

            logger.info("=== Submitting Completed Items ===")
            self.submit_completed_projects_and_tasks()

            logger.info("=== Data Generation Complete ===")

            # Final summary - count only our company's data
            final_projects = self.api.get_list(
                "Project", filters={"company": Config.COMPANY_NAME}, fields=["name"])

            # Count tasks belonging to our company's projects
            final_tasks = []
            if final_projects:
                project_names = [proj["name"] for proj in final_projects]
                for project_name in project_names:
                    try:
                        project_tasks = self.api.get_list("Task",
                                                          filters={
                                                              "project": project_name},
                                                          fields=["name"])
                        final_tasks.extend(project_tasks)
                    except Exception as e:
                        logger.warning(
                            f"Could not count tasks for project {project_name}: {e}")

            final_project_count = len(final_projects)
            final_task_count = len(final_tasks)

            logger.info("Final Summary:")
            logger.info(
                f"- Total Projects for {Config.COMPANY_NAME}: {final_project_count}")
            logger.info(
                f"- Total Tasks for {Config.COMPANY_NAME}: {final_task_count}")
            logger.info(
                f"- Average Tasks per Project: {final_task_count / max(1, final_project_count):.1f}")

            # Check if target was reached
            if final_project_count >= Config.PROJECT_COUNT and final_task_count >= Config.TOTAL_TASKS:
                logger.info("✅ TARGET SUCCESSFULLY REACHED!")
            else:
                logger.info(
                    "⚠️ Target not fully reached. You may want to run the script again.")

        except Exception as e:
            logger.error(f"Fatal error during data generation: {str(e)}")
            raise

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🎯 ERPNext Project Module Dummy Data Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {Config.BASE_URL}")
        print(f"🏢 Company: {Config.COMPANY_NAME}")
        print(
            f"🔑 Using API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")
        print(f"📊 Target Projects: {Config.PROJECT_COUNT}")
        print(f"📋 Tasks per Project: {Config.TASKS_PER_PROJECT}")
        print(f"📈 Total Tasks: {Config.TOTAL_TASKS}")
        print(
            f"📅 Date Range: {Config.START_DATE.strftime('%Y-%m-%d')} to {Config.END_DATE.strftime('%Y-%m-%d')}")
        print("=" * 80)

        try:
            # Generate all project and task data
            self.generate_all_data()

            print("\n" + "=" * 80)
            print("✅ PROJECT DATA GENERATION COMPLETED!")
            print("=" * 80)
            print("📋 Check the log file for detailed information:")
            print("   erpnext_project_data_generation.log")
            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during data generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Project creation permissions")
            print(f"2. Verify ERPNext is running at {Config.BASE_URL}")
            print("3. Make sure the company name in .env matches ERPNext")
            print("4. Check the log file for detailed error information")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Project Data Generation...")

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
        "\nThis will create projects and tasks in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = ProjectDataGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

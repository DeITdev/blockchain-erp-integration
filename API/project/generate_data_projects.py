#!/usr/bin/env python3
"""
ERPNext Project Module Dummy Data Generator
Generates realistic dummy data for Project and Task modules in ERPNext v16.
Creates 10 projects with 50 tasks each (500 tasks total).
Author: ERPNext Project Data Generator
Version: 1.0.0
"""

import requests
import json
import random
import logging
import time
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

# Initialize Faker
fake = Faker()

# Configuration


class Config:
    # API Configuration
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"  # Adjust based on your Docker setup

    # Data Volumes
    PROJECT_COUNT = 10
    TASKS_PER_PROJECT = 50
    TOTAL_TASKS = PROJECT_COUNT * TASKS_PER_PROJECT  # 500 tasks

    # Date Range for 2025
    START_DATE = datetime(2025, 1, 1)
    END_DATE = datetime(2025, 12, 31)

    # Company Details (MUST MATCH EXISTING SETUP)
    # Ensure this exactly matches your ERPNext company name
    COMPANY_NAME = "PT Fiyansa Mulya"
    COMPANY_ABBR = "PFM"

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

        # Fetch existing employees for task assignment
        self._fetch_employees()

        # Fetch existing master data options
        self.master_data_options = {}
        self._fetch_master_data_options()

        # Project templates with realistic names and descriptions
        self.project_templates = [
            {
                "name": "Blockchain Research Initiative",
                "description": "Research and development of blockchain technology solutions for supply chain transparency",
                "priority": "High"
            },
            {
                "name": "IoT Infrastructure Installation",
                "description": "Deployment of IoT sensors and monitoring systems across manufacturing facilities",
                "priority": "Medium"
            },
            {
                "name": "Flutter Mobile Application Development",
                "description": "Development of cross-platform mobile application using Flutter framework",
                "priority": "High"
            },
            {
                "name": "AI-Powered Analytics Platform",
                "description": "Implementation of machine learning algorithms for predictive analytics and business intelligence",
                "priority": "High"
            },
            {
                "name": "Cloud Migration Project",
                "description": "Migration of on-premises infrastructure to cloud-based solutions for improved scalability",
                "priority": "Medium"
            },
            {
                "name": "Cybersecurity Enhancement Program",
                "description": "Comprehensive security audit and implementation of advanced cybersecurity measures",
                "priority": "High"
            },
            {
                "name": "ERP System Integration",
                "description": "Integration of various business systems with the main ERP platform for unified operations",
                "priority": "Medium"
            },
            {
                "name": "Digital Marketing Automation",
                "description": "Development and implementation of automated marketing workflows and customer engagement systems",
                "priority": "Medium"
            },
            {
                "name": "Smart Factory Implementation",
                "description": "Transformation of traditional manufacturing processes into smart, automated production lines",
                "priority": "High"
            },
            {
                "name": "Data Warehouse Modernization",
                "description": "Upgrade and optimization of data storage and analytics infrastructure for better performance",
                "priority": "Low"
            }
        ]

        # Task templates for different project phases
        self.task_templates = [
            # Project Initiation Phase
            "Project Scope Definition", "Stakeholder Analysis", "Requirements Gathering",
            "Feasibility Study", "Risk Assessment", "Budget Planning", "Resource Allocation",
            "Project Charter Creation", "Team Formation", "Communication Plan Setup",

            # Planning Phase
            "Technical Architecture Design", "System Requirements Analysis", "Database Schema Design",
            "User Interface Mockups", "API Specification", "Security Framework Planning",
            "Quality Assurance Strategy", "Testing Framework Setup", "Deployment Planning",
            "Performance Benchmarking", "Compliance Review", "Vendor Evaluation",

            # Development Phase
            "Backend Development - Core Modules", "Frontend Development - User Interface",
            "Database Implementation", "API Development", "Integration Development",
            "Authentication System", "Authorization Framework", "Data Migration Scripts",
            "Performance Optimization", "Security Implementation", "Error Handling",
            "Logging and Monitoring Setup", "Documentation Creation", "Code Review",

            # Testing Phase
            "Unit Testing", "Integration Testing", "System Testing", "User Acceptance Testing",
            "Performance Testing", "Security Testing", "Load Testing", "Stress Testing",
            "Regression Testing", "Cross-browser Testing", "Mobile Compatibility Testing",
            "Accessibility Testing", "Bug Fixing and Retesting", "Test Documentation",

            # Deployment Phase
            "Production Environment Setup", "Deployment Scripts", "Database Migration",
            "System Configuration", "SSL Certificate Setup", "DNS Configuration",
            "Load Balancer Configuration", "Monitoring Tools Setup", "Backup Strategy Implementation",
            "Disaster Recovery Planning", "Go-Live Preparation", "User Training",

            # Post-Launch Phase
            "System Monitoring", "Performance Analysis", "User Feedback Collection",
            "Bug Fixes and Maintenance", "Feature Enhancement", "Security Updates",
            "System Optimization", "Documentation Updates", "Knowledge Transfer",
            "Project Closure", "Lessons Learned Documentation", "Final Reporting"
        ]

    def _fetch_master_data_options(self):
        """
        Fetches existing options for linked fields (Project Type, Department, etc.)
        directly from ERPNext to ensure valid values are used.
        """
        logger.info(
            "Fetching master data options (Project Type, Department, etc.)...")

        try:
            # Fetch Project Types
            project_types = self.api.get_list("Project Type", fields=["name"])
            self.master_data_options["Project Type"] = [
                pt["name"] for pt in project_types] if project_types else ["Internal"]
            logger.info(
                f"Found Project Types: {self.master_data_options['Project Type']}")

            # Fetch Departments
            departments = self.api.get_list("Department", fields=["name"])
            self.master_data_options["Department"] = [
                dept["name"] for dept in departments] if departments else ["All Departments"]
            logger.info(
                f"Found Departments: {self.master_data_options['Department']}")

        except Exception as e:
            logger.warning(
                f"Could not fetch all master data options: {str(e)}. Using fallback values.")
            # Fallback values if fetch fails
            self.master_data_options["Project Type"] = ["Internal", "External"]
            self.master_data_options["Department"] = ["All Departments"]

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

        # Get available project templates that haven't been created yet
        available_templates = []
        for template in self.project_templates:
            if not self.api.check_exists("Project", template["name"]):
                available_templates.append(template)

        logger.info(
            f"Available templates for creation: {len(available_templates)}")

        for i, template in enumerate(available_templates[:projects_to_create]):
            project_name = template["name"]

            # Check if project already exists
            if self.api.check_exists("Project", project_name):
                logger.debug(
                    f"Project '{project_name}' already exists, skipping.")
                continue

            # Generate project dates
            expected_start = self.generate_date_in_range(
                Config.START_DATE,
                datetime(2025, 6, 30)  # Start projects in first half of 2025
            )

            # Project duration between 3-12 months
            duration_days = random.randint(90, 365)
            expected_end_date = datetime.strptime(
                expected_start, "%Y-%m-%d") + timedelta(days=duration_days)

            # Ensure end date is within 2025
            if expected_end_date > Config.END_DATE:
                expected_end_date = Config.END_DATE

            project_data = {
                "project_name": project_name,
                "project_type": random.choice(self.master_data_options["Project Type"]) if self.master_data_options["Project Type"] else None,
                # More weight on Open
                "status": random.choice(["Open", "Open", "Open", "Completed", "On Hold"]),
                "priority": template["priority"],
                "company": Config.COMPANY_NAME,
                "expected_start_date": expected_start,
                "expected_end_date": expected_end_date.strftime("%Y-%m-%d"),
                # 50M - 2B IDR
                "estimated_costing": random.randint(50_000_000, 2_000_000_000),
                "notes": template["description"],
                "percent_complete_method": "Task Completion",
                "collect_progress": 1
            }

            # Add department if available
            if self.master_data_options["Department"]:
                project_data["department"] = random.choice(
                    self.master_data_options["Department"])

            try:
                project = self.api.create_doc("Project", project_data)
                # Handle different response formats
                project_name_for_cache = project.get("name") or project.get(
                    "project_name") or template["name"]
                project_cache_entry = {
                    "name": project_name_for_cache,
                    "project_name": template["name"]
                }
                self.projects.append(project_cache_entry)
                projects_created_count += 1
                logger.info(
                    f"Created project: '{template['name']}' (ID: {project_name_for_cache})")

                # Small delay to avoid overwhelming the server
                time.sleep(0.5)

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

        total_tasks_created = 0

        for project in self.projects:
            logger.info(
                f"Creating tasks for project: '{project.get('project_name', project.get('name'))}'")

            try:
                # Get project details
                project_doc = self.api.get_doc("Project", project["name"])
                project_start = datetime.strptime(project_doc.get(
                    "expected_start_date", Config.START_DATE.strftime("%Y-%m-%d")), "%Y-%m-%d")
                project_end = datetime.strptime(project_doc.get(
                    "expected_end_date", Config.END_DATE.strftime("%Y-%m-%d")), "%Y-%m-%d")

                # Check existing tasks for this project
                existing_tasks = self.api.get_list("Task",
                                                   filters={
                                                       "project": project["name"]},
                                                   fields=["name", "subject"])

                existing_task_count = len(existing_tasks)
                tasks_to_create = Config.TASKS_PER_PROJECT - existing_task_count

                if tasks_to_create <= 0:
                    logger.info(
                        f"Project '{project.get('project_name')}' already has {existing_task_count} tasks. Skipping.")
                    continue

                logger.info(
                    f"Creating {tasks_to_create} tasks for project '{project.get('project_name')}'")

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

                # Randomize task templates to avoid predictable patterns
                shuffled_tasks = random.sample(self.task_templates, min(
                    len(self.task_templates), tasks_to_create))

                for i in range(tasks_to_create):
                    if i < len(shuffled_tasks):
                        task_subject = f"{shuffled_tasks[i]} - {project.get('project_name', 'Project')}"
                    else:
                        # Fallback for additional tasks
                        task_subject = f"Task {i+1} - {project.get('project_name', 'Project')}"

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
                        "project": project["name"],
                        "status": random.choice(["Open", "Working", "Completed", "Cancelled"]),
                        "priority": random.choice(["Low", "Medium", "High"]),
                        "expected_start_date": task_start.strftime("%Y-%m-%d"),
                        "expected_end_date": task_end.strftime("%Y-%m-%d"),
                        # 1-10 working days in hours
                        "expected_time": random.randint(8, 80),
                        "description": f"Detailed implementation of {shuffled_tasks[i] if i < len(shuffled_tasks) else 'project task'} for the project",
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
                    f"Created {tasks_created_for_project} tasks for project '{project.get('project_name')}'")

            except Exception as e:
                logger.error(
                    f"Error creating tasks for project '{project.get('project_name', project.get('name'))}': {str(e)}")
                continue

        logger.info(f"Total tasks created: {total_tasks_created}")

    def submit_completed_projects_and_tasks(self):
        """Submit projects and tasks that are marked as completed"""
        logger.info("Submitting completed projects and tasks...")

        submitted_projects = 0
        submitted_tasks = 0

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
                        f"Submitted project: {project['project_name']}")
                except Exception as e:
                    logger.warning(
                        f"Failed to submit project {project['project_name']}: {str(e)}")

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
                except Exception as e:
                    logger.warning(
                        f"Failed to submit task {task['subject']}: {str(e)}")

        except Exception as e:
            logger.error(f"Error during submission process: {str(e)}")

        logger.info(
            f"Submitted {submitted_projects} projects and {submitted_tasks} tasks")

    def check_existing_data(self):
        """Check if we already have sufficient data and skip generation if target is reached"""
        logger.info("Checking existing project and task data...")

        try:
            # Get current counts
            existing_projects = self.api.get_list(
                "Project", filters={"company": Config.COMPANY_NAME}, fields=["name"])
            existing_tasks = self.api.get_list("Task", fields=["name"])

            current_project_count = len(existing_projects)
            current_task_count = len(existing_tasks)

            logger.info(
                f"Current data: {current_project_count} projects, {current_task_count} tasks")
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
            # Check if target is already reached
            if self.check_existing_data():
                logger.info("=== SKIPPING DATA GENERATION ===")
                logger.info(
                    "Target already reached. Use --force flag if you want to create additional data.")
                return

            logger.info("=== Creating Projects ===")
            self.create_projects()

            logger.info("=== Creating Tasks for Projects ===")
            self.create_tasks_for_projects()

            logger.info("=== Submitting Completed Items ===")
            self.submit_completed_projects_and_tasks()

            logger.info("=== Data Generation Complete ===")

            # Final summary
            final_project_count = len(self.api.get_list(
                "Project", filters={"company": Config.COMPANY_NAME}, fields=["name"]))
            final_task_count = len(self.api.get_list("Task", fields=["name"]))

            logger.info("Final Summary:")
            logger.info(f"- Total Projects: {final_project_count}")
            logger.info(f"- Total Tasks: {final_task_count}")
            logger.info(
                f"- Average Tasks per Project: {final_task_count / max(1, final_project_count):.1f}")

            # Check if target was reached
            if final_project_count >= Config.PROJECT_COUNT and final_task_count >= Config.TOTAL_TASKS:
                logger.info("TARGET SUCCESSFULLY REACHED!")
            else:
                logger.info(
                    "Target not fully reached. You may want to run the script again.")

        except Exception as e:
            logger.error(f"Fatal error during data generation: {str(e)}")
            raise


def main():
    """Main entry point"""
    print("=" * 80)
    print("ERPNext Project Module Dummy Data Generator")
    print("=" * 80)
    print(f"API Endpoint: {Config.BASE_URL}")
    print(f"Company: {Config.COMPANY_NAME}")
    print(f"Target Projects: {Config.PROJECT_COUNT}")
    print(f"Tasks per Project: {Config.TASKS_PER_PROJECT}")
    print(f"Total Tasks: {Config.TOTAL_TASKS}")
    print(
        f"Date Range: {Config.START_DATE.strftime('%Y-%m-%d')} to {Config.END_DATE.strftime('%Y-%m-%d')}")
    print("=" * 80)

    response = input(
        "\nThis script will create projects and tasks in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = ProjectDataGenerator()
        generator.generate_all_data()

        print("\n" + "=" * 80)
        print("Project data generation completed successfully!")
        print("Check the log file for detailed information: erpnext_project_data_generation.log")
        print("=" * 80)
    except Exception as e:
        print(f"\n" + "=" * 80)
        print(f"Data generation failed due to a critical error: {e}")
        print("Please check the log file 'erpnext_project_data_generation.log' for more details.")
        print("=" * 80)


if __name__ == "__main__":
    main()

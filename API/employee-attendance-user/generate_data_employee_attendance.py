#!/usr/bin/env python3
"""
ERPNext Dummy Data Generator (Pure HR Focus: Employees & Attendance)
Generates realistic dummy data for existing ERPNext v16 setup.
Automatically ensures essential Roles and a Holiday List.
Focus: Employees and Attendance only (no accounting or other modules).
Author: ERPNext Data Generator
Version: 3.0.0
"""

import requests
import json
import random
import logging
import time  # Added for sleep functionality
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

# Initialize Faker for Indonesian locale
fake = Faker('id_ID')

# Configuration


class Config:
    # API Configuration
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"  # Adjust based on your Docker setup

    # Data Volumes
    USER_COUNT = 50  # Users are prerequisite for Employees
    EMPLOYEE_COUNT = 50  # Target number of employees

    # Date Range for 2025
    START_DATE = datetime(2025, 1, 1)
    END_DATE = datetime(2025, 12, 31)

    # Company Details (MUST MATCH EXISTING SETUP)
    # Ensure this exactly matches your ERPNext company name
    COMPANY_NAME = "PT Fiyansa Mulya"
    COMPANY_ABBR = "PFM"  # Used for email, etc.

    # Batch Processing
    BATCH_SIZE = 50
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # seconds


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # Changed log file name
        logging.FileHandler('erpnext_hr_data_generation.log'),
        StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


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
            # Using session.request for flexibility with json/params based on method
            response = self.session.request(method, url, json=data if method in [
                                            "POST", "PUT"] else None, params=data if method == "GET" else None)
            response.raise_for_status()  # This will raise an HTTPError for 4xx/5xx responses
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
                raise  # Re-raise the exception after retries are exhausted

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

        # Fix for "TypeError: get_list() got multiple values for argument 'doctype'"
        # Remove 'doctype' from params, as it's already in the URL path.
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
        """Check if document exists. For specific doctypes, uses filters for robust check."""
        try:
            if doctype == "User":
                result = self.get_list(
                    doctype, filters={"email": name}, fields=["name"])
                return len(result) > 0
            elif doctype == "Employee":
                result = self.get_list(doctype, filters={
                                       "employee_name": name, "company": Config.COMPANY_NAME}, fields=["name"])
                return len(result) > 0
            elif doctype == "Attendance":
                # Assuming 'name' contains employee and date like "EMP-001-2025-01-01"
                parts = name.split('-')
                if len(parts) >= 2:  # At least employee ID and date should be there
                    employee_id = "-".join(parts[:-3]
                                           ) if len(parts) > 3 else parts[0]
                    # Year-MM-DD is last 3 parts
                    attendance_date = "-".join(parts[-3:])
                    if employee_id and attendance_date:
                        result = self.get_list(doctype, filters={
                                               "employee": employee_id, "attendance_date": attendance_date}, fields=["name"])
                        return len(result) > 0
                logger.warning(
                    f"Could not parse name '{name}' for Attendance check_exists, trying direct get_doc.")
                self.get_doc(doctype, name)  # Fallback to direct get_doc
                return True
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


class DataGenerator:
    """Generates realistic dummy data using Faker"""

    def __init__(self):
        self.fake = Faker('id_ID')  # Initialize Faker for Indonesian locale
        self.api = ERPNextAPI()

        # Cache for generated data
        self.users = []
        self.employees = []
        # Cache for fetched master data options (Gender, Dept, etc.)
        self.master_data_options = {}

        # --- Prerequisite Setup Order --- (Called ONLY HERE)
        self._ensure_essential_roles_exist()
        self._ensure_holiday_list_exists()
        self._fetch_master_data_options()

    def _ensure_essential_roles_exist(self):
        """
        Ensures essential ERPNext Roles (not Role Profiles) exist as per the provided JSON.
        These are directly used for User creation.
        """
        logger.info(
            "Ensuring essential ERPNext Roles exist (based on provided JSON data)...")

        essential_roles = [
            "Employee", "Desk User", "Accounts Manager", "Accounts User",
            "Sales Manager", "Sales User", "Stock Manager", "Stock User",
            "HR Manager", "HR User", "System Manager", "Administrator"
        ]

        existing_roles = self.api.get_list(
            "Role", fields=["name", "role_name"])
        existing_role_names = {role["role_name"] for role in existing_roles}

        roles_created_count = 0
        for role_name in essential_roles:
            if role_name not in existing_role_names:
                logger.info(
                    f"Role '{role_name}' not found. Attempting to create it.")
                role_data = {
                    "name": role_name,  # IMPORTANT: Explicitly set the document name
                    "role_name": role_name,
                    "desk_access": 1
                }
                try:
                    self.api.create_doc("Role", role_data)
                    roles_created_count += 1
                    logger.info(f"Created Role: '{role_name}'")
                except Exception as e:
                    logger.warning(
                        f"Failed to create Role '{role_name}': {str(e)}")
            else:
                logger.debug(f"Role '{role_name}' already exists.")
        logger.info(
            f"Ensured {roles_created_count} essential roles (created or found).")

    def _ensure_holiday_list_exists(self):
        """Ensures the default holiday list for the current year exists."""
        holiday_list_name = f"Holiday List {datetime.now().year}"
        logger.info(f"Ensuring Holiday List: '{holiday_list_name}' exists...")

        if not self.api.check_exists("Holiday List", holiday_list_name):
            logger.info(
                f"Holiday List '{holiday_list_name}' not found. Attempting to create it.")
            holidays = [
                {"holiday_date": f"{datetime.now().year}-01-01",
                 "description": "Tahun Baru Masehi"},
                {"holiday_date": f"{datetime.now().year}-02-08",
                 "description": "Isra Miraj Nabi Muhammad SAW"},
                {"holiday_date": f"{datetime.now().year}-03-31",
                 "description": "Hari Raya Nyepi Tahun Baru Saka 1947"},
                {"holiday_date": f"{datetime.now().year}-04-18",
                 "description": "Wafat Yesus Kristus"},
                {"holiday_date": f"{datetime.now().year}-04-30",
                 "description": "Hari Raya Idul Fitri 1446 H (Estimasi)"},
                {"holiday_date": f"{datetime.now().year}-05-01",
                 "description": "Hari Buruh Internasional"},
                {"holiday_date": f"{datetime.now().year}-05-29",
                 "description": "Kenaikan Isa Al Masih"},
                {"holiday_date": f"{datetime.now().year}-06-01",
                 "description": "Hari Lahir Pancasila"},
                {"holiday_date": f"{datetime.now().year}-06-02",
                 "description": "Hari Raya Waisak 2569 BE (Estimasi)"},
                {"holiday_date": f"{datetime.now().year}-06-17",
                 "description": "Hari Raya Idul Adha 1446 H (Estimasi)"},
                {"holiday_date": f"{datetime.now().year}-07-07",
                 "description": "Tahun Baru Islam 1447 H"},
                {"holiday_date": f"{datetime.now().year}-08-17",
                 "description": "Hari Kemerdekaan RI"},
                {"holiday_date": f"{datetime.now().year}-09-16",
                 "description": "Maulid Nabi Muhammad SAW"},
                {"holiday_date": f"{datetime.now().year}-12-25",
                 "description": "Hari Raya Natal"}
            ]

            holiday_data = {
                "holiday_list_name": holiday_list_name,
                "from_date": f"{datetime.now().year}-01-01",
                "to_date": f"{datetime.now().year}-12-31",
                "holidays": holidays
            }
            try:
                self.api.create_doc("Holiday List", holiday_data)
                logger.info(f"Created Holiday List: '{holiday_list_name}'")
            except Exception as e:
                logger.warning(
                    f"Failed to create Holiday List '{holiday_list_name}': {str(e)}")
        else:
            logger.debug(f"Holiday List '{holiday_list_name}' already exists.")

    def _fetch_master_data_options(self):
        """
        Fetches existing options for linked fields (Gender, Department, Designation, Employment Type)
        directly from ERPNext to ensure valid values are used.
        If lists are empty, it attempts to create some standard English defaults.
        """
        logger.info(
            "Fetching master data options (Gender, Department, Designation, Employment Type)...")

        # Helper to fetch and create if empty
        def _get_and_create_if_empty(doctype_name: str, field_name: str, default_options: List[str], parent_field: Optional[str] = None, parent_value: Optional[str] = None) -> List[str]:
            fetched_options = []
            try:
                records = self.api.get_list(doctype_name, fields=["name"])
                fetched_options = [r["name"] for r in records]

                # If no options found, try to create default ones
                if not fetched_options:
                    logger.warning(
                        f"No {doctype_name}s found in ERPNext. Attempting to create default {doctype_name}s.")
                    for option_name in default_options:
                        # Ensure option_name is not empty for logging
                        if not option_name:
                            continue
                        if not self.api.check_exists(doctype_name, option_name):
                            try:
                                # Use field_name as well as 'name'
                                create_data = {
                                    field_name: option_name, "name": option_name}
                                if parent_field and parent_value:
                                    # For Department parent_department
                                    create_data[parent_field] = parent_value
                                self.api.create_doc(doctype_name, create_data)
                                fetched_options.append(option_name)
                                logger.info(
                                    f"Created {doctype_name}: '{option_name}'")
                            except Exception as e:
                                logger.warning(
                                    f"Failed to create {doctype_name} '{option_name}': {str(e)}")
                    # After attempted creation, re-verify fetched_options
                    if not fetched_options:  # If still empty after creation attempts
                        records = self.api.get_list(
                            doctype_name, fields=["name"])
                        fetched_options = [r["name"] for r in records]

                logger.info(f"Fetched {doctype_name}s: {fetched_options}")
            except Exception as e:
                logger.error(
                    f"Failed to fetch or create {doctype_name}s. This might be a critical ERPNext issue. Error: {str(e)}")
                fetched_options = []  # Ensure it's an empty list if API call itself fails

            # Always ensure at least one fallback option if everything else fails.
            # This is the last resort to prevent random.choice from failing.
            if not fetched_options:
                logger.critical(
                    f"CRITICAL: {doctype_name}s list is still empty after all attempts. Using hardcoded emergency fallback: '{default_options[0]}'. Data may be inconsistent.")
                # Return first default option as last resort
                return [default_options[0]]
            return fetched_options

        self.master_data_options["Gender"] = _get_and_create_if_empty(
            "Gender", "gender", ["Male", "Female", "Other"])

        self.master_data_options["Department"] = _get_and_create_if_empty(
            # Use some Indonesian and English common
            "Department", "department_name", [
                "Sales", "Human Resources", "Kualitas", "Produksi", "IT"],
            # Assuming "All Departments" is a default group
            "parent_department", "All Departments")

        self.master_data_options["Designation"] = _get_and_create_if_empty(
            "Designation", "designation_name", ["Employee", "Manager", "Analyst", "Engineer", "Supervisor"])

        self.master_data_options["Employment Type"] = _get_and_create_if_empty(
            "Employment Type", "employment_type_name", ["Full-time", "Part-time", "Contract", "Intern"])

        logger.info("Finished fetching and ensuring master data options.")

    def generate_phone(self) -> str:
        """Generate valid Indonesian phone number."""
        return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"

    def generate_date_in_range(self, start_date: datetime, end_date: datetime, exclude_weekends: bool = True) -> str:
        """Generate random date within range."""
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

    def generate_time(self) -> str:
        """Generate random time."""
        hour = random.randint(8, 18)
        minute = random.choice([0, 15, 30, 45])
        return f"{hour:02d}:{minute:02d}:00"

    def create_users(self):
        """Create user accounts."""
        logger.info(f"Creating {Config.USER_COUNT} users...")

        # Fetch existing users
        # Need to fetch roles from existing users separately, as 'roles' is a child table
        existing_users_raw = self.api.get_list(
            "User", fields=["name", "email"])
        self.users = []
        for user_raw in existing_users_raw:
            try:
                # Fetch full user doc for roles
                user_doc = self.api.get_doc("User", user_raw["name"])
                user_roles = [r["role"] for r in user_doc.get("roles", [])]
                self.users.append(
                    {"name": user_raw["name"], "email": user_raw["email"], "roles": user_roles})
            except Exception as e:
                logger.warning(
                    f"Could not fetch full details for existing user '{user_raw.get('name', user_raw.get('email'))}': {str(e)}. Skipping this user in cache.")

        logger.info(f"Found {len(self.users)} existing users.")

        # --- SMART SKIP LOGIC ---
        if len(self.users) >= Config.USER_COUNT:
            logger.info(
                f"Already have {len(self.users)} users (>= target {Config.USER_COUNT}). Skipping new user creation.")
            return
        # --- END SMART SKIP ---

        users_to_create = Config.USER_COUNT - len(self.users)
        logger.info(
            f"Creating {users_to_create} new users to reach target {Config.USER_COUNT}...")

        users_created_count = 0

        direct_roles_for_users = [
            "Employee", "Sales User", "Purchase User", "Stock User", "Accounts User", "HR User"]

        for i in range(users_to_create):
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            # Ensure unique email by appending a unique counter based on total existing + new
            email = f"{first_name.lower()}.{last_name.lower()}_{len(self.users) + i}@{Config.COMPANY_ABBR.lower()}.com"

            if self.api.check_exists("User", email):
                logger.debug(
                    f"User with email {email} already exists, skipping. (This should not happen for new emails in this loop)")
                continue

            assigned_role = random.choice(direct_roles_for_users)

            user_data = {
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "enabled": 1,
                "send_welcome_email": 0,
                "language": "en",
                "time_zone": "Asia/Jakarta",
                "roles": [{"role": assigned_role}]
            }

            try:
                user = self.api.create_doc("User", user_data)
                user_with_roles = {**user, "roles": [assigned_role]}
                self.users.append(user_with_roles)
                users_created_count += 1
                logger.debug(
                    f"Created user: {email} with role: {assigned_role}")
                time.sleep(1.5)  # Increased delay to avoid throttling
            except Exception as e:
                logger.warning(f"Failed to create user {email}: {str(e)}")
                time.sleep(3)  # Longer sleep on failure

        logger.info(
            f"Created {users_created_count} new users. Total users (existing + new): {len(self.users)}")

    def create_employees(self):
        """Create employee records with proper date handling."""
        logger.info(f"Creating {Config.EMPLOYEE_COUNT} employees...")

        existing_employees = self.api.get_list("Employee", filters={"company": Config.COMPANY_NAME}, fields=[
                                               "name", "employee_name", "designation", "user_id"])
        self.employees = existing_employees
        logger.info(
            f"Found {len(self.employees)} existing employees for '{Config.COMPANY_NAME}'.")

        # Prepare available users for linking (those not already linked to an employee)
        linked_user_ids = {emp.get("user_id")
                           for emp in self.employees if emp.get("user_id")}
        available_users = [u for u in self.users if u and isinstance(u, dict) and u.get(
            "name") and u["name"] not in linked_user_ids and u["name"] not in ["Administrator", "Guest"]]
        random.shuffle(available_users)
        user_assign_idx = 0

        # --- SMART SKIP LOGIC ---
        if len(self.employees) >= Config.EMPLOYEE_COUNT:
            logger.info(
                f"Already have {len(self.employees)} employees (>= target {Config.EMPLOYEE_COUNT}). Skipping new employee creation.")
            return
        # --- END SMART SKIP ---

        employees_to_create = Config.EMPLOYEE_COUNT - len(self.employees)
        logger.info(
            f"Creating {employees_to_create} new employees to reach target {Config.EMPLOYEE_COUNT}...")

        employees_created_count = 0

        # Use fetched master data options for employee creation
        genders = self.master_data_options["Gender"]
        departments = self.master_data_options["Department"]
        designations = self.master_data_options["Designation"]
        employment_types = self.master_data_options["Employment Type"]

        for i in range(employees_to_create):
            employee_name = f"Pegawai-{len(self.employees) + i + 1:03d}"

            if self.api.check_exists("Employee", employee_name):
                logger.debug(
                    f"Employee '{employee_name}' already exists, skipping. (This should not happen for new names in this loop)")
                continue

            first_name = "Pegawai"
            last_name = f"{len(self.employees) + i + 1:03d}"

            # FIXED: Ensure joining date is BEFORE 2025 so attendance can be created for 2025
            # Generate joining dates in 2024 or earlier to allow full 2025 attendance
            date_of_joining = self.generate_date_in_range(
                datetime(2020, 1, 1),
                # Changed from Config.START_DATE to end of 2024
                datetime(2024, 12, 31)
            )

            date_of_birth = self.generate_date_in_range(
                datetime(1960, 1, 1),
                datetime(2000, 12, 31)
            )

            employee_data = {
                "employee_name": employee_name,
                "first_name": first_name,
                "last_name": last_name,
                "company": Config.COMPANY_NAME,
                "status": "Active",
                "gender": random.choice(genders),  # Use fetched gender options
                "date_of_birth": date_of_birth,
                "date_of_joining": date_of_joining,
                # Use fetched department options
                "department": random.choice(departments),
                # Use fetched designation options
                "designation": random.choice(designations),
                # Use fetched employment type options
                "employment_type": random.choice(employment_types),
                "holiday_list": f"Holiday List {datetime.now().year}",

                # Actual email address
                "company_email": f"{first_name.lower()}.{last_name.lower()}@{Config.COMPANY_ABBR.lower()}.com",
                # Option to select source of preferred email
                "prefered_contact_email": "Company Email",
                "personal_email": self.fake.email(),  # Personal email

                "cell_number": self.generate_phone(),
                "permanent_address": self.fake.address().replace('\n', ', '),
                "current_address": self.fake.address().replace('\n', ', '),
                "emergency_contact_name": self.fake.name(),
                "emergency_phone_number": self.generate_phone(),
                "bank_name": random.choice(["Bank Mandiri", "BCA", "BRI", "BNI"]),
                "bank_ac_no": str(random.randint(1000000000, 9999999999)),
                "salary_mode": "Bank"
            }

            if user_assign_idx < len(available_users):
                employee_data["user_id"] = available_users[user_assign_idx]["name"]
                user_assign_idx += 1
            else:
                logger.warning(
                    f"No more unlinked users available for employee '{employee_name}'. Employee will be created without a linked User.")

            # Add employee hierarchy (20% are managers)
            if employees_created_count > 10 and random.random() < 0.8:
                # Managers must be selected from already existing or created employees
                active_managers = [e for e in self.employees if any(m in e.get(
                    "designation", "") for m in ["Manager", "Manajer", "Director", "Direktur", "VP"])]
                if active_managers:
                    employee_data["reports_to"] = random.choice(active_managers)[
                        "name"]

            try:
                employee = self.api.create_doc("Employee", employee_data)
                self.employees.append(employee)
                employees_created_count += 1
                logger.debug(
                    f"Created employee: '{employee_name}' with joining date: {date_of_joining}")
            except Exception as e:
                logger.warning(
                    f"Failed to create employee '{employee_name}': {str(e)}")

        logger.info(
            f"Created {employees_created_count} new employees. Total employees (existing + new): {len(self.employees)}")

    def create_attendance_records(self):
        """Create attendance records for employees."""
        logger.info(
            "Creating attendance records for all employees across 2025...")

        attendance_count = 0

        if not self.employees:
            logger.warning(
                "No employees found. Skipping attendance record creation.")
            return

        # Process each employee individually to respect their joining date
        for employee in self.employees:
            try:
                # Get employee's joining date - this is critical!
                employee_doc = self.api.get_doc("Employee", employee["name"])
                joining_date_str = employee_doc.get("date_of_joining")

                if not joining_date_str:
                    logger.warning(
                        f"Employee '{employee['employee_name']}' has no joining date. Skipping attendance creation.")
                    continue

                # Parse joining date
                joining_date = datetime.strptime(joining_date_str, "%Y-%m-%d")

                # Determine the start date for attendance (later of joining date or 2025-01-01)
                attendance_start_date = max(joining_date, Config.START_DATE)

                logger.debug(
                    f"Creating attendance for '{employee['employee_name']}' from {attendance_start_date.strftime('%Y-%m-%d')} to {Config.END_DATE.strftime('%Y-%m-%d')}")

                current_date = attendance_start_date

                while current_date <= Config.END_DATE:
                    # Check if attendance already exists for this employee and date
                    existing_attendance = self.api.get_list("Attendance", filters={
                        "employee": employee["name"],
                        "attendance_date": current_date.strftime("%Y-%m-%d")
                    }, fields=["name"])

                    if existing_attendance:
                        logger.debug(
                            f"Attendance for '{employee['employee_name']}' on {current_date.strftime('%Y-%m-%d')} already exists, skipping.")
                        current_date += timedelta(days=1)
                        continue

                    # Determine attendance status based on day type
                    if current_date.weekday() >= 5:  # Weekend (Saturday = 5, Sunday = 6)
                        status = random.choices(
                            ["Absent", "On Leave"], weights=[0.7, 0.3])[0]
                    else:  # Weekday
                        status = random.choices(
                            ["Present", "Half Day", "Work From Home",
                                "Absent", "On Leave"],
                            weights=[0.65, 0.15, 0.10, 0.05, 0.05]
                        )[0]

                    attendance_data = {
                        "employee": employee["name"],
                        "attendance_date": current_date.strftime("%Y-%m-%d"),
                        "status": status,
                        "company": Config.COMPANY_NAME,
                    }

                    # Add time details for working statuses
                    if status in ["Present", "Half Day", "Work From Home"]:
                        check_in_time = f"{current_date.strftime('%Y-%m-%d')} 08:{random.randint(0, 30):02d}:00"
                        check_out_time = f"{current_date.strftime('%Y-%m-%d')} 17:{random.randint(0, 45):02d}:00"
                        working_hours = random.uniform(
                            7.0, 9.0) if status == "Present" else random.uniform(3.0, 4.5)

                        attendance_data.update({
                            "check_in_time": check_in_time,
                            "check_out_time": check_out_time,
                            "working_hours": working_hours
                        })

                    try:
                        self.api.create_doc("Attendance", attendance_data)
                        attendance_count += 1
                        logger.debug(
                            f"Created attendance for '{employee['employee_name']}' on {current_date.strftime('%Y-%m-%d')} with status: {status}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to create attendance for '{employee['employee_name']}' on {current_date.strftime('%Y-%m-%d')} with status {status}: {str(e)}")

                    current_date += timedelta(days=1)

            except Exception as e:
                logger.error(
                    f"Error processing attendance for employee '{employee.get('employee_name', employee.get('name'))}': {str(e)}")
                continue

        logger.info(
            f"Created {attendance_count} attendance records from regular employee processing.")

        # Now ensure we have at least 200 attendance records by adding random ones
        self.ensure_minimum_attendance_records(target_count=200)

    def ensure_minimum_attendance_records(self, target_count=200):
        """Ensure we have at least the target number of attendance records by adding random ones."""
        logger.info(
            f"Ensuring we have at least {target_count} attendance records...")

        try:
            # Get current attendance count (all records, don't exclude any)
            current_attendance = self.api.get_list("Attendance",
                                                   filters={
                                                       "company": Config.COMPANY_NAME},
                                                   fields=["name", "employee",
                                                           "attendance_date", "status", "docstatus"]
                                                   )

            current_count = len(current_attendance)
            logger.info(f"Current attendance records: {current_count}")

            if current_count >= target_count:
                logger.info(
                    f"Already have {current_count} attendance records (>= target {target_count}). No additional records needed.")
                return

            # Calculate how many more we need
            additional_needed = target_count - current_count
            logger.info(
                f"Need to create {additional_needed} additional attendance records to reach target {target_count}.")

            # Get list of existing attendance to avoid duplicates
            existing_combinations = set()
            for att in current_attendance:
                existing_combinations.add(
                    (att["employee"], att["attendance_date"]))

            # Create additional random attendance records
            created_count = 0
            max_attempts = additional_needed * 10  # Increase attempts to handle duplicates
            attempts = 0

            while created_count < additional_needed and attempts < max_attempts:
                attempts += 1

                # Select random employee
                employee = random.choice(self.employees)

                # Get employee's joining date to ensure we don't create attendance before joining
                try:
                    employee_doc = self.api.get_doc(
                        "Employee", employee["name"])
                    joining_date_str = employee_doc.get("date_of_joining")

                    if not joining_date_str:
                        continue

                    joining_date = datetime.strptime(
                        joining_date_str, "%Y-%m-%d")
                    earliest_date = max(joining_date, Config.START_DATE)

                    # Generate random date within 2025 and after joining date
                    random_date = self.generate_random_date_in_2025(
                        earliest_date)

                    # Check if this combination already exists
                    if (employee["name"], random_date) in existing_combinations:
                        continue

                    # Create attendance record
                    status = self.generate_random_attendance_status(
                        random_date)

                    attendance_data = {
                        "employee": employee["name"],
                        "attendance_date": random_date,
                        "status": status,
                        "company": Config.COMPANY_NAME,
                    }

                    # Add time details for working statuses
                    if status in ["Present", "Half Day", "Work From Home"]:
                        date_obj = datetime.strptime(random_date, "%Y-%m-%d")
                        check_in_time = f"{random_date} 08:{random.randint(0, 30):02d}:00"
                        check_out_time = f"{random_date} 17:{random.randint(0, 45):02d}:00"
                        working_hours = random.uniform(
                            7.0, 9.0) if status == "Present" else random.uniform(3.0, 4.5)

                        attendance_data.update({
                            "check_in_time": check_in_time,
                            "check_out_time": check_out_time,
                            "working_hours": working_hours
                        })

                    try:
                        self.api.create_doc("Attendance", attendance_data)
                        existing_combinations.add(
                            (employee["name"], random_date))
                        created_count += 1
                        logger.debug(
                            f"Created additional attendance #{created_count} for '{employee['employee_name']}' on {random_date} with status: {status}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to create additional attendance for '{employee['employee_name']}' on {random_date}: {str(e)}")

                except Exception as e:
                    logger.warning(
                        f"Error processing additional attendance for employee '{employee.get('employee_name')}': {str(e)}")
                    continue

            logger.info(
                f"Created {created_count} additional attendance records. Total should now be {current_count + created_count}.")

        except Exception as e:
            logger.error(
                f"Error in ensure_minimum_attendance_records: {str(e)}")

    def create_additional_random_attendance(self, target_count=200):
        """Create additional random attendance records to reach target count."""
        logger.info(
            f"Creating additional random attendance records to reach {target_count} total...")

        try:
            # Get current attendance count
            current_attendance = self.api.get_list("Attendance",
                                                   filters={
                                                       "company": Config.COMPANY_NAME},
                                                   fields=[
                                                       "name", "employee", "attendance_date", "status"]
                                                   )

            current_count = len(current_attendance)
            logger.info(f"Current attendance records: {current_count}")

            if current_count >= target_count:
                logger.info(
                    f"Already have {current_count} attendance records (>= target {target_count}). No additional records needed.")
                return

            # Calculate how many more we need
            additional_needed = target_count - current_count
            logger.info(
                f"Creating {additional_needed} additional random attendance records...")

            # Get list of existing attendance to avoid duplicates
            existing_combinations = set()
            for att in current_attendance:
                existing_combinations.add(
                    (att["employee"], att["attendance_date"]))

            # Create additional random attendance records
            created_count = 0
            max_attempts = additional_needed * 15  # More attempts for larger dataset
            attempts = 0

            while created_count < additional_needed and attempts < max_attempts:
                attempts += 1

                # Select random employee
                employee = random.choice(self.employees)

                # Generate completely random date in 2025 (respecting joining date)
                try:
                    employee_doc = self.api.get_doc(
                        "Employee", employee["name"])
                    joining_date_str = employee_doc.get("date_of_joining")

                    if not joining_date_str:
                        # If no joining date, use start of 2025
                        earliest_date = Config.START_DATE
                    else:
                        joining_date = datetime.strptime(
                            joining_date_str, "%Y-%m-%d")
                        earliest_date = max(joining_date, Config.START_DATE)

                    # Generate random date within 2025
                    random_date = self.generate_random_date_in_2025(
                        earliest_date)

                    # Check if this combination already exists
                    if (employee["name"], random_date) in existing_combinations:
                        continue

                    # Create realistic attendance record
                    status = self.generate_random_attendance_status(
                        random_date)

                    attendance_data = {
                        "employee": employee["name"],
                        "attendance_date": random_date,
                        "status": status,
                        "company": Config.COMPANY_NAME,
                    }

                    # Add time details for working statuses
                    if status in ["Present", "Half Day", "Work From Home"]:
                        check_in_time = f"{random_date} 08:{random.randint(0, 30):02d}:00"
                        check_out_time = f"{random_date} 17:{random.randint(0, 45):02d}:00"
                        working_hours = random.uniform(
                            7.0, 9.0) if status == "Present" else random.uniform(3.0, 4.5)

                        attendance_data.update({
                            "check_in_time": check_in_time,
                            "check_out_time": check_out_time,
                            "working_hours": working_hours
                        })

                    try:
                        new_attendance = self.api.create_doc(
                            "Attendance", attendance_data)
                        existing_combinations.add(
                            (employee["name"], random_date))
                        created_count += 1

                        if created_count % 20 == 0:  # Progress update every 20 records
                            logger.info(
                                f"Progress: Created {created_count}/{additional_needed} additional attendance records...")

                        logger.debug(
                            f"Created random attendance #{created_count} for '{employee['employee_name']}' on {random_date} with status: {status}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to create random attendance for '{employee['employee_name']}' on {random_date}: {str(e)}")

                except Exception as e:
                    logger.warning(
                        f"Error processing random attendance for employee '{employee.get('employee_name')}': {str(e)}")
                    continue

            logger.info(
                f"Successfully created {created_count} additional random attendance records!")
            logger.info(
                f"Total attendance records should now be approximately {current_count + created_count}")

        except Exception as e:
            logger.error(
                f"Error in create_additional_random_attendance: {str(e)}")

    def submit_all_draft_attendance(self):
        """Submit all draft attendance records."""
        logger.info("Submitting all draft attendance records...")

        try:
            # Get all draft attendance records (docstatus = 0)
            draft_attendance = self.api.get_list("Attendance",
                                                 filters={
                                                     "docstatus": 0, "company": Config.COMPANY_NAME},
                                                 fields=[
                                                     "name", "employee", "attendance_date", "status"]
                                                 )

            if not draft_attendance:
                logger.info("No draft attendance records found to submit.")
                return

            logger.info(
                f"Found {len(draft_attendance)} draft attendance records to submit.")

            submitted_count = 0
            failed_count = 0

            for attendance in draft_attendance:
                try:
                    # Submit the attendance record
                    self.api.submit_doc("Attendance", attendance["name"])
                    submitted_count += 1

                    if submitted_count % 50 == 0:  # Progress update every 50 submissions
                        logger.info(
                            f"Submission progress: {submitted_count}/{len(draft_attendance)} completed...")

                    logger.debug(f"Submitted attendance: {attendance['name']}")

                    # Small delay to avoid overwhelming the server
                    time.sleep(0.1)

                except Exception as e:
                    failed_count += 1
                    logger.warning(
                        f"Failed to submit attendance {attendance['name']}: {str(e)}")

            logger.info(
                f"Attendance submission completed: {submitted_count} submitted, {failed_count} failed.")

        except Exception as e:
            logger.error(
                f"Error while submitting draft attendance records: {str(e)}")

    def generate_random_date_in_2025(self, earliest_date):
        """Generate a random date within 2025, but not before the earliest_date."""
        # Ensure earliest_date is within 2025
        start_date = max(earliest_date, Config.START_DATE)
        end_date = min(Config.END_DATE, datetime(2025, 12, 31))

        if start_date > end_date:
            # If joining date is after 2025, use the start of 2025 anyway for dummy data
            start_date = Config.START_DATE
            end_date = Config.END_DATE

        # Generate random date
        days_between = (end_date - start_date).days
        if days_between <= 0:
            return start_date.strftime("%Y-%m-%d")

        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime("%Y-%m-%d")

    def generate_random_attendance_status(self, date_str):
        """Generate random attendance status based on date (weekday/weekend)."""
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        if date_obj.weekday() >= 5:  # Weekend
            return random.choices(["Absent", "On Leave"], weights=[0.7, 0.3])[0]
        else:  # Weekday
            return random.choices(
                ["Present", "Half Day", "Work From Home", "Absent", "On Leave"],
                weights=[0.65, 0.15, 0.10, 0.05, 0.05]
            )[0]

    def cancel_all_submitted_attendance(self):
        """Cancel all submitted attendance records so they can be edited."""
        logger.info("Cancelling all submitted attendance records...")

        try:
            # Get all submitted attendance records (docstatus = 1)
            submitted_attendance = self.api.get_list("Attendance",
                                                     filters={
                                                         "docstatus": 1, "company": Config.COMPANY_NAME},
                                                     fields=[
                                                         "name", "employee", "attendance_date", "status"]
                                                     )

            if not submitted_attendance:
                logger.info("No submitted attendance records found to cancel.")
                return

            logger.info(
                f"Found {len(submitted_attendance)} submitted attendance records to cancel.")

            cancelled_count = 0
            failed_count = 0

            for attendance in submitted_attendance:
                try:
                    # Cancel the attendance record (set docstatus to 2)
                    self.api.update_doc(
                        "Attendance", attendance["name"], {"docstatus": 2})
                    cancelled_count += 1
                    logger.debug(f"Cancelled attendance: {attendance['name']}")

                    # Small delay to avoid overwhelming the server
                    time.sleep(0.1)

                except Exception as e:
                    failed_count += 1
                    logger.warning(
                        f"Failed to cancel attendance {attendance['name']}: {str(e)}")

            logger.info(
                f"Attendance cancellation completed: {cancelled_count} cancelled, {failed_count} failed.")

        except Exception as e:
            logger.error(
                f"Error while cancelling submitted attendance records: {str(e)}")

    def delete_all_cancelled_attendance(self):
        """Delete all cancelled attendance records to clean up before creating new ones."""
        logger.info("Deleting all cancelled attendance records...")

        try:
            # Get all cancelled attendance records (docstatus = 2)
            cancelled_attendance = self.api.get_list("Attendance",
                                                     filters={
                                                         "docstatus": 2, "company": Config.COMPANY_NAME},
                                                     fields=[
                                                         "name", "employee", "attendance_date", "status"]
                                                     )

            if not cancelled_attendance:
                logger.info("No cancelled attendance records found to delete.")
                return

            logger.info(
                f"Found {len(cancelled_attendance)} cancelled attendance records to delete.")

            deleted_count = 0
            failed_count = 0

            for attendance in cancelled_attendance:
                try:
                    # Delete the cancelled attendance record
                    self.api._make_request(
                        "DELETE", f"resource/Attendance/{attendance['name']}")
                    deleted_count += 1
                    logger.debug(
                        f"Deleted cancelled attendance: {attendance['name']}")

                    # Small delay to avoid overwhelming the server
                    time.sleep(0.1)

                except Exception as e:
                    failed_count += 1
                    logger.warning(
                        f"Failed to delete cancelled attendance {attendance['name']}: {str(e)}")

            logger.info(
                f"Attendance deletion completed: {deleted_count} deleted, {failed_count} failed.")

        except Exception as e:
            logger.error(
                f"Error while deleting cancelled attendance records: {str(e)}")

    def create_fresh_attendance_records(self, target_count=50):
        """Create fresh attendance records to reach target count, avoiding the cancel/amend complexity."""
        logger.info(
            f"Creating fresh attendance records to reach target of {target_count}...")

        try:
            # Get current attendance count (only non-cancelled records)
            current_attendance = self.api.get_list("Attendance",
                                                   filters=[
                                                       # Exclude cancelled records
                                                       ["docstatus", "!=", 2],
                                                       ["company", "=",
                                                           Config.COMPANY_NAME]
                                                   ],
                                                   fields=[
                                                       "name", "employee", "attendance_date", "status"]
                                                   )

            current_count = len(current_attendance)
            logger.info(
                f"Current non-cancelled attendance records: {current_count}")

            if current_count >= target_count:
                logger.info(
                    f"Already have {current_count} attendance records (>= target {target_count}). No additional records needed.")
                return

            # Calculate how many more we need
            additional_needed = target_count - current_count
            logger.info(
                f"Need to create {additional_needed} additional attendance records to reach target {target_count}.")

            # Get list of existing attendance to avoid duplicates
            existing_combinations = set()
            for att in current_attendance:
                existing_combinations.add(
                    (att["employee"], att["attendance_date"]))

            # Create additional random attendance records
            created_count = 0
            # Increase attempts to handle more duplicates
            max_attempts = additional_needed * 10
            attempts = 0

            while created_count < additional_needed and attempts < max_attempts:
                attempts += 1

                # Select random employee
                employee = random.choice(self.employees)

                # Get employee's joining date to ensure we don't create attendance before joining
                try:
                    employee_doc = self.api.get_doc(
                        "Employee", employee["name"])
                    joining_date_str = employee_doc.get("date_of_joining")

                    if not joining_date_str:
                        continue

                    joining_date = datetime.strptime(
                        joining_date_str, "%Y-%m-%d")
                    earliest_date = max(joining_date, Config.START_DATE)

                    # Generate random date within 2025 and after joining date
                    random_date = self.generate_random_date_in_2025(
                        earliest_date)

                    # Check if this combination already exists
                    if (employee["name"], random_date) in existing_combinations:
                        continue

                    # Create attendance record
                    status = self.generate_random_attendance_status(
                        random_date)

                    attendance_data = {
                        "employee": employee["name"],
                        "attendance_date": random_date,
                        "status": status,
                        "company": Config.COMPANY_NAME,
                    }

                    # Add time details for working statuses
                    if status in ["Present", "Half Day", "Work From Home"]:
                        date_obj = datetime.strptime(random_date, "%Y-%m-%d")
                        check_in_time = f"{random_date} 08:{random.randint(0, 30):02d}:00"
                        check_out_time = f"{random_date} 17:{random.randint(0, 45):02d}:00"
                        working_hours = random.uniform(
                            7.0, 9.0) if status == "Present" else random.uniform(3.0, 4.5)

                        attendance_data.update({
                            "check_in_time": check_in_time,
                            "check_out_time": check_out_time,
                            "working_hours": working_hours
                        })

                    try:
                        self.api.create_doc("Attendance", attendance_data)
                        existing_combinations.add(
                            (employee["name"], random_date))
                        created_count += 1
                        logger.debug(
                            f"Created fresh attendance #{created_count} for '{employee['employee_name']}' on {random_date} with status: {status}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to create fresh attendance for '{employee['employee_name']}' on {random_date}: {str(e)}")

                except Exception as e:
                    logger.warning(
                        f"Error processing fresh attendance for employee '{employee.get('employee_name')}': {str(e)}")
                    continue

            logger.info(
                f"Created {created_count} fresh attendance records. Total should now be around {current_count + created_count}.")

        except Exception as e:
            logger.error(f"Error in create_fresh_attendance_records: {str(e)}")

    def resubmit_all_attendance(self):
        """Submit all draft attendance records (simpler approach without amend complexity)."""
        logger.info("Submitting all draft attendance records...")

        try:
            # Get all draft attendance records (docstatus = 0)
            draft_attendance = self.api.get_list("Attendance",
                                                 filters={
                                                     "docstatus": 0, "company": Config.COMPANY_NAME},
                                                 fields=[
                                                     "name", "employee", "attendance_date", "status"]
                                                 )

            if not draft_attendance:
                logger.info("No draft attendance records found to submit.")
                return

            logger.info(
                f"Found {len(draft_attendance)} draft attendance records to submit.")

            submitted_count = 0
            failed_count = 0

            for attendance in draft_attendance:
                try:
                    # Submit the attendance record
                    self.api.submit_doc("Attendance", attendance["name"])
                    submitted_count += 1
                    logger.debug(f"Submitted attendance: {attendance['name']}")

                    # Small delay to avoid overwhelming the server
                    time.sleep(0.1)

                except Exception as e:
                    failed_count += 1
                    logger.warning(
                        f"Failed to submit attendance {attendance['name']}: {str(e)}")

            logger.info(
                f"Attendance submission completed: {submitted_count} submitted, {failed_count} failed.")

        except Exception as e:
            logger.error(
                f"Error while submitting draft attendance records: {str(e)}")

    # --- Placeholder functions for non-HR modules ---
    def create_customers(self):
        logger.info("Skipping customer creation.")

    def create_suppliers(self):
        logger.info("Skipping supplier creation.")

    def create_items(self):
        logger.info("Skipping item creation.")

    def create_warehouses(self):
        logger.info("Skipping warehouse creation.")

    def create_projects(self):
        logger.info("Skipping project creation.")

    def create_sales_invoices(self):
        logger.info("Skipping sales invoice creation.")

    def create_purchase_invoices(self):
        logger.info("Skipping purchase invoice creation.")

    def create_stock_entries(self):
        logger.info("Skipping stock entry creation.")

    def create_salary_slips(self):
        logger.info("Skipping salary slip creation.")

    def create_salary_structure(self):
        logger.info("Skipping salary structure creation.")

    def _ensure_salary_components_exist(self):
        logger.info(
            "Skipping salary components ensure (not directly needed for Emp/Attendance).")

    def _ensure_default_accounts_exist(self):
        logger.info(
            "Skipping default accounts ensure (not directly needed for Emp/Attendance).")

    def fetch_accounts(self):
        logger.info(
            "Skipping account fetch (not directly needed for Emp/Attendance).")

    def create_assets(self):
        logger.info("Skipping asset creation.")

    def generate_all_data(self):
        """Main method to generate all data (focused on HR module)."""
        logger.info(
            "Starting ERPNext dummy data generation (Focus: Employees & Attendance)...")
        logger.info(
            f"Target: {Config.EMPLOYEE_COUNT} employees and their attendance records for {Config.START_DATE.year}.")

        try:
            logger.info("=== Performing Prerequisite Setup (HR Focused) ===")
            # These methods are now only called ONCE in DataGenerator.__init__
            # self._ensure_essential_roles_exist()
            # self._ensure_holiday_list_exists()
            # self._fetch_master_data_options()

            logger.info("=== Creating Master Data (HR Focused) ===")
            self.create_users()
            self.create_employees()

            # These are placeholder calls; they will simply log that they are skipping.
            self.create_customers()
            self.create_suppliers()
            self.create_warehouses()
            self.create_items()
            self.create_assets()

            logger.info("=== Creating Transactions (HR Focused) ===")
            self.create_attendance_records()

            # Add more random attendance records to reach target count
            logger.info(
                "=== Creating Additional Random Attendance Records ===")
            self.create_additional_random_attendance(target_count=200)

            # Submit all draft attendance records
            logger.info("=== Submitting All Draft Attendance Records ===")
            self.submit_all_draft_attendance()

            # These are placeholder calls; they will simply log that they are skipping.
            self.create_projects()
            self.create_sales_invoices()
            self.create_purchase_invoices()
            self.create_stock_entries()
            self.create_salary_slips()
            self.create_salary_structure()

            logger.info("=== Data Generation Complete (HR Focus) ===")
            logger.info("Summary (HR Focused):")
            logger.info(f"- Users (created/found): {len(self.users)}")
            logger.info(f"- Employees (created/found): {len(self.employees)}")

            # Add final attendance count to summary
            final_attendance_count = len(self.api.get_list("Attendance", filters={
                "company": Config.COMPANY_NAME}, fields=["name"]))
            logger.info(
                f"- Attendance records (final): {final_attendance_count}")

        except Exception as e:
            logger.error(f"Fatal error during data generation: {str(e)}")
            raise


def main():
    """Main entry point."""
    print("=" * 80)
    print("ERPNext Dummy Data Generator (Focused: Employees & Attendance)")
    print("=" * 80)
    print(f"API Endpoint: {Config.BASE_URL}")
    print(f"Company: {Config.COMPANY_NAME}")
    print(f"Target Employees: {Config.EMPLOYEE_COUNT}")
    print(f"Attendance Date Range: Full year 2025")
    print("=" * 80)

    response = input(
        "\nThis script will create/update employees and their attendance records. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = DataGenerator()
        generator.generate_all_data()

        print("\n" + "=" * 80)
        print("Data generation completed successfully!")
        print("Check the log file for detailed information: erpnext_hr_data_generation.log")
        print("=" * 80)
    except Exception as e:
        print(f"\n" + "=" * 80)
        print(f"Data generation failed due to a critical error: {e}")
        print(
            "Please check the log file 'erpnext_hr_data_generation.log' for more details.")
        print("=" * 80)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Dummy Data Generator (Revised with Automated Prerequisite Setup)
Generates realistic dummy data for existing ERPNext v16 setup,
including automatic creation of essential Role Profiles and Accounts if missing.
Author: ERPNext Data Generator
Version: 1.2.0
"""

import requests
import json
import random
import logging
from datetime import datetime, timedelta
from faker import Faker
import time
from typing import Dict, List, Any, Optional
import sys

# Initialize Faker
fake = Faker()

# Configuration
class Config:
    # API Configuration
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"  # Adjust based on your Docker setup
    
    # Data Volumes
    USER_COUNT = 50
    EMPLOYEE_COUNT = 100
    CUSTOMER_COUNT = 80
    SUPPLIER_COUNT = 40
    ITEM_COUNT = 200
    TRANSACTION_COUNT = 500 # This is a placeholder, actual transactions will vary

    # Date Range for 2025 (These values will still be used for generating transaction dates)
    START_DATE = datetime(2025, 1, 1)
    END_DATE = datetime(2025, 12, 31)
    
    # Company Details (MUST MATCH EXISTING SETUP OR BE CREATED MANUALLY BEFORE RUNNING)
    COMPANY_NAME = "PT Fiyansa Mulya" # Ensure this exactly matches your ERPNext company name
    COMPANY_ABBR = "PFM"
    DEFAULT_CURRENCY = "IDR" # Ensure this matches your company's default currency
    
    # Batch Processing
    BATCH_SIZE = 50
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # seconds

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('erpnext_data_generation.log'),
        logging.StreamHandler(sys.stdout)
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
            if method == "GET":
                response = self.session.get(url, params=data)
            elif method == "POST":
                response = self.session.post(url, json=data)
            elif method == "PUT":
                response = self.session.put(url, json=data)
            elif method == "DELETE":
                response = self.session.delete(url)
            
            response.raise_for_status() # This will raise an HTTPError for 4xx/5xx responses
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if retry_count < Config.RETRY_ATTEMPTS:
                logger.warning(f"Request failed to {url}, retrying... ({retry_count + 1}/{Config.RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(f"Request failed after {Config.RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
                raise # Re-raise the exception after retries are exhausted
    
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
        """Check if document exists by fetching it. Returns True if exists, False otherwise."""
        try:
            # For "User" doctype, checking by email is more robust than by name field which is usually "User Name"
            if doctype == "User":
                result = self.get_list(doctype, filters={"email": name}, fields=["name"])
                return len(result) > 0
            else:
                self.get_doc(doctype, name)
                return True
        except requests.exceptions.HTTPError as e:
            # Check specifically for 404 Not Found, otherwise re-raise or log as an error
            if e.response.status_code == 404:
                return False
            else:
                logger.error(f"Error checking existence of {doctype} {name} (HTTP {e.response.status_code}): {e.response.text}")
                # If it's a 500, re-raise as it's a server issue
                if e.response.status_code == 500:
                    raise
                return False # For other client errors, treat as not existing for generation purposes
        except Exception as e:
            logger.error(f"Unexpected error checking existence of {doctype} {name}: {e}")
            raise # Re-raise other unexpected errors


class DataGenerator:
    """Generates realistic dummy data using Faker"""
    
    def __init__(self):
        self.fake = Faker()
        self.api = ERPNextAPI()
        
        # Cache for generated data
        self.users = []
        self.employees = []
        self.customers = []
        self.suppliers = []
        self.items = []
        self.warehouses = []
        
        # New: `accounts_cache` stores the full name of accounts
        self.accounts_cache: Dict[str, str] = {} 

        # --- Prerequisite Setup Order ---
        # 1. Ensure Role Profiles (needed for Users)
        self._ensure_role_profiles_exist()
        # 2. Ensure basic Salary Components (needed for Salary Structures)
        self._ensure_salary_components_exist()
        # 3. Ensure Holiday List (needed for Employees)
        self._ensure_holiday_list_exists()
        # 4. Fetch/ensure core Accounts (needed for many transactions)
        # This will be called after basic structure is ready.
        self.fetch_accounts() 

    def _ensure_role_profiles_exist(self):
        """Ensures essential Role Profiles exist, creating them if necessary."""
        logger.info("Ensuring essential Role Profiles exist...")
        
        # Define role profiles and the roles they should contain
        role_profiles_to_ensure = {
            "Standard Employee": ["Employee", "Desk User"],
            "Accounts Manager": ["Accounts Manager", "Accounts User", "Employee", "Sales User", "Purchase User"],
            "Sales Manager": ["Sales Manager", "Sales User", "Employee", "Accounts User"],
            "Stock User": ["Stock User", "Employee"],
            "HR User": ["HR User", "Employee"]
        }

        for profile_name, roles_list in role_profiles_to_ensure.items():
            if not self.api.check_exists("Role Profile", profile_name):
                logger.info(f"Role Profile '{profile_name}' not found. Attempting to create it.")
                
                # Construct roles child table data
                roles_data = [{"role": role_name} for role_name in roles_list]
                
                profile_data = {
                    "role_profile_name": profile_name,
                    "roles": roles_data
                }
                try:
                    self.api.create_doc("Role Profile", profile_data)
                    logger.info(f"Created Role Profile: '{profile_name}' with roles: {', '.join(roles_list)}")
                except Exception as e:
                    logger.warning(f"Failed to create Role Profile '{profile_name}': {str(e)}")
            else:
                logger.debug(f"Role Profile '{profile_name}' already exists.")

    def _ensure_default_accounts_exist(self):
        """
        Ensures a basic set of standard accounts exists for the company.
        This is a best-effort attempt and assumes a standard CoA structure.
        """
        logger.info(f"Ensuring default accounts for company '{Config.COMPANY_NAME}' exist...")

        # Define critical accounts and their expected types/parents.
        # ERPNext usually creates these with " - COMPANY_ABBR" suffix.
        # This function won't create the entire CoA, just the leaf nodes needed for transactions.
        # Root types are crucial for creating new accounts.
        # For simplicity, we'll try to find parent groups or create them if truly missing basic roots.
        
        accounts_to_check = [
            {"name_part": "Sales", "type": "Income", "parent_hint": "Direct Incomes"},
            {"name_part": "Cost of Goods Sold", "type": "Expense", "parent_hint": "Cost of Revenues"},
            {"name_part": "Debtors", "type": "Asset", "parent_hint": "Current Assets"}, # Renamed from Receivable to Asset
            {"name_part": "Creditors", "type": "Liability", "parent_hint": "Current Liabilities"}, # Renamed from Payable to Liability
            {"name_part": "Cash", "type": "Asset", "parent_hint": "Bank Accounts"},
            {"name_part": "Basic Salary", "type": "Expense", "parent_hint": "Salaries"}, # Assuming a "Salaries" group under Expenses
            {"name_part": "House Rent Allowance", "type": "Expense", "parent_hint": "Salaries"},
            {"name_part": "Special Allowance", "type": "Expense", "parent_hint": "Salaries"},
            {"name_part": "Professional Tax Payable", "type": "Liability", "parent_hint": "Duties and Taxes"}, # Or Current Liabilities
            {"name_part": "Provident Fund Payable", "type": "Liability", "parent_hint": "Current Liabilities"} # Or Duties and Taxes
        ]

        # First, try to fetch/cache existing root accounts
        root_accounts_map = {}
        try:
            # Fetch all accounts to build a comprehensive map for parent lookups
            all_company_accounts = self.api.get_list("Account", filters={"company": Config.COMPANY_NAME}, fields=["name", "is_group", "account_type", "parent_account", "root_type"])
            for acc in all_company_accounts:
                root_accounts_map[acc["name"]] = acc # Store full account object
                # Add common name hints for parent lookups
                if acc["is_group"]:
                    if "Income" in acc["name"] and acc["root_type"] == "Income":
                        root_accounts_map["Direct Incomes"] = acc["name"]
                    if "Cost of Revenues" in acc["name"] and acc["root_type"] == "Expense":
                        root_accounts_map["Cost of Revenues"] = acc["name"]
                    if "Current Assets" in acc["name"] and acc["root_type"] == "Asset":
                        root_accounts_map["Current Assets"] = acc["name"]
                    if "Bank Accounts" in acc["name"] and acc["root_type"] == "Asset":
                        root_accounts_map["Bank Accounts"] = acc["name"]
                    if "Current Liabilities" in acc["name"] and acc["root_type"] == "Liability":
                        root_accounts_map["Current Liabilities"] = acc["name"]
                    if "Duties and Taxes" in acc["name"] and acc["root_type"] == "Liability":
                        root_accounts_map["Duties and Taxes"] = acc["name"]
                    if "Salaries" in acc["name"] and acc["root_type"] == "Expense": # Assuming "Salaries" group
                        root_accounts_map["Salaries"] = acc["name"]

        except Exception as e:
            logger.warning(f"Could not fetch all existing accounts for parent mapping: {e}. Account creation might be limited.")


        for acc_info in accounts_to_check:
            expected_full_name = f"{acc_info['name_part']} - {Config.COMPANY_ABBR}"
            
            # Check if account already exists using the name
            if self.api.check_exists("Account", expected_full_name):
                logger.debug(f"Account '{expected_full_name}' already exists.")
                continue

            logger.info(f"Account '{expected_full_name}' not found. Attempting to create it.")
            
            parent_account_name = None
            # Try to find the parent account based on hints
            if acc_info["parent_hint"] in root_accounts_map:
                parent_account_name = root_accounts_map[acc_info["parent_hint"]]
            elif acc_info["parent_hint"] + f" - {Config.COMPANY_ABBR}" in root_accounts_map:
                parent_account_name = root_accounts_map[acc_info["parent_hint"] + f" - {Config.COMPANY_ABBR}"]
            else:
                logger.warning(f"Could not find a suitable parent account for '{expected_full_name}' with hint '{acc_info['parent_hint']}'. Skipping creation of this account. Please ensure default CoA is properly setup.")
                continue # Skip if parent cannot be reliably determined

            account_data = {
                "account_name": acc_info["name_part"], # The short name used in ERPNext's tree view
                "parent_account": parent_account_name,
                "account_type": acc_info["type"],
                "company": Config.COMPANY_NAME,
                "is_group": 0, # These are ledger accounts, not groups
                "root_type": acc_info["type"] # Should match parent's root_type or be set explicitly
            }

            try:
                # Need to use the specific API endpoint for creating Accounts correctly with full name
                # Frappe's new_doc or save API often handles the full_name generation.
                # However, for direct API, sometimes we just pass account_name and parent.
                # The 'name' field is derived. We set account_name and let ERPNext form the full name.
                new_acc = self.api.create_doc("Account", account_data)
                logger.info(f"Created Account: '{new_acc['name']}' (ID: {new_acc['name']})")
                self.accounts_cache[acc_info['name_part']] = new_acc['name'] # Cache the full name
                self.accounts_cache[acc_info['type']] = new_acc['name'] # Cache by type as fallback
            except Exception as e:
                logger.warning(f"Failed to create Account '{expected_full_name}': {str(e)}")
        
    def fetch_accounts(self):
        """
        Fetches existing accounts from the system and populates the cache.
        This is crucial to get the exact account names including company abbreviation.
        This method is called AFTER Role Profiles and essential accounts are ensured.
        """
        logger.info("Fetching existing accounts for company: %s...", Config.COMPANY_NAME)
        try:
            # Fetch all accounts for the company
            accounts = self.api.get_list("Account", filters={"company": Config.COMPANY_NAME}, fields=["name", "account_name", "account_type", "is_group"])
            
            if not accounts:
                logger.error(f"No accounts found for company '{Config.COMPANY_NAME}' even after attempting creation. Please ensure your Chart of Accounts is set up in ERPNext for this company. Data generation may fail.")
                return

            self.accounts_cache.clear() # Clear cache before re-populating

            for account in accounts:
                full_account_name = account["name"] # The actual ID/name of the account
                short_account_name = account.get("account_name")
                account_type = account.get("account_type")

                if short_account_name:
                    self.accounts_cache[short_account_name] = full_account_name
                if account_type:
                    self.accounts_cache[account_type] = full_account_name # Store last one by type as fallback

                # Also map common names (without company abbr) to their full ERPNext equivalents
                # This makes `get_account` more intuitive.
                if "Sales" in full_account_name:
                    self.accounts_cache["Sales"] = full_account_name
                if "Cost of Goods Sold" in full_account_name:
                    self.accounts_cache["Cost of Goods Sold"] = full_account_name
                if "Debtors" in full_account_name:
                    self.accounts_cache["Debtors"] = full_account_name
                if "Creditors" in full_account_name:
                    self.accounts_cache["Creditors"] = full_account_name
                if "Cash" in full_account_name:
                    self.accounts_cache["Cash"] = full_account_name
                if "Salary" in full_account_name: # For generic salary expense
                    self.accounts_cache["Salary"] = full_account_name
                if "Basic Salary" in full_account_name:
                    self.accounts_cache["Basic Salary"] = full_account_name
                if "House Rent Allowance" in full_account_name:
                    self.accounts_cache["House Rent Allowance"] = full_account_name
                if "Special Allowance" in full_account_name:
                    self.accounts_cache["Special Allowance"] = full_account_name
                if "Professional Tax Payable" in full_account_name:
                    self.accounts_cache["Professional Tax Payable"] = full_account_name
                if "Provident Fund Payable" in full_account_name:
                    self.accounts_cache["Provident Fund Payable"] = full_account_name

            logger.info(f"Cached {len(self.accounts_cache)} relevant accounts for '{Config.COMPANY_NAME}'.")
        except Exception as e:
            logger.error(f"CRITICAL ERROR: Failed to fetch accounts from ERPNext. This means your Chart of Accounts for '{Config.COMPANY_NAME}' is likely not properly set up or accessible. Data generation will likely fail. Error: {e}")
            raise # Re-raise as this is a critical dependency
    
    def get_account(self, common_name: str, account_type_hint: str) -> Optional[str]:
        """
        Retrieves the full ERPNext account name from the cache.
        Uses common_name first, then falls back to account_type_hint if available.
        Returns None if not found, logging a warning.
        """
        # Prioritize exact common name match (e.g., "Sales", "Cash")
        if common_name in self.accounts_cache:
            return self.accounts_cache[common_name]
        
        # Fallback to account name with company abbr (e.g., "Sales - PFM")
        full_name_with_abbr = f"{common_name} - {Config.COMPANY_ABBR}"
        if full_name_with_abbr in self.accounts_cache:
            return self.accounts_cache[full_name_with_abbr]

        # Fallback to account type hint if a more specific name isn't found
        # This is very generic and might pick any account of that type
        if account_type_hint in self.accounts_cache:
            logger.warning(f"Specific account '{common_name}' not found. Using a generic account of type '{account_type_hint}': {self.accounts_cache[account_type_hint]}")
            return self.accounts_cache[account_type_hint]

        logger.error(f"CRITICAL: Required account for '{common_name}' (Type: '{account_type_hint}') not found in ERPNext cache for company '{Config.COMPANY_NAME}'. Data generation for related doctypes may fail. Please ensure this account exists and matches your company's chart of accounts.")
        return None
    
    def generate_phone(self) -> str:
        """Generate valid Indonesian phone number."""
        # Generates a number like +6281234567890
        return f"+628{random.randint(100000000, 9999999999):010d}"
    
    def generate_date_in_range(self, start_date: datetime, end_date: datetime, exclude_weekends: bool = True) -> str:
        """Generate random date within range."""
        if start_date > end_date:
            logger.warning(f"Invalid date range: start_date ({start_date}) is after end_date ({end_date}). Swapping dates.")
            start_date, end_date = end_date, start_date # Swap them to prevent error

        days_between = (end_date - start_date).days
        if days_between < 0: # This should not happen after swap, but as a safeguard
            return start_date.strftime("%Y-%m-%d")

        while True:
            random_days = random.randint(0, days_between)
            date = start_date + timedelta(days=random_days)
            
            if exclude_weekends and date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                continue
            return date.strftime("%Y-%m-%d")
    
    def generate_time(self) -> str:
        """Generate random time."""
        hour = random.randint(8, 18)
        minute = random.choice([0, 15, 30, 45])
        return f"{hour:02d}:{minute:02d}:00"
    
    def _ensure_holiday_list_exists(self):
        """Ensures the default holiday list for the current year exists."""
        holiday_list_name = f"Holiday List {datetime.now().year}"
        logger.info(f"Ensuring Holiday List: '{holiday_list_name}' exists...")
        
        if not self.api.check_exists("Holiday List", holiday_list_name):
            logger.info(f"Holiday List '{holiday_list_name}' not found. Attempting to create it.")
            # Example holidays for Indonesia, adjust as needed
            # You might want to fetch actual Indonesian holidays for the year
            holidays = [
                {"holiday_date": f"{datetime.now().year}-01-01", "description": "New Year's Day"},
                {"holiday_date": f"{datetime.now().year}-02-10", "description": "Isra Miraj"},
                {"holiday_date": f"{datetime.now().year}-03-11", "description": "Nyepi Day"},
                {"holiday_date": f"{datetime.now().year}-03-29", "description": "Good Friday"},
                {"holiday_date": f"{datetime.now().year}-04-10", "description": "Eid al-Fitr (estimation)"},
                {"holiday_date": f"{datetime.now().year}-04-11", "description": "Eid al-Fitr (estimation)"},
                {"holiday_date": f"{datetime.now().year}-05-01", "description": "Labor Day"},
                {"holiday_date": f"{datetime.now().year}-05-09", "description": "Ascension Day of Jesus Christ"},
                {"holiday_date": f"{datetime.now().year}-05-23", "description": "Vesak Day"},
                {"holiday_date": f"{datetime.now().year}-06-01", "description": "Pancasila Day"},
                {"holiday_date": f"{datetime.now().year}-06-17", "description": "Eid al-Adha (estimation)"},
                {"holiday_date": f"{datetime.now().year}-07-07", "description": "Islamic New Year"},
                {"holiday_date": f"{datetime.now().year}-08-17", "description": "Independence Day"},
                {"holiday_date": f"{datetime.now().year}-09-16", "description": "Prophet Muhammad's Birthday"},
                {"holiday_date": f"{datetime.now().year}-12-25", "description": "Christmas Day"}
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
                logger.warning(f"Failed to create Holiday List '{holiday_list_name}': {str(e)}")
        else:
            logger.debug(f"Holiday List '{holiday_list_name}' already exists.")

    def create_users(self):
        """Create user accounts."""
        logger.info(f"Creating {Config.USER_COUNT} users...")
        
        # Fetch existing users
        existing_users = self.api.get_list("User", fields=["name", "email"])
        self.users = [{"name": user["name"], "email": user["email"]} for user in existing_users]
        logger.info(f"Found {len(self.users)} existing users.")

        users_created_count = 0
        
        # Role Profiles are ensured by _ensure_role_profiles_exist()
        role_profile_options = ["Standard Employee", "Accounts Manager", "Sales Manager", "Stock User", "HR User"]
        
        for i in range(Config.USER_COUNT):
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            email = f"{first_name.lower()}.{last_name.lower()}_{len(self.users) + i}@{Config.COMPANY_ABBR.lower()}.com"
            
            # Check if user already exists based on email (using refined check_exists)
            if self.api.check_exists("User", email):
                logger.debug(f"User with email {email} already exists, skipping.")
                continue

            user_data = {
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "enabled": 1,
                "send_welcome_email": 0, # Important for dummy data
                "language": "en",
                "time_zone": "Asia/Jakarta", # Updated for Indonesia
                "role_profile_name": random.choice(role_profile_options)
            }
            
            try:
                user = self.api.create_doc("User", user_data)
                self.users.append(user)
                users_created_count += 1
                logger.debug(f"Created user: {email}")
            except Exception as e:
                logger.warning(f"Failed to create user {email}: {str(e)}")
            
        logger.info(f"Created {users_created_count} new users. Total users (existing + new): {len(self.users)}")
    
    def create_employees(self):
        """Create employee records."""
        logger.info(f"Creating {Config.EMPLOYEE_COUNT} employees...")
        
        # Fetch existing employees
        existing_employees = self.api.get_list("Employee", fields=["name", "employee_name", "designation", "user_id"])
        self.employees = existing_employees
        logger.info(f"Found {len(self.employees)} existing employees.")

        # Ensure we have enough users
        # Filter out users already linked to employees
        linked_user_ids = {emp.get("user_id") for emp in self.employees if emp.get("user_id")}
        available_users = [u for u in self.users if u.get("name") not in linked_user_ids and u.get("name") not in ["Administrator", "Guest"]]
        random.shuffle(available_users) # Shuffle to get a good mix for new employees
        user_idx = 0 # To assign users sequentially to new employees

        employees_created_count = 0
        departments = ["Sales", "Marketing", "Operations", "Finance", "HR", "IT", "Production", "Quality", "R&D", "Logistics"]
        designations = ["Manager", "Senior Executive", "Executive", "Assistant", "Supervisor", "Team Lead", "Director", "VP", "Analyst", "Engineer"]
        employment_types = ["Full-time", "Part-time", "Contract", "Intern"]
        
        for i in range(Config.EMPLOYEE_COUNT):
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            employee_name = f"{first_name} {last_name}"
            
            # Check if employee already exists by name
            if any(e["employee_name"] == employee_name for e in self.employees):
                logger.debug(f"Employee {employee_name} already exists, skipping.")
                continue

            # Calculate dates
            date_of_joining = self.generate_date_in_range(
                datetime(2020, 1, 1),
                Config.START_DATE # Ensure joining date is before 2025 for existing employees
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
                "gender": random.choice(["Male", "Female", "Other"]),
                "date_of_birth": date_of_birth,
                "date_of_joining": date_of_joining,
                "department": random.choice(departments),
                "designation": random.choice(designations),
                "employment_type": random.choice(employment_types),
                "holiday_list": f"Holiday List {datetime.now().year}", # This Holiday List is ensured to exist
                "prefered_contact_email": f"{first_name.lower()}.{last_name.lower()}@{Config.COMPANY_ABBR.lower()}.com",
                "cell_number": self.generate_phone(),
                "personal_email": self.fake.email(),
                "permanent_address": self.fake.address().replace('\n', ', '),
                "current_address": self.fake.address().replace('\n', ', '),
                "emergency_contact_name": self.fake.name(),
                "emergency_phone_number": self.generate_phone(),
                "bank_name": random.choice(["Bank Mandiri", "BCA", "BRI", "BNI"]), # Updated for Indonesia
                "bank_ac_no": str(random.randint(1000000000, 9999999999)),
                "salary_mode": "Bank"
            }
            
            # Link to a unique user if available
            if user_idx < len(available_users):
                employee_data["user_id"] = available_users[user_idx]["name"]
                user_idx += 1
            
            # Add employee hierarchy (20% are managers)
            if employees_created_count > 10 and random.random() < 0.8: # Only assign reports_to after some employees exist
                managers = [e for e in self.employees if "Manager" in e.get("designation", "")]
                if managers:
                    employee_data["reports_to"] = random.choice(managers)["name"]
            
            try:
                employee = self.api.create_doc("Employee", employee_data)
                self.employees.append(employee)
                employees_created_count += 1
                logger.debug(f"Created employee: {employee_name}")
            except Exception as e:
                logger.warning(f"Failed to create employee {employee_name}: {str(e)}")
            
        logger.info(f"Created {employees_created_count} new employees. Total employees (existing + new): {len(self.employees)}")
    
    def create_customers(self):
        """Create customer records."""
        logger.info(f"Creating {Config.CUSTOMER_COUNT} customers...")
        
        # Fetch existing customers
        existing_customers = self.api.get_list("Customer", filters={"company": Config.COMPANY_NAME}, fields=["name", "customer_name", "customer_group", "territory"])
        self.customers = existing_customers
        logger.info(f"Found {len(self.customers)} existing customers.")

        customers_created_count = 0
        customer_types = ["Company", "Individual"]
        customer_groups = ["Commercial", "Retail", "Government", "Non-Profit", "SMB"] # Added SMB
        territories = ["Jakarta", "Surabaya", "Bandung", "Medan", "Denpasar", "Makassar", "Palembang"] # Updated for Indonesia
        
        for i in range(Config.CUSTOMER_COUNT):
            customer_type = random.choice(customer_types)
            
            if customer_type == "Company":
                customer_name = self.fake.company() + f" {random.randint(1,99):02d}" # Add number to increase uniqueness
            else:
                customer_name = self.fake.name() + f" {random.randint(1,99):02d}" # Add number to increase uniqueness
            
            # Check if customer already exists
            if any(c["customer_name"] == customer_name for c in self.customers):
                logger.debug(f"Customer {customer_name} already exists, skipping.")
                continue

            customer_data = {
                "customer_name": customer_name,
                "customer_type": customer_type,
                "customer_group": random.choice(customer_groups),
                "territory": random.choice(territories),
                "default_currency": Config.DEFAULT_CURRENCY,
                "credit_limit": random.randint(10_000_000, 100_000_000), # Adjusted for IDR
                "payment_terms": random.choice(["Net 30", "Net 15", "Due on Receipt"]),
                "customer_primary_contact": self.fake.name(),
                "customer_primary_address": self.fake.address().replace('\n', ', '),
                "mobile_no": self.generate_phone(),
                "email_id": self.fake.company_email() if customer_type == "Company" else self.fake.email(),
                "website": self.fake.url() if customer_type == "Company" else None,
                "tax_id": f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}" # Example Tax ID
            }
            
            try:
                customer = self.api.create_doc("Customer", customer_data)
                self.customers.append(customer)
                customers_created_count += 1
                logger.debug(f"Created customer: {customer_name}")
            except Exception as e:
                logger.warning(f"Failed to create customer {customer_name}: {str(e)}")
            
        logger.info(f"Created {customers_created_count} new customers. Total customers (existing + new): {len(self.customers)}")
    
    def create_suppliers(self):
        """Create supplier records."""
        logger.info(f"Creating {Config.SUPPLIER_COUNT} suppliers...")
        
        # Fetch existing suppliers
        existing_suppliers = self.api.get_list("Supplier", filters={"company": Config.COMPANY_NAME}, fields=["name", "supplier_name", "default_currency"])
        self.suppliers = existing_suppliers
        logger.info(f"Found {len(self.suppliers)} existing suppliers.")

        suppliers_created_count = 0
        supplier_groups = ["Raw Material", "Services", "Consumables", "Sub Assemblies", "Components"] # Added Components
        supplier_types = ["Company", "Individual"]
        
        for i in range(Config.SUPPLIER_COUNT):
            supplier_type = random.choice(supplier_types)
            
            if supplier_type == "Company":
                supplier_name = f"{self.fake.company()} Supplies {random.randint(1,99):02d}"
            else:
                supplier_name = f"{self.fake.name()} Trading {random.randint(1,99):02d}"
            
            # Check if supplier already exists
            if any(s["supplier_name"] == supplier_name for s in self.suppliers):
                logger.debug(f"Supplier {supplier_name} already exists, skipping.")
                continue

            supplier_data = {
                "supplier_name": supplier_name,
                "supplier_group": random.choice(supplier_groups), # Ensure these Supplier Groups exist
                "supplier_type": supplier_type,
                "country": self.fake.country(), # Random country
                "tax_id": f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}",
                "default_currency": random.choice([Config.DEFAULT_CURRENCY, "SGD", "USD", "JPY", "EUR"]), # More relevant currencies for IDR company
                "payment_terms": random.choice(["Net 30", "Net 45", "Net 60"]),
                "supplier_primary_contact": self.fake.name(),
                "supplier_primary_address": self.fake.address().replace('\n', ', '),
                "mobile_no": self.generate_phone(),
                "email_id": self.fake.company_email(),
                "website": self.fake.url() if supplier_type == "Company" else None,
                "is_internal_supplier": 0,
                "allow_purchase_invoice_creation_without_purchase_order": 1,
                "allow_purchase_invoice_creation_without_purchase_receipt": 1
            }
            
            try:
                supplier = self.api.create_doc("Supplier", supplier_data)
                self.suppliers.append(supplier)
                suppliers_created_count += 1
                logger.debug(f"Created supplier: {supplier_name}")
            except Exception as e:
                logger.warning(f"Failed to create supplier {supplier_name}: {str(e)}")
            
        logger.info(f"Created {suppliers_created_count} new suppliers. Total suppliers (existing + new): {len(self.suppliers)}")
    
    def create_items(self):
        """Create item records."""
        logger.info(f"Creating {Config.ITEM_COUNT} items...")
        
        # Fetch existing items
        existing_items = self.api.get_list("Item", filters={"company": Config.COMPANY_NAME}, fields=["item_code", "item_name", "item_group", "stock_uom", "is_sales_item", "is_purchase_item", "is_stock_item", "valuation_rate", "standard_rate", "description"])
        self.items = existing_items
        logger.info(f"Found {len(self.items)} existing items.")

        items_created_count = 0
        # Item categories for manufacturing
        item_groups = {
            "Raw Materials": ["Steel", "Aluminum", "Plastic", "Rubber", "Wood", "Fabric", "Chemical"],
            "Components": ["Electronic Chip", "Sensor", "Connector", "Gear", "Valve", "Pump"],
            "Sub Assemblies": ["Control Unit", "Hydraulic Module", "Power Supply", "Display Assembly"],
            "Finished Goods": ["Industrial Robot", "Automated Drone", "Smart Sensor Device", "IoT Gateway", "Precision Tool"],
            "Consumables": ["Lubricant", "Cleaning Agent", "Gloves", "Cutting Fluid", "Welding Rods"]
        }
        
        uoms = ["Pcs", "Kg", "Meter", "Liter", "Box", "Pack", "Set", "Unit", "Roll", "Sheet"] # Adjusted for Indonesia
        
        for group, base_names in item_groups.items():
            # Ensure Item Group exists
            if not self.api.check_exists("Item Group", group):
                try:
                    self.api.create_doc("Item Group", {"item_group_name": group, "is_group": 0, "parent_item_group": "All Item Groups"})
                    logger.info(f"Created Item Group: {group}")
                except Exception as e:
                    logger.warning(f"Failed to create Item Group {group}: {str(e)}")
                    continue # Skip creating items for this group if group creation fails

            items_per_group = Config.ITEM_COUNT // len(item_groups)
            
            for i in range(items_per_group):
                base_name = random.choice(base_names)
                item_code = f"{group[:3].upper()}-{base_name[:3].upper()}-{random.randint(1000, 9999)}"
                
                # Check if item already exists
                if any(item["item_code"] == item_code for item in self.items):
                    logger.debug(f"Item {item_code} already exists, skipping.")
                    continue
                    
                item_name = f"{base_name} Model {chr(65 + (i % 26))}"
                
                # Determine item properties based on group
                is_stock_item = 1
                is_purchase_item = group not in ["Finished Goods", "Sub Assemblies"]
                is_sales_item = group in ["Finished Goods", "Sub Assemblies"]
                has_serial_no = group in ["Finished Goods", "Sub Assemblies"] and random.random() < 0.3
                has_batch_no = group in ["Raw Materials", "Consumables"] and random.random() < 0.5
                
                item_data = {
                    "item_code": item_code,
                    "item_name": item_name,
                    "item_group": group, 
                    "stock_uom": random.choice(uoms), # Ensure these UOMs exist (usually default in ERPNext)
                    "is_stock_item": is_stock_item,
                    "is_purchase_item": is_purchase_item,
                    "is_sales_item": is_sales_item,
                    "has_serial_no": has_serial_no,
                    "has_batch_no": has_batch_no,
                    "valuation_method": "Moving Average",
                    "opening_stock": random.randint(10, 500) if is_stock_item else 0, # Adjusted quantities
                    "valuation_rate": random.randint(10_000, 1_000_000), # Adjusted for IDR
                    "standard_rate": random.randint(15_000, 1_500_000), # Adjusted for IDR
                    "description": f"{item_name} - High quality {base_name.lower()} for manufacturing",
                    "min_order_qty": random.choice([1, 5, 10, 20]),
                    "safety_stock": random.randint(5, 50) if is_stock_item else 0,
                    "lead_time_days": random.randint(1, 21) if is_purchase_item else 0,
                    "weight_per_unit": random.uniform(0.1, 20),
                    "weight_uom": "Kg",
                    "warranty_period": random.choice([0, 6, 12, 24]) if is_sales_item else 0
                }
                
                try:
                    item = self.api.create_doc("Item", item_data)
                    self.items.append(item)
                    items_created_count += 1
                    logger.debug(f"Created item: {item_name} ({item_code})")
                except Exception as e:
                    logger.warning(f"Failed to create item {item_name} ({item_code}): {str(e)}")
            
        logger.info(f"Created {items_created_count} new items. Total items (existing + new): {len(self.items)}")
    
    def create_warehouses(self):
        """Create warehouse records."""
        logger.info("Creating warehouses...")
        
        # Fetch existing warehouses
        existing_warehouses = self.api.get_list("Warehouse", filters={"company": Config.COMPANY_NAME}, fields=["name", "warehouse_name"])
        self.warehouses = existing_warehouses
        logger.info(f"Found {len(self.warehouses)} existing warehouses for {Config.COMPANY_NAME}.")

        warehouses_to_ensure = [
            {"name": "Main Store", "type": "Main"},
            {"name": "Raw Material Store", "type": "Raw Material"},
            {"name": "Work In Progress", "type": "WIP"},
            {"name": "Finished Goods Store", "type": "Finished Goods"},
            {"name": "Rejected Store", "type": "Scrap"},
            {"name": "Transit Warehouse", "type": "Transit"}, 
            {"name": "Quality Inspection", "type": "Quality Inspection"} 
        ]
        
        warehouses_created_count = 0
        for wh_template in warehouses_to_ensure:
            warehouse_name = f"{wh_template['name']} - {Config.COMPANY_ABBR}"
            
            # Check if warehouse exists by its constructed full name
            if any(w["warehouse_name"] == warehouse_name for w in self.warehouses):
                logger.debug(f"Warehouse {warehouse_name} already exists, skipping.")
                continue
                
            warehouse_data = {
                "warehouse_name": warehouse_name,
                "warehouse_type": wh_template["type"], # Ensure this Warehouse Type exists if custom
                "company": Config.COMPANY_NAME,
                "is_group": 0,
                "disabled": 0
            }
            
            try:
                warehouse = self.api.create_doc("Warehouse", warehouse_data)
                self.warehouses.append(warehouse)
                warehouses_created_count += 1
                logger.debug(f"Created warehouse: {warehouse_name}")
            except Exception as e:
                logger.warning(f"Failed to create warehouse {warehouse_name}: {str(e)}")
        
        logger.info(f"Created {warehouses_created_count} new warehouses. Total warehouses (existing + new): {len(self.warehouses)}")
    
    def create_projects(self):
        """Create project records."""
        logger.info("Creating projects...")
        
        # Fetch existing projects
        existing_projects = self.api.get_list("Project", filters={"company": Config.COMPANY_NAME}, fields=["name", "project_name"])
        projects = existing_projects # Start with existing projects
        logger.info(f"Found {len(projects)} existing projects.")

        projects_to_create = 30 - len(projects) # Aim for a total of 30 projects
        if projects_to_create <= 0:
            logger.info("Enough projects already exist. Skipping new project creation.")
            return

        logger.info(f"Creating {projects_to_create} new projects...")
        project_types = ["Internal", "External Client", "Product Development", "R&D", "Infrastructure Upgrade"] # More descriptive types
        project_status = ["Open", "Open", "Open", "On Hold", "Completed"]  # More weight on Open
        priorities = ["Low", "Medium", "High", "Urgent"]
        
        for i in range(projects_to_create):
            project_name = f"Proj-{self.fake.word().capitalize()}-{self.fake.uuid4().split('-')[0]}" # More unique project names
            
            # Check if project exists (less strict check for random names)
            if any(p["project_name"] == project_name for p in projects):
                logger.debug(f"Generated project name {project_name} might conflict, retrying or skipping.")
                continue
                
            # Random dates within 2025
            expected_start = self.generate_date_in_range(Config.START_DATE, Config.END_DATE)
            duration_days = random.randint(30, 240) # Longer project durations
            expected_end_date = datetime.strptime(expected_start, "%Y-%m-%d") + timedelta(days=duration_days)
            
            customer_for_project = None
            if self.customers and random.random() < 0.7: # 70% of projects are external/client projects
                customer_for_project = random.choice(self.customers)["name"]

            project_data = {
                "project_name": project_name,
                "project_type": random.choice(project_types), # Ensure these Project Types exist
                "status": random.choice(project_status),
                "priority": random.choice(priorities),
                "company": Config.COMPANY_NAME,
                "expected_start_date": expected_start,
                "expected_end_date": expected_end_date.strftime("%Y-%m-%d"),
                "estimated_costing": random.randint(10_000_000, 1_000_000_000), # Adjusted for IDR
                "customer": customer_for_project,
                "description": f"Implementation of {self.fake.catch_phrase()} for {project_name}",
                "percent_complete_method": "Task Completion"
            }
            
            try:
                project = self.api.create_doc("Project", project_data)
                projects.append(project)
                logger.debug(f"Created project: {project_name}")
                
                # Create tasks for the project
                self.create_tasks_for_project(project["name"], expected_start, expected_end_date.strftime("%Y-%m-%d"))
                
            except Exception as e:
                logger.warning(f"Failed to create project {project_name}: {str(e)}")
        
        logger.info(f"Total projects (existing + new): {len(projects)}")
        return projects
    
    def create_tasks_for_project(self, project_name: str, start_date: str, end_date: str):
        """Create tasks for a project."""
        task_templates = [
            "Initiation & Scope Definition",
            "Requirements Gathering", 
            "Technical Design",
            "Development & Coding",
            "Unit Testing",
            "Integration Testing",
            "User Acceptance Testing (UAT)",
            "Deployment & Go-Live",
            "Post-Implementation Support",
            "Project Closure"
        ]
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        total_duration = (end_dt - start_dt).days
        
        if total_duration < len(task_templates): # Ensure there's at least 1 day per task
            logger.warning(f"Project '{project_name}' duration is too short for all tasks. Adjusting task durations to 1 day per task.")
            task_duration = 1
        else:
            task_duration = total_duration // len(task_templates)

        current_date = start_dt
        for i, task_template in enumerate(task_templates):
            task_name = f"{task_template} - {project_name}"
            
            # Ensure task name is unique within the project to avoid API conflicts
            # ERPNext Task 'name' field is usually its subject. We can check by subject and project.
            existing_tasks_for_project = self.api.get_list("Task", filters={"project": project_name, "subject": task_name}, fields=["name"])
            if existing_tasks_for_project:
                logger.debug(f"Task '{task_name}' already exists for project '{project_name}', skipping.")
                current_date = min(current_date + timedelta(days=task_duration + 1), end_dt) # Advance date
                continue

            task_start = current_date
            task_end = min(current_date + timedelta(days=task_duration), end_dt)
            
            # If task_start becomes after task_end, it means we've run out of project duration
            if task_start > task_end:
                logger.debug(f"Skipping task '{task_name}' as project duration exceeded.")
                break

            task_data = {
                "subject": task_name,
                "project": project_name,
                "status": random.choice(["Open", "Working", "Completed", "Cancelled"]),
                "priority": random.choice(["Low", "Medium", "High"]),
                "expected_start_date": task_start.strftime("%Y-%m-%d"),
                "expected_end_date": task_end.strftime("%Y-%m-%d"),
                "expected_time": random.randint(20, 160), # Expected hours
                "description": f"Details for {task_template.lower()} for the project",
                "company": Config.COMPANY_NAME
            }
            
            # Assign to employee
            if self.employees:
                task_data["_assign"] = json.dumps([random.choice(self.employees)["name"]])
            
            try:
                self.api.create_doc("Task", task_data)
                logger.debug(f"Created task: {task_name}")
            except Exception as e:
                logger.warning(f"Failed to create task {task_name}: {str(e)}")
            
            current_date = task_end + timedelta(days=1) # Move to the next day for the next task
    
    def create_attendance_records(self):
        """Create attendance records for employees."""
        logger.info("Creating attendance records...")
        
        attendance_count = 0
        current_date = Config.START_DATE
        
        if not self.employees:
            logger.warning("No employees found. Skipping attendance record creation.")
            return

        while current_date <= Config.END_DATE:
            # Skip weekends
            if current_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                current_date += timedelta(days=1)
                continue
                
            # Mark attendance for 85-95% of employees each day
            present_employees = random.sample(
                self.employees, 
                int(len(self.employees) * random.uniform(0.85, 0.95))
            )
            
            for employee in present_employees:
                status = random.choices(
                    ["Present", "Half Day", "Work From Home"],
                    weights=[0.85, 0.05, 0.10]
                )[0]
                
                # ERPNext attendance records are usually unique by employee and date
                # We can't directly use check_exists("Attendance", name) because 'name' is not predictable.
                # Instead, check by filtering.
                existing_attendance = self.api.get_list("Attendance", filters={
                    "employee": employee["name"],
                    "attendance_date": current_date.strftime("%Y-%m-%d")
                }, fields=["name"])

                if existing_attendance:
                    logger.debug(f"Attendance for {employee['employee_name']} on {current_date.strftime('%Y-%m-%d')} already exists, skipping.")
                    continue

                attendance_data = {
                    "employee": employee["name"],
                    "attendance_date": current_date.strftime("%Y-%m-%d"),
                    "status": status,
                    "company": Config.COMPANY_NAME,
                    "check_in_time": f"{current_date.strftime('%Y-%m-%d')} 08:{random.randint(0, 30):02d}:00", # Slightly earlier check-in
                    "check_out_time": f"{current_date.strftime('%Y-%m-%d')} 17:{random.randint(0, 45):02d}:00", # Slightly earlier check-out
                    "working_hours": random.uniform(7.0, 9.0) if status == "Present" else random.uniform(3.0, 4.5)
                }
                
                try:
                    self.api.create_doc("Attendance", attendance_data)
                    attendance_count += 1
                    logger.debug(f"Created attendance for {employee['employee_name']} on {current_date.strftime('%Y-%m-%d')}")
                except Exception as e:
                    logger.warning(f"Failed to create attendance for {employee['employee_name']} on {current_date.strftime('%Y-%m-%d')}: {str(e)}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Created {attendance_count} attendance records")
    
    def create_sales_invoices(self):
        """Create sales invoice transactions."""
        logger.info("Creating sales invoices...")
        
        invoice_count = 0
        
        if not self.customers:
            logger.warning("No customers found. Skipping sales invoice creation.")
            return
        if not self.items:
            logger.warning("No items found. Skipping sales invoice creation.")
            return
        if not self.warehouses:
            logger.warning("No warehouses found. Skipping sales invoice creation.")
            return

        # Get accounts - ensure these accounts exist in your ERPNext Chart of Accounts
        income_account = self.get_account("Sales", "Income")
        debtors_account = self.get_account("Debtors", "Asset") # Debtors is typically an Asset account (Receivable)
        
        if not income_account or not debtors_account:
            logger.error("Required accounts for Sales Invoice not found or could not be created. Skipping sales invoices.")
            return

        saleable_items = [item for item in self.items if item.get("is_sales_item")]
        if not saleable_items:
            logger.warning("No saleable items found. Skipping sales invoice creation.")
            return

        # Fetch existing Sales Taxes and Charges template
        sales_tax_template = self.api.get_list("Sales Taxes and Charges Template", filters={"company": Config.COMPANY_NAME}, fields=["name"])
        taxes_and_charges_name = sales_tax_template[0]["name"] if sales_tax_template else None

        if not taxes_and_charges_name:
            logger.warning("No 'Sales Taxes and Charges Template' found for company. Sales Invoices will be created without taxes.")
        
        # Distribute invoices throughout the year
        for month in range(1, 13):
            invoices_this_month = random.randint(35, 50)
            
            for _ in range(invoices_this_month):
                # Select customer - apply Pareto principle
                if random.random() < 0.8 and len(self.customers) > 1:  # 80% of sales from top 20% customers
                    customer = random.choice(self.customers[:max(1, int(len(self.customers) * 0.2))])
                else:
                    customer = random.choice(self.customers)
                
                # Generate invoice date
                invoice_date = self.generate_date_in_range(
                    datetime(2025, month, 1),
                    datetime(2025, month, 28)
                )
                
                # Create invoice items
                num_items = random.randint(1, 5)
                items_data = []
                
                for _ in range(num_items):
                    item = random.choice(saleable_items)
                    qty = random.randint(1, 20)
                    rate = item.get("standard_rate", 100_000) * random.uniform(0.9, 1.1)  # ±10% price variation
                    
                    items_data.append({
                        "item_code": item["item_code"],
                        "item_name": item["item_name"],
                        "description": item.get("description", item["item_name"]),
                        "qty": qty,
                        "rate": round(rate, 2),
                        "uom": item.get("stock_uom", "Pcs"),
                        "income_account": income_account,
                        "warehouse": random.choice(self.warehouses)["name"] if item.get("is_stock_item") else None
                    })
                
                invoice_data = {
                    "naming_series": "SINV-", # Ensure this naming series exists and is active
                    "customer": customer["name"],
                    "company": Config.COMPANY_NAME,
                    "posting_date": invoice_date,
                    "due_date": (datetime.strptime(invoice_date, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d"),
                    "currency": Config.DEFAULT_CURRENCY,
                    "debit_to": debtors_account,
                    "items": items_data,
                    "taxes_and_charges": taxes_and_charges_name, # Use fetched tax template
                    "territory": customer.get("territory", "All Territories"),
                    "customer_group": customer.get("customer_group", "All Customer Groups")
                }
                
                try:
                    invoice = self.api.create_doc("Sales Invoice", invoice_data)
                    self.api.submit_doc("Sales Invoice", invoice["name"])
                    invoice_count += 1
                    logger.debug(f"Created sales invoice: {invoice['name']}")
                except Exception as e:
                    logger.warning(f"Failed to create sales invoice: {str(e)}")
        
        logger.info(f"Created {invoice_count} sales invoices")
    
    def create_purchase_invoices(self):
        """Create purchase invoice transactions."""
        logger.info("Creating purchase invoices...")
        
        invoice_count = 0
        
        if not self.suppliers:
            logger.warning("No suppliers found. Skipping purchase invoice creation.")
            return
        if not self.items:
            logger.warning("No items found. Skipping purchase invoice creation.")
            return
        if not self.warehouses:
            logger.warning("No warehouses found. Skipping purchase invoice creation.")
            return

        # Get accounts - ensure these accounts exist in your ERPNext Chart of Accounts
        expense_account = self.get_account("Cost of Goods Sold", "Expense")
        creditors_account = self.get_account("Creditors", "Liability") # Creditors is typically a Liability account (Payable)
        
        if not expense_account or not creditors_account:
            logger.error("Required accounts for Purchase Invoice not found or could not be created. Skipping purchase invoices.")
            return

        purchasable_items = [item for item in self.items if item.get("is_purchase_item")]
        if not purchasable_items:
            logger.warning("No purchasable items found. Skipping purchase invoice creation.")
            return

        # Fetch existing Purchase Taxes and Charges template
        purchase_tax_template = self.api.get_list("Purchase Taxes and Charges Template", filters={"company": Config.COMPANY_NAME}, fields=["name"])
        taxes_and_charges_name = purchase_tax_template[0]["name"] if purchase_tax_template else None

        if not taxes_and_charges_name:
            logger.warning("No 'Purchase Taxes and Charges Template' found for company. Purchase Invoices will be created without taxes.")

        # Distribute invoices throughout the year
        for month in range(1, 13):
            invoices_this_month = random.randint(25, 40)
            
            for _ in range(invoices_this_month):
                supplier = random.choice(self.suppliers)
                
                # Generate invoice date
                invoice_date = self.generate_date_in_range(
                    datetime(2025, month, 1),
                    datetime(2025, month, 28)
                )
                
                # Create invoice items
                num_items = random.randint(1, 8)
                items_data = []
                
                for _ in range(num_items):
                    item = random.choice(purchasable_items)
                    qty = random.randint(10, 100)
                    rate = item.get("valuation_rate", 50_000) * random.uniform(0.85, 1.0)  # Supplier discount
                    
                    items_data.append({
                        "item_code": item["item_code"],
                        "item_name": item["item_name"],
                        "description": item.get("description", item["item_name"]),
                        "qty": qty,
                        "rate": round(rate, 2),
                        "uom": item.get("stock_uom", "Pcs"),
                        "expense_account": expense_account,
                        "warehouse": random.choice(self.warehouses)["name"] if item.get("is_stock_item") else None
                    })
                
                invoice_data = {
                    "naming_series": "PINV-", # Ensure this naming series exists and is active
                    "supplier": supplier["name"],
                    "company": Config.COMPANY_NAME,
                    "posting_date": invoice_date,
                    "due_date": (datetime.strptime(invoice_date, "%Y-%m-%d") + timedelta(days=45)).strftime("%Y-%m-%d"),
                    "currency": supplier.get("default_currency", Config.DEFAULT_CURRENCY),
                    "credit_to": creditors_account,
                    "items": items_data,
                    "taxes_and_charges": taxes_and_charges_name # Use fetched tax template
                }
                
                try:
                    invoice = self.api.create_doc("Purchase Invoice", invoice_data)
                    self.api.submit_doc("Purchase Invoice", invoice["name"])
                    invoice_count += 1
                    logger.debug(f"Created purchase invoice: {invoice['name']}")
                except Exception as e:
                    logger.warning(f"Failed to create purchase invoice: {str(e)}")
        
        logger.info(f"Created {invoice_count} purchase invoices")
    
    def create_stock_entries(self):
        """Create stock entry transactions."""
        logger.info("Creating stock entries...")
        
        entry_count = 0
        purposes = ["Material Receipt", "Material Issue", "Material Transfer", "Manufacture", "Repack"]
        
        if not self.items:
            logger.warning("No items found. Skipping stock entry creation.")
            return
        if not self.warehouses:
            logger.warning("No warehouses found. Skipping stock entry creation.")
            return

        stock_items = [item for item in self.items if item.get("is_stock_item")]
        if not stock_items:
            logger.warning("No stockable items found. Skipping stock entry creation.")
            return

        # Create entries throughout the year
        for month in range(1, 13):
            entries_this_month = random.randint(20, 35)
            
            for _ in range(entries_this_month):
                purpose = random.choice(purposes)
                entry_date = self.generate_date_in_range(
                    datetime(2025, month, 1),
                    datetime(2025, month, 28)
                )
                
                entry_data = {
                    "purpose": purpose, # Ensure this Stock Entry Purpose exists if custom
                    "company": Config.COMPANY_NAME,
                    "posting_date": entry_date,
                    "posting_time": self.generate_time()
                }
                
                # Create items based on purpose
                items_data = []
                
                if purpose == "Material Receipt":
                    # Receiving materials
                    num_items = random.randint(1, 5)
                    target_warehouse_candidates = [w for w in self.warehouses if "Raw Material" in w["name"] or "Main Store" in w["name"] or "Stores" in w["name"]]
                    target_warehouse = random.choice(target_warehouse_candidates) if target_warehouse_candidates else None
                    if not target_warehouse:
                        logger.warning("No suitable target warehouse found for Material Receipt, skipping.")
                        continue
                    
                    for _ in range(num_items):
                        item = random.choice(stock_items)
                        items_data.append({
                            "item_code": item["item_code"],
                            "qty": random.randint(50, 200),
                            "basic_rate": item.get("valuation_rate", 100_000), # Adjusted for IDR
                            "t_warehouse": target_warehouse["name"]
                        })
                
                elif purpose == "Material Issue":
                    # Issuing materials
                    num_items = random.randint(1, 3)
                    source_warehouse_candidates = [w for w in self.warehouses if "Work In Progress" in w["name"] or "Raw Material Store" in w["name"] or "Main Store" in w["name"]]
                    source_warehouse = random.choice(source_warehouse_candidates) if source_warehouse_candidates else None
                    if not source_warehouse:
                        logger.warning("No suitable source warehouse found for Material Issue, skipping.")
                        continue
                    
                    for _ in range(num_items):
                        item = random.choice(stock_items)
                        items_data.append({
                            "item_code": item["item_code"],
                            "qty": random.randint(10, 50),
                            "s_warehouse": source_warehouse["name"]
                        })
                
                elif purpose == "Material Transfer":
                    # Transfer between warehouses
                    num_items = random.randint(1, 3)
                    if len(self.warehouses) < 2:
                        logger.warning("Not enough warehouses for Material Transfer, skipping.")
                        continue
                    source_warehouse, target_warehouse = random.sample(self.warehouses, 2) # Ensures distinct source and target
                    
                    for _ in range(num_items):
                        item = random.choice(stock_items)
                        items_data.append({
                            "item_code": item["item_code"],
                            "qty": random.randint(5, 30),
                            "s_warehouse": source_warehouse["name"],
                            "t_warehouse": target_warehouse["name"]
                        })
                
                elif purpose == "Manufacture":
                    # Manufacturing entry
                    finished_items = [item for item in stock_items if "Finished Goods" in item.get("item_group", "")]
                    raw_items = [item for item in stock_items if "Raw Materials" in item.get("item_group", "") or "Components" in item.get("item_group", "")]
                    
                    if finished_items and raw_items:
                        # Finished goods (output)
                        finished_item = random.choice(finished_items)
                        finished_goods_wh_candidates = [w for w in self.warehouses if "Finished Goods Store" in w["name"]]
                        finished_goods_warehouse = random.choice(finished_goods_wh_candidates) if finished_goods_wh_candidates else None
                        
                        if not finished_goods_warehouse:
                            logger.warning("No 'Finished Goods Store' warehouse found for Manufacture output, skipping.")
                            continue

                        items_data.append({
                            "item_code": finished_item["item_code"],
                            "qty": random.randint(1, 10), # Manufactured quantity usually smaller
                            "t_warehouse": finished_goods_warehouse["name"],
                            "is_finished_item": 1
                        })
                        
                        # Raw materials (input)
                        raw_material_wh_candidates = [w for w in self.warehouses if "Raw Material Store" in w["name"] or "Main Store" in w["name"]]
                        raw_material_warehouse = random.choice(raw_material_wh_candidates) if raw_material_wh_candidates else None

                        if not raw_material_warehouse:
                            logger.warning("No 'Raw Material Store' warehouse found for Manufacture input, skipping.")
                            continue

                        for _ in range(random.randint(2, 4)):
                            raw_item = random.choice(raw_items)
                            items_data.append({
                                "item_code": raw_item["item_code"],
                                "qty": random.randint(5, 30),
                                "s_warehouse": raw_material_warehouse["name"]
                            })
                    else:
                        logger.warning("Not enough finished/raw materials for Manufacture purpose or suitable warehouses, skipping.")
                        continue
                
                elif purpose == "Repack":
                    # Repackaging (e.g., from bulk to smaller packs)
                    num_items_input = random.randint(1,2)
                    num_items_output = random.randint(1,2)
                    
                    # Assume main store for repackaging
                    repack_warehouse_candidates = [w for w in self.warehouses if "Main Store" in w["name"] or "Stores" in w["name"]]
                    repack_warehouse = random.choice(repack_warehouse_candidates) if repack_warehouse_candidates else None

                    if not repack_warehouse:
                        logger.warning("No suitable warehouse for Repack purpose, skipping.")
                        continue

                    # Input items (larger quantity, e.g., bulk item)
                    for _ in range(num_items_input):
                        input_item = random.choice([item for item in stock_items if item.get("item_group") in ["Raw Materials", "Components"]])
                        items_data.append({
                            "item_code": input_item["item_code"],
                            "qty": random.randint(20, 100),
                            "s_warehouse": repack_warehouse["name"],
                            "is_finished_item": 0
                        })
                    
                    # Output items (smaller packs, derived from input)
                    for _ in range(num_items_output):
                        output_item = random.choice([item for item in stock_items if item.get("item_group") in ["Finished Goods", "Consumables"]])
                        items_data.append({
                            "item_code": output_item["item_code"],
                            "qty": random.randint(10, 50),
                            "t_warehouse": repack_warehouse["name"],
                            "is_finished_item": 1
                        })

                if items_data:
                    entry_data["items"] = items_data
                    
                    try:
                        entry = self.api.create_doc("Stock Entry", entry_data)
                        self.api.submit_doc("Stock Entry", entry["name"])
                        entry_count += 1
                        logger.debug(f"Created stock entry: {entry['name']} ({purpose})")
                    except Exception as e:
                        logger.warning(f"Failed to create stock entry ({purpose}): {str(e)}")
                else:
                    logger.debug(f"No items generated for stock entry purpose '{purpose}', skipping entry.")
        
        logger.info(f"Created {entry_count} stock entries")
    
    def create_salary_slips(self):
        """Create salary slips for employees."""
        logger.info("Creating salary slips...")
        
        salary_slip_count = 0
        
        if not self.employees:
            logger.warning("No employees found. Skipping salary slip creation.")
            return

        # Create salary structure first (will ensure components are there)
        self.create_salary_structure()
        
        # Get payroll account
        payment_account = self.get_account("Cash", "Asset") # Cash is an Asset account
        if not payment_account:
            logger.error("Payment account for Salary Slips not found. Skipping salary slips.")
            return

        # Fetch existing salary structures
        existing_salary_structures = self.api.get_list("Salary Structure", filters={"company": Config.COMPANY_NAME}, fields=["name"])
        salary_structure_names = [s["name"] for s in existing_salary_structures]

        if not salary_structure_names:
            logger.warning("No salary structures found. Please ensure salary structures are created. Skipping salary slips.")
            return

        # Generate salary slips for each month
        for month in range(1, 13):
            year = 2025
            # Determine start and end date for the month
            start_date_month = datetime(year, month, 1)
            end_date_month = (start_date_month + timedelta(days=32)).replace(day=1) - timedelta(days=1) # Last day of the month

            start_date_str = start_date_month.strftime("%Y-%m-%d")
            end_date_str = end_date_month.strftime("%Y-%m-%d")
            
            # Create payroll entry
            payroll_entry_name_base = f"Payroll Entry {Config.COMPANY_ABBR} {start_date_month.strftime('%b %Y')}"
            
            payroll_entry_doc = None
            try:
                # Check if Payroll Entry for this period already exists
                existing_payroll_entries = self.api.get_list("Payroll Entry", filters={
                    "company": Config.COMPANY_NAME,
                    "start_date": start_date_str,
                    "end_date": end_date_str
                }, fields=["name"])

                if existing_payroll_entries:
                    payroll_entry_doc = {"name": existing_payroll_entries[0]["name"]}
                    logger.debug(f"Payroll entry {payroll_entry_doc['name']} already exists, skipping creation.")
                else:
                    payroll_data = {
                        "naming_series": "PE-", # Or match your existing naming series if any
                        "company": Config.COMPANY_NAME,
                        "payroll_frequency": "Monthly",
                        "start_date": start_date_str,
                        "end_date": end_date_str,
                        "posting_date": end_date_str, # Usually posted on last day
                        "payment_account": payment_account,
                        "employees": [] # Employees can be added here, or individual slips linked. For dummy, linking is fine.
                    }
                    payroll_entry_doc = self.api.create_doc("Payroll Entry", payroll_data)
                    logger.debug(f"Created payroll entry: {payroll_entry_doc['name']}")
            except Exception as e:
                logger.warning(f"Failed to create/find payroll entry for month {month}: {str(e)}")
                continue # Skip salary slips for this month if payroll entry fails

            # Create individual salary slips
            for employee in random.sample(self.employees, int(len(self.employees) * random.uniform(0.90, 0.98))): # 90-98% employees get salary
                # Try to find a matching salary structure by designation, fallback to any if none specific
                employee_designation = employee.get('designation', 'Employee')
                
                # Look for structures that contain the designation or a more generic one
                relevant_structure = next((s for s in salary_structure_names if employee_designation in s), None)
                if not relevant_structure:
                    # Fallback to a "Standard Employee" structure or just any available structure
                    relevant_structure = next((s for s in salary_structure_names if "Standard Employee" in s), None)
                if not relevant_structure and salary_structure_names:
                    relevant_structure = random.choice(salary_structure_names)
                
                if not relevant_structure:
                    logger.warning(f"No suitable salary structure found for employee {employee['employee_name']} (designation: {employee_designation}), skipping salary slip.")
                    continue


                salary_slip_data = {
                    "employee": employee["name"],
                    "salary_structure": relevant_structure,
                    "company": Config.COMPANY_NAME,
                    "posting_date": end_date_str,
                    "start_date": start_date_str,
                    "end_date": end_date_str,
                    "payroll_entry": payroll_entry_doc["name"] if payroll_entry_doc else None, # Link to payroll entry
                    "currency": Config.DEFAULT_CURRENCY
                }
                
                try:
                    # Check if Salary Slip for this employee and period already exists
                    existing_slips = self.api.get_list("Salary Slip", filters={
                        "employee": employee["name"],
                        "start_date": start_date_str,
                        "end_date": end_date_str
                    }, fields=["name"])
                    
                    if not existing_slips:
                        salary_slip = self.api.create_doc("Salary Slip", salary_slip_data)
                        self.api.submit_doc("Salary Slip", salary_slip["name"])
                        salary_slip_count += 1
                        logger.debug(f"Created and submitted salary slip for: {employee['employee_name']} ({salary_slip['name']})")
                    else:
                        logger.debug(f"Salary slip for {employee['employee_name']} for {start_date_str}-{end_date_str} already exists, skipping.")

                except Exception as e:
                    logger.warning(f"Failed to create/submit salary slip for {employee['employee_name']} for {start_date_str}-{end_date_str}: {str(e)}")
                
        logger.info(f"Created {salary_slip_count} salary slips")
    
    def create_salary_structure(self):
        """Creates salary structures for different designations."""
        logger.info("Creating salary structures...")
        
        # Ensure Salary Components exist first
        self._ensure_salary_components_exist()

        # Get salary component accounts (should be in cache now from fetch_accounts/ensure_default_accounts_exist)
        basic_salary_account = self.get_account("Basic Salary", "Expense")
        hra_account = self.get_account("House Rent Allowance", "Expense")
        special_allowance_account = self.get_account("Special Allowance", "Expense")
        professional_tax_account = self.get_account("Professional Tax Payable", "Liability")
        provident_fund_account = self.get_account("Provident Fund Payable", "Liability")

        # Critical check: if any of these are None, we cannot proceed.
        if not all([basic_salary_account, hra_account, special_allowance_account, professional_tax_account, provident_fund_account]):
            logger.error("One or more required salary component accounts not found after trying to ensure them. Cannot create Salary Structures.")
            return

        designations = ["Manager", "Senior Executive", "Executive", "Assistant", "Engineer"]
        base_annual_salaries_idr = { # Annual salaries in IDR
            "Manager": 80_000_000,
            "Senior Executive": 60_000_000,
            "Executive": 45_000_000,
            "Assistant": 35_000_000,
            "Engineer": 55_000_000
        }
        
        for designation in designations:
            structure_name = f"Standard Salary Structure - {designation} ({Config.COMPANY_ABBR})" # More descriptive name
            
            # Skip if structure exists
            if self.api.check_exists("Salary Structure", structure_name):
                logger.debug(f"Salary structure '{structure_name}' already exists, skipping.")
                continue
                
            monthly_base = base_annual_salaries_idr.get(designation, 40_000_000) / 12 # Convert to monthly

            # Earnings
            earnings = [
                {
                    "salary_component": "Basic Salary",
                    "formula": f"{(monthly_base * 0.5):.2f}", # 50% of monthly base
                    "amount_based_on_formula": 1,
                    "default_amount": 0,
                    "depends_on_payment_days": 1,
                    "is_tax_enabled": 1,
                    "account": basic_salary_account
                },
                {
                    "salary_component": "House Rent Allowance",
                    "formula": f"{(monthly_base * 0.25):.2f}", # 25% of monthly base
                    "amount_based_on_formula": 1,
                    "default_amount": 0,
                    "depends_on_payment_days": 1,
                    "is_tax_enabled": 1,
                    "account": hra_account
                },
                {
                    "salary_component": "Special Allowance",
                    "formula": f"{(monthly_base * 0.25):.2f}", # 25% of monthly base
                    "amount_based_on_formula": 1,
                    "default_amount": 0,
                    "depends_on_payment_days": 1,
                    "is_tax_enabled": 1,
                    "account": special_allowance_account
                }
            ]
            
            # Deductions
            deductions = [
                {
                    "salary_component": "Professional Tax",
                    "amount": 20_000, # Example fixed deduction for IDR
                    "amount_based_on_formula": 0,
                    "default_amount": 20_000,
                    "account": professional_tax_account
                },
                {
                    "salary_component": "Provident Fund", # Or BPJS Ketenagakerjaan in Indonesia
                    "formula": "base * 0.03", # Example PF contribution (e.g., 3% of Basic Salary)
                    "amount_based_on_formula": 1,
                    "default_amount": 0,
                    "account": provident_fund_account
                }
            ]
            
            structure_data = {
                "name": structure_name,
                "company": Config.COMPANY_NAME,
                "payroll_frequency": "Monthly",
                "salary_slip_based_on_timesheet": 0,
                "earnings": earnings,
                "deductions": deductions,
                "is_active": 1,
                "from_date": "2025-01-01" # Effective from 2025
            }
            
            try:
                self.api.create_doc("Salary Structure", structure_data)
                logger.debug(f"Created salary structure: '{structure_name}'")
            except Exception as e:
                logger.warning(f"Failed to create salary structure '{structure_name}': {str(e)}")

    def _ensure_salary_components_exist(self):
        """Ensures default salary components exist, creating them if necessary."""
        logger.info("Ensuring salary components exist...")
        components = {
            "Basic Salary": {"type": "Earning", "is_gross_pay": 1, "is_flexible_benefit": 0},
            "House Rent Allowance": {"type": "Earning", "is_gross_pay": 1, "is_flexible_benefit": 0},
            "Special Allowance": {"type": "Earning", "is_gross_pay": 1, "is_flexible_benefit": 0},
            "Professional Tax": {"type": "Deduction", "is_gross_pay": 0, "is_flexible_benefit": 0},
            "Provident Fund": {"type": "Deduction", "is_gross_pay": 0, "is_flexible_benefit": 0}
        }

        for name, props in components.items():
            # ERPNext uses 'salary_component_name' as the primary unique ID for check_exists, not 'name'
            if not self.api.check_exists("Salary Component", name): 
                try:
                    component_data = {
                        "salary_component_name": name, # Use this for creation
                        "type": props["type"],
                        "is_gross_pay": props["is_gross_pay"],
                        "is_flexible_benefit": props["is_flexible_benefit"],
                        # Salary Components are typically global and not company-specific directly in their DocType.
                        # They are linked to a company via Salary Structure.
                    }
                    self.api.create_doc("Salary Component", component_data)
                    logger.info(f"Created Salary Component: '{name}'")
                except Exception as e:
                    logger.warning(f"Failed to create Salary Component '{name}': {str(e)}")
            else:
                logger.debug(f"Salary Component '{name}' already exists.")

    def create_assets(self):
        """Create asset records."""
        logger.info("Creating assets...")
        
        assets_created_count = 0
        asset_categories = [
            {"name": "Plant and Machinery", "depreciation_rate": 15},
            {"name": "Furniture and Fixtures", "depreciation_rate": 10},
            {"name": "Office Equipment", "depreciation_rate": 25},
            {"name": "Vehicles", "depreciation_rate": 20},
            {"name": "Computer Equipment", "depreciation_rate": 40}
        ]
        
        # Create asset categories first if they don't exist
        for category in asset_categories:
            if not self.api.check_exists("Asset Category", category["name"]):
                logger.info(f"Asset Category '{category['name']}' not found. Attempting to create it.")
                try:
                    cat_data = {
                        "asset_category_name": category["name"],
                        "depreciation_method": "Straight Line",
                        "total_number_of_depreciations": 5, # 5 years
                        "frequency_of_depreciation": 12,  # Monthly
                        "company": Config.COMPANY_NAME
                    }
                    self.api.create_doc("Asset Category", cat_data)
                    logger.info(f"Created Asset Category: '{category['name']}'")
                except Exception as e:
                    logger.warning(f"Failed to create asset category '{category['name']}': {str(e)}")
            else:
                logger.debug(f"Asset category '{category['name']}' already exists, skipping.")
        
        # Fetch existing assets (after categories are potentially created)
        existing_assets = self.api.get_list("Asset", filters={"company": Config.COMPANY_NAME}, fields=["name", "asset_name"])
        # Only add new assets if the count is below target
        assets_to_create = Config.ITEM_COUNT - len(existing_assets) # Reusing ITEM_COUNT for general asset volume for simplicity
        if assets_to_create <= 0:
            logger.info("Enough assets already exist. Skipping new asset creation.")
            return

        logger.info(f"Creating {assets_to_create} new assets...")
        # Re-fetch asset categories to ensure newly created ones are included for random choice
        current_asset_categories = self.api.get_list("Asset Category", fields=["name"]) 
        if not current_asset_categories:
            logger.warning("No Asset Categories found even after trying to create. Cannot create assets.")
            return

        for i in range(assets_to_create):
            category = random.choice(current_asset_categories) # Use fetched categories
            purchase_date = self.generate_date_in_range(
                datetime(2023, 1, 1),  # Assets purchased in last 2 years
                Config.START_DATE # Purchased before 2025
            )
            
            asset_name = f"{category['name']} #{random.randint(1000, 9999)}-{Config.COMPANY_ABBR}" # Ensure unique naming for new assets
            
            # Check if asset exists by its generated name
            if any(a["asset_name"] == asset_name for a in existing_assets):
                logger.debug(f"Asset {asset_name} already exists, skipping.")
                continue
                
            asset_data = {
                "asset_name": asset_name,
                "asset_category": category["name"],
                "company": Config.COMPANY_NAME,
                "purchase_date": purchase_date,
                "gross_purchase_amount": random.randint(1_000_000, 500_000_000), # Adjusted for IDR
                "purchase_receipt_amount": random.randint(1_000_000, 500_000_000),
                "available_for_use_date": purchase_date,
                "location": random.choice(["Head Office", "Factory", "Warehouse", "Site A", "Branch Office"]),
                "custodian": random.choice(self.employees)["name"] if self.employees else None,
                "department": random.choice(["Operations", "Administration", "Production", "Sales", "IT", "Finance"]),
                "calculate_depreciation": 1,
                "is_existing_asset": 0
            }
            
            try:
                asset = self.api.create_doc("Asset", asset_data)
                assets_created_count += 1
                logger.debug(f"Created asset: {asset['name']}")
            except Exception as e:
                logger.warning(f"Failed to create asset '{asset_name}': {str(e)}")
        
        logger.info(f"Created {assets_created_count} new assets. Total assets (existing + new): {len(existing_assets) + assets_created_count}")
    
    def generate_all_data(self):
        """Main method to generate all data."""
        logger.info("Starting ERPNext dummy data generation...")
        logger.info(f"Target: {Config.USER_COUNT} users, {Config.EMPLOYEE_COUNT} employees, {Config.TRANSACTION_COUNT}+ transactions")
        logger.info(f"Date range for transactions: {Config.START_DATE.strftime('%Y-%m-%d')} to {Config.END_DATE.strftime('%Y-%m-%d')}")
        
        try:
            # Master data phase - always fetch existing first
            logger.info("=== Creating Master Data (or fetching existing) ===")
            self.create_users() # Relies on Role Profiles
            self.create_employees() # Relies on Users, Holiday List
            self.create_customers()
            self.create_suppliers()
            self.create_warehouses()
            self.create_items() # Relies on Item Groups
            self.create_assets() # Relies on Asset Categories
            
            # Transaction phase
            logger.info("=== Creating Transactions ===")
            self.create_sales_invoices() # Relies on Accounts, Customers, Items, Warehouses, Tax Templates
            self.create_purchase_invoices() # Relies on Accounts, Suppliers, Items, Warehouses, Tax Templates
            self.create_stock_entries() # Relies on Items, Warehouses
            self.create_projects() # Relies on Customers
            self.create_attendance_records() # Relies on Employees
            self.create_salary_slips() # Relies on Employees, Salary Structures, Accounts
            
            logger.info("=== Data Generation Complete ===")
            logger.info("Summary:")
            logger.info(f"- Users: {len(self.users)}")
            logger.info(f"- Employees: {len(self.employees)}")
            logger.info(f"- Customers: {len(self.customers)}")
            logger.info(f"- Suppliers: {len(self.suppliers)}")
            logger.info(f"- Items: {len(self.items)}")
            logger.info(f"- Warehouses: {len(self.warehouses)}")
            
        except Exception as e:
            logger.error(f"Fatal error during data generation: {str(e)}")
            raise

def main():
    """Main entry point."""
    print("=" * 80)
    print("ERPNext Dummy Data Generator")
    print("=" * 80)
    print(f"API Endpoint: {Config.BASE_URL}")
    print(f"Company: {Config.COMPANY_NAME}")
    print(f"Date Range for Transactions: Full year 2025")
    print("=" * 80)
    
    # Confirm before proceeding
    response = input("\nThis script will create or update dummy data and essential configurations (Role Profiles, Accounts, etc.) in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return
    
    # Create generator and run
    try:
        generator = DataGenerator()
        generator.generate_all_data()
        
        print("\n" + "=" * 80)
        print("Data generation completed successfully!")
        print("Check the log file for detailed information: erpnext_data_generation.log")
        print("=" * 80)
    except Exception as e:
        print(f"\n" + "=" * 80)
        print(f"Data generation failed due to a critical error: {e}")
        print("Please check the log file 'erpnext_data_generation.log' for more details.")
        print("=" * 80)

if __name__ == "__main__":
    main()
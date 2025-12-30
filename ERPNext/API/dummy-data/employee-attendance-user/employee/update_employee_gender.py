#!/usr/bin/env python3
"""
ERPNext Employee Gender Updater
Updates employee gender for testing CDC events.
Unlike creation (3 events), updates generate only 1 event per change.
"""

import requests
import json
import random
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional
import sys


def load_env_file():
    """Load environment variables from .env.local file"""
    env_path = Path(__file__).parent.parent.parent.parent / '.env.local'
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value


load_env_file()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class ERPNextAPI:
    """API client for ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if retry_count < 3:
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None, limit: int = 500) -> List[Dict]:
        """Get list of documents"""
        params = {"limit_page_length": limit}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def update_doc(self, doctype: str, name: str, data: Dict) -> Dict:
        """Update existing document"""
        return self._make_request("PUT", f"resource/{doctype}/{name}", data)


class EmployeeGenderUpdater:
    """Updates employee gender for CDC testing"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.updated_count = 0
        self.failed_count = 0

    def fetch_all_employees(self) -> List[Dict]:
        """Fetch all employees with their gender"""
        logger.info("Fetching all employees...")
        employees = self.api.get_list(
            "Employee",
            fields=["name", "employee_name", "gender"],
            limit=0  # Get all
        )
        logger.info(f"Found {len(employees)} employees")
        return employees

    def toggle_gender(self, current_gender: str) -> str:
        """Toggle gender: Male -> Female, Female -> Male"""
        if current_gender == "Male":
            return "Female"
        elif current_gender == "Female":
            return "Male"
        else:
            # If unknown, randomly assign
            return random.choice(["Male", "Female"])

    def update_employees(self, num_to_update: int):
        """Update gender for random employees"""
        employees = self.fetch_all_employees()
        
        if not employees:
            logger.error("No employees found!")
            return
        
        # Limit to available employees
        num_to_update = min(num_to_update, len(employees))
        
        # Randomly select employees to update
        selected = random.sample(employees, num_to_update)
        
        logger.info(f"\nUpdating {num_to_update} employee(s)...")
        logger.info("-" * 50)
        
        for i, emp in enumerate(selected, 1):
            emp_name = emp.get("name")
            emp_display = emp.get("employee_name", emp_name)
            current_gender = emp.get("gender", "Unknown")
            new_gender = self.toggle_gender(current_gender)
            
            try:
                self.api.update_doc("Employee", emp_name, {"gender": new_gender})
                self.updated_count += 1
                logger.info(f"[{i}/{num_to_update}] {emp_name} ({emp_display}): {current_gender} -> {new_gender}")
            except Exception as e:
                self.failed_count += 1
                logger.error(f"[{i}/{num_to_update}] Failed to update {emp_name}: {str(e)[:80]}")
        
        logger.info("-" * 50)
        logger.info(f"Updated: {self.updated_count}")
        logger.info(f"Failed: {self.failed_count}")
        logger.info(f"\nThis should generate {self.updated_count} CDC event(s)")

    def run(self, num_to_update: int):
        """Main execution"""
        self.update_employees(num_to_update)


if __name__ == "__main__":
    try:
        # Show available employees first
        api = ERPNextAPI()
        employees = api.get_list("Employee", fields=["name"], limit=0)
        total_employees = len(employees)
        
        if total_employees == 0:
            logger.error("No employees found in the system!")
            sys.exit(1)
        
        logger.info(f"Total employees in system: {total_employees}")
        
        while True:
            try:
                num_updates = int(input(f"How many employees to update? (1-{total_employees}): "))
                if 1 <= num_updates <= total_employees:
                    break
                else:
                    logger.error(f"Please enter a number between 1 and {total_employees}")
            except ValueError:
                logger.error("Invalid input. Please enter a number")

        response = input(f"Confirming gender update for {num_updates} random employee(s). Type 'UPDATE' to confirm: ")

        if response != "UPDATE":
            logger.info("Operation cancelled")
            sys.exit(0)

        updater = EmployeeGenderUpdater()
        updater.run(num_updates)
        
    except KeyboardInterrupt:
        logger.info("\nOperation interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

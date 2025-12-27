#!/usr/bin/env python3
"""ERPNext Shift Assignment Generator - Minimalist Version"""

import requests
import json
import random
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys


def load_env():
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value


load_env()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class API:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Content-Type': 'application/json'
        })

    def request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry: int = 0) -> Dict:
        try:
            url = f"{BASE_URL}/api/{endpoint}"
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                           params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if retry < 3:
                return self.request(method, endpoint, data, retry + 1)
            raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None) -> List[Dict]:
        params = {"limit_page_length": 500}
        if filters:
            params["filters"] = json.dumps(filters)
        return self.request("GET", f"resource/{doctype}", params).get("data", [])

    def create(self, doctype: str, data: Dict) -> Dict:
        data["doctype"] = doctype
        return self.request("POST", f"resource/{doctype}", data)


class ShiftAssignmentGenerator:
    def __init__(self):
        self.api = API()
        self.created = 0
        self.failed = 0
        self.employees = []
        self.shifts = []
        self.locations = []

    def fetch_data(self):
        logger.info("Fetching employees, shifts, and locations...")
        try:
            self.employees = self.api.get_list("Employee", {"company": COMPANY_NAME, "status": "Active"})
            self.shifts = [s.get("name") for s in self.api.get_list("Shift Type")]
            self.locations = [l.get("name") for l in self.api.get_list("Shift Location")]
            
            logger.info(f"Found {len(self.employees)} employees, {len(self.shifts)} shifts, {len(self.locations)} locations")
            return len(self.employees) > 0 and len(self.shifts) > 0
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return False

    def create_assignments(self, num_per_employee: int):
        logger.info(f"Creating {num_per_employee} shift assignments per employee...")
        
        start_date = datetime(2025, 1, 1)
        
        for emp_idx, emp in enumerate(self.employees, 1):
            emp_id = emp.get("name")
            emp_name = emp.get("employee_name", emp_id)
            
            for i in range(num_per_employee):
                try:
                    start = (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
                    end = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=random.randint(7, 90))).strftime("%Y-%m-%d")
                    
                    assignment = {
                        "employee": emp_id,
                        "shift_type": random.choice(self.shifts),
                        "start_date": start,
                        "end_date": end,
                        "company": COMPANY_NAME
                    }
                    
                    if self.locations:
                        assignment["shift_location"] = random.choice(self.locations)
                    
                    self.api.create("Shift Assignment", assignment)
                    self.created += 1
                    logger.info(f"[{emp_idx}/{len(self.employees)}] {emp_name}: {start} to {end}")
                    
                except Exception as e:
                    self.failed += 1
                    logger.error(f"Failed {emp_name}: {str(e)[:80]}")

    def run(self, num: int):
        if not self.fetch_data():
            logger.error("Cannot fetch required data")
            return
        
        self.create_assignments(num)
        logger.info(f"Summary: Created {self.created}, Failed {self.failed}")


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env")
        return

    while True:
        try:
            num = int(input("Shift assignments per employee (1-10): "))
            if 1 <= num <= 10:
                break
            logger.error("Enter number between 1-10")
        except ValueError:
            logger.error("Invalid input")

    confirm = input(f"Create {num} assignments per employee? (yes/no): ")
    if confirm.lower() != 'yes':
        logger.info("Cancelled")
        return

    try:
        gen = ShiftAssignmentGenerator()
        gen.run(num)
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

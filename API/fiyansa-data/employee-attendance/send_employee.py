#!/usr/bin/env python3
"""
ERPNext Employee Creator from API Data
Joining Date from tanggal_masuk_kerja, Random DOB
No delay between creations
"""

import requests
import json
import random
import logging
import os
from pathlib import Path
from datetime import datetime
from faker import Faker
from typing import Dict, List, Optional
import sys
from logging import StreamHandler


def load_env_file():
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key] = value
        print(f"Loaded environment variables from {env_path}")
    else:
        print(f".env file not found at {env_path}")


load_env_file()

fake = Faker("id_ID")

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")

EXTERNAL_API_BASE = "https://viva.fiyansa.com/api/user-get?limit={}"

RETRY_ATTEMPTS = 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

for handler in logger.handlers:
    if isinstance(handler, StreamHandler):
        handler.stream.reconfigure(encoding="utf-8", errors="replace")


class ERPNextAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"token {API_KEY}:{API_SECRET}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Expect": "",  # fix untuk 417 error
            }
        )
        self.base_url = BASE_URL

    def _make_request(
        self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0
    ) -> Dict:
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(
                method,
                url,
                json=data if method in ["POST", "PUT"] else None,
                params=data if method == "GET" else None,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            if retry_count < RETRY_ATTEMPTS:
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def get_list(
        self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None
    ) -> List[Dict]:
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def check_exists(self, doctype: str, email: str) -> bool:
        try:
            result = self.get_list(
                doctype,
                filters={"personal_email": email, "company": COMPANY_NAME},
                fields=["name"],
            )
            return len(result) > 0
        except Exception:
            return False


class ExternalAPIClient:
    def __init__(self, limit: int):
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )
        self.api_url = EXTERNAL_API_BASE.format(limit)

    def fetch_users(self) -> List[Dict]:
        try:
            response = self.session.get(self.api_url)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                if "data" in data:
                    return data["data"]
                elif "users" in data:
                    return data["users"]
                elif "results" in data:
                    return data["results"]
                else:
                    return [data] if data else []
            else:
                return []
        except Exception:
            return []


class EmployeeCreator:
    def __init__(self, limit: int):
        self.fake = Faker("id_ID")
        self.erpnext_api = ERPNextAPI()
        self.external_api = ExternalAPIClient(limit)

    def generate_random_dob(self) -> str:
        return fake.date_of_birth(minimum_age=20, maximum_age=40).strftime("%Y-%m-%d")

    def generate_phone_number(self) -> str:
        return self.fake.phone_number()

    def parse_joining_date(self, tanggal_masuk_kerja: str) -> str:
        if not tanggal_masuk_kerja:
            return datetime.now().strftime("%Y-%m-%d")
        try:
            parsed = datetime.fromisoformat(
                tanggal_masuk_kerja.replace("Z", "").replace("T", " ")
            )
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d")

    def create_employees_from_api(self):
        users = self.external_api.fetch_users()
        if not users:
            return

        existing_employees = self.erpnext_api.get_list(
            "Employee", filters={"company": COMPANY_NAME}, fields=["personal_email"]
        )
        existing_emails = {
            emp.get("personal_email")
            for emp in existing_employees
            if emp.get("personal_email")
        }

        employees_created_count = 0
        employees_skipped_count = 0
        employees_failed_count = 0

        for idx, user in enumerate(users, 1):
            try:
                name = (user.get("name") or "").strip()
                email = (user.get("email") or "").strip()
                tanggal_masuk_kerja = (
                    user.get("tanggal_masuk_kerja") or "").strip()
                date_of_joining = self.parse_joining_date(tanggal_masuk_kerja)

                if not name or not email:
                    employees_skipped_count += 1
                    print(f"[{idx}/{len(users)}] Skipped: Missing name/email")
                    continue

                if email in existing_emails or self.erpnext_api.check_exists("Employee", email):
                    employees_skipped_count += 1
                    print(
                        f"[{idx}/{len(users)}] Skipped: {name} (already exists)")
                    continue

                date_of_birth = self.generate_random_dob()
                mobile_no = self.generate_phone_number()

                employee_data = {
                    "employee_name": name,
                    "first_name": name.split()[0] if name.split() else name,
                    "last_name": " ".join(name.split()[1:]) if len(name.split()) > 1 else "",
                    "gender": random.choice(["Male", "Female"]),
                    "date_of_birth": date_of_birth,
                    "date_of_joining": date_of_joining,
                    "company": COMPANY_NAME,
                    "status": "Active",
                    "personal_email": email,
                    "cell_number": mobile_no,
                    "prefered_contact_email": "Personal Email",
                }

                self.erpnext_api.create_doc("Employee", employee_data)
                employees_created_count += 1
                print(f"[{idx}/{len(users)}] Created: {name}")

            except Exception as e:
                employees_failed_count += 1
                print(f"[{idx}/{len(users)}] Failed: {name} ({e})")

        print("Employees Created:", employees_created_count)
        print("Employees Skipped:", employees_skipped_count)
        print("Employees Failed:", employees_failed_count)
        print("Total Processed:", len(users))

    def run(self):
        self.create_employees_from_api()
        print("\nEMPLOYEE CREATION COMPLETED!")


def main():
    print("Starting ERPNext Employee Creation...")

    if not API_KEY or not API_SECRET:
        print("Error: API_KEY and API_SECRET must be set in .env file")
        return

    try:
        limit = int(input("Enter limit for API call (e.g. 50, 100, 200): "))
    except ValueError:
        limit = 100

    response = input("Proceed with employee creation? (yes/no): ")
    if response.lower() != "yes":
        print("Operation cancelled.")
        return

    creator = EmployeeCreator(limit)
    creator.run()


if __name__ == "__main__":
    main()

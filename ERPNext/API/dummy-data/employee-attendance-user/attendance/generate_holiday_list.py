#!/usr/bin/env python3
"""ERPNext Holiday List Generator - Minimalist Version"""

import requests
import json
import logging
import os
from pathlib import Path
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

HOLIDAY_LIST_NAME = "Indonesia Holiday List 2025"
FROM_DATE = "2025-01-01"
TO_DATE = "2025-12-31"
COUNTRY = "Indonesia"
WEEKLY_OFF = "Sunday"

HOLIDAYS_2025 = [
    ("2025-01-01", "New Year Day"),
    ("2025-02-01", "Isra and Mi'raj"),
    ("2025-03-09", "Nyepi"),
    ("2025-03-29", "Good Friday"),
    ("2025-03-31", "Easter"),
    ("2025-04-10", "Eid al-Fitr 1"),
    ("2025-04-11", "Eid al-Fitr 2"),
    ("2025-04-12", "Eid al-Fitr 3"),
    ("2025-04-14", "Pancasila Day"),
    ("2025-05-01", "Labour Day"),
    ("2025-05-14", "Vesak Day"),
    ("2025-06-01", "Joint Leave"),
    ("2025-06-16", "Eid al-Adha"),
    ("2025-07-07", "Islamic New Year"),
    ("2025-08-17", "Independence Day"),
    ("2025-09-16", "Prophet Muhammad Birthday"),
    ("2025-12-25", "Christmas"),
    ("2025-12-26", "Joint Leave"),
]

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

    def get_list(self, doctype: str) -> List[Dict]:
        return self.request("GET", f"resource/{doctype}", {"limit_page_length": 500}).get("data", [])

    def create(self, doctype: str, data: Dict) -> Dict:
        data["doctype"] = doctype
        return self.request("POST", f"resource/{doctype}", data)


class HolidayListGenerator:
    def __init__(self):
        self.api = API()
        self.created = 0

    def check_exists(self):
        try:
            lists = self.api.get_list("Holiday List")
            for hl in lists:
                if hl.get("name") == HOLIDAY_LIST_NAME:
                    logger.info(f"Holiday list already exists: {HOLIDAY_LIST_NAME}")
                    return True
            return False
        except:
            return False

    def create_holiday_list(self):
        if self.check_exists():
            return
        
        logger.info(f"Creating holiday list: {HOLIDAY_LIST_NAME}")
        
        holidays = []
        for date, desc in HOLIDAYS_2025:
            holidays.append({"holiday_date": date, "description": desc})
        
        data = {
            "name": HOLIDAY_LIST_NAME,
            "holiday_list_name": HOLIDAY_LIST_NAME,
            "from_date": FROM_DATE,
            "to_date": TO_DATE,
            "country": COUNTRY,
            "weekly_off": WEEKLY_OFF,
            "holidays": holidays
        }
        
        try:
            result = self.api.create("Holiday List", data)
            self.created = 1
            logger.info(f"Created: {HOLIDAY_LIST_NAME} with {len(holidays)} holidays")
            logger.info(f"Date range: {FROM_DATE} to {TO_DATE}")
            logger.info(f"Weekly off: {WEEKLY_OFF}")
        except Exception as e:
            logger.error(f"Failed to create holiday list: {str(e)[:80]}")

    def run(self):
        self.create_holiday_list()
        logger.info(f"Summary: Created {self.created}")


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env")
        return

    confirm = input(f"Create holiday list '{HOLIDAY_LIST_NAME}' with {len(HOLIDAYS_2025)} holidays? (yes/no): ")
    if confirm.lower() != 'yes':
        logger.info("Cancelled")
        return

    try:
        gen = HolidayListGenerator()
        gen.run()
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

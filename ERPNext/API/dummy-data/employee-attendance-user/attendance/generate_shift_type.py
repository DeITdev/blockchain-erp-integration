#!/usr/bin/env python3
"""ERPNext Shift Type Generator - Minimalist Version"""

import requests
import json
import random
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

SHIFTS = [
    {"name": "Morning Shift", "start": "07:00:00", "end": "15:00:00"},
    {"name": "Day Shift", "start": "08:00:00", "end": "17:00:00"},
    {"name": "Afternoon Shift", "start": "14:00:00", "end": "22:00:00"},
    {"name": "Night Shift", "start": "22:00:00", "end": "06:00:00"},
    {"name": "Extended Day Shift", "start": "08:00:00", "end": "18:00:00"}
]

COLORS = ["Blue", "Green", "Red", "Yellow", "Cyan", "Magenta", "Orange", "Purple", "Pink", "Lime"]

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


class ShiftTypeGenerator:
    def __init__(self):
        self.api = API()
        self.created = 0
        self.existing = []

    def get_existing(self):
        try:
            self.existing = [s.get("name") for s in self.api.get_list("Shift Type")]
            logger.info(f"Found {len(self.existing)} existing shift types")
        except:
            pass

    def create_shifts(self, num: int):
        logger.info(f"Creating {num} shift types...")
        
        to_create = []
        for shift in SHIFTS[:num]:
            if shift["name"] not in self.existing:
                to_create.append(shift)
        
        if not to_create:
            logger.info("All shift types already exist")
            return 0
        
        for shift in to_create:
            try:
                data = {
                    "name": shift["name"],
                    "start_time": shift["start"],
                    "end_time": shift["end"],
                    "color": random.choice(COLORS),
                    "enable_auto_attendance": 0
                }
                self.api.create("Shift Type", data)
                self.created += 1
                logger.info(f"Created: {shift['name']} ({shift['start']} - {shift['end']})")
            except Exception as e:
                logger.error(f"Failed {shift['name']}: {str(e)[:80]}")
        
        logger.info(f"Summary: Created {self.created}")
        return self.created

    def run(self, num: int):
        self.get_existing()
        self.create_shifts(num)


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env")
        return

    while True:
        try:
            num = int(input(f"Number of shift types to create (1-{len(SHIFTS)}): "))
            if 1 <= num <= len(SHIFTS):
                break
            logger.error(f"Enter number between 1-{len(SHIFTS)}")
        except ValueError:
            logger.error("Invalid input")

    confirm = input(f"Create {num} shift types? (yes/no): ")
    if confirm.lower() != 'yes':
        logger.info("Cancelled")
        return

    try:
        gen = ShiftTypeGenerator()
        gen.run(num)
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

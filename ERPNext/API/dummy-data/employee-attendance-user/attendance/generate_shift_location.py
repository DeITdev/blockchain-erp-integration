#!/usr/bin/env python3
"""ERPNext Shift Location Generator - Minimalist Version"""

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

LOCATIONS = [
    "Head Office - Jakarta",
    "Manufacturing Plant - Surabaya",
    "Warehouse - Bandung",
    "Branch Office - Medan",
    "Distribution Center - Semarang",
    "Regional Office - Makassar",
    "Service Center - Yogyakarta",
    "Production Facility - Bekasi",
    "Sales Office - Denpasar",
    "Customer Center - Palembang"
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


class ShiftLocationGenerator:
    def __init__(self):
        self.api = API()
        self.created = 0
        self.existing = []

    def get_existing(self):
        try:
            self.existing = [l.get("location_name") for l in self.api.get_list("Shift Location")]
            logger.info(f"Found {len(self.existing)} existing shift locations")
        except:
            pass

    def create_locations(self, num: int):
        logger.info(f"Creating {num} shift locations...")
        
        for i in range(min(num, len(LOCATIONS))):
            location = LOCATIONS[i]
            
            if location in self.existing:
                logger.info(f"Skipped (exists): {location}")
                continue
            
            try:
                self.api.create("Shift Location", {"location_name": location})
                self.created += 1
                logger.info(f"Created: {location}")
            except Exception as e:
                logger.error(f"Failed {location}: {str(e)[:80]}")
        
        logger.info(f"Summary: Created {self.created}")
        return self.created

    def run(self, num: int):
        self.get_existing()
        self.create_locations(num)


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env")
        return

    while True:
        try:
            num = int(input(f"Number of shift locations to create (1-{len(LOCATIONS)}): "))
            if 1 <= num <= len(LOCATIONS):
                break
            logger.error(f"Enter number between 1-{len(LOCATIONS)}")
        except ValueError:
            logger.error("Invalid input")

    confirm = input(f"Create {num} shift locations? (yes/no): ")
    if confirm.lower() != 'yes':
        logger.info("Cancelled")
        return

    try:
        gen = ShiftLocationGenerator()
        gen.run(num)
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

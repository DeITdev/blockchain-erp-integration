#!/usr/bin/env python3
"""ERPNext Attendance Deletion Script - Minimalist Version"""

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

    def delete(self, doctype: str, name: str):
        return self.request("DELETE", f"resource/{doctype}/{name}")


class AttendanceDeleter:
    def __init__(self):
        self.api = API()
        self.deleted = 0
        self.failed = 0

    def delete_draft_attendance(self):
        logger.info("Fetching draft attendance records...")
        try:
            attendance_list = self.api.get_list("Attendance", {"docstatus": 0})
            logger.info(f"Found {len(attendance_list)} draft records")
            
            if not attendance_list:
                logger.info("No draft records to delete")
                return
            
            for record in attendance_list:
                att_name = record.get("name")
                try:
                    self.api.delete("Attendance", att_name)
                    self.deleted += 1
                    logger.info(f"Deleted: {att_name}")
                except Exception as e:
                    self.failed += 1
                    logger.error(f"Failed {att_name}: {str(e)[:80]}")
            
            logger.info(f"Summary: Deleted {self.deleted}, Failed {self.failed}")
        except Exception as e:
            logger.error(f"Error: {e}")

    def run(self):
        self.delete_draft_attendance()


def main():
    if not API_KEY or not API_SECRET:
        logger.error("API_KEY and API_SECRET required in .env")
        return

    confirm = input("Delete all draft attendance records? (yes/no): ")
    if confirm.lower() != 'yes':
        logger.info("Cancelled")
        return

    try:
        deleter = AttendanceDeleter()
        deleter.run()
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Branch Generator
Creates 5 branches with random names.
"""

import requests
import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional
import sys


def load_env_file():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent.parent.parent / '.env'
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
COMPANY_NAME = os.getenv("COMPANY_NAME")

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class BranchGenerator:
    """Branch generator"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL
        self.branch_names = [
            "Jakarta Pusat Branch",
            "Surabaya Timur Branch",
            "Bandung Utara Branch",
            "Medan Kota Branch",
            "Denpasar Selatan Branch"
        ]
        self.created_count = 0
        self.failed_count = 0

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT", "DELETE"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if retry_count < 3:
                import time
                time.sleep(2)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def create_branches(self):
        """Create branches"""
        logger.info(f"Creating {len(self.branch_names)} branches...")

        for i, branch_name in enumerate(self.branch_names, 1):
            try:
                branch_data = {"branch": branch_name}
                result = self.create_doc("Branch", branch_data)
                self.created_count += 1
                logger.info(
                    f"Created {i}/{len(self.branch_names)}: {branch_name}")

            except Exception as e:
                self.failed_count += 1
                logger.error(f"Failed to create {branch_name}: {str(e)}")

    def run(self):
        """Main execution"""
        logger.info("Starting branch creation...")
        self.create_branches()
        logger.info(f"Created: {self.created_count}")
        logger.info(f"Failed: {self.failed_count}")


if __name__ == "__main__":
    try:
        logger.info("Confirming creation of 5 branches")
        response = input("Type 'CREATE' to confirm: ")

        if response != "CREATE":
            logger.info("Operation cancelled")
            sys.exit(0)

        generator = BranchGenerator()
        generator.run()
    except KeyboardInterrupt:
        logger.info("Operation interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

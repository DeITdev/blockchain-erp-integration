#!/usr/bin/env python3
"""
ERPNext Branch Generator
Creates 5 branches with random names.
Uses environment variables from .env file for configuration.
Author: ERPNext Branch Generator
Version: 1.0.0
"""

import requests
import json
import random
import logging
import time
import os
from pathlib import Path
from typing import Dict, Optional
import sys

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    # Look for .env file in the API directory (parent of current directory)
    env_path = Path(__file__).parent.parent.parent / '.env'

    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print(f"✅ Loaded environment variables from {env_path}")
    else:
        print(f"⚠️ .env file not found at {env_path}")
        print("Using hardcoded values as fallback")


# Load environment variables
load_env_file()

# Configuration from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")

# Branch Configuration
TARGET_BRANCHES = 5

# Simple logging - console only, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class BranchGenerator:
    """Simple Branch Generator with minimal API calls"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        # Predefined branch names with Indonesian locations
        self.branch_names = [
            "Jakarta Pusat Branch",
            "Surabaya Timur Branch",
            "Bandung Utara Branch",
            "Medan Kota Branch",
            "Denpasar Selatan Branch"
        ]

        logger.info(f"🔗 API: {self.base_url}")
        logger.info(f"🏢 Company: {COMPANY_NAME}")
        logger.info(f"🔑 Key: {API_KEY[:8] if API_KEY else 'None'}...")

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create document with simple error handling"""
        url = f"{self.base_url}/api/resource/{doctype}"
        data["doctype"] = doctype

        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API Error: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text[:500]}")
            raise

    def create_branches(self):
        """Create Branch records directly"""
        print("\n" + "="*60)
        print("🏢 Creating Branches")
        print("="*60)

        created_count = 0

        for i, branch_name in enumerate(self.branch_names):
            try:
                # Create branch data with only the branch field
                branch_data = {
                    "branch": branch_name
                }

                result = self.create_doc("Branch", branch_data)
                created_count += 1

                print(f"✅ {i+1}/5: {branch_name}")
                print(f"   🆔 Document ID: {result.get('name', 'Generated')}")
                print()

                # Small delay
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"❌ Failed to create '{branch_name}': {str(e)}")
                print(f"❌ {i+1}/5: {branch_name} - FAILED")
                print(f"   Error: {str(e)[:100]}...")
                print()
                continue

        # Summary
        print("="*60)
        print("📊 SUMMARY")
        print("="*60)
        print(f"✅ Branches Created: {created_count}/5")

        if created_count > 0:
            print("🏢 Created Branches:")
            for i, name in enumerate(self.branch_names[:created_count], 1):
                print(f"   {i}. {name}")

        return created_count


def main():
    """Main entry point"""
    print("🚀 ERPNext Branch Generator")
    print("="*60)

    # Check API credentials
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"🎯 Will create {TARGET_BRANCHES} Branches:")
    branch_names = [
        "Jakarta Pusat Branch",
        "Surabaya Timur Branch",
        "Bandung Utara Branch",
        "Medan Kota Branch",
        "Denpasar Selatan Branch"
    ]

    for i, name in enumerate(branch_names, 1):
        print(f"   {i}. {name}")

    response = input(f"\nProceed? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return

    try:
        generator = BranchGenerator()
        created = generator.create_branches()

        if created > 0:
            print(f"\n🎉 SUCCESS! Created {created} Branches")
        else:
            print(f"\n❌ No branches were created. Check API permissions.")

    except Exception as e:
        print(f"\n💥 Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Check API key permissions for Branch creation")
        print("2. Verify ERPNext is running and accessible")
        print("3. Make sure you have access to create Branch documents")


if __name__ == "__main__":
    main()

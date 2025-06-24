#!/usr/bin/env python3
"""
Simple ERPNext Employee Grade Generator
Creates 5 employee grades with minimal API calls to avoid permission issues.
Uses environment variables from .env file for configuration.
Author: Simple Employee Grade Generator
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

# Configuration


class Config:
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL")
    COMPANY_NAME = os.getenv("COMPANY_NAME")

    # Employee Grade Configuration
    TARGET_GRADES = 5
    DEFAULT_SALARY_STRUCTURE = "Fiyansa Structure Salary"

    # Base Pay Range (in IDR)
    MIN_BASE_PAY = 5_000_000    # 5 million IDR
    MAX_BASE_PAY = 25_000_000   # 25 million IDR


# Simple logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleEmployeeGradeGenerator:
    """Simple Employee Grade Generator with minimal API calls"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

        # Predefined grade names
        self.grade_names = [
            "Junior Associate",
            "Senior Associate",
            "Assistant Manager",
            "Manager",
            "Senior Manager"
        ]

        logger.info(f"🔗 API: {self.base_url}")
        logger.info(f"🏢 Company: {Config.COMPANY_NAME}")
        logger.info(f"🔑 Key: {Config.API_KEY[:8]}...")

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

    def generate_random_base_pay(self) -> int:
        """Generate random base pay"""
        return random.randint(Config.MIN_BASE_PAY, Config.MAX_BASE_PAY)

    def create_employee_grades(self):
        """Create Employee Grade records directly"""
        print("\n" + "="*60)
        print("📊 Creating Employee Grades")
        print("="*60)

        created_count = 0
        total_budget = 0

        for i, grade_name in enumerate(self.grade_names):
            try:
                base_pay = self.generate_random_base_pay()

                # Create minimal grade data
                grade_data = {
                    "name": grade_name,
                    "default_base_pay": base_pay
                }

                # Try to add salary structure - if it fails, continue without it
                try:
                    grade_data["default_salary_structure"] = Config.DEFAULT_SALARY_STRUCTURE
                    result = self.create_doc("Employee Grade", grade_data)
                    salary_structure_status = "✅ Linked"
                except Exception as e:
                    # If fails with salary structure, try without it
                    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                        logger.warning(
                            f"⚠️ Salary structure not found, creating without it...")
                        grade_data_no_salary = {
                            "name": grade_name,
                            "default_base_pay": base_pay
                        }
                        result = self.create_doc(
                            "Employee Grade", grade_data_no_salary)
                        salary_structure_status = "⚠️ Not linked"
                    else:
                        raise e

                created_count += 1
                total_budget += base_pay

                print(f"✅ {i+1}/5: {grade_name}")
                print(f"   💰 Base Pay: Rp {base_pay:,}")
                print(f"   📋 Salary Structure: {salary_structure_status}")
                print(f"   🆔 Document ID: {result.get('name', 'Generated')}")
                print()

                # Small delay
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"❌ Failed to create '{grade_name}': {str(e)}")
                print(f"❌ {i+1}/5: {grade_name} - FAILED")
                print(f"   Error: {str(e)[:100]}...")
                print()
                continue

        # Summary
        print("="*60)
        print("📊 SUMMARY")
        print("="*60)
        print(f"✅ Grades Created: {created_count}/5")
        if created_count > 0:
            print(f"💰 Total Budget: Rp {total_budget:,}")
            print(f"📊 Average Pay: Rp {total_budget // created_count:,}")
        print(f"📋 Target Salary Structure: {Config.DEFAULT_SALARY_STRUCTURE}")

        return created_count


def main():
    """Main entry point"""
    print("🚀 Simple Employee Grade Generator")
    print("="*60)

    # Check API credentials
    if not Config.API_KEY or not Config.API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"🎯 Will create {Config.TARGET_GRADES} Employee Grades:")
    for i, name in enumerate(["Junior Associate", "Senior Associate", "Assistant Manager", "Manager", "Senior Manager"], 1):
        print(f"   {i}. {name}")

    print(
        f"💰 Base Pay Range: Rp {Config.MIN_BASE_PAY:,} - Rp {Config.MAX_BASE_PAY:,}")
    print(f"📋 Salary Structure: {Config.DEFAULT_SALARY_STRUCTURE}")

    response = input(f"\nProceed? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return

    try:
        generator = SimpleEmployeeGradeGenerator()
        created = generator.create_employee_grades()

        if created > 0:
            print(f"\n🎉 SUCCESS! Created {created} Employee Grades")
        else:
            print(f"\n❌ No grades were created. Check API permissions.")

    except Exception as e:
        print(f"\n💥 Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Check API key permissions for Employee Grade creation")
        print("2. Verify ERPNext is running and accessible")
        print("3. Make sure 'Fiyansa Structure Salary' exists (optional)")


if __name__ == "__main__":
    main()

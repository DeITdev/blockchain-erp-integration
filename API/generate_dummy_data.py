#!/usr/bin/env python3
"""
ERPNext Simple Dummy Data Generator
Generates one random user for testing purposes.
Uses environment variables from .env file for configuration.
Author: ERPNext Simple Data Generator
Version: 1.0.0
"""

import requests
import json
import random
import logging
import os
from pathlib import Path
from faker import Faker

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    # Look for .env file in the API directory
    env_path = Path(__file__).parent / '.env'

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

# Initialize Faker for generating random data
fake = Faker('id_ID')  # Indonesian locale for realistic data

# Configuration


class Config:
    # API Configuration (from .env file)
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
    COMPANY_NAME = os.getenv("COMPANY_NAME", "PT Fiyansa Mulya")
    COMPANY_ABBR = os.getenv("COMPANY_ABBR", "PFM")


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ERPNextAPI:
    """Handles API interactions with ERPNext"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

        # Log the configuration being used
        logger.info(f"Using API configuration:")
        logger.info(f"  Base URL: {self.base_url}")
        logger.info(f"  Company: {Config.COMPANY_NAME}")
        logger.info(f"  API Key: {Config.API_KEY[:8]}...")

    def create_doc(self, doctype: str, data: dict) -> dict:
        """Create new document"""
        url = f"{self.base_url}/api/resource/{doctype}"
        data["doctype"] = doctype

        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response content: {e.response.text}")
            raise

    def check_exists(self, doctype: str, email: str) -> bool:
        """Check if user with email already exists"""
        try:
            url = f"{self.base_url}/api/resource/{doctype}"
            params = {
                "filters": json.dumps({"email": email}),
                "fields": json.dumps(["name"])
            }
            response = self.session.get(url, params=params)
            response.raise_for_status()

            result = response.json()
            return len(result.get("data", [])) > 0

        except Exception as e:
            logger.warning(f"Error checking if user exists: {e}")
            return False


class SimpleDataGenerator:
    """Generates simple random user data"""

    def __init__(self):
        self.api = ERPNextAPI()

    def generate_user_data(self):
        """Generate random user data"""
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Generate unique email
        base_email = f"{first_name.lower()}.{last_name.lower()}"
        email = f"{base_email}@{Config.COMPANY_ABBR.lower()}.com"

        # If email exists, add random number
        counter = 1
        original_email = email
        while self.api.check_exists("User", email):
            email = f"{base_email}{counter}@{Config.COMPANY_ABBR.lower()}.com"
            counter += 1
            if counter > 100:  # Safety check
                break

        return {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "full_name": f"{first_name} {last_name}"
        }

    def create_single_user(self):
        """Create one random user"""
        logger.info("🚀 Generating random user data...")

        # Generate random user data
        user_data_raw = self.generate_user_data()

        # Display generated data
        print(f"\n📋 Generated User Data:")
        print(f"   Name: {user_data_raw['full_name']}")
        print(f"   Email: {user_data_raw['email']}")
        print(f"   First Name: {user_data_raw['first_name']}")
        print(f"   Last Name: {user_data_raw['last_name']}")

        # Prepare user data for ERPNext
        user_data = {
            "email": user_data_raw["email"],
            "first_name": user_data_raw["first_name"],
            "last_name": user_data_raw["last_name"],
            "enabled": 1,
            "send_welcome_email": 0,  # Don't send welcome email for dummy data
            "language": "en",
            "time_zone": "Asia/Jakarta",
            "roles": [{"role": "Employee"}]  # Basic role
        }

        try:
            logger.info(f"Creating user: {user_data_raw['email']}")
            user = self.api.create_doc("User", user_data)

            logger.info(f"✅ Successfully created user!")
            print(f"\n🎉 SUCCESS!")
            print(f"   User ID: {user.get('name')}")
            print(f"   Email: {user.get('email')}")
            print(f"   Full Name: {user.get('full_name')}")

            return user

        except Exception as e:
            logger.error(f"❌ Failed to create user: {str(e)}")
            print(f"\n💥 FAILED to create user!")
            print(f"Error: {str(e)}")

            print(f"\n🔧 Troubleshooting:")
            print(f"1. Check if API key/secret have User creation permissions")
            print(f"2. Verify ERPNext is running at {Config.BASE_URL}")
            print(f"3. Make sure the 'Employee' role exists in your ERPNext")
            print(f"4. Check if email {user_data_raw['email']} already exists")

            return None

    def run(self):
        """Main execution method"""
        print("=" * 60)
        print("🎯 ERPNext Simple Dummy Data Generator")
        print("=" * 60)
        print(f"📡 API Endpoint: {Config.BASE_URL}")
        print(f"🏢 Company: {Config.COMPANY_NAME}")
        print(f"🔑 Using API Key: {Config.API_KEY[:8]}...")
        print("=" * 60)

        try:
            # Create one random user
            user = self.create_single_user()

            if user:
                print("\n" + "=" * 60)
                print("✅ SIMPLE DATA GENERATION COMPLETED!")
                print("=" * 60)
                print("📊 Summary:")
                print("   - Users created: 1")
                print(f"   - User email: {user.get('email')}")
                print(f"   - User ID: {user.get('name')}")
                print("=" * 60)
            else:
                print("\n" + "=" * 60)
                print("❌ DATA GENERATION FAILED!")
                print("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during data generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting Simple ERPNext Data Generation...")

    response = input(
        "\nThis will create ONE random user in your ERPNext. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = SimpleDataGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext User Generator
Generates 100 users with realistic dummy data for ERPNext v16.
Uses environment variables from .env file for configuration.
Author: ERPNext User Generator
Version: 1.0.0
"""

import requests
import json
import random
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

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

# Initialize Faker for generating random data
fake = Faker('id_ID')  # Indonesian locale for realistic data

# Configuration


class Config:
    # API Configuration (from .env file)
    API_KEY = os.getenv("API_KEY")
    API_SECRET = os.getenv("API_SECRET")
    BASE_URL = os.getenv("BASE_URL")
    COMPANY_NAME = os.getenv("COMPANY_NAME")
    COMPANY_ABBR = os.getenv("COMPANY_ABBR")

    # Data Volume - Set to 50 users to stay within ERPNext limits
    TARGET_USERS = 2
    DAILY_USER_LIMIT = 2  # Match target to avoid confusion

    # Retry settings - adjusted for user creation throttling
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 5  # seconds - longer delay for user creation


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set console handler encoding to handle unicode characters
for handler in logger.handlers:
    if isinstance(handler, StreamHandler):
        handler.stream.reconfigure(encoding='utf-8', errors='replace')


class ERPNextAPI:
    """Handles all API interactions with ERPNext"""

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
        logger.info(
            f"  API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < Config.RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{Config.RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {Config.RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
                if hasattr(e, 'response') and e.response:
                    logger.error(f"Response content: {e.response.text}")
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        """Get list of documents"""
        params = {
            "limit_page_length": 1000
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)

        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create new document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def check_exists(self, doctype: str, email: str) -> bool:
        """Check if user with email already exists"""
        try:
            result = self.get_list(
                doctype, filters={"email": email}, fields=["name"])
            return len(result) > 0
        except Exception as e:
            logger.warning(f"Error checking if user exists: {e}")
            return False


class UserGenerator:
    """Generates realistic user dummy data"""

    def __init__(self):
        self.fake = Faker('id_ID')  # Indonesian locale
        self.api = ERPNextAPI()
        self.users = []

        # Indonesian locations for realistic data
        self.indonesian_locations = [
            "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
            "Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
            "Bogor", "Yogyakarta", "Malang", "Denpasar", "Balikpapan"
        ]

        # Indonesian interests/hobbies
        self.interests_list = [
            "Traveling", "Photography", "Reading", "Cooking", "Music",
            "Sports", "Technology", "Gaming", "Art", "Writing",
            "Hiking", "Swimming", "Cycling", "Fitness", "Movies",
            "Dancing", "Singing", "Fishing", "Gardening", "Shopping"
        ]

    def generate_phone_number(self) -> str:
        """Generate valid Indonesian phone number"""
        return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"

    def generate_random_birth_date(self) -> str:
        """Generate random birth date (18-65 years old)"""
        start_date = datetime.now() - timedelta(days=65*365)  # 65 years ago
        end_date = datetime.now() - timedelta(days=18*365)    # 18 years ago

        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        birth_date = start_date + timedelta(days=random_days)

        return birth_date.strftime("%Y-%m-%d")

    def generate_interests(self) -> str:
        """Generate random interests"""
        num_interests = random.randint(2, 5)
        selected_interests = random.sample(self.interests_list, num_interests)
        return ", ".join(selected_interests)

    def generate_bio(self, first_name: str, interests: str) -> str:
        """Generate realistic bio"""
        templates = [
            f"Hi, I'm {first_name}. I'm passionate about {interests.split(',')[0].lower()}. Always eager to learn new things and connect with like-minded people.",
            f"Professional with interests in {interests.split(',')[0].lower()} and {interests.split(',')[1].lower() if ',' in interests else 'technology'}. Love exploring new opportunities and challenges.",
            f"{first_name} here! Enthusiastic about {interests.split(',')[0].lower()}. When I'm not working, you can find me enjoying various hobbies and spending time with family.",
            f"Experienced professional who enjoys {interests.split(',')[0].lower()}. Always looking forward to new experiences and meeting interesting people.",
            f"Hello! I'm {first_name}, someone who loves {interests.split(',')[0].lower()} and believes in continuous learning and growth."
        ]
        return random.choice(templates)

    def generate_username(self, first_name: str, last_name: str, counter: int = 0) -> str:
        """Generate unique username"""
        base_username = f"{first_name.lower()}.{last_name.lower()}"
        if counter > 0:
            return f"{base_username}{counter}"
        return base_username

    def check_existing_users(self):
        """Check existing users and determine how many to create"""
        logger.info("Checking existing users...")

        try:
            # Get existing users (exclude system users)
            existing_users = self.api.get_list("User",
                                               filters=[
                                                   ["name", "not in", [
                                                       "Administrator", "Guest"]],
                                                   ["enabled", "=", 1]
                                               ],
                                               fields=["name", "email", "first_name", "last_name"])

            current_user_count = len(existing_users)

            logger.info(f"Current users: {current_user_count}")
            logger.info(f"Target users: {Config.TARGET_USERS}")

            if current_user_count >= Config.TARGET_USERS:
                logger.info(
                    f"Already have {current_user_count} users (>= target {Config.TARGET_USERS}). Skipping new user creation.")
                return 0

            users_to_create = Config.TARGET_USERS - current_user_count
            logger.info(
                f"Need to create {users_to_create} users to reach target {Config.TARGET_USERS}")

            return users_to_create

        except Exception as e:
            logger.error(f"Error checking existing users: {str(e)}")
            return Config.TARGET_USERS

    def create_users(self):
        """Create user records with realistic data"""
        logger.info("Starting user creation...")

        # Check how many users we need to create
        users_to_create = self.check_existing_users()

        if users_to_create <= 0:
            logger.info("No new users need to be created.")
            return

        logger.info(f"Creating {users_to_create} new users...")
        logger.info(
            f"⚠️ Note: ERPNext has a daily limit of ~60 users. If creation stops, run again tomorrow.")

        # Check if we should limit today's creation
        if users_to_create > Config.DAILY_USER_LIMIT:
            logger.warning(
                f"🚦 Limiting today's creation to {Config.DAILY_USER_LIMIT} users to avoid hitting daily limit")
            logger.warning(
                f"   Run the script again tomorrow to create the remaining {users_to_create - Config.DAILY_USER_LIMIT} users")
            users_to_create = Config.DAILY_USER_LIMIT

        users_created_count = 0
        username_counter = {}  # Track username counters to ensure uniqueness

        for i in range(users_to_create):
            # Generate basic user data
            first_name = self.fake.first_name()
            middle_name = self.fake.first_name() if random.choice([
                True, False]) else ""
            last_name = self.fake.last_name()

            # Generate unique email
            email_base = f"{first_name.lower()}.{last_name.lower()}"
            email = f"{email_base}@{Config.COMPANY_ABBR.lower()}.com"

            # Handle email uniqueness
            email_counter = 1
            original_email = email
            while self.api.check_exists("User", email):
                email = f"{email_base}{email_counter}@{Config.COMPANY_ABBR.lower()}.com"
                email_counter += 1
                if email_counter > 100:  # Safety check
                    break

            # Generate unique username
            username_key = f"{first_name.lower()}.{last_name.lower()}"
            if username_key not in username_counter:
                username_counter[username_key] = 0
            else:
                username_counter[username_key] += 1

            username = self.generate_username(
                first_name, last_name, username_counter[username_key])

            # Generate other data
            gender = random.choice(["Male", "Female"])
            phone = self.generate_phone_number()
            mobile_no = self.generate_phone_number()
            birth_date = self.generate_random_birth_date()
            location = random.choice(self.indonesian_locations)
            interests = self.generate_interests()
            bio = self.generate_bio(first_name, interests)

            # Prepare user data for ERPNext
            user_data = {
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "username": username,
                "enabled": 1,
                "send_welcome_email": 0,  # Uncheck send welcome email
                "language": "en",  # English
                "time_zone": "Asia/Jakarta",
                "gender": gender,
                "phone": phone,
                "mobile_no": mobile_no,
                "birth_date": birth_date,
                "location": location,
                "interest": interests,
                "bio": bio,
                "roles": [{"role": "Employee"}]  # Basic role for all users
            }

            # Add middle name if generated
            if middle_name:
                user_data["middle_name"] = middle_name

            # Add all modules access (check all allow modules as requested)
            # Note: In ERPNext, modules are typically controlled by roles
            # We'll focus on essential modules that employees typically need
            # Empty array means no modules are blocked
            user_data["block_modules"] = []

            try:
                user = self.api.create_doc("User", user_data)

                self.users.append(user)
                users_created_count += 1

                logger.info(
                    f"✅ Created user {users_created_count}/{users_to_create}: {email}")
                logger.debug(
                    f"   - Name: {first_name} {middle_name} {last_name}".strip())
                logger.debug(f"   - Username: {username}")
                logger.debug(f"   - Gender: {gender}")
                logger.debug(f"   - Location: {location}")
                logger.debug(f"   - Birth Date: {birth_date}")
                logger.debug(f"   - Interests: {interests}")

                # No delay between user creations - removed to speed up process

            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 417:
                    # Handle ERPNext throttling - wait longer and retry
                    logger.warning(
                        f"⏳ User creation throttled for {email}. Waiting 10 seconds before retry...")
                    time.sleep(10)
                    try:
                        # Retry once after throttling
                        user = self.api.create_doc("User", user_data)
                        self.users.append(user)
                        users_created_count += 1
                        logger.info(
                            f"✅ Created user {users_created_count}/{users_to_create} (after retry): {email}")
                    except Exception as retry_error:
                        logger.error(
                            f"❌ Failed to create user {email} even after retry: {str(retry_error)}")
                        continue
                else:
                    logger.error(f"❌ Failed to create user {email}: {str(e)}")
                    continue
            except Exception as e:
                if "Throttled" in str(e):
                    # Handle ERPNext throttling - wait longer and retry
                    logger.warning(
                        f"⏳ User creation throttled for {email}. Waiting 10 seconds before retry...")
                    time.sleep(10)
                    try:
                        # Retry once after throttling
                        user = self.api.create_doc("User", user_data)
                        self.users.append(user)
                        users_created_count += 1
                        logger.info(
                            f"✅ Created user {users_created_count}/{users_to_create} (after retry): {email}")
                    except Exception as retry_error:
                        logger.error(
                            f"❌ Failed to create user {email} even after retry: {str(retry_error)}")
                        continue
                else:
                    logger.error(f"❌ Failed to create user {email}: {str(e)}")
                    # Continue with next user
                    continue

        logger.info(f"Successfully created {users_created_count} users")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("👥 ERPNext User Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {Config.BASE_URL}")
        print(f"🏢 Company: {Config.COMPANY_NAME}")
        print(
            f"🔑 Using API Key: {Config.API_KEY[:8] if Config.API_KEY else 'Not Set'}...")
        print(f"🎯 Target Users: {Config.TARGET_USERS}")
        print("=" * 80)

        try:
            # Create users
            self.create_users()

            print("\n" + "=" * 80)
            print("✅ USER GENERATION COMPLETED!")
            print("=" * 80)
            print(f"📊 Target: {Config.TARGET_USERS} users")
            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during user generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have User creation permissions")
            print(f"2. Verify ERPNext is running at {Config.BASE_URL}")
            print("3. Make sure the 'Employee' role exists in your ERPNext")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext User Generation...")

    # Check if API credentials are set
    if not Config.API_KEY or not Config.API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in API/.env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    response = input(
        f"\nThis will create up to {Config.TARGET_USERS} users in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = UserGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

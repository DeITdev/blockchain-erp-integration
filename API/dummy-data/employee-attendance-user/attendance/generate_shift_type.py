#!/usr/bin/env python3
"""
ERPNext Shift Type Generator
Creates realistic shift types with proper time ranges and holiday list integration.
Uses environment variables from .env file for configuration.
Author: ERPNext Shift Type Generator
Version: 1.0.0
"""

import requests
import json
import random
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional
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

# Shift Type Configuration
TARGET_SHIFT_TYPES = 5  # A reasonable amount of shift types

# Simple logging - console only, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class ShiftTypeGenerator:
    """Shift Type Generator with realistic shifts and holiday list integration"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        # Available holiday list (will be fetched dynamically)
        self.holiday_list = None

        # Predefined shift types with realistic times
        self.shift_templates = [
            {
                "name": "Morning Shift",
                "start_time": "07:00:00",
                "end_time": "15:00:00"
            },
            {
                "name": "Day Shift",
                "start_time": "08:00:00",
                "end_time": "17:00:00"
            },
            {
                "name": "Afternoon Shift",
                "start_time": "14:00:00",
                "end_time": "22:00:00"
            },
            {
                "name": "Night Shift",
                "start_time": "22:00:00",
                "end_time": "06:00:00"
            },
            {
                "name": "Extended Day Shift",
                "start_time": "08:00:00",
                "end_time": "18:00:00"
            }
        ]

        # Available roster colors (ERPNext valid color names)
        self.roster_colors = [
            "Blue",
            "Cyan",
            "Fuchsia",
            "Green",
            "Lime",
            "Orange",
            "Pink",
            "Red",
            "Violet",
            "Yellow"
        ]

        logger.info(f"🔗 API: {self.base_url}")
        logger.info(f"🏢 Company: {COMPANY_NAME}")
        logger.info(f"🔑 Key: {API_KEY[:8] if API_KEY else 'None'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < 3:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/3) - Error: {e}")
                time.sleep(2)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after 3 attempts for {url}: {str(e)}")
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
        """Create document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def fetch_holiday_list(self):
        """Fetch existing holiday lists and find the Indonesia Holiday List 2025"""
        logger.info("📅 Fetching existing holiday lists...")

        try:
            holiday_lists = self.get_list("Holiday List", fields=[
                                          "name", "holiday_list_name"])

            logger.info(f"Found {len(holiday_lists)} holiday lists")

            if holiday_lists:
                logger.info("Available holiday lists:")
                for hl in holiday_lists:
                    logger.info(
                        f"   - {hl.get('holiday_list_name', hl.get('name'))}")

                # Look for Indonesia Holiday List 2025 first
                for hl in holiday_lists:
                    hl_name = hl.get('holiday_list_name', hl.get('name', ''))
                    if "Indonesia" in hl_name and "2025" in hl_name:
                        self.holiday_list = hl.get('name')
                        logger.info(f"✅ Found target holiday list: {hl_name}")
                        return True

                # If not found, use the first available holiday list
                self.holiday_list = holiday_lists[0].get('name')
                logger.info(
                    f"✅ Using first available holiday list: {holiday_lists[0].get('holiday_list_name', self.holiday_list)}")
                return True
            else:
                logger.warning("⚠️ No holiday lists found")
                return False

        except Exception as e:
            logger.warning(f"⚠️ Could not fetch holiday lists: {e}")
            return False

    def check_existing_shift_types(self):
        """Check existing shift types"""
        logger.info("📊 Checking existing shift types...")

        try:
            existing_shifts = self.get_list("Shift Type", fields=["name"])
            existing_shift_names = [
                shift.get("name") for shift in existing_shifts if shift.get("name")]

            logger.info(
                f"Found {len(existing_shift_names)} existing shift types")
            if existing_shift_names:
                logger.info(f"Existing: {existing_shift_names}")

            return existing_shift_names

        except Exception as e:
            logger.error(f"Error checking existing shift types: {str(e)}")
            return []

    def create_shift_types(self):
        """Create Shift Type records"""
        print("\n" + "="*60)
        print("⏰ Creating Shift Types")
        print("="*60)

        # Check existing shift types
        existing_shift_names = self.check_existing_shift_types()

        # Filter out shift types that already exist
        shifts_to_create = []
        for shift_template in self.shift_templates:
            if shift_template["name"] not in existing_shift_names:
                shifts_to_create.append(shift_template)
            else:
                logger.info(
                    f"⏭️ Shift type '{shift_template['name']}' already exists, skipping...")

        if not shifts_to_create:
            print("✅ All shift types already exist!")
            return len(existing_shift_names)

        logger.info(f"Creating {len(shifts_to_create)} new shift types...")

        created_count = 0

        for i, shift_template in enumerate(shifts_to_create):
            try:
                # Prepare shift type data
                shift_data = {
                    "name": shift_template["name"],
                    "start_time": shift_template["start_time"],
                    "end_time": shift_template["end_time"],
                    "enable_auto_attendance": 0,  # Uncheck as requested
                    "color": random.choice(self.roster_colors)
                }

                # Add holiday list if available
                if self.holiday_list:
                    shift_data["holiday_list"] = self.holiday_list

                result = self.create_doc("Shift Type", shift_data)
                created_count += 1

                print(
                    f"✅ {created_count}/{len(shifts_to_create)}: {shift_template['name']}")
                print(
                    f"   ⏰ Time: {shift_template['start_time']} - {shift_template['end_time']}")
                print(f"   🎨 Color: {shift_data['color']}")
                print(
                    f"   📅 Holiday List: {self.holiday_list if self.holiday_list else 'None'}")
                print(f"   🤖 Auto Attendance: Disabled")
                print(f"   🆔 Document ID: {result.get('name', 'Generated')}")
                print()

                # Small delay
                time.sleep(0.3)

            except Exception as e:
                logger.error(
                    f"❌ Failed to create shift type '{shift_template['name']}': {str(e)}")
                print(
                    f"❌ {i+1}/{len(shifts_to_create)}: {shift_template['name']} - FAILED")
                print(f"   Error: {str(e)[:100]}...")
                print()

        return created_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("⏰ ERPNext Shift Type Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🎯 Target Shift Types: {TARGET_SHIFT_TYPES}")
        print("=" * 80)

        try:
            # Fetch holiday list
            holiday_found = self.fetch_holiday_list()
            if not holiday_found:
                logger.warning(
                    "⚠️ No holiday list found, shift types will be created without holiday list")

            # Create shift types
            created_count = self.create_shift_types()

            # Summary
            print("="*60)
            print("📊 SUMMARY")
            print("="*60)
            print(f"✅ Shift Types Created: {created_count}")

            if created_count > 0:
                print("⏰ Created Shift Types:")
                for i, shift in enumerate(self.shift_templates[:created_count], 1):
                    duration = self.calculate_duration(
                        shift["start_time"], shift["end_time"])
                    print(
                        f"   {i}. {shift['name']} ({shift['start_time']} - {shift['end_time']}) - {duration} hours")

            print(
                f"📅 Holiday List: {self.holiday_list if self.holiday_list else 'Not linked'}")
            print(f"🎨 Random roster colors assigned")
            print(f"🤖 Auto attendance disabled for all shifts")

            if created_count > 0:
                print("\n💡 These shift types can now be used for:")
                print("   - Employee shift assignments")
                print("   - Attendance tracking")
                print("   - Roster planning")
                print("   - Time and attendance management")

        except Exception as e:
            logger.error(f"Fatal error during shift type creation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")

    def calculate_duration(self, start_time: str, end_time: str) -> float:
        """Calculate shift duration in hours"""
        from datetime import datetime, timedelta

        try:
            start = datetime.strptime(start_time, "%H:%M:%S")
            end = datetime.strptime(end_time, "%H:%M:%S")

            # Handle overnight shifts
            if end < start:
                end += timedelta(days=1)

            duration = end - start
            return round(duration.total_seconds() / 3600, 1)
        except:
            return 8.0  # Default 8 hours


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Shift Type Generation...")

    # Check API credentials
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"\n⏰ This will create {TARGET_SHIFT_TYPES} shift types:")
    shift_templates = [
        {"name": "Morning Shift", "time": "07:00 - 15:00"},
        {"name": "Day Shift", "time": "08:00 - 17:00"},
        {"name": "Afternoon Shift", "time": "14:00 - 22:00"},
        {"name": "Night Shift", "time": "22:00 - 06:00"},
        {"name": "Extended Day Shift", "time": "08:00 - 18:00"}
    ]

    for i, shift in enumerate(shift_templates, 1):
        print(f"   {i}. {shift['name']} ({shift['time']})")

    print(f"\n📋 Features:")
    print(f"   - Linked to existing holiday list")
    print(f"   - Random roster colors")
    print(f"   - Auto attendance disabled")
    print(f"   - Realistic time ranges")

    response = input(f"\nProceed with shift type creation? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = ShiftTypeGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

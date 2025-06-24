#!/usr/bin/env python3
"""
ERPNext Holiday List Generator
Creates one holiday list with Indonesian holidays for 2025.
Uses environment variables from .env file for configuration.
Author: ERPNext Holiday List Generator
Version: 1.0.0
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
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

# Holiday List Configuration
HOLIDAY_LIST_NAME = "Indonesia Holiday List 2025"
FROM_DATE = "2025-01-01"
TO_DATE = "2025-12-31"
COLOR = "#CB2929"
COUNTRY = "Indonesia"
WEEKLY_OFF = "Sunday"

# Simple logging - console only, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class HolidayListGenerator:
    """Holiday List Generator for Indonesian holidays"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

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

    def get_indonesian_holidays_2025(self):
        """Get list of Indonesian national holidays for 2025"""
        holidays = [
            {
                "holiday_date": "2025-01-01",
                "description": "Tahun Baru Masehi (New Year's Day)"
            },
            {
                "holiday_date": "2025-01-29",
                "description": "Tahun Baru Imlek (Chinese New Year)"
            },
            {
                "holiday_date": "2025-02-08",
                "description": "Isra Miraj Nabi Muhammad SAW"
            },
            {
                "holiday_date": "2025-03-14",
                "description": "Hari Raya Nyepi (Balinese New Year)"
            },
            {
                "holiday_date": "2025-03-29",
                "description": "Wafat Isa Al Masih (Good Friday)"
            },
            {
                "holiday_date": "2025-03-31",
                "description": "Hari Raya Idul Fitri (Eid al-Fitr) - Day 1"
            },
            {
                "holiday_date": "2025-04-01",
                "description": "Hari Raya Idul Fitri (Eid al-Fitr) - Day 2"
            },
            {
                "holiday_date": "2025-05-01",
                "description": "Hari Buruh Internasional (International Labor Day)"
            },
            {
                "holiday_date": "2025-05-12",
                "description": "Hari Raya Waisak (Vesak Day)"
            },
            {
                "holiday_date": "2025-05-29",
                "description": "Kenaikan Isa Al Masih (Ascension of Jesus Christ)"
            },
            {
                "holiday_date": "2025-06-01",
                "description": "Hari Lahir Pancasila (Pancasila Day)"
            },
            {
                "holiday_date": "2025-06-07",
                "description": "Hari Raya Idul Adha (Eid al-Adha)"
            },
            {
                "holiday_date": "2025-06-28",
                "description": "Tahun Baru Islam 1447 H (Islamic New Year)"
            },
            {
                "holiday_date": "2025-08-17",
                "description": "Hari Kemerdekaan RI (Independence Day)"
            },
            {
                "holiday_date": "2025-09-05",
                "description": "Maulid Nabi Muhammad SAW (Prophet Muhammad's Birthday)"
            },
            {
                "holiday_date": "2025-12-25",
                "description": "Hari Raya Natal (Christmas Day)"
            }
        ]
        return holidays

    def create_holiday_list(self):
        """Create Holiday List with Indonesian holidays"""
        print("\n" + "="*60)
        print("🎄 Creating Holiday List")
        print("="*60)

        # Get Indonesian holidays
        holidays = self.get_indonesian_holidays_2025()

        # Prepare holiday list data
        holiday_list_data = {
            "holiday_list_name": HOLIDAY_LIST_NAME,
            "from_date": FROM_DATE,
            "to_date": TO_DATE,
            "color": COLOR,
            "country": COUNTRY,
            "weekly_off": WEEKLY_OFF,
            "holidays": holidays
        }

        try:
            result = self.create_doc("Holiday List", holiday_list_data)

            print(f"✅ Holiday List Created Successfully!")
            print(f"   📅 Name: {HOLIDAY_LIST_NAME}")
            print(f"   📅 Period: {FROM_DATE} to {TO_DATE}")
            print(f"   🎨 Color: {COLOR}")
            print(f"   🌍 Country: {COUNTRY}")
            print(f"   📅 Weekly Off: {WEEKLY_OFF}")
            print(f"   🎄 Holidays: {len(holidays)} national holidays")
            print(f"   🆔 Document ID: {result.get('name', 'Generated')}")
            print()

            # Show holiday details
            print("🎄 Indonesian National Holidays 2025:")
            print("-" * 50)
            for holiday in holidays:
                date_obj = datetime.strptime(
                    holiday["holiday_date"], "%Y-%m-%d")
                day_name = date_obj.strftime("%A")
                formatted_date = date_obj.strftime("%d %B %Y")
                print(f"   📅 {formatted_date} ({day_name})")
                print(f"      {holiday['description']}")
                print()

            return result

        except Exception as e:
            logger.error(f"❌ Failed to create holiday list: {str(e)}")
            print(f"❌ Failed to create holiday list")
            print(f"   Error: {str(e)[:100]}...")
            return None

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🎄 ERPNext Holiday List Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"📅 Holiday List: {HOLIDAY_LIST_NAME}")
        print(f"📅 Period: {FROM_DATE} to {TO_DATE}")
        print(f"🎨 Color: {COLOR}")
        print(f"🌍 Country: {COUNTRY}")
        print(f"📅 Weekly Off: {WEEKLY_OFF}")
        print("=" * 80)

        try:
            # Create holiday list
            result = self.create_holiday_list()

            if result:
                print("="*60)
                print("📊 SUMMARY")
                print("="*60)
                print("✅ Holiday List created successfully!")
                print("🎄 Features included:")
                print("   - 16 Indonesian national holidays for 2025")
                print("   - Sunday weekly offs")
                print("   - Red color theme (#CB2929)")
                print("   - Indonesia country setting")
                print("   - Full year coverage (Jan 1 - Dec 31, 2025)")
                print("="*60)
                print("💡 This holiday list can now be used for:")
                print("   - Employee attendance tracking")
                print("   - Leave management")
                print("   - Payroll calculations")
                print("   - Project planning")
            else:
                print("❌ Holiday list creation failed. Check API permissions.")

        except Exception as e:
            logger.error(f"Fatal error during holiday list creation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Holiday List Generation...")

    # Check API credentials
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"\n🎄 This will create the '{HOLIDAY_LIST_NAME}' with:")
    print(f"   📅 Period: {FROM_DATE} to {TO_DATE}")
    print(f"   🎄 16 Indonesian national holidays")
    print(f"   📅 Sunday weekly offs")
    print(f"   🎨 Color: {COLOR}")
    print(f"   🌍 Country: {COUNTRY}")

    response = input(f"\nProceed with holiday list creation? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = HolidayListGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Shift Location Generator
Creates 10 shift location records with realistic dummy data.
Uses environment variables from .env file for configuration.
Author: ERPNext Shift Location Generator
Version: 1.0.0
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional
import sys

# Load environment variables from .env file


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
        print(f"✅ Loaded environment variables from {env_path}")
    else:
        print(f"⚠️ .env file not found at {env_path}")


# Load environment variables
load_env_file()

# Configuration from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")

# Shift Location Configuration
TARGET_SHIFT_LOCATIONS = 10

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class ERPNextAPI:
    """Handles all API interactions with ERPNext"""

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

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
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

    def check_exists(self, doctype: str, name: str) -> bool:
        """Check if document exists"""
        try:
            result = self.get_list(
                doctype, filters={"location_name": name}, fields=["name"])
            return len(result) > 0
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                logger.error(
                    f"Error checking existence of {doctype} {name}: {e}")
                return False
        except Exception as e:
            logger.error(
                f"Unexpected error checking existence of {doctype} {name}: {e}")
            raise


class ShiftLocationGenerator:
    """Generates shift location records with realistic dummy data"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.shift_locations = []

        # Realistic shift location names for Indonesian company
        self.location_names = [
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

    def check_existing_shift_locations(self):
        """Check existing shift locations and determine how many to create"""
        logger.info("📊 Checking existing shift locations...")

        try:
            existing_locations = self.api.get_list("Shift Location",
                                                   fields=["name", "location_name"])

            current_count = len(existing_locations)
            self.shift_locations = existing_locations

            logger.info(f"Current shift locations: {current_count}")
            logger.info(f"Target shift locations: {TARGET_SHIFT_LOCATIONS}")

            if current_count >= TARGET_SHIFT_LOCATIONS:
                logger.info(
                    f"Already have {current_count} shift locations (>= target {TARGET_SHIFT_LOCATIONS}). Skipping new creation.")
                return 0

            locations_to_create = TARGET_SHIFT_LOCATIONS - current_count
            logger.info(
                f"Need to create {locations_to_create} shift locations to reach target {TARGET_SHIFT_LOCATIONS}")

            return locations_to_create

        except Exception as e:
            logger.error(f"Error checking existing shift locations: {str(e)}")
            return TARGET_SHIFT_LOCATIONS

    def create_shift_locations(self):
        """Create shift location records"""
        logger.info("🚀 Starting shift location creation...")

        # Check how many locations we need to create
        locations_to_create = self.check_existing_shift_locations()

        if locations_to_create <= 0:
            logger.info("No new shift locations need to be created.")
            return

        print("\n" + "="*60)
        print("📍 Creating Shift Locations")
        print("="*60)
        print(f"🎯 Target: {TARGET_SHIFT_LOCATIONS} shift locations")
        print(f"📊 Creating: {locations_to_create} new locations")
        print("="*60)

        locations_created_count = 0

        # Get existing location names to avoid duplicates
        existing_names = {loc.get("location_name")
                          for loc in self.shift_locations}

        for i in range(locations_to_create):
            # Find a location name that doesn't already exist
            location_name = None
            for name in self.location_names:
                if name not in existing_names:
                    location_name = name
                    existing_names.add(name)  # Mark as used
                    break

            # If we've used all predefined names, generate additional ones
            if not location_name:
                location_name = f"Branch Office - Location {i+1:02d}"

            # Check if location already exists (double-check)
            if self.api.check_exists("Shift Location", location_name):
                logger.debug(
                    f"Shift Location '{location_name}' already exists, skipping...")
                continue

            # Prepare shift location data
            shift_location_data = {
                "location_name": location_name
            }

            try:
                shift_location = self.api.create_doc(
                    "Shift Location", shift_location_data)
                locations_created_count += 1

                print(
                    f"✅ {locations_created_count}/{locations_to_create}: {location_name}")
                logger.info(
                    f"Created shift location: '{location_name}' (ID: {shift_location.get('name')})")

                # Small delay to avoid overwhelming the server
                time.sleep(0.2)

            except Exception as e:
                logger.error(
                    f"❌ Failed to create shift location '{location_name}': {str(e)}")
                print(
                    f"❌ {locations_created_count+1}/{locations_to_create}: {location_name} - FAILED")
                print(f"   Error: {str(e)[:100]}...")

        # Summary
        print("\n" + "="*60)
        print("📊 SUMMARY")
        print("="*60)
        print(
            f"✅ Shift Locations Created: {locations_created_count}/{locations_to_create}")
        print(
            f"📍 Total Shift Locations: {len(self.shift_locations) + locations_created_count}")
        print("="*60)

        logger.info(
            f"Successfully created {locations_created_count} shift locations")

    def display_all_shift_locations(self):
        """Display all existing shift locations"""
        try:
            all_locations = self.api.get_list(
                "Shift Location", fields=["name", "location_name"])

            if all_locations:
                print(f"\n📍 All Shift Locations ({len(all_locations)}):")
                print("-" * 50)
                for i, location in enumerate(all_locations, 1):
                    location_name = location.get("location_name", "Unknown")
                    doc_id = location.get("name", "Unknown")
                    print(f"{i:2d}. {location_name} (ID: {doc_id})")
                print("-" * 50)
            else:
                print("\n📍 No shift locations found.")

        except Exception as e:
            logger.error(f"Error displaying shift locations: {str(e)}")

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("📍 ERPNext Shift Location Generator")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🎯 Target Shift Locations: {TARGET_SHIFT_LOCATIONS}")
        print(f"📋 Field: location_name (single field)")
        print("=" * 80)

        try:
            # Create shift locations
            self.create_shift_locations()

            # Display all shift locations
            self.display_all_shift_locations()

            print(f"\n🎉 SHIFT LOCATION GENERATION COMPLETED!")

        except Exception as e:
            logger.error(
                f"Fatal error during shift location generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Shift Location Generation...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        return

    print(
        f"\n📍 This script will create {TARGET_SHIFT_LOCATIONS} shift location records")
    print(f"🏢 Company: {COMPANY_NAME}")
    print(f"📋 Each shift location has only one field: location_name")

    response = input(f"\nContinue with shift location creation? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = ShiftLocationGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

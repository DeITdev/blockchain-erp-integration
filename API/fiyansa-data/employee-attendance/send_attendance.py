#!/usr/bin/env python3
"""
ERPNext Attendance Sync from External API
Fetches attendance data from external API and creates/updates attendance records in ERPNext.
Uses environment variables from .env file for configuration.
Author: ERPNext Attendance Sync
Version: 1.0.0
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys
from logging import StreamHandler

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    # File is at: API/fiyansa-data/employee-attendance/send_attendance.py
    # .env is at: API/.env
    # Need to go up 2 directories from the file's directory (3 from the file itself)
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
COMPANY_ABBR = os.getenv("COMPANY_ABBR")

# External API Configuration
EXTERNAL_ATTENDANCE_API = "https://pre.fiyansa.com/api/attendance-get"
DEFAULT_LIMIT = 100  # Default number of records to fetch

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

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
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        logger.info(f"🔗 ERPNext API: {self.base_url}")
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

    def get_employee_by_name(self, employee_name: str) -> Optional[str]:
        """Get employee ID by name"""
        try:
            result = self.get_list("Employee",
                                   filters={"employee_name": employee_name,
                                            "company": COMPANY_NAME},
                                   fields=["name", "employee_name"])
            if result and len(result) > 0:
                return result[0]['name']
        except Exception as e:
            logger.warning(f"Error fetching employee {employee_name}: {e}")
        return None

    def check_attendance_exists(self, employee_id: str, attendance_date: str) -> bool:
        """Check if attendance already exists for employee on given date"""
        try:
            result = self.get_list("Attendance",
                                   filters={"employee": employee_id,
                                            "attendance_date": attendance_date},
                                   fields=["name"])
            return len(result) > 0
        except Exception as e:
            logger.warning(f"Error checking attendance existence: {e}")
        return False


class ExternalAPIClient:
    """Handles external API calls to fetch attendance data"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

    def fetch_attendance(self, limit: int = DEFAULT_LIMIT) -> List[Dict]:
        """Fetch attendance data from external API"""
        url = f"{EXTERNAL_ATTENDANCE_API}?limit={limit}"
        logger.info(f"🌐 Fetching attendance from: {url}")

        try:
            response = self.session.get(url)
            response.raise_for_status()

            data = response.json()

            # Handle different response structures
            if isinstance(data, dict) and 'data' in data:
                attendance_records = data['data']
            elif isinstance(data, list):
                attendance_records = data
            else:
                logger.error("❌ Unexpected API response format")
                return []

            logger.info(
                f"✅ Found {len(attendance_records)} attendance records")

            # Show sample data
            if attendance_records:
                sample = attendance_records[0]
                logger.info(f"📋 Sample record:")
                logger.info(f"   - User: {sample.get('user', 'N/A')}")
                logger.info(f"   - Date: {sample.get('date', 'N/A')}")
                logger.info(
                    f"   - Check In: {sample.get('checkin_time', 'N/A')}")
                logger.info(
                    f"   - Check Out: {sample.get('checkout_time', 'N/A')}")

            return attendance_records

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching from external API: {str(e)}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parsing JSON response: {str(e)}")
            return []


class AttendanceSyncManager:
    """Manages syncing attendance from external API to ERPNext"""

    def __init__(self):
        self.erpnext_api = ERPNextAPI()
        self.external_api = ExternalAPIClient()
        self.synced_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.no_employee_count = 0
        self.employee_cache = {}  # Cache for employee lookups
        self.missing_employees = []  # Track employees that don't exist

    def get_employee(self, employee_name: str) -> Optional[str]:
        """Get existing employee ID (NO creation)"""
        # Check cache first
        if employee_name in self.employee_cache:
            return self.employee_cache[employee_name]

        # Check if employee exists
        employee_id = self.erpnext_api.get_employee_by_name(employee_name)

        if employee_id:
            logger.info(
                f"✅ Found employee: {employee_name} (ID: {employee_id})")
            self.employee_cache[employee_name] = employee_id
            return employee_id

        # Employee doesn't exist - skip
        logger.warning(
            f"⚠️ Employee not found: {employee_name} - Skipping attendance record")
        if employee_name not in self.missing_employees:
            self.missing_employees.append(employee_name)
        self.no_employee_count += 1
        return None

    def sync_attendance_record(self, record: Dict) -> bool:
        """Sync single attendance record to ERPNext"""
        try:
            # Extract data from record
            user_name = record.get('user', '')
            attendance_date = record.get('date', '')
            checkin_time = record.get('checkin_time', '')
            checkout_time = record.get('checkout_time', 'Unknown')

            # Clean up the user name - remove UAT_ prefix if present
            if user_name.startswith('UAT_'):
                user_name = user_name[4:]  # Remove first 4 characters (UAT_)
                logger.info(
                    f"🔄 Cleaned employee name: UAT_{user_name} → {user_name}")

            # Validate required fields
            if not user_name or not attendance_date:
                logger.warning(
                    f"⚠️ Skipping record with missing data: {record}")
                self.skipped_count += 1
                return False

            # Get employee (NO creation - just check if exists)
            employee_id = self.get_employee(user_name)
            if not employee_id:
                # Employee doesn't exist - skip this attendance record
                return False

            # Check if attendance already exists
            if self.erpnext_api.check_attendance_exists(employee_id, attendance_date):
                logger.info(
                    f"⏭️ Attendance already exists for {user_name} on {attendance_date}")
                self.skipped_count += 1
                return True  # Consider it successful since data is already there

            # Prepare attendance data
            attendance_data = {
                "employee": employee_id,
                "employee_name": user_name,
                "attendance_date": attendance_date,
                "status": "Present",  # Always set to Present as requested
                "company": COMPANY_NAME,
                "docstatus": 1  # Submit the document
            }

            # Add check-in time if available
            if checkin_time and checkin_time != "Unknown":
                try:
                    # Convert to datetime format
                    in_time = f"{attendance_date} {checkin_time}"
                    attendance_data["in_time"] = in_time
                except Exception as e:
                    logger.warning(
                        f"Invalid check-in time format: {checkin_time}")

            # Add check-out time if available
            if checkout_time and checkout_time != "Unknown":
                try:
                    out_time = f"{attendance_date} {checkout_time}"
                    attendance_data["out_time"] = out_time
                except Exception as e:
                    logger.debug(
                        f"No valid check-out time for {user_name} on {attendance_date}")

            # Create attendance record
            result = self.erpnext_api.create_doc("Attendance", attendance_data)

            self.synced_count += 1
            logger.info(
                f"✅ Created attendance for {user_name} on {attendance_date}")

            return True

        except Exception as e:
            logger.error(f"❌ Error syncing attendance record: {str(e)}")
            self.failed_count += 1
            return False

    def sync_all_attendance(self, limit: int = DEFAULT_LIMIT):
        """Main sync function to fetch and create attendance records"""
        print("=" * 80)
        print("📝 ERPNext Attendance Sync from External API")
        print("=" * 80)
        print(f"📡 ERPNext API: {BASE_URL}")
        print(f"🌐 External API: {EXTERNAL_ATTENDANCE_API}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"📊 Limit: {limit} records")
        print("⚠️  NOTE: Employees must exist first! Run send_employee.py before this.")
        print("=" * 80)

        # Fetch attendance data from external API
        attendance_records = self.external_api.fetch_attendance(limit)

        if not attendance_records:
            logger.warning("⚠️ No attendance records found to sync")
            return

        print(f"\n🚀 Starting sync of {len(attendance_records)} records...")
        print("=" * 80)

        # Process each record
        for i, record in enumerate(attendance_records, 1):
            logger.info(f"\n📌 Processing record {i}/{len(attendance_records)}")

            # Show progress every 10 records
            if i % 10 == 0:
                print(
                    f"Progress: {i}/{len(attendance_records)} records processed...")

            self.sync_attendance_record(record)

            # Small delay to avoid overwhelming the server
            time.sleep(0.3)

        # Summary
        print("\n" + "=" * 80)
        print("📊 SYNC SUMMARY")
        print("=" * 80)
        print(f"✅ Successfully Synced: {self.synced_count}")
        print(f"⏭️ Skipped (existing): {self.skipped_count}")
        print(f"❌ Failed: {self.failed_count}")
        print(f"⚠️ No Employee Found: {self.no_employee_count}")
        print(f"📊 Total Processed: {len(attendance_records)}")
        print("=" * 80)

        # Show missing employees if any
        if self.missing_employees:
            print("\n⚠️ MISSING EMPLOYEES (run send_employee.py first):")
            print("=" * 80)
            for emp_name in self.missing_employees[:10]:  # Show first 10
                print(f"   - {emp_name}")
            if len(self.missing_employees) > 10:
                print(f"   ... and {len(self.missing_employees) - 10} more")
            print("=" * 80)
            print("💡 TIP: Run send_employee.py first to create these employees")

        logger.info(
            f"Sync completed: {self.synced_count} synced, {self.skipped_count} skipped, {self.failed_count} failed, {self.no_employee_count} missing employees")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Attendance Sync from External API...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    print(
        f"\n🌐 This script will fetch attendance from: {EXTERNAL_ATTENDANCE_API}")
    print(f"📊 Status will always be set to 'Present' as requested")
    print(f"⚠️  IMPORTANT: Employees must exist in ERPNext first!")
    print(f"              Run send_employee.py before running this script")
    print(f"              Attendance records for non-existent employees will be skipped")

    # Ask for limit
    try:
        limit_input = input(
            f"\nHow many records to sync? (default: {DEFAULT_LIMIT}): ").strip()
        limit = int(limit_input) if limit_input else DEFAULT_LIMIT
    except ValueError:
        limit = DEFAULT_LIMIT
        print(f"Using default limit: {limit}")

    response = input(
        f"\nProceed with syncing {limit} attendance records? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        sync_manager = AttendanceSyncManager()
        sync_manager.sync_all_attendance(limit)
    except Exception as e:
        print(f"\n💥 Error: {e}")
        logger.error(f"Fatal error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Attendance Sync from External API (Clean Output, Skip if attendance_date < joining_date)
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional
import sys
from logging import StreamHandler
from datetime import datetime

# Load environment variables


def load_env_file():
    start_dir = Path(__file__).resolve().parent
    for parent in [start_dir] + list(start_dir.parents):
        env_file = parent / '.env'
        if env_file.is_file():
            try:
                with env_file.open('r', encoding='utf-8-sig') as f:
                    for raw in f:
                        line = raw.strip()
                        if not line or line.startswith('#') or '=' not in line:
                            continue
                        k, v = line.split('=', 1)
                        k = k.strip()
                        v = v.strip().strip('"').strip("'")
                        if k and v and os.getenv(k) is None:
                            os.environ[k] = v
            except Exception:
                pass
            break


load_env_file()

# Config
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")

EXTERNAL_ATTENDANCE_API = "https://viva.fiyansa.com/api/attendance-get"
DEFAULT_LIMIT = 100

RETRY_ATTEMPTS = 3
RETRY_DELAY = 2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

for handler in logger.handlers:
    if isinstance(handler, StreamHandler):
        handler.stream.reconfigure(encoding='utf-8', errors='replace')


class ERPNextAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Expect': ''  # Fix 417 error
        })
        self.base_url = BASE_URL

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(
                method,
                url,
                json=data if method in ["POST", "PUT"] else None,
                params=data if method == "GET" else None
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            if retry_count < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise

    def get_list(self, doctype: str, filters: Optional[Dict] = None, fields: Optional[List[str]] = None) -> List[Dict]:
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def get_employee_by_name(self, employee_name: str) -> Optional[Dict]:
        try:
            result = self.get_list("Employee",
                                   filters={"employee_name": employee_name,
                                            "company": COMPANY_NAME},
                                   fields=["name", "employee_name", "date_of_joining"])
            if result and len(result) > 0:
                return result[0]
        except Exception:
            pass
        return None

    def check_attendance_exists(self, employee_id: str, attendance_date: str) -> bool:
        try:
            result = self.get_list("Attendance",
                                   filters={"employee": employee_id,
                                            "attendance_date": attendance_date},
                                   fields=["name"])
            return len(result) > 0
        except Exception:
            return False


class ExternalAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {'Accept': 'application/json', 'Content-Type': 'application/json'})

    def fetch_attendance(self, limit: int = DEFAULT_LIMIT) -> List[Dict]:
        url = f"{EXTERNAL_ATTENDANCE_API}?limit={limit}"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and 'data' in data:
                return data['data']
            elif isinstance(data, list):
                return data
            else:
                return []
        except Exception:
            return []


class AttendanceSyncManager:
    def __init__(self):
        self.erpnext_api = ERPNextAPI()
        self.external_api = ExternalAPIClient()
        self.synced_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.no_employee_count = 0
        self.employee_cache = {}
        self.missing_employees = []
        self.start_time = None
        self.processed_count = 0

    def get_employee(self, employee_name: str) -> Optional[Dict]:
        if employee_name in self.employee_cache:
            return self.employee_cache[employee_name]
        employee = self.erpnext_api.get_employee_by_name(employee_name)
        if employee:
            self.employee_cache[employee_name] = employee
            return employee
        self.no_employee_count += 1
        if employee_name not in self.missing_employees:
            self.missing_employees.append(employee_name)
        return None

    def calculate_eta(self, current_idx: int, total: int) -> str:
        """Calculate estimated time of arrival for completion"""
        if current_idx == 0 or not self.start_time:
            return "calculating..."

        elapsed = time.time() - self.start_time
        rate = current_idx / elapsed
        remaining = total - current_idx
        eta_seconds = remaining / rate if rate > 0 else 0

        if eta_seconds < 60:
            return f"{eta_seconds:.0f}s"
        elif eta_seconds < 3600:
            return f"{eta_seconds/60:.1f}m"
        else:
            return f"{eta_seconds/3600:.1f}h"

    def get_performance_stats(self) -> Dict[str, float]:
        """Get current performance statistics"""
        if not self.start_time or self.processed_count == 0:
            return {"elapsed": 0, "rate": 0}

        elapsed = time.time() - self.start_time
        rate = self.processed_count / elapsed if elapsed > 0 else 0

        return {
            "elapsed": elapsed,
            "rate": rate
        }

    def sync_attendance_record(self, record: Dict, idx: int, total: int) -> bool:
        loop_start = time.time()
        try:
            user_name = record.get('user', '')
            attendance_date = record.get('date', '')
            checkin_time = record.get('checkin_time', '')

            if not user_name or not attendance_date:
                self.skipped_count += 1
                self.processed_count = idx
                loop_time = time.time() - loop_start
                stats = self.get_performance_stats()
                eta = self.calculate_eta(idx, total)
                print(
                    f"[{idx}/{total}] Skipped: Missing user/date | Rate: {stats['rate']:.1f}/s | Loop: {loop_time:.3f}s | ETA: {eta}")
                return False

            employee = self.get_employee(user_name)
            if not employee:
                self.processed_count = idx
                loop_time = time.time() - loop_start
                stats = self.get_performance_stats()
                eta = self.calculate_eta(idx, total)
                print(
                    f"[{idx}/{total}] Skipped: {user_name} (employee not found) | Rate: {stats['rate']:.1f}/s | Loop: {loop_time:.3f}s | ETA: {eta}")
                return False

            employee_id = employee['name']
            joining_date = employee.get('date_of_joining')

            # Compare attendance_date vs joining_date
            if joining_date:
                try:
                    att_date = datetime.strptime(
                        attendance_date, "%Y-%m-%d").date()
                    join_date = datetime.strptime(
                        joining_date, "%Y-%m-%d").date()
                    if att_date < join_date:
                        self.skipped_count += 1
                        self.processed_count = idx
                        loop_time = time.time() - loop_start
                        stats = self.get_performance_stats()
                        eta = self.calculate_eta(idx, total)
                        print(
                            f"[{idx}/{total}] Skipped: {user_name} ({attendance_date} < joining {joining_date}) | Rate: {stats['rate']:.1f}/s | Loop: {loop_time:.3f}s | ETA: {eta}")
                        return False
                except Exception:
                    pass

            if self.erpnext_api.check_attendance_exists(employee_id, attendance_date):
                self.skipped_count += 1
                self.processed_count = idx
                loop_time = time.time() - loop_start
                stats = self.get_performance_stats()
                eta = self.calculate_eta(idx, total)
                print(f"[{idx}/{total}] Skipped: {user_name} ({attendance_date} already exists) | Rate: {stats['rate']:.1f}/s | Loop: {loop_time:.3f}s | ETA: {eta}")
                return True

            attendance_data = {
                "employee": employee_id,
                "employee_name": user_name,
                "attendance_date": attendance_date,
                "status": "Present",
                "company": COMPANY_NAME,
                "docstatus": 1
            }

            if checkin_time:
                attendance_data["in_time"] = f"{attendance_date} {checkin_time}"

            self.erpnext_api.create_doc("Attendance", attendance_data)
            self.synced_count += 1
            self.processed_count = idx
            loop_time = time.time() - loop_start
            stats = self.get_performance_stats()
            eta = self.calculate_eta(idx, total)
            print(f"[{idx}/{total}] Synced: {user_name} {attendance_date} | Rate: {stats['rate']:.1f}/s | Loop: {loop_time:.3f}s | ETA: {eta}")
            return True
        except Exception as e:
            self.failed_count += 1
            self.processed_count = idx
            loop_time = time.time() - loop_start
            stats = self.get_performance_stats()
            eta = self.calculate_eta(idx, total)
            print(f"[{idx}/{total}] Failed: {record.get('user', '')} {record.get('date', '')} ({e}) | Rate: {stats['rate']:.1f}/s | Loop: {loop_time:.3f}s | ETA: {eta}")
            return False

    def sync_all_attendance(self, limit: int = DEFAULT_LIMIT):
        attendance_records = self.external_api.fetch_attendance(limit)
        if not attendance_records:
            return

        # Initialize timing
        self.start_time = time.time()
        total = len(attendance_records)
        print(f"Processing {total} attendance records...")

        for idx, record in enumerate(attendance_records, 1):
            self.sync_attendance_record(record, idx, total)
            time.sleep(0.05)

        # Final performance summary
        if self.start_time:
            total_time = time.time() - self.start_time
            final_rate = total / total_time if total_time > 0 else 0

            print(f"\n=== PERFORMANCE SUMMARY ===")
            print(
                f"Total Time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
            print(f"Average Rate: {final_rate:.2f} records/second")
            print(f"Total Records: {total}")

        print("Successfully Synced:", self.synced_count)
        print("Skipped:", self.skipped_count)
        print("Failed:", self.failed_count)
        print("No Employee Found:", self.no_employee_count)
        print("Total Processed:", total)


def main():
    print("Starting ERPNext Attendance Sync...")

    if not API_KEY or not API_SECRET:
        print("Error: API_KEY and API_SECRET must be set in .env file")
        return

    try:
        limit_input = input(
            f"How many records to sync? (default: {DEFAULT_LIMIT}): ").strip()
        limit = int(limit_input) if limit_input else DEFAULT_LIMIT
    except ValueError:
        limit = DEFAULT_LIMIT

    response = input(
        f"Proceed with syncing {limit} attendance records? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        sync_manager = AttendanceSyncManager()
        sync_manager.sync_all_attendance(limit)
        print("\nATTENDANCE SYNC COMPLETED!")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")


if __name__ == "__main__":
    main()

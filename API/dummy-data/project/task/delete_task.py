#!/usr/bin/env python3
"""
ERPNext Task Deletion Script
Deletes all task records from ERPNext system.
Uses environment variables from .env file for configuration.
Author: ERPNext Task Deletion Script
Version: 1.0.0
"""

import requests
import json
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

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
        print("Using hardcoded values as fallback")


# Load environment variables
load_env_file()

# Configuration from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")
COMPANY_ABBR = os.getenv("COMPANY_ABBR")

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

        # Log the configuration being used
        logger.info(f"Using API configuration:")
        logger.info(f"  Base URL: {self.base_url}")
        logger.info(f"  Company: {COMPANY_NAME}")
        logger.info(f"  API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT", "DELETE"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()

            # Handle DELETE requests that might not return JSON
            if method == "DELETE":
                return {"success": True}
            else:
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

    def delete_doc(self, doctype: str, name: str) -> Dict:
        """Delete a document"""
        return self._make_request("DELETE", f"resource/{doctype}/{name}")


class TaskDeletor:
    """Handles task deletion with safety checks"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_tasks = []
        self.failed_deletions = []

    def get_all_tasks(self):
        """Get all tasks from ERPNext"""
        logger.info("Fetching all tasks from ERPNext...")

        try:
            all_tasks = self.api.get_list("Task",
                                          fields=["name", "subject", "project", "status", "creation", "priority"])

            logger.info(f"Found {len(all_tasks)} total tasks")
            return all_tasks

        except Exception as e:
            logger.error(f"Error fetching tasks: {str(e)}")
            return []

    def categorize_tasks(self, all_tasks):
        """Categorize tasks by status and project"""
        status_counts = {}
        project_counts = {}
        priority_counts = {}

        for task in all_tasks:
            # Count by status
            status = task.get("status", "Unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

            # Count by project
            project = task.get("project", "No Project")
            project_counts[project] = project_counts.get(project, 0) + 1

            # Count by priority
            priority = task.get("priority", "Medium")
            priority_counts[priority] = priority_counts.get(priority, 0) + 1

        logger.info(f"📊 Task categorization:")
        logger.info(f"   - By Status: {dict(list(status_counts.items())[:5])}")
        logger.info(
            f"   - By Project: {len(project_counts)} different projects")
        logger.info(f"   - By Priority: {dict(priority_counts)}")

        return status_counts, project_counts, priority_counts

    def display_tasks_summary(self, all_tasks):
        """Display summary of tasks to be deleted"""
        if not all_tasks:
            print("\n✅ No tasks found to delete.")
            return False

        print(f"\n⚠️  WARNING: This will DELETE {len(all_tasks)} tasks!")

        # Categorize tasks
        status_counts, project_counts, priority_counts = self.categorize_tasks(
            all_tasks)

        print(f"\n📊 Task Summary:")
        print(f"   - Total tasks: {len(all_tasks)}")

        # Show status breakdown
        print(f"\n📋 Tasks by Status:")
        for status, count in status_counts.items():
            print(f"   - {status}: {count} tasks")

        # Show priority breakdown
        print(f"\n🔥 Tasks by Priority:")
        for priority, count in priority_counts.items():
            print(f"   - {priority}: {count} tasks")

        # Show project breakdown (top 10)
        print(f"\n📁 Tasks by Project (top 10):")
        sorted_projects = sorted(
            project_counts.items(), key=lambda x: x[1], reverse=True)
        for project, count in sorted_projects[:10]:
            project_display = project if project != "No Project" else "Unassigned"
            print(f"   - {project_display}: {count} tasks")

        if len(sorted_projects) > 10:
            remaining = sum(count for _, count in sorted_projects[10:])
            print(
                f"   - ... and {len(sorted_projects) - 10} more projects ({remaining} tasks)")

        # Show sample tasks
        print(f"\n📝 Sample tasks to be deleted (first 10):")
        for i, task in enumerate(all_tasks[:10]):
            subject = task.get("subject", "No Subject")
            status = task.get("status", "Unknown")
            project = task.get("project", "No Project")
            priority = task.get("priority", "Medium")

            # Status indicators
            status_indicator = {"Open": "🔵", "Working": "🟡", "Pending Review": "🟠",
                                "Overdue": "🔴", "Template": "⚪", "Completed": "🟢",
                                "Cancelled": "⚫"}.get(status, "❓")

            # Priority indicators
            priority_indicator = {"Low": "🟢", "Medium": "🟡",
                                  "High": "🔴", "Critical": "🚨"}.get(priority, "⚪")

            print(f"   {i+1}. {subject}")
            print(
                f"      {status_indicator} {status} | {priority_indicator} {priority} | 📁 {project}")

        if len(all_tasks) > 10:
            print(f"   ... and {len(all_tasks) - 10} more tasks")

        return True

    def confirm_deletion(self, all_tasks):
        """Ask for user confirmation before deletion"""
        if not self.display_tasks_summary(all_tasks):
            return False

        print(f"\n🚨 IMPORTANT:")
        print(f"   - This will delete ALL task records from the system")
        print(f"   - Task assignments and time logs will also be affected")
        print(f"   - This action CANNOT be undone!")
        print(f"   - All project progress tracking will be lost")

        response = input(
            f"\nAre you sure you want to DELETE ALL {len(all_tasks)} tasks? Type 'DELETE ALL TASKS' to confirm: ")
        return response == "DELETE ALL TASKS"

    def delete_tasks(self, tasks_to_delete):
        """Delete the specified tasks"""
        logger.info(f"Starting deletion of {len(tasks_to_delete)} tasks...")

        deleted_count = 0
        failed_count = 0

        for i, task in enumerate(tasks_to_delete):
            task_name = task.get("name", "Unknown")
            task_subject = task.get("subject", "No Subject")
            task_status = task.get("status", "Unknown")
            task_project = task.get("project", "No Project")

            try:
                logger.info(
                    f"🗑️ Deleting task {i+1}/{len(tasks_to_delete)}: {task_subject}")

                # Attempt to delete the task
                self.api.delete_doc("Task", task_name)

                self.deleted_tasks.append(task)
                deleted_count += 1

                logger.info(f"✅ Successfully deleted: {task_subject}")

                # Progress update every 20 deletions
                if deleted_count % 20 == 0:
                    logger.info(
                        f"📊 Progress: {deleted_count}/{len(tasks_to_delete)} completed")

            except requests.exceptions.HTTPError as e:
                failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)

                if e.response and e.response.status_code == 417:
                    # Handle potential throttling
                    logger.warning(
                        f"⏳ Deletion throttled for {task_subject}. Waiting 5 seconds...")
                    time.sleep(5)
                    try:
                        # Retry once
                        self.api.delete_doc("Task", task_name)
                        self.deleted_tasks.append(task)
                        deleted_count += 1
                        logger.info(
                            f"✅ Successfully deleted (after retry): {task_subject}")
                    except Exception as retry_error:
                        logger.error(
                            f"❌ Failed to delete {task_subject} even after retry: {str(retry_error)}")
                        self.failed_deletions.append(
                            {"task": task, "error": str(retry_error)})
                elif e.response and e.response.status_code == 403:
                    logger.error(
                        f"❌ Permission denied for {task_subject}: {error_msg}")
                    self.failed_deletions.append(
                        {"task": task, "error": f"Permission denied: {error_msg}"})
                elif e.response and e.response.status_code == 409:
                    logger.error(
                        f"❌ Cannot delete {task_subject} (may have dependencies): {error_msg}")
                    self.failed_deletions.append(
                        {"task": task, "error": f"Has dependencies: {error_msg}"})
                else:
                    logger.error(
                        f"❌ Failed to delete {task_subject}: {error_msg}")
                    self.failed_deletions.append(
                        {"task": task, "error": error_msg})

            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Failed to delete {task_subject}: {str(e)}")
                self.failed_deletions.append({"task": task, "error": str(e)})

        logger.info(
            f"Deletion completed: {deleted_count} deleted, {failed_count} failed")
        return deleted_count, failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🗑️ ERPNext Task Deletion Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🔑 Using API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")
        print("⚠️  WARNING: This will delete ALL tasks from the system!")
        print("=" * 80)

        try:
            # Step 1: Get all tasks
            all_tasks = self.get_all_tasks()
            if not all_tasks:
                print("✅ No tasks found to delete")
                return

            # Step 2: Confirm deletion
            if not self.confirm_deletion(all_tasks):
                print("Operation cancelled.")
                return

            # Step 3: Delete tasks
            deleted_count, failed_count = self.delete_tasks(all_tasks)

            # Step 4: Final summary
            print("\n" + "=" * 80)
            print("✅ TASK DELETION COMPLETED!")
            print("=" * 80)
            print(f"📊 Summary:")
            print(f"   - Tasks deleted: {deleted_count}")
            print(f"   - Tasks failed: {failed_count}")
            print(f"   - Total processed: {len(all_tasks)}")

            if self.failed_deletions:
                print(f"\n❌ Failed deletions ({len(self.failed_deletions)}):")
                # Show first 5 failures
                for failure in self.failed_deletions[:5]:
                    task = failure["task"]
                    error = failure["error"]
                    subject = task.get("subject", "Unknown")
                    print(f"   - {subject}: {error}")
                if len(self.failed_deletions) > 5:
                    print(
                        f"   ... and {len(self.failed_deletions) - 5} more failures")

            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during task deletion: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Task deletion permissions")
            print(f"2. Verify ERPNext is running at {BASE_URL}")
            print("3. Some tasks may have dependencies and cannot be deleted")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Task Deletion...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in API/.env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    print(f"\n⚠️ WARNING: This script will delete ALL tasks from ERPNext")
    print(f"   - All task records will be permanently removed")
    print(f"   - Task assignments and progress will be lost")
    print(f"   - Project tracking data will be affected")

    response = input(
        f"\nDo you want to proceed with task deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = TaskDeletor()
        deletor.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

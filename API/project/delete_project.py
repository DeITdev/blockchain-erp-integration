#!/usr/bin/env python3
"""
ERPNext Project Deletion Script
Deletes all project records from ERPNext system.
Uses environment variables from .env file for configuration.
Author: ERPNext Project Deletion Script
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
    env_path = Path(__file__).parent.parent / '.env'

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


class ProjectDeletor:
    """Handles project deletion with safety checks"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_projects = []
        self.failed_deletions = []

    def get_all_projects(self):
        """Get all projects from ERPNext"""
        logger.info("Fetching all projects from ERPNext...")

        try:
            all_projects = self.api.get_list("Project",
                                             fields=["name", "project_name", "status", "project_type", "priority",
                                                     "expected_start_date", "expected_end_date", "company", "creation"])

            logger.info(f"Found {len(all_projects)} total projects")
            return all_projects

        except Exception as e:
            logger.error(f"Error fetching projects: {str(e)}")
            return []

    def categorize_projects(self, all_projects):
        """Categorize projects by status, type, and priority"""
        status_counts = {}
        type_counts = {}
        priority_counts = {}
        company_counts = {}

        for project in all_projects:
            # Count by status
            status = project.get("status", "Unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

            # Count by type
            project_type = project.get("project_type", "Unknown")
            type_counts[project_type] = type_counts.get(project_type, 0) + 1

            # Count by priority
            priority = project.get("priority", "Medium")
            priority_counts[priority] = priority_counts.get(priority, 0) + 1

            # Count by company
            company = project.get("company", "Unknown")
            company_counts[company] = company_counts.get(company, 0) + 1

        logger.info(f"📊 Project categorization:")
        logger.info(f"   - By Status: {dict(list(status_counts.items())[:5])}")
        logger.info(f"   - By Type: {dict(type_counts)}")
        logger.info(f"   - By Priority: {dict(priority_counts)}")
        logger.info(
            f"   - By Company: {dict(list(company_counts.items())[:3])}")

        return status_counts, type_counts, priority_counts, company_counts

    def display_projects_summary(self, all_projects):
        """Display summary of projects to be deleted"""
        if not all_projects:
            print("\n✅ No projects found to delete.")
            return False

        print(f"\n⚠️  WARNING: This will DELETE {len(all_projects)} projects!")

        # Categorize projects
        status_counts, type_counts, priority_counts, company_counts = self.categorize_projects(
            all_projects)

        print(f"\n📊 Project Summary:")
        print(f"   - Total projects: {len(all_projects)}")

        # Show status breakdown
        print(f"\n📋 Projects by Status:")
        for status, count in status_counts.items():
            print(f"   - {status}: {count} projects")

        # Show type breakdown
        print(f"\n📁 Projects by Type:")
        for project_type, count in type_counts.items():
            print(f"   - {project_type}: {count} projects")

        # Show priority breakdown
        print(f"\n🔥 Projects by Priority:")
        for priority, count in priority_counts.items():
            print(f"   - {priority}: {count} projects")

        # Show company breakdown
        print(f"\n🏢 Projects by Company:")
        for company, count in company_counts.items():
            print(f"   - {company}: {count} projects")

        # Show sample projects
        print(f"\n📝 Sample projects to be deleted (first 10):")
        for i, project in enumerate(all_projects[:10]):
            project_name = project.get("project_name", "No Name")
            status = project.get("status", "Unknown")
            project_type = project.get("project_type", "Unknown")
            priority = project.get("priority", "Medium")

            # Status indicators
            status_indicator = {"Open": "🟢", "Completed": "🔵", "Cancelled": "🔴",
                                "Template": "⚪", "On Hold": "🟡"}.get(status, "❓")

            # Priority indicators
            priority_indicator = {"Low": "🟢", "Medium": "🟡",
                                  "High": "🔴", "Critical": "🚨"}.get(priority, "⚪")

            print(f"   {i+1}. {project_name}")
            print(
                f"      {status_indicator} {status} | {priority_indicator} {priority} | 📁 {project_type}")

        if len(all_projects) > 10:
            print(f"   ... and {len(all_projects) - 10} more projects")

        return True

    def confirm_deletion(self, all_projects):
        """Ask for user confirmation before deletion"""
        if not self.display_projects_summary(all_projects):
            return False

        print(f"\n🚨 IMPORTANT:")
        print(f"   - This will delete ALL project records from the system")
        print(f"   - All associated tasks and timesheets will become orphaned")
        print(f"   - Project progress tracking will be lost")
        print(f"   - This action CANNOT be undone!")

        response = input(
            f"\nAre you sure you want to DELETE ALL {len(all_projects)} projects? Type 'DELETE ALL PROJECTS' to confirm: ")
        return response == "DELETE ALL PROJECTS"

    def delete_projects(self, projects_to_delete):
        """Delete the specified projects"""
        logger.info(
            f"Starting deletion of {len(projects_to_delete)} projects...")

        deleted_count = 0
        failed_count = 0

        for i, project in enumerate(projects_to_delete):
            project_name = project.get("name", "Unknown")
            project_display_name = project.get("project_name", "Unknown")
            project_status = project.get("status", "Unknown")
            project_type = project.get("project_type", "Unknown")

            try:
                logger.info(
                    f"🗑️ Deleting project {i+1}/{len(projects_to_delete)}: {project_display_name}")

                # Attempt to delete the project
                self.api.delete_doc("Project", project_name)

                self.deleted_projects.append(project)
                deleted_count += 1

                logger.info(f"✅ Successfully deleted: {project_display_name}")

                # Progress update every 10 deletions
                if deleted_count % 10 == 0:
                    logger.info(
                        f"📊 Progress: {deleted_count}/{len(projects_to_delete)} completed")

            except requests.exceptions.HTTPError as e:
                failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)

                if e.response and e.response.status_code == 417:
                    # Handle potential throttling
                    logger.warning(
                        f"⏳ Deletion throttled for {project_display_name}. Waiting 5 seconds...")
                    time.sleep(5)
                    try:
                        # Retry once
                        self.api.delete_doc("Project", project_name)
                        self.deleted_projects.append(project)
                        deleted_count += 1
                        logger.info(
                            f"✅ Successfully deleted (after retry): {project_display_name}")
                    except Exception as retry_error:
                        logger.error(
                            f"❌ Failed to delete {project_display_name} even after retry: {str(retry_error)}")
                        self.failed_deletions.append(
                            {"project": project, "error": str(retry_error)})
                elif e.response and e.response.status_code == 403:
                    logger.error(
                        f"❌ Permission denied for {project_display_name}: {error_msg}")
                    self.failed_deletions.append(
                        {"project": project, "error": f"Permission denied: {error_msg}"})
                elif e.response and e.response.status_code == 409:
                    logger.error(
                        f"❌ Cannot delete {project_display_name} (may have dependencies): {error_msg}")
                    self.failed_deletions.append(
                        {"project": project, "error": f"Has dependencies: {error_msg}"})
                else:
                    logger.error(
                        f"❌ Failed to delete {project_display_name}: {error_msg}")
                    self.failed_deletions.append(
                        {"project": project, "error": error_msg})

            except Exception as e:
                failed_count += 1
                logger.error(
                    f"❌ Failed to delete {project_display_name}: {str(e)}")
                self.failed_deletions.append(
                    {"project": project, "error": str(e)})

        logger.info(
            f"Deletion completed: {deleted_count} deleted, {failed_count} failed")
        return deleted_count, failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🗑️ ERPNext Project Deletion Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🔑 Using API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")
        print("⚠️  WARNING: This will delete ALL projects from the system!")
        print("=" * 80)

        try:
            # Step 1: Get all projects
            all_projects = self.get_all_projects()
            if not all_projects:
                print("✅ No projects found to delete")
                return

            # Step 2: Confirm deletion
            if not self.confirm_deletion(all_projects):
                print("Operation cancelled.")
                return

            # Step 3: Delete projects
            deleted_count, failed_count = self.delete_projects(all_projects)

            # Step 4: Final summary
            print("\n" + "=" * 80)
            print("✅ PROJECT DELETION COMPLETED!")
            print("=" * 80)
            print(f"📊 Summary:")
            print(f"   - Projects deleted: {deleted_count}")
            print(f"   - Projects failed: {failed_count}")
            print(f"   - Total processed: {len(all_projects)}")

            if self.failed_deletions:
                print(f"\n❌ Failed deletions ({len(self.failed_deletions)}):")
                # Show first 5 failures
                for failure in self.failed_deletions[:5]:
                    project = failure["project"]
                    error = failure["error"]
                    project_name = project.get("project_name", "Unknown")
                    print(f"   - {project_name}: {error}")
                if len(self.failed_deletions) > 5:
                    print(
                        f"   ... and {len(self.failed_deletions) - 5} more failures")

            print("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during project deletion: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Project deletion permissions")
            print(f"2. Verify ERPNext is running at {BASE_URL}")
            print("3. Some projects may have dependencies and cannot be deleted")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Project Deletion...")

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

    print(f"\n⚠️ WARNING: This script will delete ALL projects from ERPNext")
    print(f"   - All project records will be permanently removed")
    print(f"   - Associated tasks will become orphaned")
    print(f"   - Project progress tracking will be lost")

    response = input(
        f"\nDo you want to proceed with project deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = ProjectDeletor()
        deletor.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

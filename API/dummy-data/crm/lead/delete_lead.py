#!/usr/bin/env python3
"""
ERPNext Lead Deletion Script
Deletes all lead records from ERPNext system.
Uses environment variables from .env file for configuration.
Author: ERPNext Lead Deletion Script
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


class LeadDeletor:
    """Handles lead deletion with safety checks"""

    def __init__(self):
        self.api = ERPNextAPI()
        self.deleted_leads = []
        self.failed_deletions = []

    def get_all_leads(self):
        """Get all leads from ERPNext"""
        logger.info("Fetching all leads from ERPNext...")

        try:
            all_leads = self.api.get_list(
                "Lead", fields=["name", "lead_name", "status"])
            logger.info(f"Found {len(all_leads)} total leads")
            return all_leads

        except Exception as e:
            logger.error(f"Error fetching leads: {str(e)}")
            return []

    def confirm_deletion(self, all_leads):
        """Ask for user confirmation before deletion"""
        if not all_leads:
            print("\n✅ No leads found to delete.")
            return False

        print(f"\n⚠️  WARNING: This will DELETE {len(all_leads)} leads!")
        print(f"\n📊 Lead Summary:")
        print(f"   - Total leads: {len(all_leads)}")

        # Show sample leads
        print(f"\n📝 Sample leads to be deleted (first 10):")
        for i, lead in enumerate(all_leads[:10]):
            lead_name = lead.get("lead_name", lead.get("name", "Unknown"))
            status = lead.get("status", "Unknown")
            print(f"   {i+1}. {lead_name} - Status: {status}")

        if len(all_leads) > 10:
            print(f"   ... and {len(all_leads) - 10} more leads")

        print(f"\n🚨 THIS ACTION CANNOT BE UNDONE!")
        response = input(
            f"\nAre you sure you want to DELETE ALL {len(all_leads)} leads? Type 'DELETE ALL LEADS' to confirm: ")

        return response == "DELETE ALL LEADS"

    def delete_leads(self, leads_to_delete):
        """Delete all leads"""
        logger.info(f"Starting deletion of {len(leads_to_delete)} leads...")

        deleted_count = 0
        failed_count = 0

        for i, lead in enumerate(leads_to_delete):
            try:
                lead_name = lead.get("name")
                lead_display = lead.get("lead_name", lead_name)

                logger.info(
                    f"🗑️ Deleting lead {i+1}/{len(leads_to_delete)}: {lead_display}")

                # Delete the lead
                self.api.delete_doc("Lead", lead_name)

                self.deleted_leads.append(lead)
                deleted_count += 1

                logger.info(f"✅ Successfully deleted: {lead_display}")

                # Progress update every 25 deletions
                if deleted_count % 25 == 0:
                    logger.info(
                        f"📊 Progress: {deleted_count}/{len(leads_to_delete)} completed")

                # Small delay to avoid overwhelming server
                time.sleep(0.1)

            except requests.exceptions.HTTPError as e:
                failed_count += 1
                error_msg = f"HTTP {e.response.status_code}" if e.response else str(
                    e)

                if e.response and e.response.status_code == 417:
                    # Handle potential throttling
                    logger.warning(
                        f"⏳ Deletion throttled for {lead_display}. Waiting 5 seconds...")
                    time.sleep(5)
                    try:
                        # Retry once
                        self.api.delete_doc("Lead", lead_name)
                        self.deleted_leads.append(lead)
                        deleted_count += 1
                        logger.info(
                            f"✅ Successfully deleted (after retry): {lead_display}")
                    except Exception as retry_error:
                        logger.error(
                            f"❌ Failed to delete {lead_display} even after retry: {str(retry_error)}")
                        self.failed_deletions.append(
                            {"lead": lead, "error": str(retry_error)})
                elif e.response and e.response.status_code == 403:
                    logger.error(
                        f"❌ Permission denied for {lead_display}: {error_msg}")
                    self.failed_deletions.append(
                        {"lead": lead, "error": f"Permission denied: {error_msg}"})
                elif e.response and e.response.status_code == 409:
                    logger.error(
                        f"❌ Cannot delete {lead_display} (may have dependencies): {error_msg}")
                    self.failed_deletions.append(
                        {"lead": lead, "error": f"Has dependencies: {error_msg}"})
                else:
                    logger.error(
                        f"❌ Failed to delete {lead_display}: {error_msg}")
                    self.failed_deletions.append(
                        {"lead": lead, "error": error_msg})

            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Failed to delete {lead_display}: {str(e)}")
                self.failed_deletions.append({"lead": lead, "error": str(e)})

        logger.info(
            f"Deletion completed: {deleted_count} deleted, {failed_count} failed")
        return deleted_count, failed_count

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("🗑️ ERPNext Lead Deletion Script")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print("⚠️  WARNING: This will delete ALL leads from the system!")
        print("=" * 80)

        try:
            # Get all leads
            all_leads = self.get_all_leads()
            if not all_leads:
                print("✅ No leads found to delete")
                return

            # Confirm deletion
            if not self.confirm_deletion(all_leads):
                print("Operation cancelled.")
                return

            # Delete leads
            deleted_count, failed_count = self.delete_leads(all_leads)

            # Summary
            print("\n" + "="*60)
            print("📊 DELETION SUMMARY")
            print("="*60)
            print(f"✅ Successfully Deleted: {deleted_count} leads")
            print(f"❌ Failed Deletions: {failed_count} leads")
            print(f"📊 Total Processed: {len(all_leads)} leads")

            if deleted_count > 0:
                print(f"\n🎉 Successfully deleted {deleted_count} leads!")

            if self.failed_deletions:
                print(f"\n❌ Failed deletions ({len(self.failed_deletions)}):")
                for failure in self.failed_deletions[:5]:
                    lead = failure["lead"]
                    error = failure["error"]
                    lead_name = lead.get("lead_name", "Unknown")
                    print(f"   - {lead_name}: {error}")
                if len(self.failed_deletions) > 5:
                    print(
                        f"   ... and {len(self.failed_deletions) - 5} more failures")

            print("="*60)

        except Exception as e:
            logger.error(f"Fatal error during lead deletion: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Lead Deletion...")

    # Check API credentials
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in .env file")
        return

    print(f"\n🗑️ This will DELETE ALL leads:")
    print(f"   🏢 All lead records will be permanently removed")
    print(f"   🚨 This action CANNOT be undone!")

    response = input(
        f"\nDo you want to proceed with lead deletion? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        deletor = LeadDeletor()
        deletor.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

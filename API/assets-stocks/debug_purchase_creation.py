#!/usr/bin/env python3
"""
Debug Purchase Receipt Issue & Fixed Asset Generator
Identifies and fixes the Purchase Receipt creation problem
"""

import requests
import json
import random
import logging
import time
import os
from datetime import datetime, timedelta

# Configuration


class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya"

    TARGET_ITEMS = 50
    TARGET_ASSETS = 50
    MASTER_DATA_FILE = "erpnext_master_data.json"


# Logging setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DebugAssetGenerator:
    def __init__(self):
        self.api = self.create_api_client()
        self.master_data = {}

    def create_api_client(self):
        session = requests.Session()
        session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        return session

    def make_request(self, method, endpoint, data=None):
        url = f"{Config.BASE_URL}/api/{endpoint}"
        try:
            response = self.api.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                        params=data if method == "GET" else None)

            # Debug: Print response details
            logger.info(f"API Response Status: {response.status_code}")
            if response.status_code >= 400:
                logger.error(f"Error Response: {response.text[:1000]}")

            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API request failed: {method} {url} - {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Full Response: {e.response.text}")
            raise

    def get_list(self, doctype, filters=None, fields=None):
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self.make_request("GET", f"resource/{doctype}", params).get("data", [])

    def create_doc(self, doctype, data):
        data["doctype"] = doctype
        logger.info(
            f"Creating {doctype} with data: {json.dumps(data, indent=2)}")
        return self.make_request("POST", f"resource/{doctype}", data)

    def submit_doc(self, doctype, name):
        """Submit a document"""
        return self.make_request("PUT", f"resource/{doctype}/{name}", {"docstatus": 1})

    def load_master_data_from_json(self):
        """Load master data from JSON file"""
        if not os.path.exists(Config.MASTER_DATA_FILE):
            logger.error(
                f"❌ Master data file {Config.MASTER_DATA_FILE} not found!")
            return False

        try:
            with open(Config.MASTER_DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.master_data = data["master_data"]

            logger.info(f"✅ Loaded master data from {Config.MASTER_DATA_FILE}")
            return True

        except Exception as e:
            logger.error(f"Failed to load master data: {e}")
            return False

    def debug_purchase_receipt_creation(self):
        """Debug the Purchase Receipt creation issue"""
        logger.info("🔍 DEBUG: Testing Purchase Receipt creation...")

        # Test with minimal data first
        if not self.master_data.get("suppliers"):
            logger.error("❌ No suppliers in master data")
            return False

        if not self.master_data.get("warehouses"):
            logger.error("❌ No warehouses in master data")
            return False

        supplier = self.master_data["suppliers"][0]
        warehouse = self.master_data["warehouses"][0]

        logger.info(f"Using supplier: {supplier}")
        logger.info(f"Using warehouse: {warehouse}")

        # Create a simple test item first
        test_item_code = f"TEST-DEBUG-{random.randint(1000, 9999)}"

        try:
            # Create test item
            test_item_data = {
                "item_code": test_item_code,
                "item_name": "Debug Test Item",
                "item_group": self.master_data["item_groups"][0],
                "stock_uom": self.master_data["default_units_of_measure"][0],
                "is_stock_item": 1,  # Make it a stock item for purchase receipt
                "is_purchase_item": 1
            }

            logger.info("Creating test item...")
            item_result = self.create_doc("Item", test_item_data)
            logger.info(f"Test item created: {item_result}")

            time.sleep(1)  # Wait for item to be processed

            # Now try to create Purchase Receipt
            pr_data = {
                "supplier": supplier["name"],
                "company": Config.COMPANY_NAME,
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "posting_time": "10:00:00",
                "items": [{
                    "item_code": test_item_code,
                    "item_name": "Debug Test Item",
                    "warehouse": warehouse["name"],
                    "qty": 1,
                    "rate": 100000,
                    "amount": 100000
                }]
            }

            logger.info("Creating test Purchase Receipt...")
            pr_result = self.create_doc("Purchase Receipt", pr_data)
            logger.info(f"Purchase Receipt result: {pr_result}")

            # Check the structure of the result
            if isinstance(pr_result, dict):
                logger.info(f"PR Result keys: {list(pr_result.keys())}")

                # Try different ways to get the name
                pr_name = None
                if "name" in pr_result:
                    pr_name = pr_result["name"]
                elif "data" in pr_result and isinstance(pr_result["data"], dict):
                    pr_name = pr_result["data"].get("name")
                elif "message" in pr_result and isinstance(pr_result["message"], dict):
                    pr_name = pr_result["message"].get("name")

                if pr_name:
                    logger.info(
                        f"✅ Purchase Receipt created successfully: {pr_name}")

                    # Try to submit it
                    try:
                        self.submit_doc("Purchase Receipt", pr_name)
                        logger.info(f"✅ Purchase Receipt submitted: {pr_name}")
                    except Exception as e:
                        logger.error(f"❌ Failed to submit PR: {e}")

                    return True
                else:
                    logger.error(
                        f"❌ Could not find 'name' in PR result: {pr_result}")
                    return False
            else:
                logger.error(f"❌ Unexpected PR result type: {type(pr_result)}")
                return False

        except Exception as e:
            logger.error(f"❌ Debug Purchase Receipt creation failed: {e}")
            return False

    def test_different_pr_approaches(self):
        """Test different approaches to Purchase Receipt creation"""
        logger.info("🧪 Testing different Purchase Receipt approaches...")

        if not self.master_data.get("suppliers") or not self.master_data.get("warehouses"):
            logger.error("❌ Missing suppliers or warehouses")
            return

        supplier = self.master_data["suppliers"][0]
        warehouse = self.master_data["warehouses"][0]

        # Get an existing item to test with
        existing_items = self.get_list("Item",
                                       filters={"is_purchase_item": 1,
                                                "is_stock_item": 1},
                                       fields=["name", "item_code", "item_name", "stock_uom"])

        if not existing_items:
            logger.error("❌ No existing purchasable stock items found")
            return

        test_item = existing_items[0]
        logger.info(f"Using existing item for test: {test_item}")

        # Approach 1: Minimal Purchase Receipt
        logger.info("🔬 Approach 1: Minimal Purchase Receipt")
        try:
            pr_data_minimal = {
                "supplier": supplier["name"],
                "company": Config.COMPANY_NAME,
                "items": [{
                    "item_code": test_item["item_code"],
                    "warehouse": warehouse["name"],
                    "qty": 1,
                    "rate": 50000
                }]
            }

            pr_result = self.create_doc("Purchase Receipt", pr_data_minimal)
            logger.info(f"✅ Minimal PR created: {pr_result}")

        except Exception as e:
            logger.error(f"❌ Minimal PR failed: {e}")

        # Approach 2: Purchase Receipt without Fixed Asset items
        logger.info("🔬 Approach 2: Purchase Receipt with complete data")
        try:
            pr_data_complete = {
                "supplier": supplier["name"],
                "company": Config.COMPANY_NAME,
                "posting_date": datetime.now().strftime("%Y-%m-%d"),
                "posting_time": "10:00:00",
                # Try set_warehouse instead
                "set_warehouse": warehouse["name"],
                "items": [{
                    "item_code": test_item["item_code"],
                    "item_name": test_item["item_name"],
                    "qty": 1,
                    "uom": test_item["stock_uom"],
                    "rate": 50000,
                    "amount": 50000
                }]
            }

            pr_result = self.create_doc("Purchase Receipt", pr_data_complete)
            logger.info(f"✅ Complete PR created: {pr_result}")

        except Exception as e:
            logger.error(f"❌ Complete PR failed: {e}")

    def create_asset_without_purchase_receipt(self):
        """Try creating assets without Purchase Receipt to test if that's the issue"""
        logger.info("🏢 Testing Asset creation without Purchase Receipt...")

        # Get existing Fixed Asset items
        existing_fa_items = self.get_list("Item",
                                          filters={"is_fixed_asset": 1},
                                          fields=["name", "item_code", "item_name", "valuation_rate"])

        if not existing_fa_items:
            logger.error("❌ No Fixed Asset items found")
            return

        test_item = existing_fa_items[0]
        logger.info(f"Using Fixed Asset item: {test_item}")

        try:
            asset_data = {
                "asset_name": f"Test Asset {random.randint(1000, 9999)}",
                "item_code": test_item["item_code"],
                "company": Config.COMPANY_NAME,
                "location": self.master_data["locations"][0] if self.master_data.get("locations") else "Head Office",
                "purchase_date": datetime.now().strftime("%Y-%m-%d"),
                "available_for_use_date": datetime.now().strftime("%Y-%m-%d"),
                "gross_purchase_amount": test_item.get("valuation_rate", 5_000_000),
                "asset_owner": "Company",
                "is_existing_asset": 0,
                "calculate_depreciation": 0,
                "status": "Draft"
            }

            # Add department if available
            if self.master_data.get("departments"):
                asset_data["department"] = self.master_data["departments"][0]

            asset_result = self.create_doc("Asset", asset_data)
            logger.info(f"✅ Asset created without PR: {asset_result}")

            # Try to submit it
            try:
                asset_name = asset_result.get("name")
                if asset_name:
                    self.submit_doc("Asset", asset_name)
                    logger.info(f"✅ Asset submitted: {asset_name}")
                else:
                    logger.warning(
                        "⚠️ Asset created but no name found for submission")

            except Exception as e:
                logger.error(f"❌ Asset submission failed: {e}")

        except Exception as e:
            logger.error(f"❌ Asset creation failed: {e}")

    def run_debug_session(self):
        """Run complete debug session"""
        print("\n" + "="*80)
        print("🔍 DEBUG SESSION: Purchase Receipt & Asset Creation Issues")
        print("="*80)

        try:
            # Load master data
            if not self.load_master_data_from_json():
                print("❌ Cannot proceed without master data")
                return

            print("\n1. 🔍 Debugging Purchase Receipt creation...")
            self.debug_purchase_receipt_creation()

            print("\n2. 🧪 Testing different PR approaches...")
            self.test_different_pr_approaches()

            print("\n3. 🏢 Testing Asset creation without PR...")
            self.create_asset_without_purchase_receipt()

            print("\n" + "="*80)
            print("✅ DEBUG SESSION COMPLETED")
            print("Check the logs above to see what's working and what's failing.")
            print("="*80)

        except Exception as e:
            logger.error(f"Debug session failed: {e}")
            print(f"❌ Error: {e}")


def main():
    debugger = DebugAssetGenerator()
    debugger.run_debug_session()


if __name__ == "__main__":
    main()

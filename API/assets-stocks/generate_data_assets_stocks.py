#!/usr/bin/env python3
"""
Fixed Asset Generator with Proper Warehouses
Fixes the warehouse issue by using only leaf warehouses (not group warehouses) for Purchase Receipts
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


class FixedAssetGeneratorProperWarehouses:
    def __init__(self):
        self.api = self.create_api_client()
        self.master_data = {}
        self.leaf_warehouses = []  # Only leaf warehouses for transactions

        # Track creation
        self.created_items = []
        self.created_purchase_receipts = []
        self.created_assets = []

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
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API request failed: {method} {url} - {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text[:500]}")
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

    def fetch_leaf_warehouses(self):
        """Fetch only leaf warehouses (is_group = 0) for transactions"""
        logger.info("🏭 Fetching leaf warehouses for transactions...")

        try:
            # Get all warehouses for the company that are NOT groups
            warehouses = self.get_list("Warehouse",
                                       filters={
                                           "company": Config.COMPANY_NAME,
                                           "is_group": 0,  # Only leaf warehouses
                                           "disabled": 0   # Only active warehouses
                                       },
                                       fields=["name", "warehouse_name"])

            self.leaf_warehouses = warehouses
            logger.info(
                f"✅ Found {len(self.leaf_warehouses)} leaf warehouses for transactions")

            if self.leaf_warehouses:
                logger.info(f"📦 Available warehouses:")
                # Show first 5
                for i, wh in enumerate(self.leaf_warehouses[:5]):
                    logger.info(
                        f"   {i+1}. {wh['warehouse_name']} ({wh['name']})")
                if len(self.leaf_warehouses) > 5:
                    logger.info(
                        f"   ... and {len(self.leaf_warehouses) - 5} more")
            else:
                logger.error(
                    "❌ No leaf warehouses found! Cannot create Purchase Receipts.")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to fetch leaf warehouses: {e}")
            return False

    def check_existing_data(self):
        """Check existing data and determine what needs to be created"""
        logger.info("🔍 Checking existing data...")

        try:
            # Check Fixed Asset items
            existing_items = self.get_list("Item",
                                           filters={"is_fixed_asset": 1},
                                           fields=["name", "item_code", "item_name"])
            items_count = len(existing_items)

            # Check Assets
            existing_assets = self.get_list("Asset",
                                            filters={
                                                "company": Config.COMPANY_NAME},
                                            fields=["name", "asset_name"])
            assets_count = len(existing_assets)

            logger.info(f"📊 Current Status:")
            logger.info(
                f"   - Fixed Asset Items: {items_count}/{Config.TARGET_ITEMS}")
            logger.info(f"   - Assets: {assets_count}/{Config.TARGET_ASSETS}")

            items_needed = max(0, Config.TARGET_ITEMS - items_count)
            assets_needed = max(0, Config.TARGET_ASSETS - assets_count)

            if items_needed == 0 and assets_needed == 0:
                logger.info(
                    "🎉 TARGET REACHED! You already have enough items and assets.")
                return "COMPLETE"

            return {
                "existing_items": items_count,
                "existing_assets": assets_count,
                "items_needed": items_needed,
                "assets_needed": assets_needed,
                "existing_item_data": existing_items
            }

        except Exception as e:
            logger.error(f"Failed to check existing data: {e}")
            return None

    def create_fixed_asset_items(self, count_needed):
        """Create Fixed Asset items"""
        logger.info(f"📦 Creating {count_needed} Fixed Asset Items...")

        asset_templates = [
            {"name": "Business Laptop", "category": "Computer Equipment",
                "price_range": (10_000_000, 20_000_000)},
            {"name": "Desktop Computer", "category": "Computer Equipment",
                "price_range": (8_000_000, 15_000_000)},
            {"name": "Office Chair", "category": "Office Furniture",
                "price_range": (2_000_000, 8_000_000)},
            {"name": "Meeting Table", "category": "Office Furniture",
                "price_range": (5_000_000, 15_000_000)},
            {"name": "Network Router", "category": "IT Equipment",
                "price_range": (3_000_000, 10_000_000)},
            {"name": "Laser Printer", "category": "IT Equipment",
                "price_range": (2_500_000, 8_000_000)},
            {"name": "Security Camera", "category": "IT Equipment",
                "price_range": (1_500_000, 5_000_000)},
            {"name": "Filing Cabinet", "category": "Office Furniture",
                "price_range": (1_000_000, 4_000_000)},
            {"name": "Projector", "category": "IT Equipment",
                "price_range": (4_000_000, 12_000_000)},
            {"name": "Executive Desk", "category": "Office Furniture",
                "price_range": (3_000_000, 10_000_000)}
        ]

        for i in range(count_needed):
            try:
                template = random.choice(asset_templates)
                price = random.randint(
                    template["price_range"][0], template["price_range"][1])

                # Find matching asset category
                asset_category = None
                for cat in self.master_data["asset_categories"]:
                    if template["category"] in cat or cat in template["category"]:
                        asset_category = cat
                        break

                if not asset_category:
                    asset_category = random.choice(
                        self.master_data["asset_categories"])

                # Generate unique item code and name
                item_code = f"FA-{template['name'][:3].upper()}-{random.randint(10000, 99999)}"
                item_name = f"{template['name']} {random.choice(['Pro', 'Standard', 'Premium', 'Basic', 'Advanced'])} #{i+1:03d}"

                # Create Fixed Asset Item
                item_data = {
                    "item_code": item_code,
                    "item_name": item_name,
                    "item_group": random.choice(self.master_data["item_groups"]),
                    "stock_uom": random.choice(self.master_data["default_units_of_measure"]),
                    "is_fixed_asset": 1,  # Fixed Asset = True
                    "is_stock_item": 0,   # Maintain Stock = False
                    "asset_category": asset_category,
                    "is_purchase_item": 1,
                    "is_sales_item": 0,
                    "valuation_rate": price,
                    "standard_rate": int(price * 1.15),
                    "description": f"High quality {template['name'].lower()} for business operations"
                }

                item_result = self.create_doc("Item", item_data)

                self.created_items.append({
                    "item_code": item_code,
                    "item_name": item_name,
                    "price": price,
                    "category": asset_category,
                    "doc_name": item_result.get("name", item_code)
                })

                logger.info(
                    f"✅ Created Item {i+1}/{count_needed}: {item_code}")
                time.sleep(0.2)

            except Exception as e:
                logger.error(f"❌ Failed to create item {i+1}: {e}")

        logger.info(f"Created {len(self.created_items)} Fixed Asset Items")

    def create_purchase_receipt_for_item(self, item_data):
        """Create Purchase Receipt for an item using proper leaf warehouse"""
        try:
            if not self.master_data["suppliers"]:
                logger.error("❌ No suppliers available")
                return None

            if not self.leaf_warehouses:
                logger.error("❌ No leaf warehouses available")
                return None

            supplier = random.choice(self.master_data["suppliers"])
            warehouse = random.choice(
                self.leaf_warehouses)  # Use leaf warehouse
            purchase_date = self.random_date_last_6_months()

            # Create Purchase Receipt with proper warehouse
            pr_data = {
                "supplier": supplier["name"],
                "company": Config.COMPANY_NAME,
                "posting_date": purchase_date,
                "posting_time": f"{random.randint(9, 16):02d}:{random.randint(0, 59):02d}:00",
                "items": [{
                    "item_code": item_data["item_code"],
                    "item_name": item_data["item_name"],
                    # Use the warehouse name from leaf warehouses
                    "warehouse": warehouse["name"],
                    "qty": 1,
                    "rate": item_data["price"],
                    "amount": item_data["price"]
                }]
            }

            pr_result = self.create_doc("Purchase Receipt", pr_data)

            # Extract PR name from the result
            pr_name = None
            if isinstance(pr_result, dict):
                if "name" in pr_result:
                    pr_name = pr_result["name"]
                elif "data" in pr_result and isinstance(pr_result["data"], dict):
                    pr_name = pr_result["data"].get("name")

            if not pr_name:
                logger.error(
                    f"❌ Could not get PR name from result: {pr_result}")
                return None

            # Submit Purchase Receipt
            self.submit_doc("Purchase Receipt", pr_name)

            logger.info(f"✅ Created & Submitted Purchase Receipt: {pr_name}")

            return {
                "name": pr_name,
                "item_code": item_data["item_code"],
                "supplier": supplier["name"],
                "purchase_date": purchase_date,
                "warehouse": warehouse["name"]
            }

        except Exception as e:
            logger.error(
                f"❌ Failed to create Purchase Receipt for {item_data['item_code']}: {e}")
            return None

    def create_asset_from_purchase_data(self, item_data, pr_data):
        """Create Asset from item and purchase receipt data"""
        try:
            available_date = self.random_date_last_3_months()

            asset_data = {
                "asset_name": f"{item_data['item_name']} Asset",
                "item_code": item_data["item_code"],
                "company": Config.COMPANY_NAME,
                "location": random.choice(self.master_data["locations"]) if self.master_data.get("locations") else "Head Office",
                "purchase_date": pr_data["purchase_date"],
                "available_for_use_date": available_date,
                "gross_purchase_amount": item_data["price"],
                "purchase_amount": item_data["price"],
                # Link to purchase receipt
                "purchase_receipt": pr_data["name"],
                "asset_owner": "Company",
                "asset_owner_company": Config.COMPANY_NAME,
                "is_existing_asset": 0,
                "calculate_depreciation": 0,
                "status": "Draft",
                "asset_quantity": 1
            }

            # Add department if available
            if self.master_data.get("departments"):
                asset_data["department"] = random.choice(
                    self.master_data["departments"])

            asset_result = self.create_doc("Asset", asset_data)

            # Extract asset name from result
            asset_name = None
            if isinstance(asset_result, dict):
                if "name" in asset_result:
                    asset_name = asset_result["name"]
                elif "data" in asset_result and isinstance(asset_result["data"], dict):
                    asset_name = asset_result["data"].get("name")

            # Try to submit asset
            if asset_name:
                try:
                    self.submit_doc("Asset", asset_name)
                    logger.info(f"✅ Created & Submitted Asset: {asset_name}")
                except Exception as e:
                    logger.warning(
                        f"⚠️ Asset created but submission failed: {e}")
            else:
                logger.warning(
                    "⚠️ Asset created but no name found for submission")

            return {
                "name": asset_name,
                "asset_name": asset_data["asset_name"],
                "item_code": item_data["item_code"],
                "purchase_receipt": pr_data["name"],
                "price": item_data["price"]
            }

        except Exception as e:
            logger.error(
                f"❌ Failed to create asset for {item_data['item_code']}: {e}")
            return None

    def generate_assets_with_purchase_receipts(self, count_needed):
        """Generate assets with proper purchase receipt workflow"""
        logger.info(
            f"🏢 Creating {count_needed} Assets with Purchase Receipts...")

        # Use newly created items for asset creation
        items_for_assets = self.created_items[:count_needed]

        if len(items_for_assets) < count_needed:
            # Get existing Fixed Asset items if we need more
            existing_items = self.get_list("Item",
                                           filters={"is_fixed_asset": 1},
                                           fields=["name", "item_code", "item_name", "valuation_rate"])

            for existing_item in existing_items:
                if len(items_for_assets) >= count_needed:
                    break

                items_for_assets.append({
                    "item_code": existing_item["item_code"],
                    "item_name": existing_item["item_name"],
                    "price": existing_item.get("valuation_rate", 5_000_000),
                    "doc_name": existing_item["name"]
                })

        # Create assets with purchase receipts
        for i, item_data in enumerate(items_for_assets[:count_needed]):
            try:
                # Step 1: Create Purchase Receipt
                pr_data = self.create_purchase_receipt_for_item(item_data)
                if not pr_data:
                    logger.error(
                        f"❌ Failed to create PR for item {i+1}, skipping asset creation")
                    continue

                self.created_purchase_receipts.append(pr_data)

                # Step 2: Create Asset linked to Purchase Receipt
                asset_data = self.create_asset_from_purchase_data(
                    item_data, pr_data)
                if asset_data:
                    self.created_assets.append(asset_data)
                    logger.info(
                        f"✅ Completed asset {i+1}/{count_needed} with full workflow")

                time.sleep(0.5)  # Delay between creations

            except Exception as e:
                logger.error(f"❌ Failed to create asset {i+1}: {e}")

        logger.info(
            f"Created {len(self.created_assets)} assets with purchase receipts")

    def random_date_last_6_months(self):
        """Generate random date in the last 6 months"""
        start = datetime.now() - timedelta(days=180)
        end = datetime.now() - timedelta(days=30)
        delta = end - start
        return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

    def random_date_last_3_months(self):
        """Generate random date in the last 3 months"""
        start = datetime.now() - timedelta(days=90)
        end = datetime.now()
        delta = end - start
        return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

    def run_complete_workflow(self):
        """Run complete asset generation workflow with proper warehouses"""
        print("\n" + "="*80)
        print("🎯 COMPLETE ASSET WORKFLOW WITH PROPER WAREHOUSES")
        print("="*80)
        print(
            f"Target: {Config.TARGET_ITEMS} Fixed Asset Items, {Config.TARGET_ASSETS} Assets")
        print("This version fixes the warehouse issue by using only leaf warehouses.")
        print("="*80)

        try:
            start_time = datetime.now()

            # Step 1: Load master data
            if not self.load_master_data_from_json():
                print("❌ Cannot proceed without master data")
                return

            # Step 2: Fetch proper leaf warehouses
            if not self.fetch_leaf_warehouses():
                print("❌ Cannot proceed without proper warehouses")
                return

            # Step 3: Check existing data
            status = self.check_existing_data()
            if status == "COMPLETE":
                print("🎉 All targets already reached!")
                return
            if not status:
                print("❌ Failed to check existing data")
                return

            # Step 4: Create items if needed
            if status["items_needed"] > 0:
                self.create_fixed_asset_items(status["items_needed"])
            else:
                logger.info("✅ Enough Fixed Asset Items already exist")

            # Step 5: Create assets with purchase receipts
            if status["assets_needed"] > 0:
                self.generate_assets_with_purchase_receipts(
                    status["assets_needed"])
            else:
                logger.info("✅ Enough Assets already exist")

            end_time = datetime.now()
            duration = end_time - start_time

            # Final Summary
            final_items = status["existing_items"] + len(self.created_items)
            final_assets = status["existing_assets"] + len(self.created_assets)

            print("\n" + "="*80)
            print("✅ COMPLETE WORKFLOW FINISHED!")
            print("="*80)
            print(f"📊 Final Results:")
            print(
                f"   - Fixed Asset Items: {final_items}/{Config.TARGET_ITEMS} ({'✅ TARGET REACHED' if final_items >= Config.TARGET_ITEMS else '⚠️ INCOMPLETE'})")
            print(
                f"   - Assets: {final_assets}/{Config.TARGET_ASSETS} ({'✅ TARGET REACHED' if final_assets >= Config.TARGET_ASSETS else '⚠️ INCOMPLETE'})")
            print(f"   - New Items Created: {len(self.created_items)}")
            print(
                f"   - Purchase Receipts: {len(self.created_purchase_receipts)}")
            print(f"   - New Assets Created: {len(self.created_assets)}")
            print(f"⏰ Total time: {duration}")
            print("="*80)
            print("🎯 Your complete asset system is ready!")
            print("   - All Purchase Receipts are submitted")
            print("   - All assets are linked to Purchase Receipts")
            print("   - Using proper leaf warehouses for transactions")
            print("="*80)

        except Exception as e:
            logger.error(f"Complete workflow failed: {e}")
            print(f"❌ Error: {e}")


def main():
    generator = FixedAssetGeneratorProperWarehouses()
    generator.run_complete_workflow()


if __name__ == "__main__":
    main()

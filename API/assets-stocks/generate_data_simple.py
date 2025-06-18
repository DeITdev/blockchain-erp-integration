#!/usr/bin/env python3
"""
Simple ERPNext Assets & Stock Generator
Creates essential dummy data with minimal dependencies to avoid validation errors.
"""

import requests
import json
import random
import logging
import time
from datetime import datetime, timedelta
from faker import Faker

# Configuration


class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya"

    # Target counts
    ITEM_COUNT = 50
    ASSET_COUNT = 25


# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleAssetsGenerator:
    def __init__(self):
        self.fake = Faker('id_ID')
        self.api = self.create_api_client()

        # Cache existing data
        self.existing_uoms = []
        self.existing_item_groups = []
        self.existing_asset_categories = []
        self.existing_warehouses = []

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
            response = self.api.request(method, url, json=data if method in [
                                        "POST", "PUT"] else None, params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API request failed: {e}")
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

    def fetch_existing_data(self):
        """Fetch existing master data to use as references"""
        logger.info("📊 Fetching existing master data...")

        try:
            self.existing_uoms = self.get_list("UOM", fields=["name"])
            self.existing_item_groups = self.get_list(
                "Item Group", fields=["name"])
            self.existing_asset_categories = self.get_list(
                "Asset Category", fields=["name"])
            self.existing_warehouses = self.get_list(
                "Warehouse", filters={"company": Config.COMPANY_NAME}, fields=["name"])

            logger.info(
                f"Found: {len(self.existing_uoms)} UOMs, {len(self.existing_item_groups)} Item Groups, {len(self.existing_asset_categories)} Asset Categories, {len(self.existing_warehouses)} Warehouses")

        except Exception as e:
            logger.warning(f"Could not fetch all master data: {e}")

    def create_basic_items(self):
        """Create basic items using existing master data"""
        logger.info("📦 Creating basic items...")

        existing_items = self.get_list(
            "Item", fields=["name", "item_code", "item_name"])

        if len(existing_items) >= Config.ITEM_COUNT:
            logger.info(f"Already have {len(existing_items)} items")
            return existing_items  # Return the existing items, not None

        items_to_create = Config.ITEM_COUNT - len(existing_items)
        created_count = 0

        # Use existing master data or defaults
        default_uom = self.existing_uoms[0]["name"] if self.existing_uoms else "Nos"
        default_item_group = self.existing_item_groups[0][
            "name"] if self.existing_item_groups else "All Item Groups"

        # Simple item categories
        simple_items = [
            "Laptop", "Desktop", "Printer", "Chair", "Desk", "Table",
            "Monitor", "Keyboard", "Mouse", "Cabinet", "Whiteboard", "Projector",
            "Phone", "Tablet", "Scanner", "Camera", "Speaker", "Headset",
            "Notebook", "Pen", "Marker", "Stapler", "Calculator", "Clock"
        ]

        for i in range(items_to_create):
            item_name = random.choice(simple_items)
            item_code = f"ITM-{item_name[:3].upper()}-{random.randint(1000, 9999)}"

            # Check if item code already exists
            if any(item.get("item_code") == item_code for item in existing_items):
                continue

            item_data = {
                "item_code": item_code,
                "item_name": f"{item_name} {random.choice(['Standard', 'Premium', 'Basic'])}",
                "item_group": default_item_group,
                "stock_uom": default_uom,
                "is_stock_item": random.choice([0, 1]),
                "is_purchase_item": 1,
                "is_sales_item": random.choice([0, 1]),
                "valuation_rate": random.randint(100_000, 10_000_000),  # IDR
                "standard_rate": random.randint(150_000, 15_000_000),   # IDR
                "description": f"Quality {item_name.lower()} for office use"
            }

            try:
                item = self.create_doc("Item", item_data)
                existing_items.append(item)
                created_count += 1

                if created_count % 10 == 0:
                    logger.info(
                        f"✅ Created {created_count}/{items_to_create} items...")

                time.sleep(0.2)
            except Exception as e:
                logger.warning(f"Failed to create item {item_code}: {e}")

        logger.info(f"Created {created_count} new items")
        return existing_items  # Always return the items list

    def create_basic_assets(self, items):
        """Create basic assets using minimal required fields"""
        logger.info("🏢 Creating basic assets...")

        existing_assets = self.get_list(
            "Asset", filters={"company": Config.COMPANY_NAME}, fields=["name", "asset_name"])

        if len(existing_assets) >= Config.ASSET_COUNT:
            logger.info(f"Already have {len(existing_assets)} assets")
            return

        assets_to_create = Config.ASSET_COUNT - len(existing_assets)
        created_count = 0

        # Use items that could be assets - fix the key access issue
        potential_asset_items = []
        for item in items:
            item_name = item.get("item_name", item.get("name", "Unknown Item"))
            if any(keyword in item_name.lower() for keyword in ["laptop", "desktop", "chair", "desk", "printer", "monitor", "projector"]):
                potential_asset_items.append(item)

        if not potential_asset_items:
            # Use first 20 items as fallback
            potential_asset_items = items[:20]

        if not potential_asset_items:
            logger.warning("No items available to create assets")
            return

        for i in range(assets_to_create):
            if not potential_asset_items:
                break

            item = random.choice(potential_asset_items)

            # Safe access to item fields
            item_code = item.get("item_code", item.get(
                "name", f"ITEM-{random.randint(1000, 9999)}"))
            item_name = item.get("item_name", item.get("name", "Asset Item"))

            asset_name = f"{item_name} Asset #{random.randint(100, 999)}"

            # Generate dates
            purchase_date = self.generate_date_in_range(
                datetime(2023, 1, 1),
                datetime(2025, 6, 1)
            )

            # Minimal asset data - only absolutely required fields
            asset_data = {
                "asset_name": asset_name,
                "item_code": item_code,
                "company": Config.COMPANY_NAME,
                "purchase_date": purchase_date,
                "available_for_use_date": purchase_date,
                # IDR
                "gross_purchase_amount": random.randint(1_000_000, 50_000_000),
                "status": "Draft",
                "asset_owner": "Company",
                "is_existing_asset": 0,
                "calculate_depreciation": 0,  # Disable to avoid complex setup
                "location": random.choice([
                    "Jakarta Office", "Surabaya Office", "Head Office",
                    "IT Department", "Finance Department", "Main Building"
                ])
            }

            try:
                asset = self.create_doc("Asset", asset_data)
                existing_assets.append(asset)
                created_count += 1

                if created_count % 5 == 0:
                    logger.info(
                        f"✅ Created {created_count}/{assets_to_create} assets...")

                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"Failed to create asset {asset_name}: {e}")
                # Try with even more minimal data
                try:
                    minimal_asset_data = {
                        "asset_name": asset_name,
                        "company": Config.COMPANY_NAME,
                        "purchase_date": purchase_date,
                        "gross_purchase_amount": random.randint(1_000_000, 10_000_000)
                    }
                    asset = self.create_doc("Asset", minimal_asset_data)
                    existing_assets.append(asset)
                    created_count += 1
                    logger.info(f"✅ Created minimal asset: {asset_name}")
                    time.sleep(0.3)
                except Exception as e2:
                    logger.error(
                        f"Failed to create minimal asset {asset_name}: {e2}")

        logger.info(f"Created {created_count} new assets")

    def generate_date_in_range(self, start_date, end_date):
        """Generate random date within range"""
        days_between = (end_date - start_date).days
        if days_between <= 0:
            return start_date.strftime("%Y-%m-%d")
        random_days = random.randint(0, days_between)
        date = start_date + timedelta(days=random_days)
        return date.strftime("%Y-%m-%d")

    def run(self):
        """Run the complete generation process"""
        logger.info("🚀 Starting Simple Assets & Stock Generation...")

        print("\n" + "="*60)
        print("📊 SIMPLE ASSETS & STOCK GENERATOR")
        print("="*60)
        print("This creates essential data with minimal dependencies:")
        print(f"- {Config.ITEM_COUNT} Items")
        print(f"- {Config.ASSET_COUNT} Assets")
        print("- Uses existing master data where possible")
        print("="*60)

        response = input("Proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("Cancelled.")
            return

        try:
            start_time = datetime.now()

            # Step 1: Get existing master data
            self.fetch_existing_data()

            # Step 2: Create items
            items = self.create_basic_items()

            # Step 3: Create assets
            self.create_basic_assets(items)

            end_time = datetime.now()
            duration = end_time - start_time

            # Final summary
            final_items = len(self.get_list("Item", fields=["name"]))
            final_assets = len(self.get_list("Asset", filters={
                               "company": Config.COMPANY_NAME}, fields=["name"]))

            print("\n" + "="*60)
            print("✅ SIMPLE GENERATION COMPLETED!")
            print("="*60)
            print(f"📊 Results:")
            print(f"   - Items: {final_items}")
            print(f"   - Assets: {final_assets}")
            print(f"⏰ Time: {duration}")
            print("="*60)
            print("🎯 Ready for:")
            print("   - Basic asset tracking")
            print("   - Item management")
            print("   - Purchase workflows")
            print("="*60)

        except Exception as e:
            logger.error(f"Generation failed: {e}")
            print(f"\n❌ Error: {e}")


def main():
    generator = SimpleAssetsGenerator()
    generator.run()


if __name__ == "__main__":
    main()

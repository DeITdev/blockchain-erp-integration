#!/usr/bin/env python3
"""
ERPNext Items Only Generator
Creates items and basic stock data without complex asset categories.
Focuses on what works reliably.
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
    ITEM_COUNT = 75
    STOCK_ENTRY_COUNT = 25


# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ItemsOnlyGenerator:
    def __init__(self):
        self.fake = Faker('id_ID')
        self.api = self.create_api_client()

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

    def create_items(self):
        """Create a variety of business items"""
        logger.info("📦 Creating Items...")

        existing_items = self.get_list("Item", fields=["name", "item_code"])

        if len(existing_items) >= Config.ITEM_COUNT:
            logger.info(f"Already have {len(existing_items)} items")
            return existing_items

        items_to_create = Config.ITEM_COUNT - len(existing_items)
        created_count = 0

        # Get existing UOM and Item Group
        uoms = self.get_list("UOM", fields=["name"])
        item_groups = self.get_list("Item Group", fields=["name"])

        default_uom = uoms[0]["name"] if uoms else "Nos"
        default_item_group = item_groups[0]["name"] if item_groups else "All Item Groups"

        # Comprehensive item list for business
        business_items = [
            # Office Equipment
            ("Laptop Dell Inspiron", "Office Equipment", True, False),
            ("Desktop Computer HP", "Office Equipment", True, False),
            ("Printer Canon", "Office Equipment", True, False),
            ("Monitor Samsung 24inch", "Office Equipment", True, False),
            ("Keyboard Logitech", "Office Equipment", True, True),
            ("Mouse Wireless", "Office Equipment", True, True),
            ("Webcam HD", "Office Equipment", True, True),
            ("Headset Bluetooth", "Office Equipment", True, True),

            # Office Furniture
            ("Executive Chair", "Office Furniture", False, False),
            ("Office Desk Wood", "Office Furniture", False, False),
            ("Meeting Table", "Office Furniture", False, False),
            ("Filing Cabinet", "Office Furniture", False, False),
            ("Whiteboard Magnetic", "Office Furniture", False, False),
            ("Bookshelf Metal", "Office Furniture", False, False),

            # Office Supplies
            ("A4 Paper Ream", "Office Supplies", True, True),
            ("Ballpoint Pen Blue", "Office Supplies", True, True),
            ("Stapler Heavy Duty", "Office Supplies", True, True),
            ("Paper Clips Box", "Office Supplies", True, True),
            ("Folder Plastic", "Office Supplies", True, True),
            ("Marker Permanent", "Office Supplies", True, True),
            ("Sticky Notes", "Office Supplies", True, True),
            ("Calculator Scientific", "Office Supplies", True, True),

            # IT Equipment
            ("Router WiFi", "IT Equipment", True, False),
            ("Network Switch", "IT Equipment", True, False),
            ("UPS Battery Backup", "IT Equipment", True, False),
            ("External Hard Drive", "IT Equipment", True, True),
            ("USB Flash Drive", "IT Equipment", True, True),
            ("HDMI Cable", "IT Equipment", True, True),
            ("Power Strip", "IT Equipment", True, True),

            # Cleaning Supplies
            ("Tissue Box", "Cleaning Supplies", True, True),
            ("Hand Sanitizer", "Cleaning Supplies", True, True),
            ("Glass Cleaner", "Cleaning Supplies", True, True),
            ("Vacuum Cleaner", "Cleaning Supplies", False, False),
            ("Trash Bin", "Cleaning Supplies", False, False),

            # Pantry Items
            ("Coffee Arabica", "Pantry", True, True),
            ("Tea Bags", "Pantry", True, True),
            ("Sugar White", "Pantry", True, True),
            ("Bottled Water", "Pantry", True, True),
            ("Paper Cups", "Pantry", True, True),
            ("Microwave Oven", "Pantry", False, False),
            ("Water Dispenser", "Pantry", False, False),

            # Security Equipment
            ("CCTV Camera", "Security", True, False),
            ("Access Card", "Security", True, True),
            ("Fire Extinguisher", "Security", False, False),
            ("Security Lock", "Security", True, True),

            # Vehicle Equipment
            ("Company Car Toyota", "Vehicles", False, False),
            ("Motorcycle Honda", "Vehicles", False, False),
            ("Car Tire", "Vehicle Parts", True, True),
            ("Engine Oil", "Vehicle Parts", True, True)
        ]

        # Create items from the list
        for i in range(min(items_to_create, len(business_items))):
            item_name, category, is_stock, is_purchase = business_items[i]
            item_code = f"{category[:3].upper()}-{random.randint(1000, 9999)}"

            # Check if item code already exists
            if any(item.get("item_code") == item_code for item in existing_items):
                item_code = f"{category[:3].upper()}-{random.randint(5000, 9999)}"

            item_data = {
                "item_code": item_code,
                "item_name": item_name,
                "item_group": default_item_group,
                "stock_uom": default_uom,
                "is_stock_item": 1 if is_stock else 0,
                "is_purchase_item": 1 if is_purchase else 0,
                "is_sales_item": random.choice([0, 1]),
                "valuation_rate": random.randint(50_000, 5_000_000),   # IDR
                "standard_rate": random.randint(75_000, 7_500_000),    # IDR
                "description": f"Quality {item_name.lower()} for business operations",
                "weight_per_unit": random.uniform(0.1, 25.0),
                "weight_uom": "Kg"
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
                # Try with minimal data
                try:
                    minimal_data = {
                        "item_code": item_code,
                        "item_name": item_name,
                        "item_group": default_item_group,
                        "stock_uom": default_uom
                    }
                    item = self.create_doc("Item", minimal_data)
                    existing_items.append(item)
                    created_count += 1
                    logger.info(f"✅ Created minimal item: {item_code}")
                    time.sleep(0.2)
                except Exception as e2:
                    logger.error(
                        f"Failed to create minimal item {item_code}: {e2}")

        logger.info(
            f"Created {created_count} new items. Total: {len(existing_items)}")
        return existing_items

    def create_stock_entries(self, items):
        """Create stock movement entries"""
        logger.info("📦 Creating Stock Entries...")

        # Get warehouses
        warehouses = self.get_list("Warehouse", filters={
                                   "company": Config.COMPANY_NAME}, fields=["name"])
        if not warehouses:
            logger.warning("No warehouses found for stock entries")
            return

        # Get stock items only
        stock_items = [item for item in items if item.get(
            "is_stock_item") == 1]
        if not stock_items:
            logger.warning("No stock items found for stock entries")
            return

        created_count = 0

        for i in range(Config.STOCK_ENTRY_COUNT):
            entry_date = self.generate_date_in_range(
                datetime(2024, 1, 1),
                datetime(2025, 6, 18)
            )

            # Simple material receipt
            entry_data = {
                "purpose": "Material Receipt",
                "company": Config.COMPANY_NAME,
                "posting_date": entry_date,
                "posting_time": f"{random.randint(8, 17):02d}:{random.randint(0, 59):02d}:00"
            }

            # Add items
            items_data = []
            target_warehouse = random.choice(warehouses)["name"]

            for _ in range(random.randint(1, 5)):
                item = random.choice(stock_items)
                items_data.append({
                    "item_code": item.get("item_code", item.get("name")),
                    "qty": random.randint(10, 100),
                    "basic_rate": random.randint(50_000, 500_000),
                    "t_warehouse": target_warehouse
                })

            entry_data["items"] = items_data

            try:
                entry = self.create_doc("Stock Entry", entry_data)
                created_count += 1

                if created_count % 5 == 0:
                    logger.info(
                        f"✅ Created {created_count}/{Config.STOCK_ENTRY_COUNT} stock entries...")

                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to create stock entry: {e}")

        logger.info(f"Created {created_count} stock entries")

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
        logger.info("🚀 Starting Items & Stock Generation...")

        print("\n" + "="*60)
        print("📦 ITEMS & STOCK GENERATOR")
        print("="*60)
        print("Creates essential business items and stock movements:")
        print(f"- {Config.ITEM_COUNT} Business Items")
        print(f"- {Config.STOCK_ENTRY_COUNT} Stock Movements")
        print("- Office equipment, furniture, supplies")
        print("- IT equipment, cleaning supplies, pantry items")
        print("- No complex asset categories")
        print("="*60)

        response = input("Proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("Cancelled.")
            return

        try:
            start_time = datetime.now()

            # Create items
            items = self.create_items()

            # Create stock movements
            self.create_stock_entries(items)

            end_time = datetime.now()
            duration = end_time - start_time

            # Final summary
            final_items = len(self.get_list("Item", fields=["name"]))

            print("\n" + "="*60)
            print("✅ ITEMS & STOCK GENERATION COMPLETED!")
            print("="*60)
            print(f"📊 Results:")
            print(f"   - Items: {final_items}")
            print(f"   - Stock Entries: Created successfully")
            print(f"⏰ Time: {duration}")
            print("="*60)
            print("🎯 Ready for:")
            print("   - Inventory management")
            print("   - Purchase workflows")
            print("   - Stock tracking")
            print("   - Business operations")
            print("="*60)

        except Exception as e:
            logger.error(f"Generation failed: {e}")
            print(f"\n❌ Error: {e}")


def main():
    generator = ItemsOnlyGenerator()
    generator.run()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Assets & Stock Module Data Generator
Creates realistic dummy data for Assets, Items, Stock Entries, and related master data.
These modules are interconnected and will be created together.
"""

import requests
import json
import random
import logging
import time
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, List, Optional

# Initialize Faker for Indonesian locale
fake = Faker('id_ID')

# Configuration


class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya"
    COMPANY_ABBR = "PFM"

    # Data Volumes
    ITEM_COUNT = 100        # Items (for stock/assets)
    ASSET_COUNT = 50        # Fixed Assets
    STOCK_ENTRY_COUNT = 30  # Stock movements
    LOCATION_COUNT = 10     # Locations for assets

    # Date ranges
    START_DATE = datetime(2023, 1, 1)  # Assets purchased over past 2 years
    END_DATE = datetime(2025, 6, 18)


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ERPNextAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

    def _make_request(self, method: str, endpoint: str, data=None, retry_count: int = 0):
        url = f"{self.base_url}/api/{endpoint}"
        try:
            response = self.session.request(method, url, json=data if method in [
                                            "POST", "PUT"] else None, params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if retry_count < 3:
                logger.warning(
                    f"Request failed, retrying... ({retry_count + 1}/3)")
                time.sleep(1)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(f"Request failed after 3 attempts: {e}")
                raise

    def get_list(self, doctype: str, filters=None, fields=None):
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def create_doc(self, doctype: str, data: Dict):
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def check_exists(self, doctype: str, name: str):
        try:
            self._make_request("GET", f"resource/{doctype}/{name}")
            return True
        except:
            return False


class AssetsStockGenerator:
    def __init__(self):
        self.fake = Faker('id_ID')
        self.api = ERPNextAPI()

        # Data caches
        self.locations = []
        self.asset_categories = []
        self.item_groups = []
        self.warehouses = []
        self.items = []
        self.assets = []
        self.suppliers = []
        self.asset_accounts = {}  # For asset category accounts

        # Asset and stock data for Indonesia
        self.indonesian_locations = [
            "Jakarta Pusat", "Jakarta Selatan", "Jakarta Utara", "Jakarta Barat", "Jakarta Timur",
            "Surabaya", "Bandung", "Medan", "Bekasi", "Tangerang", "Depok", "Semarang",
            "Palembang", "Makassar", "Batam", "Denpasar", "Balikpapan", "Banjarmasin"
        ]

        self.asset_types = {
            "Computer Equipment": ["Laptop", "Desktop PC", "Server", "Monitor", "Printer", "Scanner"],
            "Office Furniture": ["Desk", "Chair", "Cabinet", "Table", "Bookshelf", "Sofa"],
            "Vehicles": ["Sedan", "SUV", "Truck", "Motorcycle", "Van", "Bus"],
            "Machinery": ["Generator", "Compressor", "Pump", "Motor", "Conveyor", "Crane"],
            "Building Infrastructure": ["HVAC System", "Security System", "Fire System", "Elevator"]
        }

    def generate_date_in_range(self, start_date: datetime, end_date: datetime) -> str:
        days_between = (end_date - start_date).days
        if days_between <= 0:
            return start_date.strftime("%Y-%m-%d")
        random_days = random.randint(0, days_between)
        date = start_date + timedelta(days=random_days)
        return date.strftime("%Y-%m-%d")

    def ensure_locations(self):
        """Create location master data"""
        logger.info("🏢 Creating Locations...")

        existing_locations = self.api.get_list(
            "Location", fields=["name", "location_name"])
        self.locations = existing_locations

        if len(self.locations) >= Config.LOCATION_COUNT:
            logger.info(f"Already have {len(self.locations)} locations")
            return

        locations_to_create = Config.LOCATION_COUNT - len(self.locations)
        created_count = 0

        for i in range(locations_to_create):
            location_name = random.choice(self.indonesian_locations)

            # Make unique by adding building/floor info
            unique_location = f"{location_name} - Building {chr(65 + i)} Floor {random.randint(1, 10)}"

            # Check if exists
            if any(loc.get("location_name") == unique_location for loc in self.locations):
                continue

            location_data = {
                "location_name": unique_location,
                "is_group": 0,
                "parent_location": None
            }

            try:
                location = self.api.create_doc("Location", location_data)
                self.locations.append(location)
                created_count += 1
                logger.info(f"✅ Created location: {unique_location}")
                time.sleep(0.2)
            except Exception as e:
                logger.warning(
                    f"Failed to create location {unique_location}: {e}")

        logger.info(
            f"Created {created_count} new locations. Total: {len(self.locations)}")

    def ensure_uoms(self):
        """Create Unit of Measures"""
        logger.info("📏 Creating Unit of Measures...")

        existing_uoms = self.api.get_list("UOM", fields=["name", "uom_name"])
        existing_uom_names = {uom.get("uom_name") for uom in existing_uoms}

        uoms_to_ensure = [
            "Nos", "Pcs", "Unit", "Set", "Kg", "Meter", "Liter", "Box", "Pack"
        ]
        created_count = 0

        for uom_name in uoms_to_ensure:
            if uom_name in existing_uom_names:
                continue

            uom_data = {
                "uom_name": uom_name,
                "name": uom_name
            }

            try:
                uom = self.api.create_doc("UOM", uom_data)
                created_count += 1
                logger.info(f"✅ Created UOM: {uom_name}")
                time.sleep(0.2)
            except Exception as e:
                logger.warning(f"Failed to create UOM {uom_name}: {e}")

        logger.info(f"Created {created_count} new UOMs")

    def fetch_accounts(self):
        """Fetch existing accounts for asset categories"""
        logger.info("💰 Fetching Accounts...")

        try:
            accounts = self.api.get_list("Account",
                                         filters={
                                             "company": Config.COMPANY_NAME},
                                         fields=["name", "account_name", "account_type"])

            # Find asset-related accounts
            self.asset_accounts = {}
            for account in accounts:
                if "Asset" in account.get("account_name", ""):
                    if "Accumulated Depreciation" in account.get("account_name", ""):
                        self.asset_accounts["accumulated_depreciation"] = account["name"]
                    elif "Depreciation Expense" in account.get("account_name", ""):
                        self.asset_accounts["depreciation_expense"] = account["name"]
                    else:
                        self.asset_accounts["fixed_asset"] = account["name"]

            # Fallback to generic accounts if specific ones not found
            if not self.asset_accounts:
                logger.warning(
                    "No specific asset accounts found, using generic accounts")
                for account in accounts:
                    if account.get("account_type") == "Asset":
                        self.asset_accounts["fixed_asset"] = account["name"]
                        break

            logger.info(
                f"Found asset accounts: {list(self.asset_accounts.keys())}")

        except Exception as e:
            logger.error(f"Failed to fetch accounts: {e}")
            self.asset_accounts = {}
        """Create asset category master data"""
        logger.info("📁 Creating Asset Categories...")

        existing_categories = self.api.get_list(
            "Asset Category", fields=["name", "asset_category_name"])
        self.asset_categories = existing_categories

        categories_to_ensure = list(self.asset_types.keys())
        created_count = 0

        for category_name in categories_to_ensure:
            if any(cat.get("asset_category_name") == category_name for cat in self.asset_categories):
                continue

            category_data = {
                "asset_category_name": category_name,
                "depreciation_method": "Straight Line",
                "total_number_of_depreciations": random.randint(3, 10),
                "frequency_of_depreciation": 12,  # Monthly
                "company": Config.COMPANY_NAME
            }

            try:
                category = self.api.create_doc("Asset Category", category_data)
                self.asset_categories.append(category)
                created_count += 1
                logger.info(f"✅ Created asset category: {category_name}")
                time.sleep(0.2)
            except Exception as e:
                logger.warning(
                    f"Failed to create asset category {category_name}: {e}")

        logger.info(f"Created {created_count} new asset categories")

    def ensure_item_groups(self):
        """Create item group master data"""
        logger.info("📦 Creating Item Groups...")

        existing_groups = self.api.get_list(
            "Item Group", fields=["name", "item_group_name"])
        self.item_groups = existing_groups

        groups_to_ensure = [
            "Raw Materials", "Finished Goods", "Components", "Consumables",
            "Assets", "Services", "Tools", "Software"
        ]
        created_count = 0

        for group_name in groups_to_ensure:
            if any(grp.get("item_group_name") == group_name for grp in self.item_groups):
                continue

            group_data = {
                "item_group_name": group_name,
                "is_group": 0,
                "parent_item_group": "All Item Groups"
            }

            try:
                group = self.api.create_doc("Item Group", group_data)
                self.item_groups.append(group)
                created_count += 1
                logger.info(f"✅ Created item group: {group_name}")
                time.sleep(0.2)
            except Exception as e:
                logger.warning(
                    f"Failed to create item group {group_name}: {e}")

        logger.info(f"Created {created_count} new item groups")

    def ensure_warehouses(self):
        """Create warehouse master data"""
        logger.info("🏭 Creating Warehouses...")

        existing_warehouses = self.api.get_list("Warehouse",
                                                filters={
                                                    "company": Config.COMPANY_NAME},
                                                fields=["name", "warehouse_name"])
        self.warehouses = existing_warehouses

        warehouses_to_ensure = [
            "Main Store", "Raw Material Store", "Finished Goods Store",
            "Work In Progress", "Assets Store", "Rejected Store"
        ]
        created_count = 0

        for warehouse_base in warehouses_to_ensure:
            warehouse_name = f"{warehouse_base} - {Config.COMPANY_ABBR}"

            if any(wh.get("warehouse_name") == warehouse_name for wh in self.warehouses):
                continue

            warehouse_data = {
                "warehouse_name": warehouse_name,
                "company": Config.COMPANY_NAME,
                "is_group": 0
            }

            try:
                warehouse = self.api.create_doc("Warehouse", warehouse_data)
                self.warehouses.append(warehouse)
                created_count += 1
                logger.info(f"✅ Created warehouse: {warehouse_name}")
                time.sleep(0.2)
            except Exception as e:
                logger.warning(
                    f"Failed to create warehouse {warehouse_name}: {e}")

        logger.info(f"Created {created_count} new warehouses")

    def fetch_suppliers(self):
        """Fetch existing suppliers for purchase data"""
        logger.info("👥 Fetching Suppliers...")

        existing_suppliers = self.api.get_list(
            "Supplier", fields=["name", "supplier_name"])
        self.suppliers = existing_suppliers

        if not self.suppliers:
            logger.warning("No suppliers found. Creating basic suppliers...")
            self.create_basic_suppliers()

        logger.info(f"Found {len(self.suppliers)} suppliers")

    def create_basic_suppliers(self):
        """Create basic suppliers if none exist"""
        basic_suppliers = [
            "PT Supplier Teknologi", "CV Mebel Indonesia", "PT Otomotif Jaya",
            "CV Elektronik Mandiri", "PT Supplier Umum"
        ]

        for supplier_name in basic_suppliers:
            supplier_data = {
                "supplier_name": supplier_name,
                "supplier_type": "Company"
            }

            try:
                supplier = self.api.create_doc("Supplier", supplier_data)
                self.suppliers.append(supplier)
                time.sleep(0.2)
            except Exception as e:
                logger.warning(
                    f"Failed to create supplier {supplier_name}: {e}")

    def create_items(self):
        """Create item master data"""
        logger.info("📱 Creating Items...")

        existing_items = self.api.get_list(
            "Item", fields=["name", "item_code", "item_name", "item_group"])
        self.items = existing_items

        if len(self.items) >= Config.ITEM_COUNT:
            logger.info(f"Already have {len(self.items)} items")
            return

        items_to_create = Config.ITEM_COUNT - len(self.items)
        created_count = 0

        # Get available UOMs
        available_uoms = self.api.get_list("UOM", fields=["name"])
        uom_names = [uom["name"]
                     for uom in available_uoms] if available_uoms else ["Nos"]

        # Get available asset categories
        available_categories = [cat.get(
            "asset_category_name") for cat in self.asset_categories if cat.get("asset_category_name")]

        # Create items for different categories
        for i in range(items_to_create):
            # Choose category and specific item
            category = random.choice(list(self.asset_types.keys()))
            specific_items = self.asset_types[category]
            specific_item = random.choice(specific_items)

            item_code = f"{category[:3].upper()}-{specific_item[:3].upper()}-{random.randint(1000, 9999)}"
            item_name = f"{specific_item} {random.choice(['Pro', 'Standard', 'Premium', 'Basic', 'Advanced'])}"

            # Check if item code exists
            if any(item.get("item_code") == item_code for item in self.items):
                continue

            # Determine item properties
            is_asset_item = category in [
                "Computer Equipment", "Office Furniture", "Vehicles", "Machinery"]
            is_stock_item = random.choice([0, 1]) if not is_asset_item else 0

            item_group = "Assets" if is_asset_item else random.choice([
                "Raw Materials", "Finished Goods", "Components", "Consumables"
            ])

            # Basic item data
            item_data = {
                "item_code": item_code,
                "item_name": item_name,
                "item_group": item_group,
                "stock_uom": random.choice(uom_names),
                "is_stock_item": is_stock_item,
                "is_purchase_item": 1,
                "is_sales_item": random.choice([0, 1]),
                "valuation_rate": random.randint(100_000, 50_000_000),  # IDR
                "standard_rate": random.randint(150_000, 75_000_000),   # IDR
                "description": f"High quality {specific_item.lower()} for business operations",
                "weight_per_unit": random.uniform(0.1, 50.0),
                "weight_uom": "Kg"
            }

            # Add asset-specific fields only if we have asset categories
            if is_asset_item and available_categories and category in available_categories:
                item_data["is_fixed_asset"] = 1
                item_data["asset_category"] = category
            else:
                item_data["is_fixed_asset"] = 0

            try:
                item = self.api.create_doc("Item", item_data)
                self.items.append(item)
                created_count += 1

                if created_count % 10 == 0:
                    logger.info(
                        f"✅ Created {created_count}/{items_to_create} items...")

                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"Failed to create item {item_code}: {e}")
                # Try with minimal data
                try:
                    minimal_item_data = {
                        "item_code": item_code,
                        "item_name": item_name,
                        "item_group": "All Item Groups",  # Use default group
                        "stock_uom": uom_names[0] if uom_names else "Nos"
                    }
                    item = self.api.create_doc("Item", minimal_item_data)
                    self.items.append(item)
                    created_count += 1
                    logger.info(f"✅ Created minimal item: {item_code}")
                    time.sleep(0.3)
                except Exception as e2:
                    logger.error(
                        f"Failed to create minimal item {item_code}: {e2}")

        logger.info(
            f"Created {created_count} new items. Total: {len(self.items)}")

    def create_assets(self):
        """Create fixed assets"""
        logger.info("🏭 Creating Assets...")

        existing_assets = self.api.get_list("Asset",
                                            filters={
                                                "company": Config.COMPANY_NAME},
                                            fields=["name", "asset_name", "item_code"])
        self.assets = existing_assets

        if len(self.assets) >= Config.ASSET_COUNT:
            logger.info(f"Already have {len(self.assets)} assets")
            return

        # Get asset items
        asset_items = [item for item in self.items if item.get(
            "is_fixed_asset") == 1]
        if not asset_items:
            logger.warning(
                "No asset items found. Creating assets with available items.")
            asset_items = random.sample(self.items, min(20, len(self.items)))

        assets_to_create = Config.ASSET_COUNT - len(self.assets)
        created_count = 0

        for i in range(assets_to_create):
            # Choose random item and category
            item = random.choice(asset_items)
            category = random.choice(
                [cat["asset_category_name"] for cat in self.asset_categories])
            location = random.choice(self.locations)[
                "location_name"] if self.locations else "Head Office"
            supplier = random.choice(
                self.suppliers) if self.suppliers else None

            asset_name = f"{item['item_name']} #{random.randint(1000, 9999)}"

            # Purchase dates
            purchase_date = self.generate_date_in_range(
                Config.START_DATE, Config.END_DATE)
            available_date = self.generate_date_in_range(
                datetime.strptime(purchase_date, "%Y-%m-%d"),
                datetime.strptime(purchase_date, "%Y-%m-%d") +
                timedelta(days=30)
            )

            # Financial details
            gross_amount = random.randint(1_000_000, 100_000_000)  # IDR

            asset_data = {
                "asset_name": asset_name,
                "item_code": item["item_code"],
                "asset_category": category,
                "location": location,
                "company": Config.COMPANY_NAME,
                "supplier": supplier["name"] if supplier else None,

                # Purchase Information
                "purchase_date": purchase_date,
                "available_for_use_date": available_date,
                "gross_purchase_amount": gross_amount,
                "purchase_amount": gross_amount,
                "asset_quantity": 1,

                # Status and Ownership
                "status": "Draft",
                "asset_owner": "Company",
                "asset_owner_company": Config.COMPANY_NAME,
                "is_existing_asset": 0,

                # Depreciation
                "calculate_depreciation": 1,
                "depreciation_method": "Straight Line",
                "total_number_of_depreciations": random.randint(3, 8),
                "frequency_of_depreciation": 12,

                # Additional details
                "custodian": None,  # Can be assigned later
                "department": random.choice(["IT", "Finance", "Operations", "HR", "Management"]),
                "cost_center": f"Main - {Config.COMPANY_ABBR}",

                # Insurance (optional)
                "insurance_start_date": purchase_date if random.choice([True, False]) else None,
                "insurance_end_date": self.generate_date_in_range(
                    datetime.strptime(
                        purchase_date, "%Y-%m-%d") + timedelta(days=365),
                    datetime.strptime(
                        purchase_date, "%Y-%m-%d") + timedelta(days=1095)
                ) if random.choice([True, False]) else None,

                # Maintenance
                "maintenance_required": random.choice([0, 1])
            }

            try:
                asset = self.api.create_doc("Asset", asset_data)
                self.assets.append(asset)
                created_count += 1

                if created_count % 5 == 0:
                    logger.info(
                        f"✅ Created {created_count}/{assets_to_create} assets...")

                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to create asset {asset_name}: {e}")

        logger.info(
            f"Created {created_count} new assets. Total: {len(self.assets)}")

    def create_stock_entries(self):
        """Create stock movement entries"""
        logger.info("📦 Creating Stock Entries...")

        # Get stock items only
        stock_items = [item for item in self.items if item.get(
            "is_stock_item") == 1]
        if not stock_items:
            logger.warning("No stock items found for stock entries")
            return

        if not self.warehouses:
            logger.warning("No warehouses found for stock entries")
            return

        created_count = 0
        purposes = ["Material Receipt", "Material Issue",
                    "Material Transfer", "Repack"]

        for i in range(Config.STOCK_ENTRY_COUNT):
            purpose = random.choice(purposes)
            entry_date = self.generate_date_in_range(
                Config.START_DATE, Config.END_DATE)

            entry_data = {
                "purpose": purpose,
                "company": Config.COMPANY_NAME,
                "posting_date": entry_date,
                "posting_time": f"{random.randint(8, 17):02d}:{random.randint(0, 59):02d}:00"
            }

            # Create items based on purpose
            items_data = []

            if purpose == "Material Receipt":
                # Receiving stock
                warehouse = random.choice(
                    [w for w in self.warehouses if "Store" in w["warehouse_name"]])

                for _ in range(random.randint(1, 5)):
                    item = random.choice(stock_items)
                    items_data.append({
                        "item_code": item["item_code"],
                        "qty": random.randint(10, 100),
                        "basic_rate": item.get("valuation_rate", 100_000),
                        "t_warehouse": warehouse["warehouse_name"]
                    })

            elif purpose == "Material Issue":
                # Issuing stock
                warehouse = random.choice(self.warehouses)

                for _ in range(random.randint(1, 3)):
                    item = random.choice(stock_items)
                    items_data.append({
                        "item_code": item["item_code"],
                        "qty": random.randint(1, 20),
                        "s_warehouse": warehouse["warehouse_name"]
                    })

            elif purpose == "Material Transfer":
                # Transfer between warehouses
                if len(self.warehouses) >= 2:
                    source_wh, target_wh = random.sample(self.warehouses, 2)

                    for _ in range(random.randint(1, 3)):
                        item = random.choice(stock_items)
                        items_data.append({
                            "item_code": item["item_code"],
                            "qty": random.randint(5, 30),
                            "s_warehouse": source_wh["warehouse_name"],
                            "t_warehouse": target_wh["warehouse_name"]
                        })

            if items_data:
                entry_data["items"] = items_data

                try:
                    entry = self.api.create_doc("Stock Entry", entry_data)
                    created_count += 1

                    if created_count % 5 == 0:
                        logger.info(
                            f"✅ Created {created_count}/{Config.STOCK_ENTRY_COUNT} stock entries...")

                    time.sleep(0.5)
                except Exception as e:
                    logger.warning(
                        f"Failed to create stock entry ({purpose}): {e}")

        logger.info(f"Created {created_count} stock entries")

    def generate_all_data(self):
        """Main method to generate all assets and stock data"""
        logger.info("🚀 Starting Assets & Stock Module Data Generation...")

        print("\n" + "="*80)
        print("📊 ERPNext Assets & Stock Module Data Generator")
        print("="*80)
        print(f"Target Data:")
        print(f"- Items: {Config.ITEM_COUNT}")
        print(f"- Assets: {Config.ASSET_COUNT}")
        print(f"- Stock Entries: {Config.STOCK_ENTRY_COUNT}")
        print(f"- Locations: {Config.LOCATION_COUNT}")
        print(f"- Company: {Config.COMPANY_NAME}")
        print("="*80)

        response = input("\nProceed with data generation? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled.")
            return

        try:
            start_time = datetime.now()

            # Phase 1: Master Data
            logger.info("=== Phase 1: Creating Master Data ===")
            self.ensure_uoms()              # Create UOMs first
            self.fetch_accounts()           # Get accounts for asset categories
            self.ensure_locations()
            self.ensure_asset_categories()  # Now with proper accounts
            self.ensure_item_groups()
            self.ensure_warehouses()
            self.fetch_suppliers()

            # Phase 2: Items
            logger.info("=== Phase 2: Creating Items ===")
            self.create_items()

            # Phase 3: Assets
            logger.info("=== Phase 3: Creating Assets ===")
            self.create_assets()

            # Phase 4: Stock Movements
            logger.info("=== Phase 4: Creating Stock Entries ===")
            self.create_stock_entries()

            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("=== Assets & Stock Data Generation Complete ===")

            # Final summary
            final_items = len(self.api.get_list("Item", fields=["name"]))
            final_assets = len(self.api.get_list("Asset", filters={
                               "company": Config.COMPANY_NAME}, fields=["name"]))
            final_locations = len(self.api.get_list(
                "Location", fields=["name"]))

            print("\n" + "="*80)
            print("✅ ASSETS & STOCK DATA GENERATION COMPLETED!")
            print("="*80)
            print(f"📊 Final Summary:")
            print(f"   - Items: {final_items}")
            print(f"   - Assets: {final_assets}")
            print(f"   - Locations: {final_locations}")
            print(f"   - Warehouses: {len(self.warehouses)}")
            print(f"   - Asset Categories: {len(self.asset_categories)}")
            print(f"⏰ Total time: {duration}")
            print("="*80)
            print("🎯 Ready for:")
            print("   - Asset management and tracking")
            print("   - Stock movements and inventory")
            print("   - Depreciation calculations")
            print("   - Purchase and procurement workflows")
            print("="*80)

        except Exception as e:
            logger.error(f"Fatal error during data generation: {e}")
            raise


def main():
    """Main entry point"""
    try:
        generator = AssetsStockGenerator()
        generator.generate_all_data()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()

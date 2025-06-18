#!/usr/bin/env python3
"""
Create Assets from Existing Items
Simple script that creates assets using existing items without complex dependencies.
"""

import requests
import json
import random
import logging
import time
from datetime import datetime, timedelta

# Configuration


class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya"
    ASSET_COUNT = 30


# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AssetFromItemsGenerator:
    def __init__(self):
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

    def generate_date_in_range(self, start_date, end_date):
        """Generate random date within range"""
        days_between = (end_date - start_date).days
        if days_between <= 0:
            return start_date.strftime("%Y-%m-%d")
        random_days = random.randint(0, days_between)
        date = start_date + timedelta(days=random_days)
        return date.strftime("%Y-%m-%d")

    def create_assets_from_items(self):
        """Create assets using existing items - no asset categories required"""
        logger.info("🏢 Creating Assets from existing Items...")

        # Get existing items
        all_items = self.get_list(
            "Item", fields=["name", "item_code", "item_name", "valuation_rate"])
        logger.info(f"Found {len(all_items)} total items")

        # Get existing assets
        existing_assets = self.get_list(
            "Asset", filters={"company": Config.COMPANY_NAME}, fields=["name", "asset_name"])
        logger.info(f"Found {len(existing_assets)} existing assets")

        if len(existing_assets) >= Config.ASSET_COUNT:
            logger.info(
                f"Already have {len(existing_assets)} assets (>= target {Config.ASSET_COUNT})")
            return

        assets_to_create = Config.ASSET_COUNT - len(existing_assets)
        logger.info(f"Creating {assets_to_create} new assets...")

        # Filter items that could be assets (equipment, furniture, vehicles, etc.)
        asset_keywords = ["laptop", "desktop", "computer", "printer", "monitor", "chair", "desk",
                          "table", "cabinet", "projector", "camera", "scanner", "server", "router",
                          "vehicle", "car", "motorcycle", "generator", "machinery", "equipment"]

        potential_asset_items = []
        for item in all_items:
            item_name = item.get("item_name", item.get("name", "")).lower()
            if any(keyword in item_name for keyword in asset_keywords):
                potential_asset_items.append(item)

        # If no specific asset items found, use random items
        if not potential_asset_items:
            potential_asset_items = random.sample(
                all_items, min(50, len(all_items)))
            logger.info(
                f"No specific asset items found, using {len(potential_asset_items)} random items")
        else:
            logger.info(
                f"Found {len(potential_asset_items)} potential asset items")

        created_count = 0

        for i in range(assets_to_create):
            if not potential_asset_items:
                logger.warning("No more items available for asset creation")
                break

            # Select random item
            item = random.choice(potential_asset_items)

            # Safe field access
            item_code = item.get("item_code", item.get(
                "name", f"ITEM-{random.randint(1000, 9999)}"))
            item_name = item.get("item_name", item.get("name", "Asset Item"))
            valuation_rate = item.get(
                "valuation_rate", random.randint(1_000_000, 10_000_000))

            # Create unique asset name
            asset_name = f"{item_name} - Asset #{random.randint(1000, 9999)}"

            # Generate purchase date
            purchase_date = self.generate_date_in_range(
                datetime(2022, 1, 1),
                datetime(2025, 6, 1)
            )

            # Calculate purchase amount based on valuation
            if valuation_rate and valuation_rate > 0:
                purchase_amount = int(
                    valuation_rate * random.uniform(0.8, 1.2))
            else:
                purchase_amount = random.randint(1_000_000, 20_000_000)

            # Create asset with MINIMAL required fields only
            asset_data = {
                "asset_name": asset_name,
                "company": Config.COMPANY_NAME,
                "purchase_date": purchase_date,
                "gross_purchase_amount": purchase_amount,
                "asset_owner": "Company",
                "is_existing_asset": 0,
                "calculate_depreciation": 0,  # Disable depreciation to avoid complexity
                "location": random.choice([
                    "Jakarta Office", "Surabaya Office", "Head Office",
                    "IT Department", "Finance Department", "Operations",
                    "Main Building Floor 1", "Main Building Floor 2"
                ])
            }

            # Only add item_code if the item exists
            if item_code and item_code != "ITEM-":
                asset_data["item_code"] = item_code

            try:
                asset = self.create_doc("Asset", asset_data)
                created_count += 1
                logger.info(f"✅ Created asset: {asset_name}")

                if created_count % 5 == 0:
                    logger.info(
                        f"Progress: {created_count}/{assets_to_create} assets created")

                time.sleep(0.3)  # Delay to avoid overwhelming server

            except Exception as e:
                logger.warning(f"Failed to create asset {asset_name}: {e}")

                # Try with even more minimal data
                try:
                    minimal_asset_data = {
                        "asset_name": asset_name,
                        "company": Config.COMPANY_NAME,
                        "purchase_date": purchase_date,
                        "gross_purchase_amount": purchase_amount
                    }
                    asset = self.create_doc("Asset", minimal_asset_data)
                    created_count += 1
                    logger.info(f"✅ Created minimal asset: {asset_name}")
                    time.sleep(0.3)
                except Exception as e2:
                    logger.error(
                        f"Failed to create minimal asset {asset_name}: {e2}")

        logger.info(f"Successfully created {created_count} new assets!")
        return created_count

    def run(self):
        """Run the asset generation process"""
        logger.info("🚀 Starting Asset Generation from Items...")

        print("\n" + "="*70)
        print("🏢 ASSET GENERATOR (From Existing Items)")
        print("="*70)
        print("Creates assets using your existing items:")
        print(f"- Target: {Config.ASSET_COUNT} assets")
        print("- Uses existing items as asset templates")
        print("- No complex asset categories required")
        print("- Minimal field requirements")
        print("- Realistic purchase dates and amounts")
        print("="*70)

        response = input("Proceed with asset creation? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled.")
            return

        try:
            start_time = datetime.now()

            # Create assets
            created_count = self.create_assets_from_items()

            end_time = datetime.now()
            duration = end_time - start_time

            # Final summary
            total_assets = len(self.get_list("Asset", filters={
                               "company": Config.COMPANY_NAME}, fields=["name"]))

            print("\n" + "="*70)
            print("✅ ASSET GENERATION COMPLETED!")
            print("="*70)
            print(f"📊 Results:")
            print(f"   - New assets created: {created_count}")
            print(f"   - Total assets: {total_assets}")
            print(f"⏰ Generation time: {duration}")
            print("="*70)
            print("🎯 Your assets are ready for:")
            print("   - Asset tracking and management")
            print("   - Location assignment")
            print("   - Purchase record keeping")
            print("   - Basic asset reporting")
            print("="*70)

        except Exception as e:
            logger.error(f"Asset generation failed: {e}")
            print(f"\n❌ Error: {e}")


def main():
    generator = AssetFromItemsGenerator()
    generator.run()


if __name__ == "__main__":
    main()

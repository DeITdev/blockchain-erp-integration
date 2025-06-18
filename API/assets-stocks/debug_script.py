#!/usr/bin/env python3
"""
Debug Items Script - Investigates item data structure
"""

import requests
import json


class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya"


def debug_items():
    """Debug items to understand the data structure"""

    # Create API client
    session = requests.Session()
    session.headers.update({
        'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    })

    def make_request(method, endpoint, data=None):
        url = f"{Config.BASE_URL}/api/{endpoint}"
        try:
            response = session.request(method, url, json=data if method in [
                "POST", "PUT"] else None, params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response content: {e.response.text}")
            raise

    def get_list(doctype, filters=None, fields=None):
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return make_request("GET", f"resource/{doctype}", params).get("data", [])

    def get_doc(doctype, name):
        return make_request("GET", f"resource/{doctype}/{name}")

    print("="*60)
    print("🔍 DEBUGGING ITEMS AND LOCATIONS")
    print("="*60)

    # 1. Check all items
    print("\n1. Fetching all items...")
    all_items = get_list(
        "Item", fields=["name", "item_code", "item_name", "disabled"])
    print(f"Found {len(all_items)} total items")

    # Show first 10 items with their actual structure
    print("\n📦 First 10 items structure:")
    for i, item in enumerate(all_items[:10]):
        print(f"Item {i+1}: {json.dumps(item, indent=2)}")
        print("-" * 40)

    # 2. Check items with None or empty item_code
    print("\n2. Checking for problematic items...")
    problematic_items = []
    valid_items = []

    for item in all_items:
        item_code = item.get("item_code")
        item_name = item.get("item_name") or item.get("name")

        if not item_code or str(item_code).strip() == "" or str(item_code).lower() == "none":
            problematic_items.append(item)
        else:
            valid_items.append(item)

    print(
        f"❌ Problematic items (None/empty item_code): {len(problematic_items)}")
    print(f"✅ Valid items: {len(valid_items)}")

    if problematic_items:
        print("\n🚨 Problematic items sample:")
        for item in problematic_items[:5]:
            print(f"   {item}")

    # 3. Test a few valid items
    print("\n3. Testing valid items...")
    if valid_items:
        for i, item in enumerate(valid_items[:3]):
            item_code = item.get("item_code") or item.get("name")
            print(f"\nTesting item {i+1}: {item_code}")
            try:
                full_item = get_doc("Item", item_code)
                print(
                    f"   ✅ Successfully fetched item: {full_item.get('item_name')}")
            except Exception as e:
                print(f"   ❌ Failed to fetch item: {e}")

    # 4. Check locations
    print("\n4. Checking existing locations...")
    try:
        locations = get_list("Location", fields=["name", "location_name"])
        print(f"Found {len(locations)} locations:")
        for loc in locations:
            loc_name = loc.get("location_name") or loc.get("name")
            print(f"   - {loc_name}")
    except Exception as e:
        print(f"Failed to fetch locations: {e}")

    # 5. Check existing assets for reference
    print("\n5. Checking existing assets...")
    try:
        assets = get_list("Asset", filters={"company": Config.COMPANY_NAME},
                          fields=["name", "asset_name", "item_code", "location"])
        print(f"Found {len(assets)} existing assets:")
        for i, asset in enumerate(assets[:3]):
            print(f"   Asset {i+1}: {asset}")
    except Exception as e:
        print(f"Failed to fetch assets: {e}")

    print("\n" + "="*60)
    print("🎯 RECOMMENDATIONS:")
    print("="*60)

    if problematic_items:
        print(
            f"1. Fix {len(problematic_items)} items with None/empty item_code")
        print("   - These items cannot be used for asset creation")

    if valid_items:
        print(f"2. Use {len(valid_items)} valid items for asset creation")
        print("   - These items have proper item_code values")

    if locations:
        print(
            f"3. Use existing locations: {[loc.get('location_name') or loc.get('name') for loc in locations]}")
    else:
        print("3. Create basic locations first before creating assets")

    print("\n✅ Debug analysis complete!")


if __name__ == "__main__":
    debug_items()

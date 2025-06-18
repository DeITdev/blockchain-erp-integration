#!/usr/bin/env python3
"""
ERPNext CRM Debug Script
Tests Lead creation and Opportunity linking to identify the root cause
"""

import requests
import json
import time
from datetime import datetime, timedelta


class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya"


class ERPNextAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

    def _make_request(self, method: str, endpoint: str, data=None):
        url = f"{self.base_url}/api/{endpoint}"
        response = self.session.request(method, url, json=data if method in [
                                        "POST", "PUT"] else None, params=data if method == "GET" else None)
        print(f"Request: {method} {url}")
        print(f"Status: {response.status_code}")
        if response.status_code >= 400:
            print(f"Error Response: {response.text}")
        response.raise_for_status()
        return response.json()

    def get_list(self, doctype: str, filters=None, fields=None):
        params = {"limit_page_length": 1000}
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = json.dumps(fields)
        return self._make_request("GET", "resource/" + doctype, params).get("data", [])

    def get_doc(self, doctype: str, name: str):
        return self._make_request("GET", f"resource/{doctype}/{name}")

    def create_doc(self, doctype: str, data: dict):
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)


def debug_crm_linking():
    """Debug CRM linking issues step by step"""
    api = ERPNextAPI()

    print("=" * 60)
    print("CRM DEBUG - Testing Lead to Opportunity Linking")
    print("=" * 60)

    # Step 1: Check existing leads
    print("\n1. Checking existing leads...")
    existing_leads = api.get_list(
        "Lead", filters={"company": Config.COMPANY_NAME}, fields=["name", "lead_name"])
    print(f"Found {len(existing_leads)} existing leads")

    if existing_leads:
        print("Sample leads:")
        for i, lead in enumerate(existing_leads[:3]):
            print(f"  - {lead}")

        # Test with an existing lead
        test_lead = existing_leads[0]
        test_lead_name = test_lead["lead_name"]
        test_doc_name = test_lead["name"]

        print(
            f"\n2. Testing with existing lead: '{test_lead_name}' (doc: {test_doc_name})")

        # Try to fetch the full lead document
        try:
            full_lead = api.get_doc("Lead", test_doc_name)
            print(f"✅ Successfully fetched full lead document")
            print(f"   - Name: {full_lead.get('name')}")
            print(f"   - Lead Name: {full_lead.get('lead_name')}")
            print(f"   - Company: {full_lead.get('company')}")
            print(f"   - Status: {full_lead.get('status')}")
        except Exception as e:
            print(f"❌ Failed to fetch full lead document: {e}")
            return

        # Step 3: Try creating an opportunity
        print(
            f"\n3. Attempting to create opportunity for '{test_lead_name}'...")

        opportunity_data = {
            "opportunity_from": "Lead",
            "party_name": test_lead_name,
            "company": Config.COMPANY_NAME,
            "opportunity_type": "Sales",
            "status": "Open"
        }

        print(f"Opportunity data: {json.dumps(opportunity_data, indent=2)}")

        try:
            opportunity = api.create_doc("Opportunity", opportunity_data)
            print(f"✅ Successfully created opportunity!")
            print(f"   - Name: {opportunity.get('name')}")
            print(f"   - Party Name: {opportunity.get('party_name')}")
        except Exception as e:
            print(f"❌ Failed to create opportunity: {e}")

            # Try alternative approaches
            print(f"\n4. Trying alternative approaches...")

            # Approach 1: Use document name as party_name
            print(
                f"4a. Trying with document name '{test_doc_name}' as party_name...")
            opportunity_data_alt1 = opportunity_data.copy()
            opportunity_data_alt1["party_name"] = test_doc_name
            try:
                opportunity = api.create_doc(
                    "Opportunity", opportunity_data_alt1)
                print(f"✅ Success with document name as party_name!")
                print(f"   - Name: {opportunity.get('name')}")
            except Exception as e:
                print(f"❌ Failed with document name: {e}")

            # Approach 2: Check if Lead is in draft state
            print(f"\n4b. Checking lead status...")
            if full_lead.get('docstatus') == 0:
                print("⚠️ Lead is in draft state (docstatus=0)")
                print("   Opportunities might require submitted leads")
            elif full_lead.get('docstatus') == 1:
                print("✅ Lead is submitted (docstatus=1)")
            else:
                print(f"❓ Lead docstatus: {full_lead.get('docstatus')}")

    else:
        print("No existing leads found. Creating a test lead...")

        # Create a simple test lead
        lead_data = {
            "lead_name": "Debug Test Lead 001",
            "company": Config.COMPANY_NAME,
            "first_name": "Debug",
            "last_name": "Test"
        }

        try:
            new_lead = api.create_doc("Lead", lead_data)
            print(f"✅ Created test lead: {new_lead.get('name')}")

            # Wait a bit
            print("Waiting 5 seconds...")
            time.sleep(5)

            # Try to create opportunity
            opportunity_data = {
                "opportunity_from": "Lead",
                "party_name": "Debug Test Lead 001",
                "company": Config.COMPANY_NAME,
                "opportunity_type": "Sales",
                "status": "Open"
            }

            try:
                opportunity = api.create_doc("Opportunity", opportunity_data)
                print(f"✅ Successfully created opportunity for new lead!")
            except Exception as e:
                print(f"❌ Failed to create opportunity for new lead: {e}")

        except Exception as e:
            print(f"❌ Failed to create test lead: {e}")

    print("\n" + "=" * 60)
    print("DEBUG COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    debug_crm_linking()

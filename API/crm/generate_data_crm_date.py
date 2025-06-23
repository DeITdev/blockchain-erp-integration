#!/usr/bin/env python3
"""
ERPNext Timestamp Updater - Server Script Approach
This script creates a Server Script in ERPNext that can update timestamps
without triggering validation errors.
"""

import requests
import json
import random
import logging
from datetime import datetime, timedelta

# Configuration


class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya"


# Logging Configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ERPNextTimestampUpdater:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

    def create_server_script(self):
        """Create a Server Script in ERPNext to handle timestamp updates"""

        # Server Script code that will run in ERPNext context
        script_code = '''
import frappe
import random
from datetime import datetime, timedelta

@frappe.whitelist()
def randomize_crm_timestamps():
    """Randomize CRM creation timestamps"""
    
    def generate_random_datetime(start_date, end_date):
        days_between = (end_date - start_date).days
        if days_between <= 0:
            return start_date
        
        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)
        
        # Random time during business hours
        random_hour = random.randint(8, 18)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        
        return random_date.replace(
            hour=random_hour, 
            minute=random_minute, 
            second=random_second
        )
    
    # Get territories for opportunities
    territories = frappe.get_all("Territory", pluck="name")
    if not territories:
        territories = ["All Territories"]
    
    results = {
        "leads_updated": 0,
        "opportunities_updated": 0, 
        "customers_updated": 0,
        "errors": []
    }
    
    try:
        # Update Leads
        leads = frappe.get_all("Lead", 
                              filters={"company": "PT Fiyansa Mulya"}, 
                              fields=["name"])
        
        start_date = datetime(2024, 6, 1)
        end_date = datetime(2025, 6, 15)
        
        for lead in leads:
            try:
                random_datetime = generate_random_datetime(start_date, end_date)
                
                # Direct database update to bypass validation
                frappe.db.sql("""
                    UPDATE `tabLead` 
                    SET creation = %s, modified = %s 
                    WHERE name = %s
                """, (random_datetime, random_datetime, lead.name))
                
                results["leads_updated"] += 1
                
            except Exception as e:
                results["errors"].append(f"Lead {lead.name}: {str(e)}")
        
        # Update Opportunities
        opportunities = frappe.get_all("Opportunity", 
                                     filters={"company": "PT Fiyansa Mulya"}, 
                                     fields=["name"])
        
        start_date = datetime(2024, 8, 1)
        end_date = datetime(2025, 6, 18)
        
        for opp in opportunities:
            try:
                random_datetime = generate_random_datetime(start_date, end_date)
                territory = random.choice(territories)
                
                # Direct database update with territory
                frappe.db.sql("""
                    UPDATE `tabOpportunity` 
                    SET creation = %s, modified = %s, territory = %s
                    WHERE name = %s
                """, (random_datetime, random_datetime, territory, opp.name))
                
                results["opportunities_updated"] += 1
                
            except Exception as e:
                results["errors"].append(f"Opportunity {opp.name}: {str(e)}")
        
        # Update Customers
        customers = frappe.get_all("Customer", fields=["name"])
        
        start_date = datetime(2024, 10, 1)
        end_date = datetime(2025, 6, 18)
        
        for customer in customers:
            try:
                random_datetime = generate_random_datetime(start_date, end_date)
                
                # Direct database update
                frappe.db.sql("""
                    UPDATE `tabCustomer` 
                    SET creation = %s, modified = %s 
                    WHERE name = %s
                """, (random_datetime, random_datetime, customer.name))
                
                results["customers_updated"] += 1
                
            except Exception as e:
                results["errors"].append(f"Customer {customer.name}: {str(e)}")
        
        # Commit the changes
        frappe.db.commit()
        
    except Exception as e:
        frappe.db.rollback()
        results["errors"].append(f"Fatal error: {str(e)}")
    
    return results
'''

        # Create the Server Script document
        server_script_data = {
            "doctype": "Server Script",
            "name": "CRM Timestamp Randomizer",
            "script_type": "API",
            "api_method": "randomize_crm_timestamps",
            "allow_guest": 0,
            "enabled": 1,
            "script": script_code
        }

        try:
            # Check if script already exists
            try:
                existing_script = self.session.get(
                    f"{self.base_url}/api/resource/Server Script/CRM Timestamp Randomizer")
                if existing_script.status_code == 200:
                    logger.info("Server Script already exists, updating it...")
                    response = self.session.put(
                        f"{self.base_url}/api/resource/Server Script/CRM Timestamp Randomizer",
                        json=server_script_data
                    )
                else:
                    raise Exception("Script doesn't exist")
            except:
                logger.info("Creating new Server Script...")
                response = self.session.post(
                    f"{self.base_url}/api/resource/Server Script",
                    json=server_script_data
                )

            response.raise_for_status()
            logger.info("✅ Server Script created/updated successfully!")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to create Server Script: {e}")
            return False

    def execute_timestamp_randomization(self):
        """Execute the timestamp randomization via the Server Script"""
        try:
            logger.info("🚀 Executing timestamp randomization...")

            # Call the custom API method
            response = self.session.post(
                f"{self.base_url}/api/method/randomize_crm_timestamps",
                json={}
            )
            response.raise_for_status()

            result = response.json()

            if "message" in result:
                results = result["message"]

                logger.info("🎉 Timestamp randomization completed!")
                logger.info(
                    f"✅ Leads updated: {results.get('leads_updated', 0)}")
                logger.info(
                    f"✅ Opportunities updated: {results.get('opportunities_updated', 0)}")
                logger.info(
                    f"✅ Customers updated: {results.get('customers_updated', 0)}")

                if results.get('errors'):
                    logger.warning(
                        f"⚠️ Errors encountered: {len(results['errors'])}")
                    for error in results['errors'][:5]:  # Show first 5 errors
                        logger.warning(f"   - {error}")

                print("\n" + "="*60)
                print("✅ CRM TIMESTAMPS RANDOMIZED SUCCESSFULLY!")
                print("="*60)
                print(f"📊 Results:")
                print(f"   - Leads: {results.get('leads_updated', 0)} updated")
                print(
                    f"   - Opportunities: {results.get('opportunities_updated', 0)} updated")
                print(
                    f"   - Customers: {results.get('customers_updated', 0)} updated")
                if results.get('errors'):
                    print(f"   - Errors: {len(results['errors'])}")
                print("="*60)
                print("📅 Date ranges applied:")
                print("   - Leads: June 2024 → June 2025")
                print("   - Opportunities: August 2024 → June 2025 (+ territories)")
                print("   - Customers: October 2024 → June 2025")
                print("="*60)

                return True
            else:
                logger.error("❌ Unexpected response format")
                return False

        except Exception as e:
            logger.error(f"❌ Failed to execute timestamp randomization: {e}")
            return False

    def run_complete_process(self):
        """Run the complete timestamp randomization process"""
        logger.info("🔧 Setting up ERPNext timestamp randomization...")

        print("\n" + "="*60)
        print("📅 ERPNext CRM TIMESTAMP RANDOMIZER")
        print("="*60)
        print("This script will:")
        print("1. Create a Server Script in ERPNext")
        print("2. Execute direct database updates for timestamps")
        print("3. Bypass all validation issues")
        print("")
        print("⚡ This is the most reliable method!")
        print("="*60)

        response = input("\nProceed with timestamp randomization? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled.")
            return

        # Step 1: Create the Server Script
        if not self.create_server_script():
            print("❌ Failed to create Server Script. Cannot continue.")
            return

        # Step 2: Execute the randomization
        if self.execute_timestamp_randomization():
            print("\n🎉 SUCCESS! All CRM timestamps have been randomized!")
        else:
            print("\n❌ Failed to execute timestamp randomization.")


def main():
    """Main entry point"""
    try:
        updater = ERPNextTimestampUpdater()
        updater.run_complete_process()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()

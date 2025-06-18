from logging import StreamHandler
import sys
from typing import Dict, List, Any, Optional
from faker import Faker
from datetime import datetime, timedelta
import time
import logging
import random
import json
import requests


def generate_phone(self) -> str:
    """Generate valid Indonesian phone number"""
    return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"  # !/usr/bin/env python3


"""
ERPNext CRM Module Dummy Data Generator (Fixed Version)
Generates realistic dummy data for Lead, Opportunity, and Customer doctypes in ERPNext v16.
Fixed to handle link validation errors and ensure proper data relationships.
Author: ERPNext CRM Data Generator
Version: 2.1.0
"""


# Initialize Faker for Indonesian locale
fake = Faker('id_ID')

# Configuration


class Config:
    # API Configuration
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"  # Adjust based on your Docker setup

    # Data Volumes
    LEAD_COUNT = 50
    OPPORTUNITY_COUNT = 50  # Increased from 30 to 50
    CUSTOMER_COUNT = 20     # Some opportunities convert to customers

    # Date Range for 2025
    START_DATE = datetime(2025, 1, 1)
    END_DATE = datetime(2025, 12, 31)

    # Company Details (MUST MATCH EXISTING SETUP)
    COMPANY_NAME = "PT Fiyansa Mulya"
    COMPANY_ABBR = "PFM"

    # Batch Processing
    BATCH_SIZE = 50
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # seconds

    # Link validation delays - reduced since we found the root cause
    CREATE_DELAY = 0.5  # Small delay after creating records
    LINK_VALIDATION_DELAY = 1.0  # Small delay before linking
    PHASE_DELAY = 3  # Reduced delay between phases


# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            'erpnext_crm_data_generation.log', encoding='utf-8'),
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
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in [
                                            "POST", "PUT"] else None, params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if retry_count < Config.RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{Config.RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(Config.RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                logger.error(
                    f"Request failed after {Config.RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
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

    def get_doc(self, doctype: str, name: str) -> Dict:
        """Get single document"""
        return self._make_request("GET", f"resource/{doctype}/{name}")

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create new document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)

    def update_doc(self, doctype: str, name: str, data: Dict) -> Dict:
        """Update existing document"""
        return self._make_request("PUT", f"resource/{doctype}/{name}", data)

    def submit_doc(self, doctype: str, name: str) -> Dict:
        """Submit a document"""
        return self.update_doc(doctype, name, {"docstatus": 1})

    def check_exists(self, doctype: str, name: str) -> bool:
        """Check if document exists"""
        try:
            if doctype == "Lead":
                result = self.get_list(
                    doctype, filters={"lead_name": name}, fields=["name"])
                return len(result) > 0
            elif doctype == "Opportunity":
                result = self.get_list(
                    doctype, filters={"party_name": name}, fields=["name"])
                return len(result) > 0
            elif doctype == "Customer":
                result = self.get_list(
                    doctype, filters={"customer_name": name}, fields=["name"])
                return len(result) > 0
            else:
                self.get_doc(doctype, name)
                return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                logger.error(
                    f"Error checking existence of {doctype} {name} (HTTP {e.response.status_code}): {e.response.text}")
                if e.response.status_code == 500:
                    raise
                return False
        except Exception as e:
            logger.error(
                f"Unexpected error checking existence of {doctype} {name}: {e}")
            raise


class CRMDataGenerator:
    """Generates realistic CRM dummy data with proper link validation"""

    def __init__(self):
        self.fake = Faker('id_ID')
        self.api = ERPNextAPI()

        # Cache for generated data with full details
        self.leads = []
        self.opportunities = []
        self.customers = []

        # Indonesian company names
        self.indonesian_companies = [
            "PT Maju Bersama", "CV Sukses Mandiri", "PT Digital Indonesia",
            "CV Teknologi Nusantara", "PT Berkah Jaya", "CV Global Solutions",
            "PT Innovasi Modern", "CV Karya Utama", "PT Solusi Terpadu",
            "CV Mandiri Sejahtera", "PT Cerdas Indonesia", "CV Prima Teknologi"
        ]

        # Cache for master data
        self.territories = []
        self.fetch_territories()

    def fetch_territories(self):
        """Fetch existing territories from ERPNext"""
        try:
            territories = self.api.get_list(
                "Territory", fields=["name", "territory_name"])
            self.territories = [t.get("name")
                                for t in territories if t.get("name")]

            if not self.territories:
                # Default territories if none exist
                self.territories = ["All Territories"]
                logger.warning(
                    "No territories found, using default 'All Territories'")

            logger.info(
                f"Found {len(self.territories)} territories: {self.territories[:5]}...")

        except Exception as e:
            logger.warning(f"Failed to fetch territories: {e}. Using default.")
            self.territories = ["All Territories"]

    def randomize_creation_dates(self):
        """Randomize creation dates for existing CRM data to spread across time series"""
        logger.info("🔄 Randomizing creation dates for existing CRM data...")

        if not (self.leads or self.opportunities or self.customers):
            logger.warning("No CRM data found to randomize dates")
            return

        # Date ranges for realistic time series
        lead_start_date = datetime(2024, 6, 1)  # Leads created over past year
        lead_end_date = datetime(2025, 6, 15)

        # Opportunities created after some leads
        opp_start_date = datetime(2024, 8, 1)
        opp_end_date = datetime(2025, 6, 18)

        # Customers created after opportunities
        customer_start_date = datetime(2024, 10, 1)
        customer_end_date = datetime(2025, 6, 18)

        randomized_count = 0

        # Randomize Lead creation dates
        logger.info(f"Randomizing {len(self.leads)} lead creation dates...")
        for lead in self.leads:
            if lead.get('name'):
                try:
                    random_date = self.generate_date_in_range(
                        lead_start_date, lead_end_date, exclude_weekends=False)
                    random_time = f"{random.randint(8, 17):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                    creation_datetime = f"{random_date} {random_time}"

                    # Update the document
                    update_data = {
                        "creation": creation_datetime,
                        "modified": creation_datetime
                    }

                    self.api.update_doc("Lead", lead['name'], update_data)
                    randomized_count += 1

                    if randomized_count % 10 == 0:
                        logger.info(f"Updated {randomized_count} leads...")
                        # Small delay to avoid overwhelming server
                        time.sleep(0.2)

                except Exception as e:
                    logger.warning(
                        f"Failed to update lead {lead.get('name')}: {e}")

        # Randomize Opportunity creation dates
        logger.info(
            f"Randomizing {len(self.opportunities)} opportunity creation dates...")
        for opp in self.opportunities:
            if opp.get('name'):
                try:
                    random_date = self.generate_date_in_range(
                        opp_start_date, opp_end_date, exclude_weekends=False)
                    random_time = f"{random.randint(8, 17):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                    creation_datetime = f"{random_date} {random_time}"

                    # Also add territory while we're updating
                    territory = random.choice(
                        self.territories) if self.territories else "All Territories"

                    update_data = {
                        "creation": creation_datetime,
                        "modified": creation_datetime,
                        "territory": territory
                    }

                    self.api.update_doc(
                        "Opportunity", opp['name'], update_data)
                    randomized_count += 1

                    if randomized_count % 10 == 0:
                        logger.info(
                            f"Updated {randomized_count} total records...")
                        time.sleep(0.2)

                except Exception as e:
                    logger.warning(
                        f"Failed to update opportunity {opp.get('name')}: {e}")

        # Randomize Customer creation dates
        logger.info(
            f"Randomizing {len(self.customers)} customer creation dates...")
        for customer in self.customers:
            if customer.get('name'):
                try:
                    random_date = self.generate_date_in_range(
                        customer_start_date, customer_end_date, exclude_weekends=False)
                    random_time = f"{random.randint(8, 17):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                    creation_datetime = f"{random_date} {random_time}"

                    update_data = {
                        "creation": creation_datetime,
                        "modified": creation_datetime
                    }

                    self.api.update_doc(
                        "Customer", customer['name'], update_data)
                    randomized_count += 1

                    if randomized_count % 10 == 0:
                        logger.info(
                            f"Updated {randomized_count} total records...")
                        time.sleep(0.2)

                except Exception as e:
                    logger.warning(
                        f"Failed to update customer {customer.get('name')}: {e}")

        logger.info(
            f"✅ Successfully randomized creation dates for {randomized_count} records!")
        logger.info(f"📅 Date ranges used:")
        logger.info(
            f"   - Leads: {lead_start_date.strftime('%Y-%m-%d')} to {lead_end_date.strftime('%Y-%m-%d')}")
        logger.info(
            f"   - Opportunities: {opp_start_date.strftime('%Y-%m-%d')} to {opp_end_date.strftime('%Y-%m-%d')}")
        logger.info(
            f"   - Customers: {customer_start_date.strftime('%Y-%m-%d')} to {customer_end_date.strftime('%Y-%m-%d')}")
        """Generate valid Indonesian phone number"""
        return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"

    def generate_date_in_range(self, start_date: datetime, end_date: datetime) -> str:
        """Generate random date within range"""
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        days_between = (end_date - start_date).days
        if days_between < 0:
            return start_date.strftime("%Y-%m-%d")

        random_days = random.randint(0, days_between)
        date = start_date + timedelta(days=random_days)
        return date.strftime("%Y-%m-%d")

    def check_existing_data(self):
        """Check existing CRM data and cache it (always return False to allow processing)"""
        logger.info("Checking existing CRM data...")

        try:
            existing_leads = self.api.get_list(
                "Lead", filters={"company": Config.COMPANY_NAME}, fields=["name", "lead_name"])
            existing_opportunities = self.api.get_list(
                "Opportunity", filters={"company": Config.COMPANY_NAME}, fields=["name", "party_name"])
            existing_customers = self.api.get_list(
                "Customer", fields=["name", "customer_name"])

            # Cache existing data
            self.leads = existing_leads
            self.opportunities = existing_opportunities
            self.customers = existing_customers

            current_lead_count = len(existing_leads)
            current_opportunity_count = len(existing_opportunities)
            current_customer_count = len(existing_customers)

            logger.info(
                f"Current CRM data: {current_lead_count} leads, {current_opportunity_count} opportunities, {current_customer_count} customers")
            logger.info(
                f"Target CRM data: {Config.LEAD_COUNT} leads, {Config.OPPORTUNITY_COUNT} opportunities, {Config.CUSTOMER_COUNT} customers")

            # Always return False so we can process existing data
            return False

        except Exception as e:
            logger.error(f"Error checking existing CRM data: {str(e)}")
            return False

    def create_leads(self):
        """Create lead records with proper validation"""
        logger.info(f"Creating leads...")

        # Check if we already have enough leads
        if len(self.leads) >= Config.LEAD_COUNT:
            logger.info(
                f"Already have {len(self.leads)} leads (>= target {Config.LEAD_COUNT}). Skipping new lead creation.")
            return

        leads_to_create = Config.LEAD_COUNT - len(self.leads)
        logger.info(
            f"Creating {leads_to_create} new leads to reach target {Config.LEAD_COUNT}...")

        leads_created_count = 0

        for i in range(leads_to_create):
            # Determine if this is a company or individual lead
            is_company = random.choice([True, False])

            if is_company:
                company_name = random.choice(
                    self.indonesian_companies) + f" {random.randint(1, 999):03d}"
                lead_name = company_name
                first_name = self.fake.first_name()
                last_name = self.fake.last_name()
            else:
                first_name = self.fake.first_name()
                last_name = self.fake.last_name()
                lead_name = f"{first_name} {last_name}"
                company_name = random.choice(self.indonesian_companies)

            # Check if lead already exists
            if any(lead.get('lead_name') == lead_name for lead in self.leads):
                logger.debug(
                    f"Lead '{lead_name}' already exists in cache, skipping.")
                continue

            # Create lead with minimal data
            lead_data = {
                "lead_name": lead_name,
                "company": Config.COMPANY_NAME,
                "first_name": first_name,
                "last_name": last_name,
                "company_name": company_name
            }

            # Safe email generation
            email_domain = company_name.lower().replace(
                ' ', '').replace('pt', '').replace('cv', '')[:10]
            lead_data["email_id"] = f"{first_name.lower()}.{last_name.lower()}@{email_domain}.com"
            lead_data["mobile_no"] = self.generate_phone()

            try:
                lead = self.api.create_doc("Lead", lead_data)

                # Cache the lead with full details
                lead_cache_entry = {
                    "name": lead.get("name"),
                    "lead_name": lead_name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "company_name": company_name
                }
                self.leads.append(lead_cache_entry)
                leads_created_count += 1

                logger.debug(
                    f"Created lead: '{lead_name}' (ID: {lead_cache_entry['name']})")

                # Important: Add delay after creation to ensure database commit
                time.sleep(Config.CREATE_DELAY)

            except Exception as e:
                logger.warning(
                    f"Failed to create lead '{lead_name}': {str(e)}")

        logger.info(
            f"Created {leads_created_count} new leads. Total leads: {len(self.leads)}")

    def create_opportunity_for_lead(self, lead_data: Dict) -> Optional[Dict]:
        """Create opportunity using document name as party_name"""
        party_name = lead_data.get("lead_name")  # For display/logging
        # This is what we actually need!
        lead_doc_name = lead_data.get("name")

        if not lead_doc_name:
            logger.warning(
                f"Lead data has no document name, cannot create opportunity. Lead data: {lead_data}")
            return None

        # Random territory selection
        territory = random.choice(
            self.territories) if self.territories else "All Territories"

        # Use the document name as party_name - this is the key fix!
        opportunity_data = {
            "opportunity_from": "Lead",
            "party_name": lead_doc_name,  # Use document name, not lead_name!
            "company": Config.COMPANY_NAME,
            "opportunity_type": "Sales",
            "status": "Open",
            "territory": territory,  # Add territory
            "expected_closing": self.generate_date_in_range(
                datetime.now() + timedelta(days=30),
                Config.END_DATE
            ),
            "opportunity_amount": random.randint(10_000_000, 1_000_000_000)
        }

        try:
            logger.debug(
                f"Creating opportunity with party_name='{lead_doc_name}' (doc name) for lead '{party_name}' in territory '{territory}'")
            opportunity = self.api.create_doc("Opportunity", opportunity_data)
            logger.debug(
                f"Successfully created opportunity for '{party_name}' using doc name '{lead_doc_name}' in territory '{territory}'")
            return opportunity
        except Exception as e:
            logger.error(
                f"Failed to create opportunity for lead '{party_name}' (doc: {lead_doc_name}): {e}")
            return None

    def create_opportunities(self):
        """Create opportunity records from leads using correct document names"""
        logger.info(f"Creating opportunities...")

        # Check if we already have enough opportunities
        if len(self.opportunities) >= Config.OPPORTUNITY_COUNT:
            logger.info(
                f"Already have {len(self.opportunities)} opportunities (>= target {Config.OPPORTUNITY_COUNT}). Skipping new opportunity creation.")
            return

        opportunities_to_create = Config.OPPORTUNITY_COUNT - \
            len(self.opportunities)
        logger.info(
            f"Creating {opportunities_to_create} new opportunities to reach target {Config.OPPORTUNITY_COUNT}...")

        if not self.leads:
            logger.warning("No leads available to create opportunities from.")
            return

        opportunities_created_count = 0

        # Get leads that haven't been converted to opportunities yet
        # Check against document names, not lead names
        existing_opportunity_parties = {
            opp.get('party_name') for opp in self.opportunities}
        available_leads = [lead for lead in self.leads
                           if lead.get('name') not in existing_opportunity_parties]

        if not available_leads:
            logger.warning("No unconverted leads available for opportunities.")
            # Show some debug info
            if self.leads and self.opportunities:
                logger.info(
                    f"Sample lead names: {[l.get('name') for l in self.leads[:3]]}")
                logger.info(
                    f"Sample opportunity parties: {[o.get('party_name') for o in self.opportunities[:3]]}")
            return

        # Shuffle to get random selection
        random.shuffle(available_leads)

        logger.info(
            f"Found {len(available_leads)} available leads for conversion")

        for i in range(min(opportunities_to_create, len(available_leads))):
            lead = available_leads[i]
            lead_display_name = lead.get("lead_name", "Unknown")
            lead_doc_name = lead.get("name")

            if not lead_doc_name:
                logger.warning(
                    f"Lead at index {i} has no document name, skipping. Lead data: {lead}")
                continue

            logger.debug(
                f"Processing lead: '{lead_display_name}' (doc: {lead_doc_name})")

            # Use the simplified creation method that works
            opportunity = self.create_opportunity_for_lead(lead)

            if opportunity:
                opportunity_cache_entry = {
                    "name": opportunity.get("name"),
                    "party_name": lead_doc_name,  # Store the document name used
                    "party_display_name": lead_display_name,  # Store display name for reference
                    "opportunity_from": "Lead"
                }
                self.opportunities.append(opportunity_cache_entry)
                opportunities_created_count += 1

                logger.info(
                    f"✅ Created opportunity for '{lead_display_name}' (doc: {lead_doc_name}) -> Opportunity ID: {opportunity_cache_entry['name']}")

                # Small delay after successful creation
                time.sleep(Config.CREATE_DELAY)
            else:
                logger.error(
                    f"❌ Failed to create opportunity for lead '{lead_display_name}' (doc: {lead_doc_name})")

        logger.info(
            f"Created {opportunities_created_count} new opportunities. Total opportunities: {len(self.opportunities)}")

        if opportunities_created_count == 0 and opportunities_to_create > 0:
            logger.error("🚨 No opportunities were created!")
            logger.info("This might be due to:")
            logger.info("1. All leads already converted to opportunities")
            logger.info("2. Lead data missing document names")
            logger.info("3. Permission issues")

            # Debug: Show some sample lead data
            if self.leads:
                logger.info(f"Sample lead data for debugging:")
                for i, lead in enumerate(self.leads[:3]):
                    logger.info(f"  Lead {i+1}: {lead}")
        elif opportunities_created_count > 0:
            logger.info(
                f"🎉 Successfully created {opportunities_created_count} opportunities!")

    def create_customers(self):
        """Create customer records from opportunities using correct document names"""
        logger.info(f"Creating customers...")

        # Check if we already have enough customers
        if len(self.customers) >= Config.CUSTOMER_COUNT:
            logger.info(
                f"Already have {len(self.customers)} customers (>= target {Config.CUSTOMER_COUNT}). Skipping new customer creation.")
            return

        customers_to_create = Config.CUSTOMER_COUNT - len(self.customers)
        logger.info(
            f"Creating {customers_to_create} new customers to reach target {Config.CUSTOMER_COUNT}...")

        if not self.opportunities:
            logger.warning(
                "No opportunities available to create customers from.")
            return

        customers_created_count = 0

        # Get opportunities that haven't been converted to customers yet
        existing_customer_names = {
            cust.get('customer_name') for cust in self.customers}

        # Create customer names based on the opportunity's party display name or party name
        available_opportunities = []
        for opp in self.opportunities:
            # Try to get a human-readable name for the customer
            customer_name = opp.get(
                'party_display_name') or opp.get('party_name')
            if customer_name and customer_name not in existing_customer_names:
                opp_copy = opp.copy()
                opp_copy['proposed_customer_name'] = customer_name
                available_opportunities.append(opp_copy)

        if not available_opportunities:
            logger.warning(
                "No unconverted opportunities available for customers.")
            return

        # Shuffle to get random selection
        random.shuffle(available_opportunities)

        for i in range(min(customers_to_create, len(available_opportunities))):
            opportunity = available_opportunities[i]
            customer_name = opportunity.get("proposed_customer_name")

            if not customer_name:
                logger.warning(
                    f"Opportunity at index {i} has no proposed customer name, skipping.")
                continue

            # Determine customer type
            is_company = "PT " in customer_name or "CV " in customer_name
            customer_type = "Company" if is_company else "Individual"

            # Create customer with minimal data
            customer_data = {
                "customer_name": customer_name,
                "customer_type": customer_type,
                "default_currency": "IDR"
            }

            # Add names for individuals
            if not is_company:
                names = customer_name.split()
                if len(names) >= 2:
                    customer_data["first_name"] = names[0]
                    customer_data["last_name"] = " ".join(names[1:])

            try:
                customer = self.api.create_doc("Customer", customer_data)

                customer_cache_entry = {
                    "name": customer.get("name"),
                    "customer_name": customer_name,
                    "customer_type": customer_type
                }
                self.customers.append(customer_cache_entry)
                customers_created_count += 1

                logger.info(
                    f"✅ Created customer: '{customer_name}' (ID: {customer_cache_entry['name']})")

                # Delay after creation
                time.sleep(Config.CREATE_DELAY)

            except Exception as e:
                logger.warning(
                    f"Failed to create customer '{customer_name}': {str(e)}")

        logger.info(
            f"Created {customers_created_count} new customers. Total customers: {len(self.customers)}")

        if customers_created_count > 0:
            logger.info(
                f"🎉 Successfully created {customers_created_count} customers!")

    def generate_all_data(self):
        """Main method to generate all CRM data with proper sequencing"""
        logger.info("Starting ERPNext CRM module dummy data generation...")
        logger.info(
            f"Target: {Config.LEAD_COUNT} leads, {Config.OPPORTUNITY_COUNT} opportunities, {Config.CUSTOMER_COUNT} customers")

        try:
            # Check existing data and cache it
            self.check_existing_data()

            # Determine what needs to be done
            current_leads = len(self.leads)
            current_opportunities = len(self.opportunities)
            current_customers = len(self.customers)

            needs_leads = current_leads < Config.LEAD_COUNT
            needs_opportunities = current_opportunities < Config.OPPORTUNITY_COUNT
            needs_customers = current_customers < Config.CUSTOMER_COUNT

            if needs_leads or needs_opportunities or needs_customers:
                logger.info("=== CREATING MISSING CRM DATA ===")

                if needs_leads:
                    logger.info("=== Phase 1: Creating Leads ===")
                    self.create_leads()
                    time.sleep(Config.PHASE_DELAY)
                else:
                    logger.info("=== Phase 1: Leads (Sufficient) ===")
                    logger.info(
                        f"Already have {current_leads} leads (>= target {Config.LEAD_COUNT})")

                if needs_opportunities:
                    logger.info("=== Phase 2: Creating Opportunities ===")
                    self.create_opportunities()
                    time.sleep(Config.PHASE_DELAY)
                else:
                    logger.info("=== Phase 2: Opportunities (Sufficient) ===")
                    logger.info(
                        f"Already have {current_opportunities} opportunities (>= target {Config.OPPORTUNITY_COUNT})")

                if needs_customers:
                    logger.info("=== Phase 3: Creating Customers ===")
                    self.create_customers()
                    time.sleep(Config.PHASE_DELAY)
                else:
                    logger.info("=== Phase 3: Customers (Sufficient) ===")
                    logger.info(
                        f"Already have {current_customers} customers (>= target {Config.CUSTOMER_COUNT})")

                # Refresh data after creation
                self.check_existing_data()
            else:
                logger.info("=== ALL TARGETS ALREADY REACHED ===")
                logger.info(f"✅ Leads: {current_leads}/{Config.LEAD_COUNT}")
                logger.info(
                    f"✅ Opportunities: {current_opportunities}/{Config.OPPORTUNITY_COUNT}")
                logger.info(
                    f"✅ Customers: {current_customers}/{Config.CUSTOMER_COUNT}")

            # ALWAYS randomize dates regardless of whether we created new data
            logger.info(
                "=== RANDOMIZING CREATION DATES FOR ALL EXISTING DATA ===")
            self.randomize_creation_dates()

            logger.info("=== CRM Data Processing Complete ===")

            # Final summary
            final_lead_count = len(self.api.get_list(
                "Lead", filters={"company": Config.COMPANY_NAME}, fields=["name"]))
            final_opportunity_count = len(self.api.get_list("Opportunity", filters={
                                          "company": Config.COMPANY_NAME}, fields=["name"]))
            final_customer_count = len(
                self.api.get_list("Customer", fields=["name"]))

            logger.info("Final Summary:")
            logger.info(f"- Total Leads: {final_lead_count}")
            logger.info(f"- Total Opportunities: {final_opportunity_count}")
            logger.info(f"- Total Customers: {final_customer_count}")

            # Check if target was reached
            if (final_lead_count >= Config.LEAD_COUNT and
                final_opportunity_count >= Config.OPPORTUNITY_COUNT and
                    final_customer_count >= Config.CUSTOMER_COUNT):
                logger.info("✅ TARGET SUCCESSFULLY REACHED!")
            else:
                logger.info(
                    "⚠️ Target not fully reached. You may want to run the script again.")

        except Exception as e:
            logger.error(f"Fatal error during CRM data generation: {str(e)}")
            raise


def main():
    """Main entry point"""
    print("=" * 80)
    print("ERPNext CRM Module Dummy Data Generator (Fixed Version)")
    print("=" * 80)
    print(f"API Endpoint: {Config.BASE_URL}")
    print(f"Company: {Config.COMPANY_NAME}")
    print(f"Target Leads: {Config.LEAD_COUNT}")
    print(f"Target Opportunities: {Config.OPPORTUNITY_COUNT}")
    print(f"Target Customers: {Config.CUSTOMER_COUNT}")
    print("=" * 80)
    print("This version includes fixes for link validation errors.")
    print("Features:")
    print("- Randomizes creation dates across realistic time series")
    print("- Adds territories to opportunities")
    print("- Handles existing data gracefully")
    print("=" * 80)

    response = input(
        "\nThis script will create/update CRM data and randomize creation dates. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = CRMDataGenerator()
        generator.generate_all_data()

        print("\n" + "=" * 80)
        print("✅ CRM data generation completed successfully!")
        print(
            "Check the log file for detailed information: erpnext_crm_data_generation.log")
        print("=" * 80)
    except Exception as e:
        print(f"\n" + "=" * 80)
        print(f"❌ CRM data generation failed: {e}")
        print("Please check the log file for more details.")
        print("=" * 80)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ERPNext Lead Generator
Creates lead records with realistic dummy data following specified requirements.
Uses environment variables from .env file for configuration.
Author: ERPNext Lead Generator
Version: 1.1.0 - Fixed source field and random status distribution
"""

import requests
import json
import random
import logging
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, List, Any, Optional
import sys
from logging import StreamHandler

# Load environment variables from .env file


def load_env_file():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent.parent.parent / '.env'

    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print(f"✅ Loaded environment variables from {env_path}")
    else:
        print(f"⚠️ .env file not found at {env_path}")
        print("Using hardcoded values as fallback")


# Load environment variables
load_env_file()

# Initialize Faker for generating random data
fake = Faker('id_ID')  # Indonesian locale for realistic data

# Configuration from environment variables
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = os.getenv("BASE_URL")
COMPANY_NAME = os.getenv("COMPANY_NAME")
COMPANY_ABBR = os.getenv("COMPANY_ABBR")

# Lead Configuration
TARGET_LEADS = 100  # Number of leads to create

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
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
            'Authorization': f'token {API_KEY}:{API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = BASE_URL

        logger.info(f"Using API configuration:")
        logger.info(f"  Base URL: {self.base_url}")
        logger.info(f"  Company: {COMPANY_NAME}")
        logger.info(f"  API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, retry_count: int = 0, silent_404: bool = False) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}/api/{endpoint}"

        try:
            response = self.session.request(method, url, json=data if method in ["POST", "PUT"] else None,
                                            params=data if method == "GET" else None)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            # Handle 404 errors silently if requested (for optional doctypes)
            if silent_404 and hasattr(e, 'response') and e.response.status_code == 404:
                raise e

            if retry_count < RETRY_ATTEMPTS:
                logger.warning(
                    f"Request failed to {url}, retrying... ({retry_count + 1}/{RETRY_ATTEMPTS}) - Error: {e}")
                time.sleep(RETRY_DELAY)
                return self._make_request(method, endpoint, data, retry_count + 1, silent_404)
            else:
                logger.error(
                    f"Request failed after {RETRY_ATTEMPTS} attempts for {url}: {str(e)}")
                if hasattr(e, 'response') and e.response:
                    logger.error(f"Response content: {e.response.text}")
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

    def create_doc(self, doctype: str, data: Dict) -> Dict:
        """Create new document"""
        data["doctype"] = doctype
        return self._make_request("POST", f"resource/{doctype}", data)


class LeadGenerator:
    """Generates lead records with realistic data"""

    def __init__(self):
        self.fake = Faker('id_ID')
        self.api = ERPNextAPI()
        self.leads = []
        self.field_options = {}

        # Fetch field options
        self._fetch_field_options()

    def _fetch_field_options(self):
        """Use hardcoded options for specified fields, fetch others from ERPNext"""
        logger.info("Loading field options...")

        # Hardcoded options based on your specifications
        self.field_options = {
            "salutation": ["Mr", "Ms", "Mrs", "Dr", "Prof"],
            "status": ["Lead", "Open", "Replied", "Opportunity", "Quotation", "Lost Quotation", "Interested", "Converted", "Do Not Contact"],
            "lead_type": ["Client", "Channel Partner", "Consultant"],
            "request_type": ["Product Enquiry", "Request for Information", "Suggestions", "Other"],
            "no_of_employees": ["1-10", "11-50", "51-200", "201-500", "501-1000", "1000+"],
            "qualification_status": ["Unqualified", "In Process", "Qualified"],
            "qualified_by": ["Administrator"]
        }

        # Fetch these fields from ERPNext to ensure they exist
        self._fetch_territory_options()
        self._fetch_industry_options()
        self._fetch_utm_source_options()  # Fixed: Now fetches UTM Source properly
        self._fetch_market_segment_options()

        logger.info("✅ Field options loaded:")
        for key, values in self.field_options.items():
            if isinstance(values, list) and len(values) > 0:
                logger.info(f"    - {key}: {len(values)} options")

    def _fetch_territory_options(self):
        """Fetch territory options - with fallback if fails"""
        try:
            territories = self.api.get_list("Territory", fields=["name"])
            if territories:
                self.field_options["territory"] = [t["name"]
                                                   for t in territories if t.get("name")]
                logger.info(
                    f"Found {len(self.field_options['territory'])} territories")
            else:
                # Use fallback if no territories found
                self.field_options["territory"] = [
                    "Indonesia", "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Palembang"]
                logger.info(
                    "No territories found, using fallback Indonesian cities")
        except Exception as e:
            logger.warning(f"Could not fetch territories: {e}")
            # Use fallback Indonesian territories
            self.field_options["territory"] = [
                "Indonesia", "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Palembang"]
            logger.info("Using fallback territory options")

    def _fetch_industry_options(self):
        """Fetch industry options from ERPNext"""
        try:
            industries = self.api.get_list("Industry Type", fields=["name"])
            if industries:
                self.field_options["industry"] = [i["name"]
                                                  for i in industries if i.get("name")]
                logger.info(
                    f"Found {len(self.field_options['industry'])} industries")
            else:
                # Use basic fallback if no industries found
                self.field_options["industry"] = [
                    "Manufacturing", "Services", "Technology"]
                logger.info("No industries found, using basic fallback")
        except Exception as e:
            logger.warning(f"Could not fetch industries: {e}")
            # Use basic fallback
            self.field_options["industry"] = [
                "Manufacturing", "Services", "Technology"]
            logger.info("Using fallback industry options")

    def _fetch_utm_source_options(self):
        """Fetch UTM Source options from ERPNext - Fixed implementation"""
        try:
            logger.info("Fetching UTM Source options...")
            utm_sources = self.api.get_list("UTM Source", fields=["name"])

            if utm_sources:
                # Extract the 'name' field which contains the UTM Source ID/name
                self.field_options["utm_source"] = [s["name"]
                                                    for s in utm_sources if s.get("name")]
                logger.info(
                    f"✅ Found {len(self.field_options['utm_source'])} UTM Sources from ERPNext")

                # Show sample UTM sources
                sample_sources = self.field_options["utm_source"][:5]
                logger.info(f"    Sample UTM Sources: {sample_sources}")

            else:
                # Use fallback if no UTM sources found
                logger.warning(
                    "No UTM Sources found in ERPNext, creating fallback options")
                self._create_fallback_utm_sources()

        except Exception as e:
            logger.warning(f"Could not fetch UTM Source: {e}")
            logger.info("Creating fallback UTM Source options")
            self._create_fallback_utm_sources()

    def _create_fallback_utm_sources(self):
        """Create fallback UTM Source options"""
        self.field_options["utm_source"] = [
            "Website", "Google Ads", "Facebook", "Instagram", "LinkedIn",
            "Email Campaign", "Phone Call", "Referral", "Trade Show",
            "Cold Email", "Social Media", "Print Advertisement",
            "Radio", "Television", "Word of Mouth", "Direct Mail"
        ]
        logger.info(
            f"Using {len(self.field_options['utm_source'])} fallback UTM Source options")

    def _fetch_market_segment_options(self):
        """Fetch market segment options from ERPNext"""
        try:
            segments = self.api.get_list("Market Segment", fields=["name"])
            if segments:
                self.field_options["market_segment"] = [s["name"]
                                                        for s in segments if s.get("name")]
                logger.info(
                    f"Found {len(self.field_options['market_segment'])} market segments")
            else:
                # Use basic fallback if no segments found
                self.field_options["market_segment"] = [
                    "Lower Income", "Middle Income", "Upper Income"]
                logger.info("No market segments found, using basic fallback")
        except Exception as e:
            logger.warning(f"Could not fetch market segments: {e}")
            # Use basic fallback
            self.field_options["market_segment"] = [
                "Lower Income", "Middle Income", "Upper Income"]
            logger.info("Using fallback market segment options")

    def generate_phone_number(self) -> str:
        """Generate valid Indonesian phone number"""
        return f"+628{random.randint(100_000_000, 9_999_999_999):010d}"

    def generate_website(self, company_name: str) -> str:
        """Generate realistic website URL"""
        clean_name = company_name.lower().replace(
            " ", "").replace(".", "").replace(",", "")[:15]
        domains = ["com", "co.id", "id", "net"]
        return f"https://www.{clean_name}.{random.choice(domains)}"

    def generate_organization_name(self) -> str:
        """Generate realistic Indonesian organization name"""
        company_types = ["PT", "CV", "UD", "Toko"]
        business_words = [
            "Maju", "Jaya", "Sukses", "Makmur", "Sejahtera", "Bersama", "Mandiri", "Prima",
            "Utama", "Abadi", "Sentosa", "Indah", "Gemilang", "Cahaya", "Bintang", "Mulia"
        ]
        business_sectors = [
            "Trading", "Indonesia", "Nusantara", "Persada", "Karya", "Tehnik", "Elektronik",
            "Furniture", "Textiles", "Food", "Beverages", "Construction", "Engineering"
        ]

        company_type = random.choice(company_types)
        word1 = random.choice(business_words)
        word2 = random.choice(business_sectors)

        return f"{company_type} {word1} {word2}"

    def generate_job_title(self) -> str:
        """Generate realistic job title"""
        titles = [
            "General Manager", "Operations Manager", "Sales Manager", "Marketing Manager",
            "Business Development Manager", "Project Manager", "Finance Manager", "HR Manager",
            "IT Manager", "Production Manager", "Quality Manager", "Purchasing Manager",
            "Director", "Vice President", "CEO", "CTO", "CFO", "COO",
            "Sales Executive", "Business Analyst", "Marketing Executive", "Account Manager",
            "Senior Consultant", "Operations Executive", "Finance Executive"
        ]
        return random.choice(titles)

    def generate_date_before_june_2025(self) -> str:
        """Generate random date before June 2025"""
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 5, 31)

        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)

        return random_date.strftime("%Y-%m-%d")

    def generate_random_status(self) -> str:
        """Generate completely random status - no bias towards 'Lead'"""
        if "status" in self.field_options:
            return random.choice(self.field_options["status"])
        else:
            # Fallback
            return random.choice(["Lead", "Open", "Replied", "Opportunity", "Converted"])

    def check_existing_leads(self):
        """Check existing leads and determine how many to create"""
        logger.info("Checking existing leads...")

        try:
            existing_leads = self.api.get_list("Lead",
                                               filters={
                                                   "company": COMPANY_NAME},
                                               fields=["name", "lead_name", "status"])

            current_lead_count = len(existing_leads)

            logger.info(f"Current leads: {current_lead_count}")
            logger.info(f"Target leads: {TARGET_LEADS}")

            if current_lead_count >= TARGET_LEADS:
                logger.info(
                    f"Already have {current_lead_count} leads (>= target {TARGET_LEADS}). Skipping new lead creation.")
                return 0

            leads_to_create = TARGET_LEADS - current_lead_count
            logger.info(
                f"Need to create {leads_to_create} leads to reach target {TARGET_LEADS}")

            return leads_to_create

        except Exception as e:
            logger.error(f"Error checking existing leads: {str(e)}")
            return TARGET_LEADS

    def create_leads(self):
        """Create lead records with realistic data"""
        logger.info("🚀 Starting lead creation...")

        # Check how many leads we need to create
        leads_to_create = self.check_existing_leads()

        if leads_to_create <= 0:
            logger.info("No new leads need to be created.")
            return

        print("\n" + "=" * 80)
        print("📊 Creating Leads")
        print("=" * 80)
        print(f"🎯 Target Leads: {TARGET_LEADS}")
        print(f"📝 Creating: {leads_to_create} new leads")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"📊 Status Distribution: Random (no bias)")
        print(f"📡 Source Field: UTM Source")
        print("=" * 80)

        created_leads = []

        for i in range(leads_to_create):
            try:
                # Generate basic personal information
                salutation = random.choice(
                    self.field_options.get("salutation", ["Mr", "Ms"]))
                first_name = self.fake.first_name()
                middle_name = self.fake.first_name() if random.choice([
                    True, False]) else ""
                last_name = self.fake.last_name()

                # Construct full name
                full_name_parts = [first_name]
                if middle_name:
                    full_name_parts.append(middle_name)
                full_name_parts.append(last_name)
                lead_name = " ".join(full_name_parts)

                # Generate other data
                gender = random.choice(["Male", "Female"])
                job_title = self.generate_job_title()
                organization_name = self.generate_organization_name()

                # Generate contact information
                email = f"{first_name.lower()}.{last_name.lower()}@{organization_name.replace(' ', '').replace('.', '').lower()[:10]}.com"
                website = self.generate_website(organization_name)
                mobile_no = self.generate_phone_number()
                whatsapp = mobile_no  # Same as mobile for simplicity
                phone = self.generate_phone_number()
                phone_ext = str(random.randint(100, 999))

                # Create lead data
                lead_data = {
                    # Basic Information
                    "salutation": salutation,
                    "first_name": first_name,
                    "last_name": last_name,
                    "lead_name": lead_name,
                    "gender": gender,
                    "job_title": job_title,
                    "lead_owner": "Administrator",  # As required
                    "status": self.generate_random_status(),  # Now completely random
                    "lead_type": random.choice(self.field_options.get("lead_type", ["Client"])),
                    "request_type": random.choice(self.field_options.get("request_type", ["Product Enquiry"])),

                    # Contact Information
                    "email_id": email,
                    "website": website,
                    "mobile_no": mobile_no,
                    "whatsapp": whatsapp,
                    "phone": phone,
                    "phone_ext": phone_ext,

                    # Organization
                    "organization_name": organization_name,
                    # IDR
                    "annual_revenue": random.randint(100_000_000, 10_000_000_000),
                    "territory": random.choice(self.field_options.get("territory", ["Indonesia"])),
                    "no_of_employees": random.choice(self.field_options.get("no_of_employees", ["11-50"])),
                    "industry": random.choice(self.field_options.get("industry", ["Manufacturing"])),
                    "fax": f"+62-21-{random.randint(1000000, 9999999)}",
                    "market_segment": random.choice(self.field_options.get("market_segment", ["Middle Income"])),

                    # Address
                    "city": self.fake.city(),
                    "state": random.choice(["DKI Jakarta", "Jawa Barat", "Jawa Timur", "Jawa Tengah", "Sumatera Utara"]),
                    "country": "Indonesia",  # As required

                    # Analytics - Fixed: Now uses UTM Source field correctly
                    # Changed 'source' to 'utm_source' as requested
                    "utm_source": random.choice(self.field_options.get("utm_source", ["Website"])),

                    # Qualification
                    "qualification_status": random.choice(self.field_options.get("qualification_status", ["Unqualified"])),
                    "qualified_by": random.choice(self.field_options.get("qualified_by", ["Administrator"])),
                    "qualified_on": self.generate_date_before_june_2025(),

                    # Additional Information
                    "company": COMPANY_NAME,  # As required
                    "print_language": "en",  # English as required
                }

                # Add middle name if generated
                if middle_name:
                    lead_data["middle_name"] = middle_name

                # Create lead
                lead_result = self.api.create_doc("Lead", lead_data)

                # Handle response
                lead_doc_id = None
                if isinstance(lead_result, dict):
                    lead_doc_id = (lead_result.get("name") or
                                   lead_result.get("data", {}).get("name") if isinstance(lead_result.get("data"), dict) else None or
                                   lead_result.get("message", {}).get("name") if isinstance(lead_result.get("message"), dict) else None)

                if lead_doc_id or lead_result:
                    lead_info = {
                        "name": lead_doc_id or f"Lead-{i+1}",
                        "lead_name": lead_name,
                        "organization": organization_name,
                        "status": lead_data["status"],
                        "email": email,
                        # Use utm_source here for reporting
                        "source": lead_data["utm_source"]
                    }
                    created_leads.append(lead_info)

                    # Status indicators
                    status_indicator = {"Lead": "🔥", "Open": "📂", "Replied": "💬",
                                        "Opportunity": "💎", "Converted": "✅", "Lost Quotation": "❌",
                                        "Interested": "👀", "Do Not Contact": "🚫", "Quotation": "📄"}.get(lead_data["status"], "📋")

                    logger.info(
                        f"✅ Created lead {i+1}/{leads_to_create}: {lead_name}")
                    logger.info(f"    - Organization: {organization_name}")
                    logger.info(
                        f"    - Status: {lead_data['status']} {status_indicator}")
                    # Use utm_source for display
                    logger.info(f"    - UTM Source: {lead_data['utm_source']}")
                    logger.info(f"    - Email: {email}")
                    logger.info(f"    - Territory: {lead_data['territory']}")
                    logger.info(f"    - Industry: {lead_data['industry']}")

                else:
                    logger.warning(
                        f"⚠️ Lead {i+1} created but no document ID returned")

                # Small delay to avoid overwhelming server
                # time.sleep(0.1) # Removed as per user request

            except Exception as e:
                logger.error(f"❌ Failed to create lead {i+1}: {str(e)}")

        # Final summary
        print("\n" + "=" * 80)
        print("📊 LEAD CREATION SUMMARY")
        print("=" * 80)
        print(f"✅ Leads Created: {len(created_leads)}")

        # Status distribution
        status_count = {}
        for lead in created_leads:
            status = lead.get("status", "Unknown")
            status_count[status] = status_count.get(status, 0) + 1

        print(f"\n📋 Lead Status Distribution (Random):")
        for status, count in status_count.items():
            percentage = (count / len(created_leads) *
                          100) if created_leads else 0
            status_indicator = {"Lead": "🔥", "Open": "📂", "Replied": "💬",
                                "Opportunity": "💎", "Converted": "✅", "Lost Quotation": "❌",
                                "Interested": "👀", "Do Not Contact": "🚫", "Quotation": "📄"}.get(status, "📋")
            print(
                f"    {status_indicator} {status}: {count} ({percentage:.1f}%)")

        # Source distribution
        source_count = {}
        for lead in created_leads:
            # 'source' here refers to the key in lead_info, which now holds utm_source
            source = lead.get("source", "Unknown")
            source_count[source] = source_count.get(source, 0) + 1

        print(f"\n📡 UTM Source Distribution:")
        sorted_sources = sorted(source_count.items(),
                                key=lambda x: x[1], reverse=True)
        for source, count in sorted_sources[:10]:  # Show top 10 sources
            percentage = (count / len(created_leads) *
                          100) if created_leads else 0
            print(f"    📡 {source}: {count} ({percentage:.1f}%)")

        if len(sorted_sources) > 10:
            remaining = sum(count for _, count in sorted_sources[10:])
            print(
                f"    📡 ... and {len(sorted_sources) - 10} more sources ({remaining} leads)")

        print(
            f"\n🏢 Organizations: {len(set(lead['organization'] for lead in created_leads))} unique companies")
        print("=" * 80)

        logger.info(f"Successfully created {len(created_leads)} leads")
        return created_leads

    def run(self):
        """Main execution method"""
        print("=" * 80)
        print("📊 ERPNext Lead Generator v1.1.0")
        print("=" * 80)
        print(f"📡 API Endpoint: {BASE_URL}")
        print(f"🏢 Company: {COMPANY_NAME}")
        print(f"🔑 Using API Key: {API_KEY[:8] if API_KEY else 'Not Set'}...")
        print(f"🎯 Target Leads: {TARGET_LEADS}")
        print("🔧 Fixed: UTM Source field mapping")
        print("🔧 Fixed: Random status distribution (no Lead bias)")
        print("=" * 80)

        try:
            # Create leads
            leads = self.create_leads()

            print(f"\n🎉 LEAD GENERATION COMPLETED!")
            print(f"📊 Total Leads: {len(leads) if leads else 0}")

        except Exception as e:
            logger.error(f"Fatal error during lead generation: {str(e)}")
            print(f"\n💥 FATAL ERROR: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check if API key/secret have Lead creation permissions")
            print(f"2. Verify ERPNext is running at {BASE_URL}")
            print("3. Make sure the company name matches ERPNext configuration")
            print("4. Verify UTM Source doctype exists and has data")


def main():
    """Main entry point"""
    print("🚀 Starting ERPNext Lead Generation...")

    # Check if API credentials are set
    if not API_KEY or not API_SECRET:
        print("❌ Error: API_KEY and API_SECRET must be set in API/.env file")
        print("\n📋 Required .env file format:")
        print("API_KEY=your_api_key_here")
        print("API_SECRET=your_api_secret_here")
        print("BASE_URL=http://localhost:8080")
        print("COMPANY_NAME=PT Fiyansa Mulya")
        print("COMPANY_ABBR=PFM")
        return

    response = input(
        f"\nThis will create up to {TARGET_LEADS} leads in your ERPNext instance. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return

    try:
        generator = LeadGenerator()
        generator.run()
    except Exception as e:
        print(f"\n💥 Error: {e}")


if __name__ == "__main__":
    main()

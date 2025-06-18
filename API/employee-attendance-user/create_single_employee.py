import requests
import json
import logging

# --- Configuration (Hardcoded for simplicity in this test script) ---
class Config:
    API_KEY = "24e6b0843a3d816"
    API_SECRET = "8e0e08a033d1e56"
    BASE_URL = "http://localhost:8080"
    COMPANY_NAME = "PT Fiyansa Mulya" # IMPORTANT: Must exactly match your ERPNext company name!

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- ERPNext API Interaction Class (Minimal) ---
class ERPNextAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {Config.API_KEY}:{Config.API_SECRET}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = Config.BASE_URL
    
    def create_doc(self, doctype: str, data: dict) -> dict:
        """Create a new document in ERPNext."""
        url = f"{self.base_url}/api/resource/{doctype}"
        data["doctype"] = doctype # Ensure doctype is in the payload
        logger.info(f"Attempting to create {doctype} with data: {data}")
        try:
            response = self.session.post(url, json=data)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to create {doctype}: {e}")
            logger.error(f"Response content: {e.response.text}")
            raise # Re-raise the exception after logging details
        except requests.exceptions.RequestException as e:
            logger.error(f"Network or request error: {e}")
            raise

# --- Main Logic ---
def main():
    print("--- Attempting to Create Single Employee ---")
    
    api = ERPNextAPI()

    # Define the minimal employee data
    employee_data = {
        "employee_name": "Script Test Employee 01", # Must be unique
        "first_name": "Script Test",
        "last_name": "Employee",
        "gender": "Other",
        "date_of_birth": "1990-01-01",
        "status": "Active",
        "company": Config.COMPANY_NAME, # Link to your company
        "date_of_joining": "2024-01-01", # Often a mandatory field
        # We are intentionally leaving out optional fields like:
        # "designation", "department", "user_id", "holiday_list", "reports_to"
        # to test the most basic creation.
    }

    try:
        new_employee = api.create_doc("Employee", employee_data)
        logger.info(f"Successfully created employee: {new_employee.get('name')} (Full Name: {new_employee.get('employee_name')})")
        print("\n--- Employee creation successful! ---")
    except Exception as e:
        logger.critical(f"Script failed to create employee. See logs above for details.")
        print("\n--- Employee creation FAILED. Check logs for the exact error. ---")
        print("Possible causes:")
        print("1. Your API key/secret does not have 'Create' permission for Employee doctype.")
        print("2. 'employee_name' (Script Test Employee 01) already exists.")
        print("3. One of the fields (e.g., 'company', 'date_of_joining', 'gender', 'status') is mandatory in your ERPNext setup and missing/invalid.")
        print("4. A server-side validation/hook prevented creation.")

if __name__ == "__main__":
    main()
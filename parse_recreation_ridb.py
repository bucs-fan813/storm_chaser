from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import sys
import os
import pandas as pd
from dotenv import load_dotenv
from typing import Any, List, Set

load_dotenv()

# Base URLs
BASE_CAMPSITES_URL = "https://ridb.recreation.gov/api/v1/campsites"
BASE_FACILITY_URL = "https://ridb.recreation.gov/api/v1/facilities"
BASE_ORGANIZATION_URL = "https://ridb.recreation.gov/api/v1/organizations"

API_KEY = os.environ.get('API_KEY') # https://ridb.recreation.gov/profile

HEADERS = {
    "accept": "application/json",
    "apikey": API_KEY
}

# Define the retry strategy
retries = Retry(
    total=5,                                # Maximum 5 retries
    backoff_factor=1,                       # Exponential backoff (1s, 2s, 4s, etc.)
    status_forcelist=[502, 503, 504, 429],  # Retry on these HTTP status codes
    allowed_methods=["GET"]                 # Apply retry only to GET requests
)

# Create a session and mount the adapter
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update(HEADERS)

all_rv: List[Any] = []
OFFSET: int = 0
LIMIT: int = 50
OUTPUT_CSV: str = "RV_Sites.csv"
facilities: Set[Any] = set()
total_count = 0

try:
    # First request to get total count
    initial = session.get(BASE_CAMPSITES_URL, params={"limit": 1, "offset": 0})
    initial.raise_for_status()
    total_count = initial.json().get("METADATA", {}).get("RESULTS", {}).get("TOTAL_COUNT", 0)
    # while OFFSET < 200:  # Limit to first 200 records for testing
    while OFFSET < total_count:
        campsite_response = session.get(BASE_CAMPSITES_URL, params={"limit": LIMIT, "offset": OFFSET})
        campsite_response.raise_for_status()
        campsite_data = campsite_response.json().get("RECDATA", [])

        # Filter for campsites permitting RVs
        for site in campsite_data:
            permitted = site.get("PERMITTEDEQUIPMENT", [])
            campsiteType = site.get("CampsiteType", [])
            attributes = site.get("ATTRIBUTES", [])
            facility_id = site.get("FacilityID")
            if facility_id not in facilities \
                and any(item in campsiteType.upper() for item in ["STANDARD", "RV"]) \
                and any("RV" in eq.get("EquipmentName", "") for eq in permitted) \
                and any(attr.get("AttributeName").upper() == "WATER HOOKUP" and attr.get("AttributeValue").upper() == "YES" for attr in attributes) \
                and any(attr.get("AttributeName").upper() == "ELECTRICITY HOOKUP" and attr.get("AttributeValue").upper() != "N/A" for attr in attributes) \
                and any(attr.get("AttributeName").upper() == "SEWER HOOKUP" and attr.get("AttributeValue").upper() != "N/A" for attr in attributes):

                # Fetch facility details
                facility_response = session.get(f"{BASE_FACILITY_URL}/{facility_id}", params={"full": "true"})
                facility_response.raise_for_status()
                facility_data = facility_response.json()
                organization_data = facility_data.get("ORGANIZATION", [])[0] if facility_data.get('ORGANIZATION') is not None else {}

                # Combine records
                record = {
                    **site,
                    "FacilityID": facility_id,
                    "FacilityName": facility_data.get("FacilityName"),
                    "FacilityTypeDescription": facility_data.get("FacilityTypeDescription"),
                    "FacilityLongitude": facility_data.get("FacilityLongitude"),
                    "FacilityLatitude": facility_data.get("FacilityLatitude"),
                    "FacilityOrganization": facility_data.get("ORGANIZATION", []),
                    "OrgId": organization_data.get("ParentOrgID", None),
                    "OrgName": organization_data.get("OrgName", None),
                    "OrgURLAddress": organization_data.get("OrgURLAddress", None),
                    "OrgType": organization_data.get("OrgType", None),
                    "OrgParentID": organization_data.get("OrgParentID", None),
                    "OrgAbbrevName": organization_data.get("OrgAbbrevName", None)
                    # "RecAreaName": recreational_data.get("RecAreaName",[])
                }
                facilities.add(facility_id)
                all_rv.append(record)
        print(f"Processed {len(campsite_data)} records (offset {OFFSET}); found {len(all_rv)} RV sites so far.")

        OFFSET += LIMIT
        if OFFSET >= total_count:
            break

    df = pd.json_normalize(all_rv)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"--- Parsed data saved to {OUTPUT_CSV}---")

    # Also print the CSV content to stdout
    if OFFSET <= 200:
        print(f"\n--- Sample data fetched. Total RV-capable campsites: {len(all_rv)} ---")
        df.to_csv(sys.stdout, index=False)
except requests.exceptions.RequestException as e:
    print(f"Error fetching data after retries: {e}")
except ValueError as e:
    print(f"Error parsing JSON response: {e}")

print(f"\nTotal campsites queried: {total_count}")
print(f"\nTotal RV-capable campsites fetched: {len(all_rv)}")

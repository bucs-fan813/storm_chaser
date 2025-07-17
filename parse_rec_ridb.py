import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables from .env file
load_dotenv()

# Set Globals
DATA_SOURCE: str = 'REC_RIDB'                                 # Name of data source. eg. "USDA_NASS"
TIMESTAMP: str = f'{datetime.now():%Y%m%dT%H%M%S}'            # Current timestamp
OUTPUT_CSV: str = f'{DATA_SOURCE}_{TIMESTAMP}.csv'            # Output filename
facilities= set()
API_KEY: Optional[str] = os.environ.get(f'{DATA_SOURCE}_API_KEY')
API_BASE_URL: Dict[str, str] = {
    'CAMPSITES': 'https://ridb.recreation.gov/api/v1/campsites',
    'FACILITIES': 'https://ridb.recreation.gov/api/v1/facilities',
    'ORGANIZATIONS': 'https://ridb.recreation.gov/api/v1/organizations'
}
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

# Define prototypes
# example: List[Dict[str, Any]] = []

# Fetch data from API
def fetch_data(url: str, params: Dict[str, Any]) -> List[Any]:
    metadata = session.get(url, params=params)
    metadata.raise_for_status()
    total_count: int = metadata.json().get("METADATA", {}).get("RESULTS", {}).get("TOTAL_COUNT", 0)
    if total_count == 0:
        raise ValueError("No results found")
    key: str = params.get("KEY", "")
    records: List[Any] = []
    offset: int = 0  # Initialize OFFSET locally
    limit: int = 50
    print(f'Fetching: {total_count} total records: {limit} records at a time')
    # while offset < 200:  # Limit to first 200 records for testing
    while offset < total_count:
        response = session.get(url, params={"limit": limit, "offset": offset})
        response.raise_for_status()
        chunk = response.json().get(key, [])
        for campsite in chunk:
            permitted = campsite.get("PERMITTEDEQUIPMENT", [])
            campsiteType = campsite.get("CampsiteType", [])
            attributes = campsite.get("ATTRIBUTES", [])
            facility_id = campsite.get("FacilityID")
            if facility_id not in facilities \
                and any(item in campsiteType.upper() for item in ["STANDARD", "RV"]) \
                and any("RV" in eq.get("EquipmentName", "") for eq in permitted) \
                and any(attr.get("AttributeName").upper() == "WATER HOOKUP" and attr.get("AttributeValue").upper() == "YES" for attr in attributes) \
                and any(attr.get("AttributeName").upper() == "ELECTRICITY HOOKUP" and attr.get("AttributeValue").upper() != "N/A" for attr in attributes) \
                and any(attr.get("AttributeName").upper() == "SEWER HOOKUP" and attr.get("AttributeValue").upper() != "N/A" for attr in attributes):
                
                # Fetch facility details
                facility_url: str = API_BASE_URL.get("FACILITIES", "")
                facility_response = session.get(f"{facility_url}/{facility_id}", params={"full": "true"})
                facility_response.raise_for_status()
                facility_data = facility_response.json()
                organization_data = facility_data.get("ORGANIZATION")[0] if facility_data.get('ORGANIZATION', None) is not None else {}

                # Combine records
                record = {
                    **campsite,
                    "FacilityID": facility_id,
                    "FacilityName": facility_data.get("FacilityName"),
                    "FacilityTypeDescription": facility_data.get("FacilityTypeDescription"),
                    "FacilityLongitude": facility_data.get("FacilityLongitude"),
                    "FacilityLatitude": facility_data.get("FacilityLatitude"),
                    "FacilityOrganization": facility_data.get("ORGANIZATION", []),
                    "OrgId": organization_data.get("OrgID", None),
                    "OrgName": organization_data.get("OrgName", None),
                    "OrgType": organization_data.get("OrgType", None),
                    "OrgAbbrevName": organization_data.get("OrgAbbrevName", None)
                }
                facilities.add(facility_id)
                records.append(record)
        print(f"Fetched {len(chunk)} records ({offset}-{offset + limit}); found {len(records)} matches so far.")
        offset += limit
        if offset >= total_count:
            break
    return records

# Main execution
if __name__ == "__main__":
    if len(sys.argv) == 1:
        camp_url: Optional[str] = API_BASE_URL.get("CAMPSITES", "")
        print(f"Fetching {DATA_SOURCE} data from {camp_url} =>")
        campsites = fetch_data(camp_url, {"KEY": "RECDATA" })
        if len(campsites) > 0:
            print(f"--- {DATA_SOURCE} data fetched: {len(campsites)} records ---")
            print(f"--- Saved to {OUTPUT_CSV} ---")
            df: pd.DataFrame = pd.json_normalize(campsites)
            # df.to_csv(sys.stdout, index=False)
            df.to_csv(OUTPUT_CSV, index=False)
    elif sys.argv[1] in ["--help", "-h", "/?"]:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(0)
    elif len(sys.argv) == 2:
        CSV_FILE=sys.argv[1] # CSV file path passed as command-line argument
        print(f"Fetching {DATA_SOURCE} data from {CSV_FILE}...")
    else:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(1)

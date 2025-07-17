import os
import sys
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables from .env file
load_dotenv()

# Set Globals
DATA_SOURCE: str = "NASS_USDA"
API_BASE_URL: str = "https://quickstats.nass.usda.gov/api/api_GET/"
TIMESTAMP: str = f"{datetime.now():%Y%m%dT%H%M%S}"
OUTPUT_FILENAME: str = f"NASS_USDA_{TIMESTAMP}.csv"
API_KEY = os.environ.get('NASS_API_KEY') # https://quickstats.nass.usda.gov/api
records: List[Dict] = []

# Define the retry strategy
retries = Retry(
    total=10,                                       # Maximum 5 retries
    backoff_factor=3,                               # Exponential backoff (1s, 2s, 4s, etc.)
    status_forcelist=[502, 503, 504, 429, 403],     # Retry on these HTTP status codes
    allowed_methods=["GET"],                        # Apply retry only to GET requests
    backoff_max=60                                  # Maximum backoff time of 60 seconds
)

# Create a session and mount the adapter
session: requests.Session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))
# session.headers.update(HEADERS)

# Define the list of counties to include: (county, state_abbr, focus_area)
# Waring: Check spelling and spaces! LaPaz != La Paz, Le Flore != LeFlore, etc.
counties = [
    ("Wayne", "TN", 1), ("Hardin", "TN", 1),("Lauderdale", "AL", 1), ("Tishomingo", "MS", 1),
    ("Alcorn", "MS", 1), ("Butler", "AL", 10), ("Monroe", "AL", 10), ("Wilcox", "AL", 10),
    ("Leon", "TX", 12), ("Freestone", "TX", 12), ("Anderson", "TX", 12), ("Houston", "TX", 12),
    ("Madison", "TX", 12), ("Robertson", "TX", 12), ("Limestone", "TX", 12), ("Falls", "TX", 12),
    ("Gallatin", "IL", 2), ("Pope", "IL", 2), ("Saline", "IL", 2), ("Crittenden", "KY", 2),
    ("Livingston", "KY", 2), ("Marshall", "KY", 2), ("McCracken", "KY", 2), ("Lyon", "KY", 2),
    ("Union", "KY", 2), ("Johnson", "IL", 2), ("Massac", "IL", 2), ("Williamson", "IL", 2),
    ("Greene", "MO", 11), ("Webster", "MO", 11), ("Christian", "MO", 11), ("Douglas", "MO", 11),
    ("Stone", "MO", 11), ("Lawrence", "MO", 11), ("Butler", "KS", 9), ("Elk", "KS", 9),
    ("Greenwood", "KS", 9), ("Chase", "KS", 9), ("Lyon", "KS", 9), ("Marion", "KS", 9),
    ("Harvey", "KS", 9), ("Sedgwick", "KS", 9), ("Story", "IA", 4), ("Hamilton", "IA", 4),
    ("Hardin", "IA", 4), ("Marshall", "IA", 4), ("Jasper", "IA", 4), ("Polk", "IA", 4),
    ("Boone", "IA", 4), ("Webster", "IA", 4), ("Tama", "IA", 4), ("Poweshiek", "IA", 4),
    ("Marion", "IA", 4), ("Pontotoc", "OK", 3), ("Coal", "OK", 3), ("Atoka", "OK", 3),
    ("Pushmataha", "OK", 3), ("Pittsburg", "OK", 3), ("Latimer", "OK", 3), ("LeFlore", "OK", 3),
    ("McCurtain", "OK", 3), ("Choctaw", "OK", 3), ("Bryan", "OK", 3), ("Marshall", "OK", 3),
    ("Johnston", "OK", 3), ("Murray", "OK", 3), ("Carter", "OK", 3), ("Stephens", "OK", 3),
    ("Garvin", "OK", 3), ("Comanche", "OK", 3), ("Cotton", "OK", 3), ("Jefferson", "OK", 3),
    ("Love", "OK", 3), ("Bryan", "OK", 3), ("Mohave", "AZ", 5), ("LaPaz", "AZ", 5),
    ("Yavapai", "AZ", 5), ("Clark", "NV", 5), ("Tehama", "CA", 6), ("Shasta", "CA", 6),
    ("Plumas", "CA", 6), ("Glenn", "CA", 6), ("Trinity", "CA", 6), ("Cowlitz", "WA", 7),
    ("Lewis", "WA", 7), ("Wahkiakum", "WA", 7), ("Skamania", "WA", 7), ("San Bernardino", "CA", 5),
    ("Riverside", "CA", 5), ("Imperial", "CA", 5), ("Butte", "CA", 6)
]

# Define the dictionary for states names and abbreviations
# https://code.activestate.com/recipes/577305-python-dictionary-of-us-states-and-territories/
states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}

# Define the dictionary for zones and focus areas: {focus_area: zone}
# https://goo.gl/maps/5VRgyPLMSqyMueRg6?g_st=a
zones = {
        '1': 4,
        '2': 2,
        '3': 4,
        '4': 4,
        '5': 2,
        '6': 2,
        '7': 3,
        '8': 5,
        '9': 4,
        '10': 1,
        '11': 4,
        '12': 1,
}

# Fetch average price per acre from USDA NASS API
def get_avg_price(state_name: str, county_name: str) -> float | None:
    params = {
        "key": API_KEY,
        "source_desc": "CENSUS",
        "sector_desc": "ECONOMICS",
        "group_desc": "FARMS & LAND & ASSETS",
        "commodity_desc": "AG LAND",
        "statisticcat_desc": "ASSET VALUE",
        "short_desc": "AG LAND, INCL BUILDINGS - ASSET VALUE, MEASURED IN $ / ACRE",
        "domain_desc": "TOTAL",
        "agg_level_desc": "COUNTY",
        "state_name": state_name.upper(),
        "county_name": county_name.upper(),
        "year": "2022",
        "reference_period_desc": "END OF DEC",
        "format": "JSON"
    }
    response: requests.Response = session.get(API_BASE_URL, params=params)
    response.raise_for_status()
    data: dict[str, Any] = response.json()

    if data:
        return data["data"][0].get("Value")
    return None

# Main execution
if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(f"Fetching {DATA_SOURCE} data from {API_BASE_URL} =>")
    elif sys.argv[1] in ["--help", "-h", "/?"]:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(0)
    elif len(sys.argv) == 2:
        SOURCE=sys.argv[1] # CSV file path passed as command-line argument
        print(f"Fetching FEMA NRI data from {SOURCE}...")
    else:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(1)


# TODO: Read CSV from argv[1] if provided, otherwise use API_BASE_URL
for county, abbr, focus_area in counties:
    state: str = f'{states.get(abbr)}'
    record = {
        "Place Names": f"{county} County, {abbr}",
        "COUNTY": county.upper(),
        "STATE": f"{states.get(abbr)}".upper(),
        "STATE_ABV": abbr,
        "ZONE": zones.get(focus_area.__str__()),
        "FOCUS_AREA": focus_area,
        "LANDWATCH_URL": f'https://www.landwatch.com/{state.lower().replace(" ", "-")}-land-for-sale/{county.lower().replace(" ", "-")}-county/price-under-49999/acres-under-50//sort-price-low-high',
        "PP_ACRE": f'{get_avg_price(state, county.upper())}'
    }
    records.append(record)
    print(f"Processing: {record.get('Place Names')}")

if len(records) > 0:
    print(f"--- {DATA_SOURCE} data fetched: {len(records)} records ---")
    print(f"--- Saved to {OUTPUT_FILENAME} ---")
    df: pd.DataFrame = pd.json_normalize(records)
    # df.to_csv(sys.stdout, index=False)
    df.to_csv(OUTPUT_FILENAME, index=False)

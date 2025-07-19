import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Fetch alerts from the National Weather Service API with specific filters.
# Usage: python fetch_weather_alerts.py [output_file.csv]

# Set Globals
DATA_SOURCE: str = 'NWS_ALERTS'                              # Name of data source. eg. "NASS_USDA"
TIMESTAMP: str = f'{datetime.now():%Y%m%dT%H%M%S}'           # Current timestamp
API_BASE_URL: str = 'https://api.weather.gov/alerts'         # API URL endpoint
OUTPUT_FILENAME: str = f'{DATA_SOURCE}_{TIMESTAMP}.csv'      # Output filename
records: List[Dict] = []                                     # List to hold records

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


# Default filter parameters
PARAMS = {
    "status": "actual",
    "message_type": "alert",
    "region_type": "land",
    # "urgency": ",".join(["Immediate", "Expected", "Future"]),
    "urgency": "Immediate",
    # "severity": ",".join(["Extreme", "Severe"]),
    "severity": "Extreme",
    "certainty": "Observed"
}

# NWS API requires a valid User-Agent header
HEADERS = {
    "User-Agent": "Storm Chaser",
    "Accept": "application/geo+json"
}


def fetch_all_alerts():
    """
    Retrieves all weather alerts matching the filter parameters,
    following pagination links until completion.
    Returns a list of alert feature dicts.
    """
    alerts: List = []
    url: Optional[str] = API_BASE_URL
    params: Optional[Dict[str,str]] = PARAMS.copy()

    while url:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        features = data.get("features", [])
        alerts.extend(features)

        # Determine next page URL from Link header
        link_header = response.headers.get("Link")
        next_url = None
        if link_header:
            # Parse link header, find rel="next"
            parts = link_header.split(',')
            for part in parts:
                section = part.split(';')
                if len(section) < 2:
                    continue
                url_part = section[0].strip()[1:-1]
                rel_part = section[1].strip()
                if 'rel="next"' in rel_part:
                    next_url = url_part
                    break

        url = next_url
        # After first request, parameters should not be resent
        params = None
    return alerts

def main():
    # Optionally write output to file
    OUTPUT_CSV = f'{DATA_SOURCE}_{TIMESTAMP}.csv'
    alert_areas: Set[Any] = set()
    all_alerts: List = []

    try:
        alerts = fetch_all_alerts()
        print(f"Fetched {len(alerts)} alerts.")
        for alert in alerts:
            properties = alert.get("properties", {})
            if properties.get('areaDesc') not in alert_areas:
                alert_areas.add(properties.get('areaDesc'))
                for location in properties.get('areaDesc').split(';'):
                    # Create records
                    record = {
                        "Place Names": location.strip(),
                        "Headline": properties.get('headline'),
                        "SenderName": properties.get('senderName'),
                        "Event": properties.get('event'),
                        "Severity": properties.get('severity'),
                        "Urgency": properties.get('urgency'),
                        "Certainty": properties.get('certainty'),
                    }
                    all_alerts.append(record)

        # json.dump(alerts, f, indent=2)
        df = pd.json_normalize(all_alerts)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"--- Parsed data saved to {OUTPUT_CSV}---")
        # Print JSON to stdout
        for alert in all_alerts:
            print(f"- \
{alert['Place Names']} | \
{alert['Event']} | \
{alert['Headline']} | \
{alert['Severity']} | \
{alert['Urgency']} | \
{alert['Certainty']}"
)
        print(f"{len(alert_areas)} unique alerts.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching alerts: {e}", file=sys.stderr)
        sys.exit(1)

# Main execution
if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(f"Fetching {DATA_SOURCE} data from {API_BASE_URL} =>")
    elif sys.argv[1] in ["--help", "-h", "/?"]:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(0)
    elif len(sys.argv) == 2:
        CSV_FILE=sys.argv[1] # CSV file path passed as command-line argument
        print(f"Fetching {DATA_SOURCE} data from {CSV_FILE}...")
    else:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(1)

main()

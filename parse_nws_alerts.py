import requests
import sys
import json
import pandas as pd
from typing import Any, List, Set

# Fetch alerts from the National Weather Service API with specific filters.
# Usage: python fetch_weather_alerts.py [output_file.csv]

API_URL = "https://api.weather.gov/alerts"

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
    alerts = []
    url = API_URL
    params = PARAMS.copy()

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
    output_file = None
    alertAreas: Set[Any] = set()
    all_alerts: List = []

    if len(sys.argv) > 1:
        output_file = sys.argv[1]

    try:
        alerts = fetch_all_alerts()
        print(f"Fetched {len(alerts)} alerts.")
        for alert in alerts:
            properties = alert.get("properties", {})
            if properties.get('areaDesc') not in alertAreas:
                alertAreas.add(properties.get('areaDesc'))
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

        if output_file:
            # json.dump(alerts, f, indent=2)
            df = pd.json_normalize(all_alerts)
            df.to_csv(output_file, index=False)
            print(f"--- Parsed data saved to {output_file}---")
            print(f"Alerts written to {output_file}")
        else:
            # Print JSON to stdout
            for alert in all_alerts:
                print(f"- {alert['Event']} | {alert['Headline']} | {alert['Severity']} | {alert['Urgency']} | {alert['Certainty']}")
            print(f"{len(alertAreas)} unique alerts.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching alerts: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

import os
import sys
import zipfile
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set Globals
DATA_SOURCE: str = 'FEMA_NRI'                                           # Name of data source. eg. "USDA_NASS"
TIMESTAMP: str = f'{datetime.now():%Y%m%dT%H%M%S}'                      # Current timestamp
API_BASE_URL: str = 'https://www.fema.gov/api/open'                     # API URL endpoint
OUTPUT_CSV: str = f'{DATA_SOURCE}_{TIMESTAMP}.csv'                      # Output filename
records: List[Dict] = []                                                # List to hold records
# df: Optional[pd.DataFrame] = None

# Define the retry strategy
retries = Retry(
    total=10,                                                           # Maximum 10 retries
    backoff_factor=3,                                                   # Exponential backoff (1s, 2s, 4s, etc.)
    status_forcelist=[502, 503, 504, 429, 403],                         # Retry on these HTTP status codes
    allowed_methods=["GET"],                                            # Apply retry only to GET requests
    backoff_max=60                                                      # Maximum backoff time of 60 seconds
)

# Create a session and mount the adapter
session: requests.Session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))
# session.headers.update(HEADERS)

# Define prototypes
EXTRACT_FILES = {
    "NRI_Table_Counties.csv",
    "NRI_HazardInfo.csv",
    "NRIDataDictionary.csv"
}

primary_disasters: List[str] = [
    "CFLD",
    "ERQK",
    "HRCN",
    "LNDS",
    "RFLD",
    "TRND",
    "WFIR",
    ]

# secondary_disasters = [
    # 'CWAV',
    # 'DRGT',
    # 'HWAV',
    # 'ISTM',
    # 'SWND',
    # 'TSUN',
    # 'VLCN',
    # 'WNTW'
# ]

# disasters = primary_disasters + secondary_disasters

# List of columns we want to extract
def get_selected_columns() -> List[str]:
    selected_columns = [
        "STATE",
        "STATEABBRV",
        "COUNTY",
        "POPULATION",
        "BUILDVALUE",
        "AGRIVALUE",
        "RISK_VALUE",
        "AREA",
        "RISK_SCORE",
        "RISK_RATNG",
        "EAL_VALT"
    ]

    # Add disaster-specific risk columns:
    ## _RISKV = Hazard Type Risk Index Value
    ## _RISKS = Hazard Type Risk Index Score
    ## _RISKR = Hazard Type Risk Index Rating
    for disaster in primary_disasters:
        selected_columns += [f"{disaster}_RISKV", f"{disaster}_RISKS", f"{disaster}_RISKR"]

    return selected_columns

# Function to parse FEMA National Risk Index CSV
def parse_dictionary() -> pd.Series:
    # Load the CSV file into a DataFrame
    # TODO: Download latest dictionary from if it does not exist locally
    # https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_Counties/NRI_Table_Counties.zip
    # Load the CSV file into a DataFrame
    
    dictionary_file = "NRI_HazardInfo.csv"  # Path to the dictionary CSV file
    dictionary_df: pd.DataFrame = pd.read_csv(dictionary_file)

    # Extract relevant columns (customize based on your need)
    dictionary_columns: List[str] = [
        "OID_",
        "Hazard",
        "Prefix",
        "Service",
        "Start",
        "End_",
        "TotalYears",
        "FrequencyModel",
    ]

    # Create a filtered DataFrame
    filtered_dictionary: pd.Series = dictionary_df[dictionary_columns]

    # Display basic info about columns (for reference)
    print("Columns in CSV:")
    # print(df.columns.tolist())
    return filtered_dictionary

# Function to parse FEMA National Risk Index CSV
def parse_fema_nri(disaster: str, df: pd.Series) -> pd.DataFrame:
    # Display basic info about columns (for reference)
    # print("Columns in CSV:")
    # print(df.columns.tolist())

    # Format specific columns
    # disaster_df.style.format({"POPULATION": "{:,d}"})
    # disaster_df.style.format({'RISK_VALUE': '${:,.2f}'})
    disaster_df = df.copy()
    # Add custom column concatenating COUNTY and STATE abbreviation
    # disaster_df["Place names"] = f'{disaster_df["COUNTY"]} County, {disaster_df["STATEABBRV"]}'
    disaster_df["Place names"] = (
        disaster_df["COUNTY"] + " County, " + disaster_df["STATEABBRV"]
    )
    # Filter the DataFrame where the specific risk is either 'Very High' or 'Relatively High'
    filtered_disaster_df = disaster_df[
        disaster_df[f"{disaster}_RISKR"].isin(["Very High", "Relatively High"])
    ]

    # Sort by Overall Risk Score
    sorted_df = filtered_disaster_df.sort_values(by="RISK_SCORE", ascending=False)

    # Return the sorted DataFrame
    return sorted_df

# Main execution
if __name__ == "__main__":
    if len(sys.argv) == 1:
        ZIP_FILENAME='NRI_Table_Counties.zip'
        CSV_FILE='NRI_Table_Counties.csv' # CSV file path passed as command-line argument
        # 1. Check if the zip already exists locally
        if not os.path.exists(ZIP_FILENAME):
            # print(f"Downloading {ZIP_FILENAME}...")
            print(f"Fetching {DATA_SOURCE} data from {API_BASE_URL} =>")
            resp = requests.get(f'https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_Counties/{ZIP_FILENAME}', stream=True)
            resp.raise_for_status()
            with open(ZIP_FILENAME, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Download complete.")
        else:
            print(f"{ZIP_FILENAME} already exists. Skipping download.")

        # 2. Extract only the desired CSV files
        if not os.path.exists(CSV_FILE):
            with zipfile.ZipFile(ZIP_FILENAME, "r") as z:
                for member in z.namelist():
                    filename = os.path.basename(member)
                    if filename in EXTRACT_FILES:
                        print(f"Extracting {filename}...")
                        z.extract(member, ".")
            print("Extraction complete.")
            print(f"Fetching {DATA_SOURCE} data from {CSV_FILE}...")
        else:
            print(f"{CSV_FILE} already exists. Skipping extraction.")
        df = pd.read_csv(CSV_FILE, usecols=get_selected_columns(), low_memory=False)


        # Filter the DataFrame where the overall risk is either 'Very High' or 'Relatively High'
        if df is not None:
            filtered_df: pd.Series = df[df["RISK_RATNG"].isin(["Very High", "Relatively High"])]
            for disater in primary_disasters:
                # Parse the CSV
                parsed_data = parse_fema_nri(disater, filtered_df)
                # parsed_data = parse_dictionary()

                # Save parsed data to new CSV file
                OUTPUT_CSV = f'{DATA_SOURCE}_{disater}_{TIMESTAMP}.csv'
                parsed_data.to_csv(OUTPUT_CSV.upper(), index=False)
                # parsed_data.to_csv(sys.stdout, index=False)
                print(f"--- Saved to {OUTPUT_CSV.upper()} ---")
        else:
            print("There was an error loading the CSV file.")
            sys.exit(1)
    elif sys.argv[1] in ["--help", "-h", "/?"]:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(0)
    else:
        print(f"Usage: {sys.argv[0]} [path_to_csv]")
        sys.exit(1)
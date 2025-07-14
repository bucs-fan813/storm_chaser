import sys

from typing import List
import pandas as pd

# my_dict = {"name": "Alice", "age": 30, "city": "New York"}


# Function to parse FEMA National Risk Index CSV
def parse_dictionary() -> pd.Series:
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
def parse_fema_nri(disaster: str, disaster_df: pd.DataFrame) -> pd.Series:
    # Display basic info about columns (for reference)
    # print("Columns in CSV:")
    # print(df.columns.tolist())

    # Format specific columns
    # disaster_df.style.format({"POPULATION": "{:,d}"})
    # disaster_df.style.format({'RISK_VALUE': '${:,.2f}'})

    # Add custom column concatenating COUNTY and STATE abbreviation
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
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <path_to_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]  # CSV file path passed as command-line argument
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
    #     'CWAV', 'DRGT', 'HWAV', 'ISTM',
    #     'SWND', 'TSUN', 'VLCN', 'WNTW', 
    # ]

    # disasters = primary_disasters + secondary_disasters

    # List of columns we want to extract
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
    for disater in primary_disasters:
        selected_columns += [f"{disater}_RISKV", f"{disater}_RISKS", f"{disater}_RISKR"]

    # Load the CSV file into a DataFrame
    df = pd.read_csv(input_csv, usecols=selected_columns, low_memory=False)

    # Filter the DataFrame where the overall risk is either 'Very High' or 'Relatively High'
    filtered_df = df[df["RISK_RATNG"].isin(["Very High", "Relatively High"])]

    for disater in primary_disasters:
        # Parse the CSV
        parsed_data = parse_fema_nri(disater, filtered_df)
        # parsed_data = parse_dictionary()

        # Save parsed data to new CSV file
        output_csv = f"{disater}_fema_nri.csv"
        parsed_data.to_csv(output_csv, index=False)
        print(f"Parsed data saved to {output_csv}")

        # Also print the CSV content to stdout
        print("\n--- Parsed FEMA NRI Data ---")
        parsed_data.to_csv(sys.stdout, index=False)

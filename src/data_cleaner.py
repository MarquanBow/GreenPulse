import os
import pandas as pd
from datetime import datetime, timezone

RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

def clean_air_quality_data(raw_file: str) -> pd.DataFrame:
    """
    Clean and process raw air quality data from OpenAQ.
    
    Steps:
    - Drop duplicates and missing values
    - Standardize column names
    - Convert datetime fields to UTC-aware datetimes
    - Compute daily averages per pollutant
    """

    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"Raw data file not found: {raw_file}")
    
    df = pd.read_csv(raw_file)

    if df.empty:
        print(f"No data founf in {raw_file}")
        return pd.DataFrame()
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.replace(".", "_")

    # Drop duplicates and missing essential values
    df = df.drop_duplicates()
    df = df.dropna(subset=["parameter", "value", "date_utc"])

    # Convert timestamps
    df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")

    # Drop invalid dates
    df = df.dropna(subset=["date_utc"])
    
    # Add date-only column for grouping
    df["date"] = df["date_utc"].dt.date

    # Compute daily average by parameter (pollutant)
    daily_avg = (
        df.groupby(["city", "country", "parameter", "unit", "date"])["value"]
        .mean()
        .reset_index()
        .sort_values(["date", "parameter"])
    )

    print(f"Cleaned {len(df)} records -> {len(daily_avg)} daily averages")
    return daily_avg

def save_clean_data(df: pd.DataFrame, city: str) -> str:
    """
    Save the cleaned air quality data to data/processed/ with UTC timestamp.
    """
    if df.empty:
        print("No cleaned data to save.")
        return ""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = os.path.join(PROCESSED_DIR, f"{city.lower()}_cleaned_{timestamp}.csv")
    df.to_csv(output_file, index=False)
    print(f"Clean data saved to {output_file}")
    return output_file

if __name__ == "__main__":
    # Example: Clean latest Charlotte file
    city = "Charlotte"
    raw_files = sorted(
        [f for f in os.listdir(RAW_DIR) if f.startswith(city.lower())],
        reverse=True
    )

    if not raw_files:
        print("No raw files found. Run data_fetcher.py first.")
    else:
        latest_file = os.path.join(RAW_DIR, raw_files[0])
        print(f"Cleaning {latest_file}...")
        df_clean = clean_air_quality_data(latest_file)
        save_clean_data(df_clean, city)
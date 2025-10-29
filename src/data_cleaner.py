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

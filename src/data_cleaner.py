import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")


def clean_air_quality_data(raw_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(raw_file_path)

    # Drop invalid rows
    df = df.dropna(subset=["value", "city", "parameter"])
    df["date.utc"] = pd.to_datetime(df["date.utc"], utc=True)

    # Convert to local date (just the date, not full datetime)
    df["date"] = df["date.utc"].dt.date

    # Group by date, parameter, city, and average value
    df_clean = (
        df.groupby(["city", "country", "parameter", "unit", "date"], as_index=False)
        .agg({"value": "mean"})
    )

    # Compute AQI where applicable
    df_clean["aqi"] = df_clean.apply(
        lambda row: calculate_aqi(row["parameter"], row["value"]), axis=1
    )

    return df_clean


def calculate_aqi(parameter: str, concentration: float) -> float | None:
    """Compute AQI using U.S. EPA breakpoints (simplified)."""
    # PM2.5 breakpoints (µg/m³)
    pm25_breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500),
    ]

    # NO₂ breakpoints (ppb)
    no2_breakpoints = [
        (0, 53, 0, 50),
        (54, 100, 51, 100),
        (101, 360, 101, 150),
        (361, 649, 151, 200),
        (650, 1249, 201, 300),
        (1250, 1649, 301, 400),
        (1650, 2049, 401, 500),
    ]

    if parameter.lower() == "pm25":
        breakpoints = pm25_breakpoints
    elif parameter.lower() == "no2":
        breakpoints = no2_breakpoints
    else:
        return None

    for (c_low, c_high, aqi_low, aqi_high) in breakpoints:
        if c_low <= concentration <= c_high:
            return round(
                ((aqi_high - aqi_low) / (c_high - c_low)) * (concentration - c_low)
                + aqi_low,
                1,
            )

    return None


def save_clean_data(df: pd.DataFrame, city_name: str) -> str:
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    filename = f"{city_name.lower().replace(' ', '_')}_clean.csv"
    path = os.path.join(PROCESSED_DIR, filename)
    df.to_csv(path, index=False)
    return path
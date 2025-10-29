import os 
import pandas as pd
import pytest 
from datetime import datetime, timezone
from src.data_cleaner import clean_air_quality_data, save_clean_data

# Create a small mock dataset
@pytest.fixture
def mock_raw_csv(tmp_path):
    data = {
        "city": ["Charlotte", "Charlotte", "Charlotte", None],
        "country": ["US", "US", "US", "US"],
        "parameter": ["pm25", "pm25", "no2", "pm25"],
        "value": [12.5, 15.0, 8.0, None],
        "unit": ["µg/m³", "µg/m³", "µg/m³", "µg/m³"],
        "date.utc": [
            "2025-10-25T00:00:00Z",
            "2025-10-25T01:00:00Z",
            "2025-10-25T00:00:00Z",
            "2025-10-25T02:00:00Z",
        ],
    }

    df = pd.DataFrame(data)
    file_path = tmp_path / "charlotte_raw.csv"
    df.to_csv(file_path, index=False)
    return file_path


def test_clean_air_quality_data_removes_duplicates_and_nulls(mock_raw_csv):
    df_clean = clean_air_quality_data(mock_raw_csv)

    # Should only contain valid rows
    assert "value" in df_clean.columns
    assert not df_clean.empty
    assert df_clean["value"].notna().all()
    assert df_clean["city"].notna().all()
    assert df_clean["parameter"].isin(["pm25", "no2"]).all()

    # Should have one record per day per parameter
    grouped = df_clean.groupby(["date", "parameter"]).size()
    assert (grouped == 1).all()


def test_clean_air_quality_data_aggregates_correctly(mock_raw_csv):
    df_clean = clean_air_quality_data(mock_raw_csv)

    # PM2.5 average: (12.5 + 15.0) / 2 = 13.75
    pm25_row = df_clean[df_clean["parameter"] == "pm25"].iloc[0]
    assert round(pm25_row["value"], 2) == 13.75


def test_save_clean_data_creates_file(tmp_path):
    df = pd.DataFrame({
        "city": ["Charlotte"],
        "country": ["US"],
        "parameter": ["pm25"],
        "unit": ["µg/m³"],
        "date": [datetime(2025, 10, 25).date()],
        "value": [13.7],
    })

    out_dir = tmp_path / "processed"
    os.makedirs(out_dir, exist_ok=True)

    # Temporarily change save path
    from src import data_cleaner
    data_cleaner.PROCESSED_DIR = str(out_dir)

    out_path = save_clean_data(df, "Charlotte")
    assert os.path.exists(out_path)
    assert out_path.endswith(".csv")

    saved = pd.read_csv(out_path)
    assert "value" in saved.columns
    assert abs(saved["value"].iloc[0] - 13.7) < 0.001

def test_calculate_aqi_pm25_within_range():
    from src.data_cleaner import calculate_aqi
    aqi = calculate_aqi("pm25", 25.0)  # should fall in 51–100 range
    assert 51 <= aqi <= 100


def test_calculate_aqi_no2_within_range():
    from src.data_cleaner import calculate_aqi
    aqi = calculate_aqi("no2", 120.0)
    assert 101 <= aqi <= 150
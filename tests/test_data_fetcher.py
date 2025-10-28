import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.data_fetcher import fetch_air_quality

# Mocked sample API data
MOCK_API_RESPONSE = {
    "results": [
        {
            "location": "Charlotte Uptown",
            "city": "Charlotte",
            "country": "US",
            "parameter": "pm25",
            "value": 12.3,
            "unit": "µg/m³",
            "date": {"utc": "2025-10-27T00:00:00Z"}
        },
        {
            "location": "Charlotte Uptown",
            "city": "Charlotte",
            "country": "US",
            "parameter": "no2",
            "value": 8.7,
            "unit": "µg/m³",
            "date": {"utc": "2025-10-27T00:00:00Z"}
        }
    ]
}

@pytest.fixture
def mock_success_response():
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_API_RESPONSE
    mock_resp.raise_for_status.return_value = None
    return mock_resp

@pytest.fixture
def mock_empty_response():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"results": []}
    mock_resp.raise_for_status.return_value = None
    return mock_resp

@patch("requests.get")
def test_fetch_air_quality_success(mock_get, mock_success_response):
    """Test fetching valid air quality data."""
    mock_get.return_value = mock_success_response
    df = fetch_air_quality("Charlotte", days=1)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "value" in df.columns
    assert "parameter" in df.columns
    assert df["city"].iloc[0] == "Charlotte"

@patch("requests.get")
def test_fetch_air_quality_empty(mock_get, mock_empty_response):
    """Test handling of empty API results."""
    mock_get.return_value = mock_empty_response
    df = fetch_air_quality("NowhereCity", days=1)

    assert isinstance(df, pd.DataFrame)
    assert df.empty

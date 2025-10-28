import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.openaq.org/v2/measurements"

def fetch_air_quality(city: str, days: int = 3, limit: int = 1000) -> pd.DataFrame:
    """Fetch air quality data for a given city from OpenAQ.
    
    Args:
    city (str): City name to fetch data for.
    days (int): Number of days of data to include (default: 3).
    limit (int): Maximum number of records to fetch (default: 1000).
    
    Returns:
        pd.DataFrame: DataFrame containing air quality measurements.
    """
    #Use timezone-aware UTC datatimes (replacement for deprecated utcnow)
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    params = {
        "city": city,
        "date_from": start_date.isoformat(),
        "date_to": end_date.isoformat(),
        "limit": limit,
        "sort": "desc",
        "parameter": ["pm25", "no2", "o3", "co"],
    }

    res = requests.get(BASE_URL, params=params, timeout=15)
    res.raise_for_status()
    data = res.json().get("results", [])

    if not data:
        print(f"No air quality data found for {city}.")
        return pd.DataFrame()

    df = pd.json_normalize(data)
    print(f"Retrieved {len(df)} records for {city}.")
    return df

if __name__ == "__main__":
    # Example test run
    city_name = "Charlotte"
    df = fetch_air_quality(city_name, days=3)

    if not df.empty:
        filename = f"data/raw/{city_name.lower()}_air_quality_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
        df.to_csv(filename, index=False)
        print(f"💾 Data saved to {filename}")
import logging
import pandas as pd
from stravaio import StravaIO


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)


def extract_to_dataframe(raw_data) -> pd.DataFrame:
    processed_data = [entry.to_dict() for entry in raw_data]
    return pd.DataFrame(processed_data)


def extract_strava(
    creds,
    start_date,
    end_date,
    endpoint, # For compatability with other extract steps coordinated by etl
):
    client = StravaIO(creds["access_token"])
    raw_data = client.get_logged_in_athlete_activities(after=str(start_date))
    df = extract_to_dataframe(raw_data)
    df['date'] = df['start_date_local'].dt.date
    return df

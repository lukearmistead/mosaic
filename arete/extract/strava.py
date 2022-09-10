from datetime import datetime
import pprint
import logging
import pandas as pd
from stravaio import StravaIO
from arete.utils import Creds


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)

CREDS_KEY = "strava"
CREDS_PATH = "creds.yml"
SEASON_STARTS_ON = "2021-10-01"
DATA_OUTPUT_PATH = "data/strava/activities.csv"


def extract_to_dataframe(raw_data) -> pd.DataFrame:
    processed_data = [entry.to_dict() for entry in raw_data]
    return pd.DataFrame(processed_data)


def extract_strava(
    extract_data_since=SEASON_STARTS_ON,
    creds_path=CREDS_PATH,
    creds_key=CREDS_KEY,
    data_output_path=DATA_OUTPUT_PATH,
):
    creds = Creds(creds_path, creds_key)
    client = StravaIO(creds.access_token)
    raw_data = client.get_logged_in_athlete_activities(after=extract_data_since)
    df = extract_to_dataframe(raw_data)
    df.to_csv(data_output_path, index=False)


if __name__ == "__main__":
    extract_strava(SEASON_STARTS_ON, CREDS_PATH, CREDS_KEY, DATA_OUTPUT_PATH)
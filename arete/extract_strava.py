from datetime import datetime
import pprint
import logging
import pandas as pd
from stravaio import StravaIO
from utils import Creds


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


def main(extract_data_since, creds_path, creds_key, data_output_path):
    creds = Creds(creds_path, creds_key)
    client = StravaIO(creds.access_token)
    raw_data = client.get_logged_in_athlete_activities(after=extract_data_since)
    df = extract_to_dataframe(raw_data)
    df.to_csv(data_output_path, index=False)


if __name__ == "__main__":
    main(SEASON_STARTS_ON, CREDS_PATH, CREDS_KEY, DATA_OUTPUT_PATH)

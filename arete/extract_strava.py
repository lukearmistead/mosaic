import logging
import pandas as pd
from stravaio import strava_oauth2, StravaIO
from utils import lookup_yaml, update_yaml


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)

CREDS_KEY = "strava"
CREDS_PATH = "creds.yml"
SEASON_STARTS_ON = "2021-10-01"
DATA_OUTPUT_PATH = "data/strava/activities.csv"


class Creds:
    def __init__(self, creds_path, creds_key):
        creds = lookup_yaml(creds_path)[creds_key]
        self.client_id = creds["client_id"]
        self.client_secret = creds["client_secret"]
        self.access_token = creds["access_token"]
        if not self.is_connected():
            refresh_access_token()
            update_yaml(creds_path, {creds_key: {"access_token": self.access_token}})

    def refresh_access_token(self):
        logging.info("Attempting to refresh access token through oauth2")
        output = strava_oauth2(self.client_id, self.client_secret)
        self.access_token = output["access_token"]

    def is_connected(self):
        client = StravaIO(self.access_token)
        if client.get_logged_in_athlete():
            return True
        else:
            logging.info("Access token failed to establish connection to Strava")
            return False


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

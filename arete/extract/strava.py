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


def extract_to_dataframe(raw_data) -> pd.DataFrame:
    processed_data = [entry.to_dict() for entry in raw_data]
    return pd.DataFrame(processed_data)


def extract_strava(
    start_date,
    end_date,
    creds_path,
    creds_key,
    output_path,
):
    creds = Creds(creds_path, creds_key)
    client = StravaIO(creds.access_token)
    raw_data = client.get_logged_in_athlete_activities(after=start_date)
    df = extract_to_dataframe(raw_data)
    df.to_csv(output_path, index=False)

import logging
import pandas as pd
from stravaio import StravaIO
from arete.utils import create_path_to_file_if_not_exists


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
    output_path,
):
    client = StravaIO(creds["access_token"])
    raw_data = client.get_logged_in_athlete_activities(after=str(start_date))
    df = extract_to_dataframe(raw_data)
    create_path_to_file_if_not_exists(output_path)
    df.to_csv(output_path, index=False)

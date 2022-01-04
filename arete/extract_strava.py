from datetime import datetime, timedelta
from io import StringIO
import logging
import pandas as pd
import stravalib
import requests
from stravaio import strava_oauth2, StravaIO
from utils import yaml_lookup


logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

CREDS_KEY = 'strava'
CREDS_PATH = 'creds.yml'
OUTPUT_DIR_PATH='data/strava/'

def main(creds_path, creds_key):

    # Get creds
    creds = yaml_lookup(creds_path, creds_key)
    client_id, client_secret = creds['client_id'], creds['client_secret']
    print('found client')


    output = strava_oauth2(client_id, client_secret)
    print('made connection')
    access_token = output['access_token']
    client = StravaIO(access_token)
    print('made connection 2')

    activities = client.get_logged_in_athlete_activities(after='2021-12-01')
    print(len(activities))
    print(activities[0])
    print(activities[1])
    print(activities[2])
    print(activities)


if __name__ == '__main__':
    print('Works!')
    main(CREDS_PATH, CREDS_KEY)

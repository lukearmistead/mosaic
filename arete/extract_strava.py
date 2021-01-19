""" WORK IN PROGRESS"""
from datetime import datetime, timedelta
from io import StringIO
import logging
import pandas as pd
import stravalib
import requests
from utils import yaml_lookup


logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

CREDS_KEY = 'strava'
CREDS_PATH = '../creds.yml'
OUTPUT_DIR_PATH='../data/strava//'

def main(creds_path, creds_key):

    # Get creds
    creds = yaml_lookup(creds_path, creds_key)
    client_id, client_secret, refresh_token = creds['client_id'], creds['client_secret'], creds['refresh_token']

    # # One time edit access: https://markhneedham.com/blog/2020/12/15/strava-authorization-error-missing-read-permission/
    # # Secondary source: https://medium.com/analytics-vidhya/accessing-user-data-via-the-strava-api-using-stravalib-d5bee7fdde17
    # # `stravalib` documentation: https://github.com/hozn/stravalib
    # client = stravalib.Client()
    # url = client.authorization_url(
    #     client_id=client_id,
    #     redirect_uri='http://127.0.0.1:5000/authorization',
    #     scope=['read_all', 'profile:read_all', 'activity:read_all']
    #     )
    # # http://127.0.0.1:5000/authorization?state=&code=666a95186427374f045484c3f7e7069958155929&scope=read,activity:read_all,profile:read_all,read_all
    # print(url)

    # Primary source: https://stackoverflow.com/questions/37781505/how-do-i-get-an-access-token-using-stravalib

    client_id, client_secret, refresh_token = creds['client_id'], creds['client_secret'], creds['refresh_token']
    auth_url = "https://www.strava.com/oauth/token"
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': "refresh_token",
        'f': 'json'
    }
    print("Requesting the token...\n")
    res = requests.post(auth_url, data=payload, verify=False)
    print(res.json())
    print()

    access_token = res.json()['access_token']
    expiry_ts = res.json()['expires_at']
    print("New token will expire at: ", end='\t')
    print(datetime.utcfromtimestamp(expiry_ts).strftime('%Y-%m-%d %H:%M:%S'))

    client = stravalib.Client(access_token=access_token)
    athlete = client.get_athlete()
    client = stravalib.Client(access_token=access_token)
    activities = client.get_activities(limit=2)
    print(f'Hello {athlete.firstname} {athlete.lastname}')

    header = {'Authorization': 'Bearer ' + access_token}
    params = {'per_page': 200, 'page': 1}
    activities_url = 'https://www.strava.com/api/v3/athlete/activities'
    activities = requests.get(activities_url, headers=header, params=params).json()
    print(activities)

if __name__ == '__main__':
    main(CREDS_PATH, CREDS_KEY)

from datetime import datetime, timedelta
import fitbit
import pandas as pd
import numpy as np
from python_fitbit.gather_keys_oauth2 import OAuth2Server
from utils import yaml_lookup

'''
- Add logging
- Assign ids to data
- Make OAuth2Server connection for Fitbit a touch more elegant
- Config for Fitbit API endpoints
- Place to actually store the data. Start with s3?
- Chron to coordinate 
- Backfilling
'''

CREDS_KEY = 'fitbit'
CREDS_PATH = '../creds.yml'
END_DATE = datetime.now().date()
START_DATE = END_DATE - timedelta(days=100) # Fitbit doesn't like if we request more than 100 days. Meh. Fine.
RESOURCES = [
        'activities/distance',
        'body/weight',
        'body/fat',
        'body/bmi',
        'sleep',
        'activities/heart'
        ]
KEYS = ['activities-distance',
        'body-weight',
        'body-fat',
        'body-bmi',
        'sleep',
        'activities-heart']
OUTPUT_DIR_PATH='../data/fitbit/'


def get_client(creds) -> object:
    """
    Returns a client to access data from API. Assumes that the `creds`
    dictionary keys map to the inputs of their respective client object.
    """
    server = OAuth2Server(**creds)
    server.browser_authorize()
    keys = server.fitbit.client.session.token
    for k in ['access_token', 'refresh_token']:
        v = str(keys[k])
        creds[k] = v
    client = fitbit.Fitbit(**creds)
    return client

def unload_simple_json(data, d=None):
    if not d:
        d = {'dateTime': [], 'value': []}
    for entry in data:
        for k in d.keys():
            d[k].append(entry[k])
    return d

def main(
    creds_path=CREDS_PATH,
    creds_key=CREDS_KEY,
    start_date=START_DATE,
    end_date=END_DATE,
    resources=RESOURCES,
    keys=KEYS,
    output_dir_path=OUTPUT_DIR_PATH
    ):

    creds = yaml_lookup(creds_path, creds_key)
    client = get_client(creds)
    dfs = []
    for resource, key in zip(resources, keys):
        if resource=='sleep':
            d = {'efficiency': [], 'minutesAsleep': [], 'startTime': [], 'endTime': [], 'awakeningsCount': [],
                 'dateOfSleep': []}
        else:
            d=None

        df = unload_simple_json(client.time_series(
            resource, 
            base_date=start_date, 
            end_date=end_date
            )[key], d)
        if resource == 'activities/heart':
            d = {
                'Fat Burn': [],
                'Cardio': [],
                'Peak': [],
                'dateTime': [],
                'restingHeartRate': []
            }

            d['dateTime'] = df['dateTime']
            for value in df['value']:
                try:
                    d['restingHeartRate'].append(value['restingHeartRate'])
                except:
                    d['restingHeartRate'].append(np.nan)

                # Flattening embedded list
                for k in d.keys():
                    for zone in value['heartRateZones']:
                        if zone['name'] == k:
                            d[k].append(zone['minutes'])
            df = d
        df = pd.DataFrame(df)
        if output_dir_path:
            name = resource.split('/')
            name = name[1] if len(name) > 1 else name[0]
            name = '{}_{}_to_{}.csv'.format(name, end_date, start_date)
            df.to_csv(output_dir_path + name, index=False)
    if not output_dir_path:
        return dfs


if __name__ == '__main__':
    main()

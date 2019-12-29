from datetime import datetime, timedelta
import fitbit
import inspect
import pandas as pd
import plaid
import yaml
from utils.python_fitbit.gather_keys_oauth2 import OAuth2Server

'''
FUTURE
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
OUTPUT_DIR_PATH='../data/fitbit/'


def get_creds(path, key) -> dict:
    """"
    Returns a dictionary containing the credentials relevant for a particular
    API, generally including a  client id and secret
    """
    with open(path, 'r') as stream:
        creds = yaml.safe_load(stream)[key]
    return creds


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


def main(
    creds_path=CREDS_PATH,
    creds_key=CREDS_KEY,
    start_date=START_DATE,
    end_date=END_DATE,
    resources=RESOURCES,
    output_dir_path=OUTPUT_DIR_PATH
    ):

    creds = get_creds(creds_path, creds_key)
    client = get_client(creds)
    dfs = []
    for resource in resources:
        df = client.time_series(
            resource, 
            base_date=start_date, 
            end_date=end_date
            )
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

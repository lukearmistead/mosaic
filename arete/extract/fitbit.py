from datetime import datetime, timedelta
from io import StringIO
import logging as log
import fitbit
import pandas as pd
from arete.utils import Creds

"""
- Create directories if they don't exist
- Assign ids to data
- Make OAuth2Server connection for Fitbit a touch more elegant
- Config for Fitbit API endpoints
- Place to actually store the data. Start with s3?
- Chron to coordinate 
- Backfilling
"""

log.getLogger().setLevel(log.DEBUG)

CREDS_KEY = "fitbit"
CREDS_PATH = "creds.yml"
END_DATE = datetime.now().date()
START_DATE = END_DATE - timedelta(days=300)
RESOURCES = [
    "activities/distance",
    "body/weight",
    "body/fat",
    "body/bmi",
    "sleep",
    "activities/heart",
]
OUTPUT_DIR_PATH = "data/fitbit/"


def unload_simple_json(data, d=None):
    if not d:
        d = {"dateTime": [], "value": []}
    for entry in data:
        for k in d.keys():
            d[k].append(entry[k])
    return d


def unload_heart_rate_json(processed_fitbit_payload):
    d = {
        "Fat Burn": [],
        "Cardio": [],
        "Peak": [],
        "dateTime": [],
        "restingHeartRate": [],
    }
    d["dateTime"] = processed_fitbit_payload["dateTime"]
    for all_logs in processed_fitbit_payload["value"]:
        d["restingHeartRate"].append(all_logs.get("restingHeartRate"))
        # Flattening embedded heart rate zone lists
        zone_logs = all_logs["heartRateZones"]
        zone_logs = {zone_log["name"]: zone_log for zone_log in zone_logs}
        for zone_type in ["Fat Burn", "Cardio", "Peak"]:
            d[zone_type].append(zone_logs[zone_type].get("minutes"))
    return d


def unload_fitbit_payload(raw_json_extract, resource, key):
    if resource == "sleep":
        unpack_dict = {
            "efficiency": [],
            "minutesAsleep": [],
            "startTime": [],
            "endTime": [],
            "awakeningsCount": [],
            "dateOfSleep": [],
        }
    else:
        unpack_dict = None
    processed_json = unload_simple_json(raw_json_extract[key], unpack_dict)
    if resource == "activities/heart":
        processed_json = unload_heart_rate_json(processed_json)
    df = pd.DataFrame(processed_json)
    return df


def extract_file_name_from_resource(resource):
    # Create directory if doesn't exist
    file_name = resource.split("/")
    file_name = file_name[1] if len(file_name) > 1 else file_name[0]
    return file_name


def export_to_csv(df, resource, output_dir_path=OUTPUT_DIR_PATH):
    file_name = extract_file_name_from_resource(resource)
    full_path = f"{output_dir_path}/{file_name}.csv"
    buf = StringIO()
    df.info(buf=buf)
    log.info(
        "Exporting the following DataFrame of {} to CSV\n{}".format(
            resource.upper(), buf.getvalue()
        )
    )
    df.to_csv(full_path, index=False)


def hit_api(client, resource, start_date, end_date):
    days_requested = (end_date - start_date).days
    if days_requested > 100:

        working_end_date = start_date + timedelta(days=100)
    raw_json_extract = client.time_series(
        resource, base_date=start_date, end_date=end_date
    )


def extract_fitbit(
    creds_path=CREDS_PATH,
    creds_key=CREDS_KEY,
    start_date=START_DATE,
    end_date=END_DATE,
    resources=RESOURCES,
    output_dir_path=OUTPUT_DIR_PATH,
):
    creds = Creds(creds_path, creds_key)
    client = fitbit.Fitbit(
        client_id=creds.client_id,
        client_secret=creds.client_secret,
        access_token=creds.access_token,
        refresh_token=creds.refresh_token,
        expires_at=creds.expires_at,
    )
    for resource in resources:
        key = resource.replace('/', '-')
        log.debug(resource)
        working_start_date = start_date
        working_end_date = min(working_start_date + timedelta(days=100), end_date)
        dfs = []
        more_data_to_extract = True
        while more_data_to_extract:
            log.debug(working_start_date)
            log.debug(working_end_date)
            raw_json_extract = client.time_series(
                resource, base_date=working_start_date, end_date=working_end_date
            )
            df = unload_fitbit_payload(raw_json_extract, resource, key)
            dfs.append(df)
            more_data_to_extract = working_end_date < end_date
            working_start_date = working_end_date + timedelta(days=1)
            working_end_date = min((working_start_date + timedelta(days=100)), end_date)
        export_to_csv(pd.concat(dfs, axis=0), resource, output_dir_path)


if __name__ == "__main__":
    extract_fitbit()
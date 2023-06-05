import logging as getLogger
import fitbit
import numpy as np
import pandas as pd


getLogger.getLogger().setLevel(getLogger.DEBUG)


def generate_deterministic_id_vector(df):
    return df.astype(str).agg('_'.join, axis=1).map(hash)


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


def hyphenate(endpoint):
    return endpoint.replace("/", "-")


def unload_fitbit_payload(raw_json_extract, resource):
    key = hyphenate(resource)
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


def timedelta(days):
    return np.timedelta64(days,'D')

def hit_api(client, resource, start_date, end_date):
    days_requested = (end_date - start_date).days
    if days_requested > 100:
        start_date + timedelta(days=100)
    client.time_series(resource, base_date=start_date, end_date=end_date)


def extract_fitbit(
    creds,
    start_date,
    end_date,
    endpoint,
):
    client = fitbit.Fitbit(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        access_token=creds["access_token"],
        refresh_token=creds["refresh_token"],
        expires_at=creds["expires_at"],
    )
    getLogger.debug(endpoint)
    working_start_date = start_date
    working_end_date = min(working_start_date + timedelta(days=100), end_date)
    dfs = []
    more_data_to_extract = True
    while more_data_to_extract:
        getLogger.debug(working_start_date)
        getLogger.debug(working_end_date)
        raw_json_extract = client.time_series(
            endpoint, base_date=working_start_date.item(), end_date=working_end_date.item()
        )
        df = unload_fitbit_payload(raw_json_extract, endpoint)
        df["id"] = generate_deterministic_id_vector(df)
        dfs.append(df)
        more_data_to_extract = working_end_date < end_date
        working_start_date = working_end_date + timedelta(days=1)
        working_end_date = min((working_start_date + timedelta(days=100)), end_date)
    df = pd.concat(dfs, axis=0)
    df = df.rename(columns={"dateTime": "date", "dateOfSleep": "date"})
    return df

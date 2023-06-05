import os, datetime, json
import numpy as np
import pandas as pd
from arete.extract.plaid import extract_plaid
from arete.extract.fitbit import extract_fitbit
from arete.extract.splitwise import extract_splitwise
from arete.extract.strava import extract_strava
from arete.transform.skis import transform_skis
from arete.transform.vitals import transform_vitals
from arete.transform.transactions import transform_transactions
from arete.utils import lookup_yaml, convert_string_to_date
from arete.credentials import AccessTokenManager, CredsFile
import logging


# TODO
# - Migrate all dates to numpy datetime64
# - Set up the start and end for transform steps
# - Set up last run to enable ETL running every hour


CONFIG_PATH, CREDS_PATH = "etl_config.yml", "creds.yml"


class OutputFile:
    def __init__(self, path, schema):
        self.path = path
        self._directory_path = self._get_directory_path()
        self.schema = schema

    def _get_directory_path(self):
        return os.path.dirname(self.path)

    def _directory_exists(self):
        return os.path.exists(self._directory_path)

    def _create_directory(self):
        os.makedirs(self._directory_path)

    def create_path(self):
        if not self._directory_exists():
            self._create_directory()

    def exists(self):
        return os.path.exists(self.path)

    def get_df(self):
        return self.schema.conform(pd.read_csv(self.path))

    def get_latest_row_date(self, date_col="date"):
        df = self.get_df()
        latest_row_date = np.datetime64(df[date_col].max().date())
        return latest_row_date


class Schema:
    def __init__(self, schema):
        self.schema = schema

    def conform(self, df):
        return df.astype(dtype=self.schema)[self.schema.keys()]


class LastRun:
    def __init__(self):
        self._file_path = "/tmp/last_etl_run.json"
        self._today = self._today()

    def _today(self):
        return str(datetime.date.today())

    def read(self):
        if os.path.exists(self._file_path):
            with open(self._file_path, "r") as f:
                return json.load(f).get("last_run_date")
        else:
            return None

    def update(self):
        with open(self._file_path, "w") as f:
            json.dump({"last_run_date": self._today}, f)

    def was_today(self):
        return self.read() == self._today

    def was_this_hour(self):
        "Checks if the last read of the ETL was within the present hour"
        return  # boolean value


def convert_all_dates(
    config, date_keys=["start_date", "end_date"], steps=["extract", "transform"]
):
    for step in steps:
        for step_config in config[step].values():
            for k in date_keys:
                step_config[k] = convert_string_to_date(step_config[k])
    return config


def get_refreshed_creds(key, creds_file):
    if key == "plaid":
        return creds_file.get(key)
    else:
        access_token_manager = AccessTokenManager(creds_file)
        access_token_manager.refresh(key)
        return creds_file.get(key)


# TODO - Fully migrate this into OutputFile class
def create_path_to_file_if_not_exists(full_path):
    directory_path = os.path.dirname(full_path)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def dedup_combined_data(existing_df, incremental_df):
    df = pd.concat((existing_df, incremental_df), axis=0)
    total_rows = len(df)
    df = df.drop_duplicates(subset="id")
    logging.info(
        f"Incremental extract fetched {len(incremental_df)} rows for a total of {len(df)} after dropping {total_rows - len(df)} duplicates"
    )
    return df


def run_etl(config_path=CONFIG_PATH, creds_path=CREDS_PATH):
    last_run = LastRun()
    if last_run.was_today():
        return
    config = lookup_yaml(config_path)
    creds_file = CredsFile(CREDS_PATH)

    # Extracts
    convert_all_dates(config)
    sources = [
        "strava",
        "fitbit",
        "splitwise",
        "plaid",
    ]
    extract_steps = [extract_strava, extract_fitbit, extract_splitwise, extract_plaid]
    for source, extract in zip(sources, extract_steps):
        creds = get_refreshed_creds(source, creds_file)
        source_config = config["extract"][source]
        start_date, end_date = source_config["start_date"], source_config["end_date"]
        for endpoint, endpoint_config in source_config["endpoints"].items():
            logging.info(f"Extracting {source} data from {endpoint} endpoint.")
            schema = Schema(endpoint_config["output_schema"])
            output_file = OutputFile(endpoint_config["output_path"], schema)
            output_file.create_path()
            if output_file.exists():
                start_date = max(output_file.get_latest_row_date(), start_date)
                incremental_df = extract(creds, start_date, end_date, endpoint)
                df = dedup_combined_data(
                    schema.conform(output_file.get_df()), schema.conform(incremental_df)
                )
            elif not output_file.exists():
                df = schema.conform(extract(creds, start_date, end_date, endpoint))
                logging.info(f"Full extract fetched {len(df)} rows since {start_date}")
            df.to_csv(output_file.path, index=False)

    # Transforms
    create_path_to_file_if_not_exists(config["transform"]["vitals"]["output_path"])
    endpoints = config["extract"]["fitbit"]["endpoints"]
    transform_vitals(
        extract_fitbit_hearts_path=endpoints["activities/heart"]["output_path"],
        extract_fitbit_sleeps_path=endpoints["sleep"]["output_path"],
        extract_fitbit_weights_path=endpoints["body/weight"]["output_path"],
        extract_fitbit_bmis_path=endpoints["body/bmi"]["output_path"],
        **config["transform"]["vitals"],
    )
    create_path_to_file_if_not_exists(config["transform"]["skis"]["output_path"])
    transform_skis(
        extract_strava_path=config["extract"]["strava"]["endpoints"]["activities"][
            "output_path"
        ],
        **config["transform"]["skis"],
    )
    create_path_to_file_if_not_exists(
        config["transform"]["transactions"]["output_path"]
    )
    transform_transactions(
        extract_plaid_endpoints=config["extract"]["plaid"]["endpoints"],
        extract_splitwise_path=config["extract"]["splitwise"]["endpoints"]["expenses"][
            "output_path"
        ],
        **config["transform"]["transactions"],
    )

    last_run.update()

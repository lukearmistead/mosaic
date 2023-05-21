import os, datetime
from arete.extract.plaid import extract_plaid
from arete.extract.fitbit import extract_fitbit
from arete.extract.splitwise import extract_splitwise
from arete.extract.strava import extract_strava
from arete.transform.skis import transform_skis
from arete.transform.vitals import transform_vitals
from arete.transform.transactions import transform_transactions
from arete.utils import lookup_yaml, convert_string_to_date
from arete.credentials import AccessTokenManager, CredsFile


CONFIG_PATH, CREDS_PATH = "etl_config.yml", "creds.yml"


class LastRun:
    def __init__(self):
        self._env_var = "ARETE_LAST_ETL_RUN_DATE"
        self._today = self._today()

    def _today(self):
        return str(datetime.date.today())

    def read(self):
        return os.environ.get(self._env_var)

    def update(self):
        os.environ[self._env_var] = self._today

    def was_today(self):
        return self.read() == self._today


def convert_all_dates(
    config, date_keys=["start_date", "end_date"], steps=["extract", "transform"]
):
    for step in steps:
        for step_config in config[step].values():
            for k in date_keys:
                step_config[k] = convert_string_to_date(step_config[k])
    return config


def get_refreshed_creds(key, creds_file):
    access_token_manager = AccessTokenManager(creds_file)
    access_token_manager.refresh(key)
    return creds_file.get(key)


def run_etl(config_path=CONFIG_PATH, creds_path=CREDS_PATH):
    last_run = LastRun()
    if last_run.was_today():
        return
    config = lookup_yaml(config_path)
    creds_file = CredsFile(CREDS_PATH)

    # Extracts
    convert_all_dates(config)
    sources = ["strava", "fitbit", "splitwise"]
    steps = [extract_strava, extract_fitbit, extract_splitwise]
    for source, step in zip(sources, steps):
        creds = get_refreshed_creds(source, creds_file)
        step(creds, **config["extract"][source])
    extract_plaid(creds_file.get("plaid"), **config["extract"]["plaid"])

    # Transforms
    # TODO - Set up the start and end
    endpoints = config["extract"]["fitbit"]["endpoints"]
    transform_vitals(
        extract_fitbit_hearts_path=endpoints["activities/heart"]["output_path"],
        extract_fitbit_sleeps_path=endpoints["sleep"]["output_path"],
        extract_fitbit_weights_path=endpoints["body/weight"]["output_path"],
        extract_fitbit_bmis_path=endpoints["body/bmi"]["output_path"],
        **config["transform"]["vitals"],
    )
    transform_skis(
        extract_strava_path=config["extract"]["strava"]["output_path"],
        **config["transform"]["skis"],
    )
    transform_transactions(
        extract_plaid_endpoints=config["extract"]["plaid"]["endpoints"],
        extract_splitwise_path=config["extract"]["splitwise"]["output_path"],
        **config["transform"]["transactions"],
    )

    last_run.update()

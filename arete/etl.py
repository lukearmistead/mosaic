from arete.extract.fitbit import extract_fitbit
from arete.extract.plaid import extract_plaid
from arete.extract.splitwise import extract_splitwise
from arete.extract.strava import extract_strava
from arete.transform.skis import transform_skis
from arete.transform.resting_heart_rates import transform_resting_heart_rates
from arete.transform.transactions import transform_transactions
from arete.transform.aggregate import transform_aggregate
from arete.utils import lookup_yaml, convert_string_to_date


# TODO - Should the ETL step handle the creds instead of the individual extracts?
CONFIG_PATH, CREDS_PATH = "etl_config.yml", "creds.yml"


def convert_all_dates(
    config, date_keys=["start_date", "end_date"], steps=["extract", "transform"]
):
    for step in steps:
        for step_config in config[step].values():
            for k in date_keys:
                step_config[k] = convert_string_to_date(step_config[k])
    return config


def run_etl(config_path=CONFIG_PATH, creds_path=CREDS_PATH):
    config = lookup_yaml(config_path)
    # Extract
    # Strava expects dates as strings, so we run this before the conversion step
    extract_strava(**config["extract"]["strava"])
    convert_all_dates(config)
    # TODO - Figure out a way to remove the clunky python-fitbit part of the module
    extract_fitbit(**config["extract"]["fitbit"])
    extract_plaid(**config["extract"]["plaid"])
    # TODO - Set up the start and end
    extract_splitwise(**config["extract"]["splitwise"])
    # Transform
    transform_resting_heart_rates(
        extract_fitbit_path=config["extract"]["fitbit"]["endpoints"][
            "activities/heart"
        ]["output_path"],
        **config["transform"]["resting_heart_rates"],
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
    # TODO - Rework this step to have it source from transforms instead of extracts
    transform_aggregate(
        transform_transactions_path=config["transform"]["transactions"]["output_path"],
        extract_strava_path=config["extract"]["strava"]["output_path"],
        transform_resting_heart_rates_path=config["transform"]["resting_heart_rates"][
            "output_path"
        ],
        **config["transform"]["aggregate"],
    )

import inflection
import logging as getLogger
import pandas as pd
from datetime import datetime
from arete.utils import convert_vector_to_date


def transform_resting_heart_rates(
    start_date, end_date, extract_fitbit_path, output_path
):
    # TODO - Implement relative dates then remove this placeholder
    df = pd.read_csv(extract_fitbit_path)
    df["dateTime"] = convert_vector_to_date(df["dateTime"])
    df = (
        df.loc[:, ["dateTime", "restingHeartRate"]]
        .loc[df["dateTime"].between(start_date, end_date),]
        .rename(columns={"dateTime": "date", "restingHeartRate": "resting_heart_rate"})
    )
    df.to_csv(output_path)

import logging as getLogger
import pandas as pd
from arete.utils import convert_vector_to_date


def transform_fitbit_extract(input_path, date_col, value_col, value_name):
    df = pd.read_csv(input_path)
    df[date_col] = convert_vector_to_date(df[date_col])
    df = df.loc[:, [date_col, value_col]].rename(
        columns={date_col: "date", value_col: "value"}
    )
    df["type"] = value_name
    return df


def transform_vitals(
    extract_fitbit_hearts_path,
    extract_fitbit_sleeps_path,
    extract_fitbit_weights_path,
    extract_fitbit_bmis_path,
    start_date,
    end_date,
    output_path,
):
    transform_steps = [
        {
            "input_path": extract_fitbit_hearts_path,
            "date_col": "dateTime",
            "value_col": "restingHeartRate",
            "value_name": "resting_heart_rate",
        },
        {
            "input_path": extract_fitbit_sleeps_path,
            "date_col": "dateOfSleep",
            "value_col": "minutesAsleep",
            "value_name": "sleep_hours",
        },
        {
            "input_path": extract_fitbit_weights_path,
            "date_col": "dateTime",
            "value_col": "value",
            "value_name": "weight",
        },
        {
            "input_path": extract_fitbit_bmis_path,
            "date_col": "dateTime",
            "value_col": "value",
            "value_name": "bmi",
        },
    ]
    transformed_fitbit_extracts = []
    for step in transform_steps:
        transformed_extract = transform_fitbit_extract(**step)
        transformed_fitbit_extracts.append(transformed_extract)
    df = pd.concat(transformed_fitbit_extracts)
    df.loc[df["type"] == "sleep_hours", "value"] /= 60
    (
        df.loc[df["date"].between(start_date, end_date),]
        .loc[:, ["date", "type", "value"]]
        .sort_values(["date", "type"])
        .to_csv(output_path, index=False)
    )

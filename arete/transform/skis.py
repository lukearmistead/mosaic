import inflection
import logging as getLogger
import pandas as pd
from datetime import datetime
from arete.utils import (
    convert_vector_to_date,
    snakecase_format,
    create_path_to_file_if_not_exists,
)


getLogger.getLogger().setLevel(getLogger.INFO)


def meters_to_feet(meters):
    return meters * 3.280839895


def mps_to_mph(meters_per_second):
    return meters_per_second * 2.2369362921


def transform_skis(start_date, end_date, extract_strava_path, output_path):
    df = pd.read_csv(extract_strava_path)
    getLogger.info(f"Read strava data from {extract_strava_path}:\n{df.head()}")
    df["date"] = convert_vector_to_date(df["start_date_local"])
    df["total_elevation_gain"] = meters_to_feet(df["total_elevation_gain"])
    df["type"] = df["type"].apply(lambda x: snakecase_format(x))
    df["max_speed"] = mps_to_mph(df["max_speed"])
    df = (
        df.loc[df["date"].between(start_date, end_date),]
        .loc[df["type"].isin(["backcountry_ski", "alpine_ski", "snowboard"]),]
        .loc[
            :,
            [
                "id",
                "date",
                "type",
                "total_elevation_gain",
                "max_speed",
            ],
        ]
        .sort_values("date")
    )
    getLogger.info(
        f"Outputting ski data between {start_date} and {end_date} to {output_path}:\n{df.head()}"
    )
    create_path_to_file_if_not_exists(output_path)
    df.to_csv(output_path)


if __name__ == "__main__":
    main()

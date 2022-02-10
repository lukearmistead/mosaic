import pandas as pd
from prettytable import PrettyTable
from utils import RelativeDate


strava_data_path = "data/strava/activities.csv"
SEASON_STARTS_ON = "2021-10-01"


class Ski:
    def __init__(self, season_start: str, df: pd.DataFrame):
        df = self.replace_nulls_with_activity_mean(df, "total_elevation_gain")
        ski_dates = self.date_vector(df["start_date_local"])
        df = df.loc[
            df["type"].isin(["AlpineSki", "BackcountrySki"]),
        ]
        date = RelativeDate()

        # Season metrics
        # TODO - fix date processing for ski_dates creation
        season_start = date.date_from_string(season_start)
        season_filter = ski_dates.between(season_start, date.last_sunday)
        self.date_count = self.count_ski_dates(
            df.loc[season_filter, "start_date_local"]
        )
        self.vertical_feet = self.vertical_feet_sum(
            ski_meters=df.loc[season_filter, "total_elevation_gain"]
        )
        self.max_speed = self.max_miles_per_hour(
            meters_per_second=df.loc[season_filter, "max_speed"]
        )

        # Last week metrics
        last_week_filter = ski_dates.between(date.last_monday, date.last_sunday)
        self.last_week_date_count = self.count_ski_dates(
            df.loc[last_week_filter, "start_date_local"]
        )
        self.last_week_vertical_feet = self.vertical_feet_sum(
            ski_meters=df.loc[last_week_filter, "total_elevation_gain"]
        )
        self.last_week_max_speed = self.max_miles_per_hour(
            meters_per_second=df.loc[last_week_filter, "max_speed"]
        )

    def replace_nulls_with_activity_mean(self, df, measure):
        for activity_type in df["type"].unique():
            m = df["type"] == activity_type
            avg = df.loc[m, measure].mean()
            df.loc[m, measure] = df.loc[m, measure].replace({0.0: avg})
        return df

    def count_ski_dates(self, ski_dates):
        ski_dates = pd.to_datetime(ski_dates).dt.date
        ski_date_count = len(ski_dates.unique())
        return ski_date_count

    @staticmethod
    def date_vector(mistyped_timestamp_vector):
        return pd.to_datetime(mistyped_timestamp_vector).dt.date

    @staticmethod
    def vertical_feet_sum(ski_meters):
        return ski_meters.sum() * 3.280839895

    @staticmethod
    def max_miles_per_hour(meters_per_second):
        return meters_per_second.max() * 2.2369362921


if __name__ == "__main__":
    # Tests
    df = pd.read_csv(strava_data_path)
    ski = Ski(SEASON_STARTS_ON, df)
    output = PrettyTable()
    output.field_names = ["Field", "Week", "Goal", "Agg"]
    output.align["Field"] = "l"
    for field in ["Week", "Goal", "Agg"]:
        output.align[field] = "r"
    output.float_format = ".0"
    rows = [
        ["Ski Day Count", ski.last_week_date_count, "3", ski.date_count],
        [
            "Ski Feet Sum (000s)",
            ski.last_week_vertical_feet / 1000,
            "30",
            ski.vertical_feet / 1000,
        ],
        ["Ski Mph Max", ski.last_week_max_speed, "60", ski.max_speed],
    ]
    for row in rows:
        output.add_row(row)
    print(output)

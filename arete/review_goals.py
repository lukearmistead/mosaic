import datetime
import pandas as pd
import prettytable
import time


strava_data_path = "data/strava/activities.csv"
SEASON_STARTS_ON = "2021-10-01"


def weekday_int_from_string(day_of_week):
    return time.strptime(day_of_week, "%A").tm_wday


def date_from_string(date_as_string):
    return datetime.datetime.strptime(date_as_string, "%Y-%m-%d").date()


def last_week_date(day_of_week):
    today = datetime.date.today()
    days_since_this_monday = today.weekday()  # Monday returns 0, Tuesday returns 1
    days_since_last_day_of_week = (
        days_since_this_monday + 7 - weekday_int_from_string(day_of_week)
    )
    date = today - datetime.timedelta(days=days_since_last_day_of_week)
    return date


def meters_to_feet(meter):
    return meter * 3.280839895


class Date:
    # TODO
    # Container for managing dates, including last Monday, Sunday, and season start
    pass


class Ski:
    def __init__(self, season_start_date, strava_data_path):
        df = pd.read_csv(strava_data_path)
        df = self.replace_nulls(df, "total_elevation_gain")
        df = df.loc[
            df["type"].isin(["AlpineSki", "BackcountrySki"]),
        ]
        last_monday = last_week_date("Monday")
        last_sunday = last_week_date("Sunday")
        season_start_date = date_from_string(season_start_date)
        ski_dates = pd.to_datetime(df["start_date_local"]).dt.date

        # This week metrics
        ski_season_filter = ski_dates.between(season_start_date, last_sunday)
        self.date_count = self.count_ski_dates(df["start_date_local"])
        self.vertical_feet = self.vertical_feet_sum(
            ski_meters=df["total_elevation_gain"]
        )

        # Last week metrics
        last_week_filter = ski_dates.between(last_monday, last_sunday)
        self.last_week_date_count = self.count_ski_dates(
            df.loc[last_week_filter, "start_date_local"]
        )
        self.last_week_vertical_feet = self.vertical_feet_sum(
            ski_meters=df.loc[last_week_filter, "total_elevation_gain"]
        )

    def replace_nulls(self, df, measure):
        for activity_type in df["type"].unique():
            m = df["type"] == activity_type
            avg = df.loc[m, measure].mean()
            df.loc[m, measure] = df.loc[m, measure].replace({0.0: avg})
        return df

    def count_ski_dates(self, ski_dates):
        ski_dates = pd.to_datetime(ski_dates).dt.date
        ski_date_count = len(ski_dates.unique())
        return ski_date_count

    def vertical_feet_sum(self, ski_meters):
        return meters_to_feet(ski_meters.sum())


if __name__ == "__main__":
    # Tests
    ski = Ski(SEASON_STARTS_ON, strava_data_path)
    print(
        """GOALS
               Week  Goal   Agg  Notes
Ski Days:        {}    {}   {}  sum dates
Ski Vert:        {}    {}   {}  season sum, 000s of feet
""".format(
            " " + str(ski.last_week_date_count),
            " 3",
            " " + str(ski.date_count),
            round(ski.last_week_vertical_feet / 1000),
            "30",
            round(ski.vertical_feet / 1000),
        )
    )

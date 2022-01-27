import datetime
import pandas as pd
import prettytable
import time


strava_data_path = "data/strava/activities.csv"
SEASON_STARTS_ON = "2021-10-01"


class RelativeDate:
    # TODO
    # Container for managing dates, including last Monday, Sunday, and season start
    def __init__(self):
        self.today = datetime.date.today()
        self.last_sunday = self.last_weekday_date("Sunday")
        self.last_monday = self.last_weekday_date("Monday")

    def last_weekday_date(self, day_of_week):
        days_since_last_day_of_week = (self.days_since_last_monday() - self.weekday_number(day_of_week))
        return self.today - datetime.timedelta(days=days_since_last_day_of_week)

    def days_since_last_monday(self):
        # `weekday` method returns 0 for mon, 1 for tue, etc.
        return self.today.weekday() + 7

    def weekday_number(self, day_of_week):
        return time.strptime(day_of_week, "%A").tm_wday

    def date_from_string(self, date_as_string):
        return datetime.datetime.strptime(date_as_string, "%Y-%m-%d").date()


class Ski:
    def __init__(self, season_start: str, strava_data_path: str):
        df = pd.read_csv(strava_data_path)
        df = self.replace_nulls(df, "total_elevation_gain")
        ski_filter = df["type"].isin(["AlpineSki", "BackcountrySki"])
        df = df.loc[ski_filter,]
        ski_dates = pd.to_datetime(df["start_date_local"]).dt.date
        date = RelativeDate()
        season_start = date.date_from_string(season_start)

        # Season metrics
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
        return ski_meters.sum() * 3.280839895

    def max_miles_per_hour(self, meters_per_second):
        return meters_per_second.max() * 2.2369362921


if __name__ == "__main__":
    # Tests
    ski = Ski(SEASON_STARTS_ON, strava_data_path)
    print(
        """GOALS
               Week  Goal   Agg  Notes
Ski Days:        {}    {}   {}  sum dates
Ski Vert:        {}    {}   {}  season sum, 000s of feet
Ski Mph:         {}    {}    {}  season max
""".format(
            " " + str(ski.last_week_date_count),
            " 3",
            " " + str(ski.date_count),

            round(ski.last_week_vertical_feet / 1000),
            "30",
            round(ski.vertical_feet / 1000),

            round(ski.last_week_max_speed),
            "50",
            round(ski.max_speed)
        )
    )

import datetime
import pandas as pd
from prettytable import PrettyTable


strava_data_path = "data/strava/activities.csv"
SEASON_STARTS_ON = datetime.date(2021, 10, 1)


# TODO - OOP appears to be a bad fit for data manipulation
#        Refactor to be functional


class SkiDates:
    def __init__(self, ski_dates: list):
        ski_dates = self.date_vector(ski_dates)
        self.count = self.count_unique_ski_dates(ski_dates)

    @staticmethod
    def count_unique_ski_dates(ski_dates):
        return len(set(ski_dates))

    @staticmethod
    def date_vector(mistyped_timestamp_vector):
        return list(pd.to_datetime(mistyped_timestamp_vector).date)


class SkiElevations:
    def __init__(self, vertical_meters: list):
        vertical_meters = replace_nulls_with_mean(vertical_meters)
        self.meters_sum = sum(vertical_meters)
        self.feet_sum = meters_to_feet(self.vertical_meters_sum)

    @staticmethod
    def meters_to_feet(meters):
        return meters * 3.280839895

    @staticmethod
    def replace_nulls_with_mean(vector):
        clean_vector = []
        replacement = mean(vector)
        for value in vector:
            if not value:
                clean_vector.append(replacement)
            else:
                clean_vector.append(value)
        return clean_vector

    @staticmethod
    def mean(vector):
        return sum(vector) / len(vector)


class SkiSpeeds:
    def __init__(self, meters_per_second: list):
        self.max_meters_per_second = max(vertical_meters)
        self.max_miles_per_hour = meters_to_feet(self.max_meters_per_second)

    @staticmethod
    def max_miles_per_hour(meters_per_second):
        return meters_per_second * 2.2369362921

def process_data(df):
        df = df.loc[
            df["type"].isin(["AlpineSki", "BackcountrySki"]) &
            df.between(season_start, date.last_sunday)
            ,
        ]


if __name__ == "__main__":
    # Tests
    df = pd.read_csv(strava_data_path)
    ski = Ski(SEASON_STARTS_ON, df)

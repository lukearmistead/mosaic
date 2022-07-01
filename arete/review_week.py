import datetime
from extract_strava import extract_strava
import os
import pandas as pd
from prettytable import PrettyTable, from_csv
from process_todos import ToDos, migrate_todos
from pyfiglet import Figlet
from utils import RelativeDate


STRAVA_DATA_PATH = "data/strava/activities.csv"
# TODO - Make handling of constants like these less sloppy
# Season could be a good object
# It's used in multiple places
# It's state changes
SEASON_STARTS_ON = "2021-10-01"
SEASON_STARTS_ON_DATETIME = datetime.date(2021, 10, 1)
TODO_PATH = "/Users/luke.armistead/workspace/log/todo.txt"


class ReviewFile:
    def __init__(self, review):
        self.review = review
        self.dir = "/Users/luke.armistead/workspace/log/review/"
        last_monday = RelativeDate("Monday", weeks_ago=1)
        self.file = last_monday.short_format() + "-review.txt"
        self.path = self.dir + self.file

    def write(self):
        with open(self.path, "w+") as file:
            file.write(self.review.text)
        file.close()


def fill_nulls_with_category_mean(df, category_col, target_col):
    # https://stackoverflow.com/a/46391144/4447670
    df[target_col] = df.groupby(category_col, sort=False) \
        [target_col] \
        .apply(lambda col: col.fillna(col.mean()))
    return df


def meters_to_feet(meters):
    return meters * 3.280839895


def mps_to_mph(meters_per_second):
    return meters_per_second * 2.2369362921


if __name__ == "__main__":
    # EXTRACT
    extract_strava(
        extract_data_since=SEASON_STARTS_ON,
        creds_path="creds.yml",
        creds_key="strava",
        data_output_path=STRAVA_DATA_PATH,
    )

    # PROCESS
    strava = pd.read_csv(STRAVA_DATA_PATH)

    # Nulls are encoded as zeroes for elevation gain
    strava['total_elevation_gain'] = strava['total_elevation_gain'].replace({0.0: None})
    strava = fill_nulls_with_category_mean(strava, "type", "total_elevation_gain")
    strava['start_on'] = pd.to_datetime(strava['start_date_local']).dt.date

    # Create time and activity filters
    last_monday = RelativeDate("Monday", weeks_ago=1)
    last_sunday = RelativeDate("Sunday", weeks_ago=1)
    last_week_filter = strava["start_on"].between(last_monday.date, last_sunday.date)
    season_filter = strava["start_on"].between(SEASON_STARTS_ON_DATETIME, last_sunday.date)
    season_filter = strava["start_on"].between(SEASON_STARTS_ON_DATETIME, datetime.date.today())
    is_ski = strava["type"].isin(["AlpineSki", "BackcountrySki", "Snowboard"])

    # CREATE DISPLAY DATA
    display_data = {}
    # Column 0
    ['Days of Skiing', 'Vertical Feet (Thousands)', 'Max Speed']

    # Column 1
    last_week_ski_logs = strava.loc[last_week_filter & is_ski,]
    display_data['Week'] = {
        'Ski Date Count': last_week_ski_logs['start_on'].nunique(),
        'Ski Vertical Feet': meters_to_feet(last_week_ski_logs['total_elevation_gain'].sum()),
        'Ski Top Speed': mps_to_mph(last_week_ski_logs['max_speed'].max()),
        }

    # Column 2
    this_season_ski_logs = strava.loc[season_filter & is_ski,]
    display_data['Season'] = {
        'Ski Date Count': this_season_ski_logs['start_on'].nunique(),
        'Ski Vertical Feet': meters_to_feet(this_season_ski_logs['total_elevation_gain'].sum()),
        'Ski Top Speed': mps_to_mph(this_season_ski_logs['max_speed'].max())
        }

    # INPUT INTO PRETTY TABLE
    table = PrettyTable()

    # Add row names
    table.add_column('Field', list(display_data['Week'].keys()))
    for col, row in display_data.items():
        table.add_column(col, list(row.values()))

    # Format
    table.align["Field"] = "l"
    for field in ["Week", "Season"]:
        table.align[field] = "r"
    table.float_format = ".0"
    todos = ToDos(TODO_PATH)

    f = Figlet(font="small")
    title = f.renderText("Review")
    print(title)
    print(last_monday.long_format())
    print(table)

    # Set up new task list for the week 
    # header = Figlet(font="small")
    # migrate_todos(TODO_PATH, todos, header.renderText("ToDo"))

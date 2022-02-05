from pyfiglet import Figlet
import os
import pandas as pd
from prettytable import PrettyTable
from process_ski_data import Ski
from process_todos import ToDos
from utils import RelativeDate

strava_data_path = "data/strava/activities.csv"
SEASON_STARTS_ON = "2021-10-01"
LOG_DIR = "/Users/luke.armistead/workspace/log/"
TODO_FILE = "todo.txt"
ARCHIVE_DIR = "review/"
DATE = RelativeDate()
ARCHIVE_TODO_PATH = LOG_DIR + ARCHIVE_DIR + DATE.last_monday_short + "-" + TODO_FILE
TODO_PATH = LOG_DIR + TODO_FILE


def archive_todos(path, archive_path):
    assert not os.path.exists(
        archive_path
    ), f"Discontinuing weekly refresh because file already exists at target path {archive_path}"
    print(f"Archiving weekly todo list from {path} to {archive_path}")
    os.rename(path, archive_path)


def migrate_todos(path, migrated_todos, header):
    with open(path, "w+") as todos:
        todos.write(header + "\n\n")
        for todo in migrated_todos:
            todo = todo.raw.replace(Symbol.MIGRATE, Symbol.TASK, 1)
            todos.write(todo)
    todos.close()
    print(f"Created new todo file with migrated todos at {path}")


class Review:
    def __init__(self, strava_data_path, todo_path, season_starts_on):
        self.head = self.header(title="Review")
        self.table = self.build_table()
        df = pd.read_csv(strava_data_path)
        self.skis = Ski(season_starts_on, df)
        self.todos = ToDos(todo_path)
        self.populate_table()
        print(self.table)

    def header(self, title, font="small"):
        f = Figlet(font=font)
        title = f.renderText(title)
        date = RelativeDate()
        return title + date.last_monday_long

    def build_table(self):
        table = PrettyTable()
        table.field_names = ["Field", "Week", "Goal", "Agg"]
        table.align["Field"] = "l"
        for field in ["Week", "Goal", "Agg"]:
            table.align[field] = "r"
        table.float_format = ".0"
        return table
   
    def populate_table(self):
        rows = [
            ["Ski Day Count", self.skis.last_week_date_count, "3", self.skis.date_count],
            ["Ski Feet Sum (000s)", self.skis.last_week_vertical_feet / 1000, "30", self.skis.vertical_feet / 1000],
            ["Ski Mph Max", self.skis.last_week_max_speed, "60", self.skis.max_speed],
            ["Tasks Done", self.todos.done_count, "", ""],
            ]
        for row in rows:
            self.table.add_row(row)


if __name__ == "__main__":
    review = Review(strava_data_path, TODO_PATH, SEASON_STARTS_ON)
    print(review.head)
    print(review.table)


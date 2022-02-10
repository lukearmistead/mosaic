from extract_strava import extract_strava
import os
import pandas as pd
from prettytable import PrettyTable
from process_ski_data import Ski
from process_todos import ToDos, migrate_todos
from pyfiglet import Figlet
from utils import RelativeDate

STRAVA_DATA_PATH = "data/strava/activities.csv"
SEASON_STARTS_ON = "2021-10-01"
TODO_PATH = "/Users/luke.armistead/workspace/log/todo.txt"


class Review:
    # TODO - Input strava and todo objects, not paths
    def __init__(self, skis, todos):
        self.header = self.create_header(title="Review")
        self.table = self.create_table()
        self.skis = skis
        self.todos = todos
        self.populate_table()
        self.text = self.create_review_text()

    def create_header(self, title, font="small"):
        f = Figlet(font=font)
        title = f.renderText(title)
        date = RelativeDate()
        return title + date.last_monday_long

    def create_table(self):
        table = PrettyTable()
        table.field_names = ["Field", "Week", "Goal", "Agg"]
        table.align["Field"] = "l"
        for field in ["Week", "Goal", "Agg"]:
            table.align[field] = "r"
        table.float_format = ".0"
        return table

    def populate_table(self):
        rows = [
            [
                "Ski Day Count",
                self.skis.last_week_date_count,
                "3",
                self.skis.date_count,
            ],
            [
                "Ski Feet Sum (000s)",
                self.skis.last_week_vertical_feet / 1000,
                "30",
                self.skis.vertical_feet / 1000,
            ],
            ["Ski Mph Max", self.skis.last_week_max_speed, "60", self.skis.max_speed],
            ["Tasks Done", self.todos.done_count, "", ""],
        ]
        for row in rows:
            self.table.add_row(row)

    def create_review_text(self):
        list_text = [
            self.header,
            "\nGOALS",
            self.table.get_string(),
            "\nTASKS",
            "".join(self.todos.list),
        ]
        return "\n".join(list_text)


class ReviewFile:
    def __init__(self, review):
        self.review = review
        self.dir = "/Users/luke.armistead/workspace/log/review/"
        date = RelativeDate()
        self.file = date.last_monday_short + "-review.txt"
        self.path = self.dir + self.file

    def write(self):
        with open(self.path, "w+") as file:
            file.write(self.review.text)
        file.close()


if __name__ == "__main__":
    extract_strava(
        extract_data_since=SEASON_STARTS_ON,
        creds_path="creds.yml",
        creds_key="strava",
        data_output_path=STRAVA_DATA_PATH,
    )
    df = pd.read_csv(STRAVA_DATA_PATH)
    skis = Ski(SEASON_STARTS_ON, df)
    todos = ToDos(TODO_PATH)
    review = Review(skis, todos)
    review_file = ReviewFile(review)
    review_file.write()
    head = Figlet(font="small")
    migrate_todos(TODO_PATH, todos.to_migrate, head.renderText("ToDo"))

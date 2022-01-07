import datetime
from enum import Enum
from pyfiglet import Figlet
import os


""" Anticipated Workflow
- Current week notes and tasks go in home directory
- Log of past weekly notes and tasks go in log directory with each week's review
    - Should this be organized by week or month?
- Long-standing reference materials go in reference directory
"""

LOG_DIR = "/Users/luke.armistead/workspace/log/"
TODO_FILE = "todo.txt"
ARCHIVE_DIR = "review/"
TODO_PATH = LOG_DIR + TODO_FILE
ARCHIVE_TODO_PATH = LOG_DIR + ARCHIVE_DIR + monday.strftime("%Y-%m-%d-") + TODO_FILE

class Symbol(str, Enum):
    TASK = "-"
    MIGRATE = ">"
    DONE = "x"
    DROP = "!"


class ToDo:
    def __init__(self, todo):
        self.raw = todo
        self.name = self.remove_initial_spaces(todo)
        self.symbol = self.parse_symbol(todo)

    def parse_symbol(self, todo):
        symbol = todo.strip(" ")[0]
        assert (
            symbol != Symbol.TASK
        ), f"Found a {Symbol.TASK} symbol on {todo}. Make sure to migrate all todos!"
        return symbol

    def remove_initial_spaces(self, todo):
        for i, char in enumerate(todo):
            if char != " ":
                break
        return todo[i:]


def list_todos_to_migrate(path):
    to_migrate = []
    done_count = 0
    drop_count = 0
    with open(path, "r") as todo_list:
        for todo in todo_list:
            todo = ToDo(todo)
            if todo.symbol == Symbol.MIGRATE:
                to_migrate.append(todo)
            elif todo.symbol == Symbol.DONE:
                done_count += 1
            elif todo.symbol == Symbol.DROP:
                drop_count += 1
        print(
            (
                f"Weekly tasks parsed\n"
                f"Done:     {done_count}\n"
                f"Migrated: {len(to_migrate)}\n"
                f"Dropped:  {drop_count}\n"
            )
        )
        return to_migrate


def monday_this_week():
    today = datetime.date.today()
    days_since_monday = today.weekday()  # Monday returns 0, Tuesday returns 1
    monday = today - datetime.timedelta(days=days_since_monday)
    return monday


def archive_todos(path, archive_path):
    assert not os.path.exists(
        archive_path
    ), f"Discontinuing weekly refresh because file already exists at target path {archive_path}"
    print(f"Archiving weekly todo list from {path} to {archive_path}")
    os.rename(path, archive_path)


def todo_header(monday, title="ToDo", font="small"):
    monday = monday.strftime("%B %d, %Y")
    f = Figlet(font="small")
    title = f.renderText(title)
    return title + monday


def migrate_todos(path, migrated_todos, header):
    with open(path, "w+") as todos:
        todos.write(header + "\n\n")
        for todo in migrated_todos:
            todo = todo.raw.replace(Symbol.MIGRATE, Symbol.TASK, 1)
            todos.write(todo)
    todos.close()
    print(f"Created new todo file with migrated todos at {path}")


def main(todo_path, archive_todo_path):
    new_todos = list_todos_to_migrate(todo_path)
    monday = monday_this_week()
    archive_todos(todo_path, archive_todo_path)
    header = todo_header(monday)
    migrate_todos(todo_path, new_todos, header)

if __name__ == "__main__":
    main(TODO_PATH, ARCHIVE_TODO_PATH)

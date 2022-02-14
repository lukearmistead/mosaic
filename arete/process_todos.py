from enum import Enum
from pyfiglet import Figlet
import os
from utils import RelativeDate


LOG_DIR = "/Users/luke.armistead/workspace/log/"
TODO_FILE = "todo.txt"
ARCHIVE_DIR = "review/"
DATE = RelativeDate()
ARCHIVE_TODO_PATH = LOG_DIR + ARCHIVE_DIR + DATE.last_monday.short_date_format + "-" + TODO_FILE
TODO_PATH = LOG_DIR + TODO_FILE


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
            symbol
            != Symbol.TASK
            # TODO - This is just an attribute of the task and should be handled by the
            # review function
        ), f"Found a '{Symbol.TASK}' symbol on '{todo}'. Make sure to migrate all todos!"
        return symbol

    def remove_initial_spaces(self, todo):
        for i, char in enumerate(todo):
            if char != " ":
                break
        return todo[i:]


class ToDos:
    def __init__(self, path):
        self.todo_list = open(path, "r")
        self.done_count = 0
        self.drop_count = 0
        self.list = []
        self.to_migrate = []
        self.process_todos()

    def process_todo(self, todo):
        todo = ToDo(todo)
        if todo.symbol == Symbol.MIGRATE:
            self.to_migrate.append(todo.raw)
        elif todo.symbol == Symbol.DONE:
            self.done_count += 1
        elif todo.symbol == Symbol.DROP:
            self.drop_count += 1
        else:
            return
        self.list.append(todo.name)

    def process_todos(self):
        for todo in self.todo_list:
            self.process_todo(todo)


def migrate_todos(path, migrated_todos, header):
    with open(path, "w+") as todos:
        todos.write(header + "\n\n")
        for todo in migrated_todos:
            todo = todo.replace(Symbol.MIGRATE, Symbol.TASK, 1)
            todos.write(todo)
    todos.close()
    print(f"Created new todo file with migrated todos at {path}")


if __name__ == "__main__":
    pass
    main(TODO_PATH, ARCHIVE_TODO_PATH)

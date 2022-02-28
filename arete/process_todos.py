from enum import Enum
from pyfiglet import Figlet
import os
from utils import RelativeDate


LOG_DIR = "/Users/luke.armistead/workspace/log/"
TODO_FILE = "todo-test.txt"
ARCHIVE_DIR = "review/"
LAST_MONDAY = RelativeDate("Monday", weeks_ago=1)
ARCHIVE_TODO_PATH = LOG_DIR + ARCHIVE_DIR + LAST_MONDAY.short_format() + "-" + TODO_FILE
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
        return symbol

    def remove_initial_spaces(self, todo):
        for i, char in enumerate(todo):
            if char != " ":
                break
        return todo[i:]


class ToDos:
    def __init__(self, path):
        self.todo_list = open(path, "r")
        self.task_count = 0
        self.done_count = 0
        self.drop_count = 0
        self.list = []
        self.to_migrate = []
        self.process_todos()

    def process_todo(self, todo):
        todo = ToDo(todo)
        if todo.symbol == Symbol.MIGRATE:
            self.to_migrate.append(todo)
        elif todo.symbol == Symbol.DONE:
            self.done_count += 1
        elif todo.symbol == Symbol.DROP:
            self.drop_count += 1
        elif todo.symbol == Symbol.TASK:
            self.task_count += 1
        else:
            return
        self.list.append(todo.name)

    def process_todos(self):
        for todo in self.todo_list:
            self.process_todo(todo)


def migrate_todos(path, todos, header):
    assert todos.task_count == 0, f"""{todos.task_count} task(s) prefixed by symbol "{Symbol.TASK}" require review. Please migrate before running!"""
    with open(path, "w+") as new_todo_list:
        new_todo_list.write(header + "\n\n")
        for todo in todos.to_migrate:
            todo = todo.raw.replace(Symbol.MIGRATE, Symbol.TASK, 1)
            new_todo_list.write(todo)
    new_todo_list.close()
    print(f"Created new todo file with migrated todos at {path}")


if __name__ == "__main__":
    pass
    # main(TODO_PATH, ARCHIVE_TODO_PATH)

from datetime import datetime
import logging as getLogger
import pandas as pd
import requests
from splitwise import Splitwise
from arete.utils import lookup_yaml


getLogger.getLogger().setLevel(getLogger.INFO)


def find_splitwise_object(search_id, splitwise_objects):
    for splitwise_object in splitwise_objects:
        if splitwise_object.id == search_id:
            return splitwise_object
    return None


def map_category(category_rules, expense):
    for category, rule in category_rules.items():
        rule = rule["splitwise"]["category"]
        if rule is not None and expense in rule:
            return category


def extract_splitwise(
    creds_path,
    creds_key,
    start_date,
    end_date,
    output_path,
):
    creds = lookup_yaml(creds_path)[creds_key]

    # https://splitwise.readthedocs.io/en/latest/user/authenticate.html#api-key
    client = Splitwise(
        creds["consumer_key"], creds["consumer_secret"], api_key=creds["api_key"]
    )
    my_user_id = client.getCurrentUser().id
    expenses = client.getExpenses(limit=False)
    groups = client.getGroups()
    unpacked_expenses = []
    rules = lookup_yaml("transaction_categories.yml")
    splitwise_categories = {}
    for category, rule in rules.items():
        splitwise_categories[category] = rule["splitwise"]["category"]
    for expense in expenses:
        if expense.deleted_at is not None:
            continue
        unpacked_expense = {
            "id": int(expense.id),
            "date": pd.to_datetime(expense.date).date(),
            "description": str(expense.description),
            "is_payment": bool(expense.payment),
            "cost": float(expense.cost),
            "category": str(map_category(rules, expense.category.name)),
            "user_names": [user.first_name for user in expense.users],
        }

        user = find_splitwise_object(my_user_id, expense.users)
        if user is None:
            for k in [
                "net_balance",
                "paid_share",
                "owed_share",
            ]:
                unpacked_expense[k] = None
        else:
            unpacked_expense["net_balance"] = float(user.net_balance)
            unpacked_expense["paid_share"] = float(user.paid_share)
            unpacked_expense["owed_share"] = float(user.owed_share)

        group = find_splitwise_object(expense.group_id, groups)
        if group is None:
            for k in ["group_id", "group_name"]:
                unpacked_expense[k] = None
        else:
            unpacked_expense["group_id"] = group.id
            unpacked_expense["group_name"] = group.name
        unpacked_expenses.append(unpacked_expense)

    df = pd.DataFrame(unpacked_expenses)
    getLogger.info(df.head())
    getLogger.info(df.info())
    df.to_csv(output_path, index=False)

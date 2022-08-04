import logging as log
import pandas as pd
import requests
from splitwise import Splitwise
from utils import lookup_yaml


log.getLogger().setLevel(log.INFO)


remapped_categories = {
    "Hotel": "Travel",
    "Sports": "Recreation",
    "Dining out": "Restaurants",
    "Plane": "Travel",
    "Liquor": "Bar",
    "Car": "Travel",
    "Taxi": "Travel",
    "TV/Phone/Internet": "Utilities",
    "Bus/train": "Travel",
    "Games": "Recreation",
    "Gas/fuel": "Travel",
    "Trash": "Utilities",
    "Insurance": "Utilities",
    "Groceries": "Supermarkets and Groceries",
    "General": "Shops",  # This is the def facto general category - could be improved
    "Rent": "Rent",
    "Transportation - Other": "Travel",
    "Household supplies": "Shops",
    "Furniture": "Shops",
    "Gifts": "Shops",
    "Medical expenses": "Medical",
    "Food and drink - Other": "Restaurants",
    "Water": "Utilities",
    "Heat/gas": "Utilities",
    "Electronics": "Shops",
    "Clothing": "Shops",
    "Cleaning": "Utilities",
    "Parking": "Travel",
    "Bicycle": "Shops",
    "Electricity": "Utilities",
    "Entertainment - Other": "Recreation",
    "Home - Other": "Shops",
    "Utilities - Other": "Utilities",
    "Services": "Shops",
    "Music": "Recreation",
    "Mortgage": "Rent",
}


def find_splitwise_object(search_id, splitwise_objects):
    for splitwise_object in splitwise_objects:
        if splitwise_object.id == search_id:
            return splitwise_object
    return None


def main():
    creds = lookup_yaml("creds.yml")["splitwise"]

    # https://splitwise.readthedocs.io/en/latest/user/authenticate.html#api-key
    client = Splitwise(
        creds["consumer_key"], creds["consumer_secret"], api_key=creds["api_key"]
    )
    my_user_id = client.getCurrentUser().id
    expenses = client.getExpenses(limit=False)
    groups = client.getGroups()
    unpacked_expenses = []
    for expense in expenses:
        # Useful for debugging
        # log.info(expense.description, pd.to_datetime(expense.date).date(), 'uid', user.id, 'paid', paid, 'owed', owed, 'balance', user.net_balance)
        if expense.deleted_at is not None:
            continue
        unpacked_expense = {
            "id": int(expense.id),
            "date": pd.to_datetime(expense.date).date(),
            "description": str(expense.description),
            "is_payment": bool(expense.payment),
            "cost": float(expense.cost),
            "category": str(remapped_categories[expense.category.name]),
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
    log.info(df.head())
    log.info(df.info())
    df.to_csv("data/splitwise/splitwise.csv", index=False)


if __name__ == "__main__":
    main()

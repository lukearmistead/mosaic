import ast
import os
import requests
from datetime import datetime, timedelta
import logging as log
import pandas as pd

pd.options.display.max_columns = None
pd.options.display.max_rows = None
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.item_public_token_exchange_request import (
    ItemPublicTokenExchangeRequest,
)

from ratelimit import limits, sleep_and_retry
from utils import lookup_yaml


log.getLogger().setLevel(log.INFO)
CREDS_KEY = "plaid"
ACCESS_TOKEN_KEY = "plaid_account"
CREDS_PATH = "creds.yml"

END_DATE = datetime.now().date()
# Some institution-specific limitations: https://dashboard.plaid.com/oauth-guide
# Plaid's agreement with Capital One only permits downloading the last 90 days of authorization by the user
START_DATE = datetime(2022, 3, 16).date()
# Barclaycard is actually a distinct entity from Barclays. Plaid doesn't enable access to the former.
ACCOUNTS = ["aspiration", "chase", "capital_one"]
OUTPUT_DIR_PATH = "data/plaid/"


def get_client(creds):
    configuration = plaid.Configuration(
        host=plaid.Environment.Development,
        api_key={"clientId": creds["client_id"], "secret": creds["client_secret"],},
    )
    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)
    return client


@sleep_and_retry
@limits(calls=30, period=60)
def fetch_transactions_response(client, access_token, start_date, end_date, options):
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        options=options,
    )
    try:
        response = client.transactions_get(request)
    except:
        print("Hit rate limit unexpectedly. Sleeping a full minute to reset.")
        time.sleep(60)
    return response.to_dict()


class BarCategory:  # This could potentially inherit from a "category" superclass
    def __init__(description, merchant, category, source):
        self.name = "Bar"
        self.merchants = ["benders", "schroder's"]
        self.descriptions = ["brew"]

    def breweries_are_bars(self):
        if self.name.lower().find("brew") and self.category in ["Food", "Other Food"]:
            return True
        else:
            return False


# A good start: https://plaid.com/blog/transactions-categorization-taxonomy/
proposed_categories = [
    "Payroll",
    "Rent",
    "Utility",  # Cable, etc
    "Saving",
    "Transportation",  # Planes, gas, etc
    "Transfer",  # Transfers, payments, etc.
    "Grocery",
    "Dining",
    "Bar",  # Might be able to collapse these two
    "Entertainment",  # Might bear renaming. Cost of getting outside
    "Goods",  # Shopping, amazon, netflix, etc
    "Services",  # House cleaning, dry cleaning
    "Medical",
    "Government",  # Taxes, fees, etc
]


def simplify_categories(categories):
    categories = ast.literal_eval(str(categories))
    if len(categories) == 1:
        return categories[0]
    elif categories[0] == "Food and Drink":
        return categories[1]
    elif categories[1] in ["Food and Beverage Store", "Food and Beverage"]:
        return "Food and Drink"
    elif categories[1] == "Credit Card" and categories[0] == "Payment":
        return "Credit Card Payment"
    elif categories[1] in ["Entertainment", "Arts and Entertainment"]:
        return "Recreation"
    elif categories[1] in ["Bicycles", "Sporting Goods"]:
        return "Recreation"
    elif categories[1] in [
        "Convenience Stores",
        "Bookstores",
        "Florists",
        "Personal Care",
        "Shipping and Freight",
        "Clothing and Accessories",
    ]:
        return "Shops"
    elif categories[1] in ["Cable", "Business Services"]:
        return "Utilities"
    elif categories[0] == "Travel":
        return "Travel"
    elif "Transfer" in categories:
        return "Transfer"
    elif (
        categories[0] == "Tax" or categories[1] == "Government Departments and Agencies"
    ):
        return "Government"
    elif categories[1] == "Financial":
        return categories[2]
    elif categories[0] == "Shops":
        return categories[1]
    else:
        return categories[1]


def main(
    creds_path=CREDS_PATH,
    creds_key=CREDS_KEY,
    start_date=START_DATE,
    end_date=END_DATE,
    accounts=ACCOUNTS,
    output_dir_path=OUTPUT_DIR_PATH,
):
    creds = lookup_yaml(creds_path)[creds_key]
    client = get_client(creds)
    for account in accounts:
        access_token = creds[account]["access_token"]
        # Transactions in the response are paginated, so make multiple calls
        # while increasing the offset to retrieve all transactions
        options = TransactionsGetRequestOptions()
        # options.include_original_description = True
        # options.include_personal_finance_category_beta = True
        options.include_personal_finance_category = True
        transactions = []
        has_more_transactions = True
        while has_more_transactions:
            options.offset = len(transactions)
            response = fetch_transactions_response(
                client, access_token, start_date, end_date, options
            )
            transactions += response["transactions"]
            # We create the boolean coordinating the loop after receiving the expected transaction count from the df response
            has_more_transactions = len(transactions) < response["total_transactions"]

        # Build dataframe
        df = pd.DataFrame(transactions)
        df["account_name"] = account
        df["account"] = account
        print(df.info())
        column_types = {
            "transaction_id": "object",
            "name": "object",
            "date": "datetime64[ns]",
            "category_id": "int64",
            "category": "object",
            "account_name": "object",
            "account": "object",
            "merchant_name": "object",
            "amount": "float64",
        }
        df = df.astype(dtype=column_types)[column_types.keys()]

        log.info("Cleaning up Plaid categories")

        category_by_rule = df["category"].apply(lambda cats: simplify_categories(cats))
        category_by_merchant = df["merchant_name"].map(
            {
                "Southern Pacific": "Bar",
                "Benders Bar": "Bar",
                "Tiki-ti LLC": "Bar",
                "Tonga Hut": "Bar",
                "Cityexperiences": "Travel",
                "Li Po": "Bar",
                "Tahoe Food": "Bar",
                "Palisades": "Bar",
                "Schroeder S": "Bar",
                "Truffle": "Bar",
                "Spitz": "Bar",
                "Harris Teeter Supermarkets, Inc.": "Supermarkets and Groceries",
                "Goodr LLC": "Shops",
                "Planted": "Transfer",  # Weird Aspiration kickback thing
                "Atterley": "Shops",
                "Tahoe Full": "Restaurants",
                "The Nature Stop": "Supermarkets and Groceries",
                "Wealthfront Inc.": "Financial Planning and Investments",
            }
        )
        category_by_name = df["name"].map(
            {
                "Ach 42019 Omada Heal Dir Dep": "Payroll",
                "CREDIT-TRAVEL REWARD": "Travel",
                "PURCHASE ADJUSTMENT": "Travel",
                "Convenience Check Adjustment": "Rent",
                "KAHOOT! ASA": "Digital Purchase",
                "SP PIT VIPER SUNNIES": "Shops",
                "HIGH ALTITUDE TRUCKEE": "Recreation",
                "MISSION CLIFFS": "Recreation",
                "SANDBOX VR SF POP UP": "Recreation",
                "IKON PASS": "Recreation",
                "GJUSTA GROCER": "Restaurants",
                "ALPINE MEADOWS FOOD AND B": "Bar",
                "SP PAIR OF THIEVES": "Shops",
                "Ach Square Inc Cash App": "Third Party",
                "MONROE": "Bar",
                "CKE*LINCOLN AND SOUTH BRE": "Bar",
                "2BOA STADIUM": "Bar",
                "DELAWARE NORTH LOGAN F&B": "Restaurants",
                "EMPORIUM SF": "Bar",
                "TST* Trillium Brewing Com": "Bar",
                "CATAWBA BREWING CO. CH": "Bar",
                "KAHUNA TIKI NORTH HOL": "Bar",
                "TOCK AT*CLIFF LEDE VIN": "Bar",
                "FORT POINT BEER COMPA": "Bar",
                "SP HARBOUR TOWN BAKERY": "Restaurants",
                "TST* Russian River Brewin": "Bar",
                "TST* The Roasting Company": "Restaurants",
                "Ach Square Inc Cash App": "Transfer",
                "MOMENT LLC": "Recreation",
                "ANTI-CORP": "Shops",
                "Safeway": "Supermarkets and Groceries",
            }
        )

        category_by_id = df["transaction_id"].map(
            {
                "MYd5Ye9PdOhQJM4mR0nAt1QgOne76gHpj3moL": "Travel",  # Zola wedding registry
                "Key0DwKoNXUy1KOnnvoPia6qNPv6jPH16QDbL": "Loans and Mortgages",  # Final loan repayment
            }
        )

        df["category"] = category_by_id.combine_first(
            category_by_name.combine_first(
                category_by_merchant.combine_first(category_by_rule)
            )
        )

        # Really specific logic
        has_brew_in_name = df["name"].apply(
            lambda name: name.lower().find("brew") != -1
        )
        has_doordash_in_name = df["name"].apply(
            lambda name: name.lower()[:11] == "dd doordash"
            or name.lower()[:8] == "doordash"
        )
        df.loc[has_doordash_in_name, "category"] = "Restaurants"

        # Outputs for debugging
        log.info(
            f"{account} transaction extract for {start_date} till {end_date} complete"
        )
        log.debug(
            f"""{account} earliest and latest transaction dates: {df["date"].min()}, {df["date"].max()}"""
        )
        log.debug(df.head())
        log.debug(df.info())
        log.debug(
            f"Returned {len(df)} row table, vs expectation of {response['total_transactions']}"
        )
        log.debug(response["total_transactions"])
        df.to_csv(OUTPUT_DIR_PATH + account + "_transactions.csv", index=False)


if __name__ == "__main__":
    main()

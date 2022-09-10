import ast
import os
import requests
from datetime import datetime, timedelta
import logging as log
import pandas as pd
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.item_public_token_exchange_request import (
    ItemPublicTokenExchangeRequest,
)
from ratelimit import limits, sleep_and_retry
import time
from arete.utils import lookup_yaml


pd.options.display.max_columns = None
pd.options.display.max_rows = None
log.getLogger().setLevel(log.DEBUG)



CREDS_KEY = "plaid"
ACCESS_TOKEN_KEY = "plaid_account"
CREDS_PATH = "creds.yml"
# Some institution-specific limitations: https://dashboard.plaid.com/oauth-guide
# Plaid's agreement with Capital One only permits downloading the last 90 days of authorization by the user
START_DATE = datetime(2022, 3, 16).date()
END_DATE = datetime.now().date()
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
        log.info("Hit rate limit unexpectedly. Sleeping a full minute to reset.")
        time.sleep(60)
    return response.to_dict()


def categorize_expense(category_rules, expense):
    categories = []
    log.debug(categories)
    log.debug(expense)
    for field in [
        "transaction_id",
        "name",
        "original_description",
        "personal_finance_category",
        "category",
    ]:
        log.debug("FIELD: ", field)
        for category, rules in category_rules.items():
            log.debug(f"    CATEGORY:  {category}")
            mapping = rules["plaid"][field]
            log.debug("    MAPPING:  {mapping}")
            if mapping is None:
                continue
            elif field == "personal_finance_category":
                for subfield in ["primary", "detailed"]:
                    log.debug(mapping[subfield])
                    log.debug(expense[field][subfield])
                    if mapping[subfield] is None:
                        continue
                    elif expense[field][subfield] in mapping[subfield]:
                        return category
            elif field == "category":
                subcategory_indices_by_granularity = range(
                    len(expense[field]) - 1, 0 - 1, -1
                )
                for i in subcategory_indices_by_granularity:
                    if mapping[i] is None:
                        continue
                    elif expense[field][i] in mapping[i]:
                        return category
            elif expense[field] in mapping:
                return category
    return None


def extract_plaid(
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
        options.include_original_description = True
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

        categorization_rules = lookup_yaml("transaction_categories.yml")
        tailored_categories = []
        for expense in transactions:
            cat = categorize_expense(categorization_rules, expense)
            tailored_categories.append(cat)

        # Build dataframe
        df = pd.DataFrame(transactions)
        df["account_name"] = account
        df["account"] = account
        df["raw_category"] = df["category"]
        df["category"] = tailored_categories
        column_types = {
            "transaction_id": "object",
            "name": "object",
            "date": "datetime64[ns]",
            # "category_id": "int64",
            "category": "object",
            # "raw_category": "object",
            # "personal_finance_category": "object",
            "account_name": "object",
            "account": "object",
            "merchant_name": "object",
            "amount": "float64",
        }
        df = df.astype(dtype=column_types)[column_types.keys()]

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
    extract_plaid()

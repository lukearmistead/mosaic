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
from utils import lookup_yaml


log.getLogger().setLevel(log.INFO)
CREDS_KEY = "plaid"
ACCESS_TOKEN_KEY = "plaid_account"
CREDS_PATH = "creds.yml"

END_DATE = datetime.now().date()  #  - timedelta(days=360)
START_DATE = END_DATE - timedelta(days=360)

# Some institution-specific limitations:
# Plaid's agreement with Capital One only permits downloading the last 90 days of transactions
# https://dashboard.plaid.com/oauth-guide
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
        transactions = []
        has_more_transactions = True
        while has_more_transactions:
            options.offset = len(transactions)
            response = fetch_transactions_response(
                client, access_token, start_date, end_date, options
            )
            transactions += response["transactions"]
            # We create the boolean coordinating the loop after receiving the expected transaction count from the plaid response
            has_more_transactions = len(transactions) < response["total_transactions"]

        # Build dataframe
        df = pd.DataFrame(transactions)
        column_types = {
            "transaction_id": "object",
            "datetime": "datetime64[ns]",
            "date": "datetime64[ns]",
            "account_id": "object",
            "category_id": "int64",
            "category": "object",
            "merchant_name": "object",
            "amount": "float64",
        }
        df = df.astype(dtype=column_types)[column_types.keys()]

        # Outputs for debugging
        log.info(
            f"{account} transaction extract for {start_date} till {end_date} complete"
        )
        # log.info(f"""{account} earliest and latest transaction dates: {df["date"].min()}, {df["date"].max()}""")
        log.info(df.head())
        log.info(df.info())
        log.info(
            f"Returned {len(df)} row table, vs expectation of {response['total_transactions']}"
        )
        log.info(response["total_transactions"])
        df.to_csv(OUTPUT_DIR_PATH + account + "_transactions.csv", index=False)


if __name__ == "__main__":
    main()

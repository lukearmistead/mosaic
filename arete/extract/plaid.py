import ast
import os
import requests
from datetime import datetime, timedelta
import logging as getLogger
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
getLogger.getLogger().setLevel(getLogger.DEBUG)


def get_client(client_id, client_secret):
    configuration = plaid.Configuration(
        host=plaid.Environment.Development,
        api_key={"clientId": client_id, "secret": client_secret,},
    )
    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)
    return client


class TransactionsFetcher:
    def __init__(
        self, client, access_token,
    ):
        self.client = client
        self.access_token = access_token
        self.options = TransactionsGetRequestOptions(
            include_personal_finance_category=True,
        )

    @sleep_and_retry
    @limits(calls=30, period=60)
    def _fetch_request_handler(
        self, start_date, end_date,
    ):
        request = TransactionsGetRequest(
            access_token=self.access_token,
            start_date=start_date,
            end_date=end_date,
            options=self.options,
        )
        try:
            response = self.client.transactions_get(request)
        except:
            getLogger.info(
                "Hit rate limit unexpectedly. Sleeping a full minute to reset."
            )
            time.sleep(60)
        return response.to_dict()

    def fetch(
        self, start_date, end_date,
    ):
        transactions = []
        still_more_transactions = True
        while still_more_transactions:
            self.options.offset = len(transactions)
            response = self._fetch_request_handler(start_date, end_date)
            transactions += response["transactions"]
            still_more_transactions = len(transactions) < response["total_transactions"]
        # TODO - This could be a test
        getLogger.debug(
            f"Fetched {len(transactions)} transactions vs expected {response['total_transactions']}"
        )
        return transactions


class TransactionCategorizer:
    def __init__(self, categorization_rules):
        self.categorization_rules = categorization_rules
        self.field_lookup_order = [
            "transaction_id",
            "name",
            "personal_finance_category",
            "category",
        ]

    def _matching_personal_finance_category(self, rule, lookup):
        getLogger.debug("        PERSONAL FINANCE LOOKUP")
        for specificity in ["detailed", "primary"]:
            getLogger.debug(f"        SUBRULE {specificity}:   {rule[specificity]}")
            getLogger.debug(f"        SUBLOOKUP {specificity}: {lookup[specificity]}")
            if rule[specificity] is None:
                continue
            elif lookup[specificity] in rule[specificity]:
                return True
        return False

    def _matching_category(self, rule, lookup):
        start = len(lookup) - 1
        stop = 0 - 1
        step = -1
        step_backwards_through_subcategories = range(start, stop, step)
        for i in step_backwards_through_subcategories:
            if rule[i] is None:
                continue
            elif lookup[i] in rule[i]:
                return True
        return False

    def categorize(self, transaction):
        getLogger.debug(transaction)
        for field in self.field_lookup_order:
            getLogger.debug(f"FIELD: {field}")
            for category, rules in self.categorization_rules.items():
                rule = rules["plaid"][field]
                lookup = transaction[field]
                getLogger.debug(f"    CATEGORY:  {category}")
                getLogger.debug(f"    RULE:    {rule}")
                getLogger.debug(f"    LOOKUP:  {lookup}")
                if rule is None:
                    continue
                elif (
                    field == "personal_finance_category"
                    and self._matching_personal_finance_category(rule, lookup)
                ):
                    return category
                elif field == "category" and self._matching_category(rule, lookup):
                    return category
                elif (
                    field not in ["personal_finance_category", "category"]
                    and lookup in rule
                ):
                    return category
        return None


def extract_plaid(
    creds_path, creds_key, start_date, end_date, endpoints,
):
    creds = lookup_yaml(creds_path)[creds_key]
    client = get_client(creds["client_id"], creds["client_secret"])
    for account, config in endpoints.items():
        access_token = creds[account]["access_token"]
        transactions_fetcher = TransactionsFetcher(client, access_token)
        transactions = transactions_fetcher.fetch(start_date, end_date)

        categorization_rules = lookup_yaml("transaction_categories.yml")
        transaction_categorizer = TransactionCategorizer(categorization_rules)
        tailored_categories = []
        for transaction in transactions:
            cat = transaction_categorizer.categorize(transaction)
            tailored_categories.append(cat)

        # Build dataframe
        df = pd.DataFrame(transactions)
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
            "account": "object",
            "merchant_name": "object",
            "amount": "float64",
        }
        df = df.astype(dtype=column_types)[column_types.keys()]

        # Outputs for debugging
        getLogger.info(
            f"{account} transaction extract for {start_date} till {end_date} complete"
        )
        getLogger.debug(
            f"""{account} earliest and latest transaction dates: {df["date"].min()}, {df["date"].max()}"""
        )
        getLogger.debug(df.head())
        getLogger.debug(df.info())
        df.to_csv(config["output_path"], index=False)

from arete.utils import convert_vector_to_date, convert_string_to_date
from prettytable import PrettyTable, PLAIN_COLUMNS
import ast
import logging as getLogger
import pandas as pd
import pandasql
import os

pd.options.display.max_rows = None
pd.options.display.max_columns = None
getLogger.getLogger().setLevel(getLogger.DEBUG)
transform_dir = "arete/transform/"


def read_plaid_transactions(endpoints):
    transactions = []
    for account, config in endpoints.items():
        transactions.append(pd.read_csv(config["output_path"]))
    transactions = pd.concat(transactions).sort_values("date").reset_index(drop=True)
    return transactions


def read_query(path):
    with open(path, "r") as f:
        return f.read()


def date_period_vector(v, period):
    return convert_vector_to_date(
        pd.to_datetime(v).dt.to_period(period).dt.to_timestamp().astype(str)
    )


def verbose_query(plaid, splitwise, query):
    t = PrettyTable()
    t.set_style(PLAIN_COLUMNS)
    t.add_column("old", [len(plaid), round(sum(plaid["amount"]), 2)])
    transformed_data = pandasql.sqldf(query, locals())
    t.add_column(
        "new", [len(transformed_data), round(sum(transformed_data["amount"]), 2)]
    )
    getLogger.info(t)
    return transformed_data


VENMO_PAYMENTS_FROM_ASPIRATION_TO_SPLITWISE = read_query(
    f"{transform_dir}venmo_payments_from_aspiration_to_splitwise.sql"
)
TRANSACTIONS_WITHOUT_VENMO_PAYMENTS_FROM_ASPIRATION_TO_SPLITWISE = f"""
    select plaid.*
      from plaid
           left join ({VENMO_PAYMENTS_FROM_ASPIRATION_TO_SPLITWISE}) as payments
                on payments.transaction_id = plaid.transaction_id
     where payments.transaction_id isnull
    """
TRANSACTIONS_WITH_VENMO_INCOME_NET_OF_SPLITWISE_BALANCE = read_query(
    f"{transform_dir}venmo_income_net_of_splitwise_balance.sql"
)
TRANSACTIONS_WITH_MY_SHARE_OF_GROUP_AMOUNTS = read_query(
    f"{transform_dir}transactions_with_my_share_of_group_amounts.sql"
)


def transform_transactions(
    start_date, end_date, extract_plaid_endpoints, extract_splitwise_path, output_path
):
    plaid = read_plaid_transactions(extract_plaid_endpoints)

    splitwise = pd.read_csv(extract_splitwise_path).dropna(subset=["net_balance"])

    getLogger.info(
        "Lump payments and income from Venmo disguise how cash is actually being spent, which is captured by Splitwise"
    )
    plaid = verbose_query(
        plaid,
        splitwise,
        TRANSACTIONS_WITHOUT_VENMO_PAYMENTS_FROM_ASPIRATION_TO_SPLITWISE,
    )

    getLogger.info(f"For my income from Venmo, removing Splitwise share from the total")
    shared_transaction_examples = [
        "Je3vNdZXRVU3oaOPP6p5tKz5zyDyneiqRDrrA",
        "gvX3p6KMaeIgJB9ZZ3aXSmx8JDoxXDi3yqnVN",
    ]
    is_shared_payment = plaid["transaction_id"].isin(shared_transaction_examples)
    plaid["amount"] = verbose_query(
        plaid, splitwise, TRANSACTIONS_WITH_VENMO_INCOME_NET_OF_SPLITWISE_BALANCE
    )
    getLogger.debug(
        f"""Shared Venmo payments after adjustment\n{plaid.loc[is_shared_payment, 'amount']}"""
    )

    getLogger.info(
        "Now that we have adjusted the lump payments, we can layer in Splitwise transactions to get a more nuanced understanding of spending"
    )
    getLogger.info(
        f"For group expenses paid by me, replacing full amount from Plaid with my share from Splitwise"
    )
    plaid = verbose_query(plaid, splitwise, TRANSACTIONS_WITH_MY_SHARE_OF_GROUP_AMOUNTS)

    getLogger.info(
        f"For group expenses paid by others, appending my share from Splitwise"
    )
    t = PrettyTable()
    t.set_style(PLAIN_COLUMNS)
    t.add_column("", ["count", "$"])
    t.add_column("old", [len(plaid), round(sum(plaid["amount"]), 2)])
    someone_else_paid = splitwise["paid_share"] == 0
    not_repayment = ~splitwise["is_payment"]
    split_transactions = splitwise.rename(
        columns={"id": "transaction_id", "description": "name", "owed_share": "amount"}
    )
    # TODO - Is this helping?
    within_capital_one_window = convert_vector_to_date(splitwise["date"]).between(
        start_date, end_date
    )
    split_transactions["account"] = "splitwise"
    split_transactions["merchant_name"] = None
    split_transactions["category_id"] = None
    split_transactions = split_transactions.loc[
        someone_else_paid & not_repayment & within_capital_one_window, plaid.columns
    ]
    plaid = pd.concat((plaid, split_transactions), axis=0)
    t.add_column("new", [len(plaid), round(sum(plaid["amount"]), 2)])
    getLogger.info(t)

    getLogger.info("Dropping edge cases for expenses handled by Splitwise")
    ct, amt = len(plaid), sum(plaid["amount"])
    getLogger.debug(
        "Dropping insurance expenses which occur on an odd cadence and are split between homeowners and car insurance in Plaid but are unified in Splitwise"
    )
    plaid = plaid.loc[~plaid["name"].isin(["HOMEOWNERS INSURANCE", "GEICO"])]
    getLogger.debug(
        "Amazon grocery tips are counted separately from the grocery bill but are included in the Splitwise amount."
    )
    plaid = plaid.loc[~(plaid["name"].apply(lambda s: s[:11]) == "Amazon Tips")]
    new_ct, new_amt = len(plaid), sum(plaid["amount"])
    getLogger.info(
        f"Removed {new_ct - ct} rows, resulting in a ${new_amt - amt:.2f} change in cash flow to ${new_amt:.2f}"
    )

    getLogger.info("Enriching with time dimensions and categories")
    plaid["is_variable"] = ~plaid["category"].isin(["income", "transfer", "housing"])
    plaid = plaid.loc[
        convert_vector_to_date(plaid["date"]).between(start_date, end_date),
    ]
    plaid["week"] = date_period_vector(plaid["date"], "W")
    plaid["month"] = date_period_vector(plaid["date"], "M")

    getLogger.info(f"Saving processed data to {output_path}\n{plaid.info()}")
    plaid.to_csv(output_path, index=False)


if __name__ == "__main__":
    transform_transactions()

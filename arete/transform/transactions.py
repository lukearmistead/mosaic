from prettytable import PrettyTable, PLAIN_COLUMNS
import ast
import logging as log
import pandas as pd
import pandasql
import os

pd.options.display.max_rows = None
pd.options.display.max_columns = None

log.getLogger().setLevel(log.DEBUG)
transform_dir = "arete/transform/"
output_path = "data/processed/financial_transactions.csv"


def read_plaid_transactions(path):
    transactions = []
    for file in os.listdir(path):
        transactions.append(pd.read_csv(path + file))
    transactions = pd.concat(transactions).sort_values("date").reset_index(drop=True)
    return transactions


def read_query(path):
    with open(path, "r") as f:
        return f.read()


def verbose_query(plaid, splitwise, query):
    t = PrettyTable()
    t.set_style(PLAIN_COLUMNS)
    t.add_column("old", [len(plaid), round(sum(plaid["amount"]), 2)])
    transformed_data = pandasql.sqldf(query, locals())
    t.add_column(
        "new", [len(transformed_data), round(sum(transformed_data["amount"]), 2)]
    )
    log.info(t)
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


def transform_transactions():
    plaid = read_plaid_transactions(path="data/plaid/")

    # Get plaid data
    splitwise = pd.read_csv("data/splitwise/splitwise.csv").dropna(
        subset=["net_balance"]
    )

    log.info(
        "Lump payments and income from Venmo disguise how cash is actually being spent, which is captured by Splitwise"
    )
    plaid = verbose_query(
        plaid,
        splitwise,
        TRANSACTIONS_WITHOUT_VENMO_PAYMENTS_FROM_ASPIRATION_TO_SPLITWISE,
    )

    log.info(f"For my income from Venmo, removing Splitwise share from the total")
    shared_transaction_examples = [
        "Je3vNdZXRVU3oaOPP6p5tKz5zyDyneiqRDrrA",
        "gvX3p6KMaeIgJB9ZZ3aXSmx8JDoxXDi3yqnVN",
    ]
    is_shared_payment = plaid["transaction_id"].isin(shared_transaction_examples)
    plaid["amount"] = verbose_query(
        plaid, splitwise, TRANSACTIONS_WITH_VENMO_INCOME_NET_OF_SPLITWISE_BALANCE
    )
    log.debug(
        f"""Shared Venmo payments after adjustment\n{plaid.loc[is_shared_payment, 'amount']}"""
    )

    log.info(
        "Now that we have adjusted the lump payments, we can layer in Splitwise transactions to get a more nuanced understanding of spending"
    )
    log.info(
        f"For group expenses paid by me, replacing full amount from Plaid with my share from Splitwise"
    )
    plaid = verbose_query(plaid, splitwise, TRANSACTIONS_WITH_MY_SHARE_OF_GROUP_AMOUNTS)

    log.info(f"For group expenses paid by others, appending my share from Splitwise")
    t = PrettyTable()
    t.set_style(PLAIN_COLUMNS)
    t.add_column("", ["count", "$"])
    t.add_column("old", [len(plaid), round(sum(plaid["amount"]), 2)])
    someone_else_paid = splitwise["paid_share"] == 0
    not_repayment = ~splitwise["is_payment"]
    split_transactions = splitwise.rename(
        columns={"id": "transaction_id", "description": "name", "owed_share": "amount"}
    )
    within_capital_one_window = pd.to_datetime(splitwise["date"]) >= pd.to_datetime(
        "2022-03-16"
    )
    split_transactions["account"] = "splitwise"
    split_transactions["merchant_name"] = None
    split_transactions["category_id"] = None
    split_transactions = split_transactions.loc[
        someone_else_paid & not_repayment & within_capital_one_window, plaid.columns
    ]
    plaid = pd.concat((plaid, split_transactions), axis=0)
    t.add_column("new", [len(plaid), round(sum(plaid["amount"]), 2)])
    log.info(t)

    log.info("Dropping edge cases for expenses handled by Splitwise")
    ct, amt = len(plaid), sum(plaid["amount"])
    log.debug(
        "Dropping insurance expenses which occur on an odd cadence and are split between homeowners and car insurance in Plaid but are unified in Splitwise"
    )
    plaid = plaid.loc[~plaid["name"].isin(["HOMEOWNERS INSURANCE", "GEICO"])]
    log.debug(
        "Amazon grocery tips are counted separately from the grocery bill but are included in the Splitwise amount."
    )
    plaid = plaid.loc[~(plaid["name"].apply(lambda s: s[:11]) == "Amazon Tips")]
    new_ct, new_amt = len(plaid), sum(plaid["amount"])
    log.info(
        f"Removed {new_ct - ct} rows, resulting in a ${new_amt - amt:.2f} change in cash flow to ${new_amt:.2f}"
    )

    log.info(f"Saving processed data to {output_path}\n{plaid.info()}")
    plaid.to_csv(output_path, index=False)


if __name__ == "__main__":
    transform_transactions()

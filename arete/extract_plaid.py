import os
import requests
from datetime import datetime, timedelta
import logging as log
import pandas as pd
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from utils import Creds, lookup_yaml


log.getLogger().setLevel(log.INFO)
CREDS_KEY = "plaid"
ACCESS_TOKEN_KEY = "plaid_account"
CREDS_PATH = "creds.yml"
END_DATE = datetime.now().date()
START_DATE = END_DATE - timedelta(days=5)
ACCOUNTS = ["aspiration", "barclays", "chase", "capitalone"]
OUTPUT_DIR_PATH = "../data/plaid/"


def get_transaction_data(client, access_token, start_date, end_date) -> pd.DataFrame:
    """Gets around Plaid's rate limit"""
    running_date = end_date
    dfs = []
    dumb = TransactionsGetRequest(access_token=access_token, start_date=start_date, end_date=running_date)
    while running_date > start_date:
        d = client.transactions_get(dumb)
        df = pd.DataFrame(d["transactions"])
        earliest_date = df["date"].min()
        # Check that we aren't stuck because of data gaps
        if running_date == earliest_date:
            break
        df = df.loc[
            df["date"] > earliest_date,
        ]  # Drop the stub
        dfs.append(df)
        running_date = earliest_date
    df = pd.concat(dfs)
    log.info(f"DataFrame created:\n{df.info()}")
    return df


def main(
    creds_path=CREDS_PATH,
    creds_key=CREDS_KEY,
    start_date=START_DATE,
    end_date=END_DATE,
    accounts=ACCOUNTS,
    output_dir_path=OUTPUT_DIR_PATH,
):

    creds = lookup_yaml(creds_path)[creds_key]
    '''
    Debug
    Are the client id and secret alright? 
    > Yes.

    Did I call the configuration properly?
    > Pretty sure I did, yes.

    What does stack overflow say about this?
    > https://stackoverflow.com/questions/42098126/mac-osx-python-ssl-sslerror-ssl-certificate-verify-failed-certificate-verify

    Do I have an SSL certificate?
    > Seemingly yes.

    Is there something weird about Omada's security settings?
    > TBD

    If none of the above work, let's ask Plaid Support.
    '''

    configuration = plaid.Configuration(
        host=plaid.Environment.Development,
        api_key={
            'clientId': creds['client_id'],
            'secret': creds['client_secret'],
        }
    )
    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)

    # Can I do anything with Plaid?


    # Is it the capital one account?
    access_token = creds['aspiration']['access_token']

    request = TransactionsGetRequest(access_token=access_token, start_date=start_date, end_date=end_date)
    print(request)
    response = client.transactions_get(request)
    print('ERROR CODE')
    print(response['error_code'])
    print('got response')
   
    transactions = response['transactions']
    print(transactions)
    # for account in accounts:
    #     log.info(
    #         f"Pulling transactions from {account} account from {start_date} to {end_date}"
    #     )
    #     access_token = lookup_yaml(CREDS_PATH)[creds_key][account]["access_token"]
    #     print(access_token)
    #     df = get_transaction_data(client, access_token, start_date, end_date)
    #     if output_dir_path:
    #         name = account + "_transactions"
    #         name = "{}_{}_to_{}.csv".format(name, end_date, start_date)
    #         df.to_csv(output_dir_path + name, index=False)
    #     if not output_dir_path:
    #         return dfs


if __name__ == "__main__":
    main()

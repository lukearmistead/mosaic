from datetime import datetime, timedelta
import logging as log
import pandas as pd
import plaid
from utils import yaml_lookup


log.getLogger().setLevel(log.INFO)
CREDS_KEY = "plaid"
ACCESS_TOKEN_KEY = "plaid_account"
CREDS_PATH = "../creds.yml"
END_DATE = datetime.now().date()
START_DATE = END_DATE - timedelta(days=365)
ACCOUNTS = ["aspiration", "barclays", "chase"]
OUTPUT_DIR_PATH = "../data/plaid/"


def get_transaction_data(client, access_token, start_date, end_date) -> pd.DataFrame:
    """Gets around Plaid's rate limit"""
    running_date = end_date
    dfs = []
    while running_date > start_date:
        d = client.Transactions.get(
            access_token=access_token, start_date=start_date, end_date=running_date
        )
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
    start_date=str(START_DATE),
    end_date=str(END_DATE),
    accounts=ACCOUNTS,
    output_dir_path=OUTPUT_DIR_PATH,
):

    creds = yaml_lookup(creds_path, creds_key)
    client = plaid.Client(**creds)  # Relies on config keywords matching inputs
    log.info("Established connection to Plaid")
    for account in accounts:
        log.info(
            f"Pulling transactions from {account} account from {start_date} to {end_date}"
        )
        access_token = yaml_lookup(CREDS_PATH, ACCESS_TOKEN_KEY)[account][
            "access_token"
        ]
        df = get_transaction_data(client, access_token, start_date, end_date)
        if output_dir_path:
            name = account + "_transactions"
            name = "{}_{}_to_{}.csv".format(name, end_date, start_date)
            df.to_csv(output_dir_path + name, index=False)
        if not output_dir_path:
            return dfs


if __name__ == "__main__":
    main()

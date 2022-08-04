from prettytable import PrettyTable, PLAIN_COLUMNS
import ast
import logging as log
import pandas as pd
import pandasql
import os

pd.options.display.max_rows = None
pd.options.display.max_columns = None

log.getLogger().setLevel(log.DEBUG)
output_path = 'data/processed/financial_transactions.csv'

"""
Todo
- Create enum object to hold context on category names
- Might make sense to have it govern category rules too!
- Config file somewhere for remapping categories by name?
- Consider placing queries in objects to expose their key parameters
- What about ski lease or other instances where the true payment _is_ reflected by venmo?
"""


ASPIRATION_TO_SPLITWISE_PAYMENTS_SUBQUERY = '''    
    select plaid.transaction_id,
           plaid.name,
           plaid.date,
           plaid.amount,
           plaid.name,
           splitwise.date,
           splitwise.description,
           splitwise.net_balance,
           splitwise.group_name,
           splitwise.user_names
      from plaid
           join splitwise
                -- Amount should be about the same, allowing for rounding errors
                on splitwise.net_balance between plaid.amount - 1 and plaid.amount + 1
                -- A 1 day delay between financial accounts and seems ubiquitous. We allow for 2 days for some flexibility
                and plaid.date between splitwise.date and date(splitwise.date, '+2 days')
      where plaid.account_name = 'aspiration'
            and plaid.name = 'Venmo'
            and splitwise.is_payment
            and splitwise.net_balance > 0
            and plaid.amount > 0
    '''


TRANSACTIONS_WITHOUT_PAYMENTS_TO_SPLITWISE_QUERY = f'''
    select plaid.*
      from plaid
           left join ({ASPIRATION_TO_SPLITWISE_PAYMENTS_SUBQUERY}) as payments
                on payments.transaction_id = plaid.transaction_id
    where payments.transaction_id isnull
    '''


VENMO_INCOME_NET_OF_SPLITWISE_BALANCE_QUERY = '''
select coalesce(plaid.amount - splitwise.net_balance, plaid.amount, 0) as amount_net_of_splitwise_balance
  from plaid
       left join splitwise
            -- We have to be really generous with this logic
            -- Transfers from Venmo include any other transactions that occured between transfers
            on splitwise.net_balance between plaid.amount - 1010 and plaid.amount + 1010
            -- Transfers from Venmo often occurs well after the payment from Splitwise
            and plaid.date between splitwise.date and date(splitwise.date, '+20 days')
            and plaid.name = 'Venmo'
            and splitwise.is_payment
            -- This only applies to inflows from Venmo to the checking account
            and splitwise.net_balance < 0 and plaid.amount < 0
            and plaid.account_name = 'aspiration'
    '''


SHARED_EXPENSES_QUERY = '''
select coalesce(splitwise.net_balance, plaid.amount) as amount,
       coalesce(splitwise.category, plaid.category) as category
  from plaid
       left join splitwise
            on    ((
                    -- Greater flexibility on amount accommodates tips for whole foods grocery store pickups coordinated by Amazon
                    plaid.category = 'Digital Purchase'
                    and splitwise.paid_share between plaid.amount - 15 and plaid.amount + 15
                    and plaid.date between date(splitwise.date, '-1 days') and date(splitwise.date, '+1 days')
                  ) or (
                    -- Landlords don't always cash the checks in a timely fashion so rent needs flexible time windows
                    plaid.name = 'Convenience Check Adjustment'
                    and splitwise.paid_share between plaid.amount - 5 and plaid.amount + 5
                    and plaid.date between date(splitwise.date, '-15 days') and date(splitwise.date, '+15 days')
                  ) or (
                    -- We can be specific in the general case
                    splitwise.paid_share between plaid.amount - 5 and plaid.amount + 5
                    and plaid.date between date(splitwise.date, '-1 days') and date(splitwise.date, '+1 days')
                  ))
            and not splitwise.is_payment
            -- This only applies to inflows from Venmo to the checking account
            and splitwise.paid_share > 0
            and plaid.name <> "Venmo"
    '''


# NOT USED - This is an idea for a refactor

if __name__ == "__main__":

    # Get plaid data
    path = 'data/plaid/'
    transactions = []
    for file in os.listdir(path):
        transactions.append(pd.read_csv(path + file))
    plaid = pd.concat(transactions) \
        .sort_values('date') \
        .rename(columns={'category': 'category'}) \
        .reset_index()
    splitwise = pd.read_csv('data/splitwise/splitwise.csv') \
        .dropna(subset=['net_balance'])

    log.info('Lump payments and income from Venmo disguise how cash is actually being spent, which is captured by Splitwise.')

    t = PrettyTable().set_style(PLAIN_COLUMNS)
    t.add_column('old', [len(plaid), round(sum(plaid['amount']), 2)])
    plaid = pandasql.sqldf(TRANSACTIONS_WITHOUT_PAYMENTS_TO_SPLITWISE_QUERY, locals()).drop('index', axis=1)
    t.add_column('new', [len(plaid), round(sum(plaid['amount']), 2)])
    log.info(f'Removing venmo expenses related to splitwise\n{t}')

    t = PrettyTable()
    t.add_column('', ['count', '$'])
    t.add_column('old', [len(plaid), round(sum(plaid['amount']), 2)])
    is_shared_payment = plaid['transaction_id'].isin(["Je3vNdZXRVU3oaOPP6p5tKz5zyDyneiqRDrrA", "gvX3p6KMaeIgJB9ZZ3aXSmx8JDoxXDi3yqnVN"])
    log.debug(f"""Shared Venmo payments before adjustment\n{plaid.loc[is_shared_payment, 'amount']}""")
    plaid['amount'] = pandasql.sqldf(VENMO_INCOME_NET_OF_SPLITWISE_BALANCE_QUERY, locals())
    t.add_column('new', [len(plaid), round(sum(plaid['amount']), 2)])
    log.debug(f"""Shared Venmo payments after adjustment\n{plaid.loc[is_shared_payment, 'amount']}""")
    log.info(f'For my income from Venmo, removing Splitwise share from the total\n{t}')

    log.info("Now that those are cleaned up, we need to layer Splitwise transactions back in.")
    t = PrettyTable()
    t.add_column('', ['count', '$'])
    t.add_column('old', [len(plaid), round(sum(plaid['amount']), 2)])
    plaid['amount'] = pandasql.sqldf(SHARED_EXPENSES_QUERY, locals())['amount']
    plaid['category'] = pandasql.sqldf(SHARED_EXPENSES_QUERY, locals())['category']
    t.add_column('new', [len(plaid), round(sum(plaid['amount']), 2)])
    log.info(f'For group expenses paid by me, replacing full amount from Plaid with my share from Splitwise\n{t}')


    t = PrettyTable()
    t.add_column('', ['count', '$'])
    t.add_column('old', [len(plaid), round(sum(plaid['amount']), 2)])
    someone_else_paid = splitwise['paid_share'] == 0
    not_repayment = ~splitwise['is_payment']
    split_transactions = splitwise.rename(columns={'id': 'transaction_id', 'description': 'name', 'owed_share':'amount'})
    within_capital_one_window = pd.to_datetime(splitwise['date']) >= pd.to_datetime('2022-03-16')
    split_transactions['account_name'] = 'splitwise'
    split_transactions['account'] = 'splitwise'
    split_transactions['merchant_name'] = None
    split_transactions['category_id'] = None
    split_transactions = split_transactions.loc[someone_else_paid & not_repayment & within_capital_one_window, plaid.columns]
    plaid = pd.concat((plaid, split_transactions), axis=0)
    t.add_column('new', [len(plaid), round(sum(plaid['amount']), 2)])
    log.info(f'For group expenses paid by others, appending my share from Splitwise\n{t}')

    log.info('Dropping edge cases for expenses handled by Splitwise')
    ct, amt = len(plaid), sum(plaid['amount'])
    log.debug('Dropping insurance expenses which occur on an odd cadence and are split between homeowners and car insurance in Plaid but are unified in Splitwise')
    plaid = plaid.loc[~plaid['name'].isin(["HOMEOWNERS INSURANCE", "GEICO"])]
    log.debug('Amazon grocery tips are counted separately from the grocery bill but are included in the Splitwise amount.')
    plaid = plaid.loc[~(plaid['name'].apply(lambda s: s[:11]) == "Amazon Tips")]
    new_ct, new_amt = len(plaid), sum(plaid['amount'])
    log.info(f'Removed {new_ct - ct} rows, resulting in a ${new_amt - amt:.2f} change in cash flow to ${new_amt:.2f}')

    log.info(f'Saving processed data to {output_path}\n{plaid.info()}')
    plaid.to_csv(output_path, index=False)

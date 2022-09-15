select coalesce(plaid.amount - splitwise.net_balance, plaid.amount, 0) as amount
  from plaid
       left join splitwise
            -- We have to be really generous with this logic
            -- Transfers from Venmo include any other transactions that occured between transfers
            on splitwise.net_balance between plaid.amount - 1010 and plaid.amount + 1010
            -- Transfers from Venmo often occur well after the payment from Splitwise
            and plaid.date between splitwise.date and date(splitwise.date, '+20 days')
            and plaid.name = 'Venmo'
            and splitwise.is_payment
            -- This only applies to inflows from Venmo to the checking account
            and splitwise.net_balance < 0 and plaid.amount < 0
            and plaid.account = 'aspiration'

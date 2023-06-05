select plaid.id,
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
  where plaid.account = 'aspiration'
        and plaid.name = 'Venmo'
        and splitwise.is_payment
        and splitwise.net_balance > 0
        and plaid.amount > 0

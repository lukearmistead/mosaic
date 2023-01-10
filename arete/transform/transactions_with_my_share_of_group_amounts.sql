select plaid.transaction_id,
       plaid.name,
       plaid.date,
       coalesce(splitwise.category, plaid.category) as category,
       plaid.account,
       plaid.merchant_name,
       coalesce(splitwise.net_balance, plaid.amount) as amount
  from plaid
       left join splitwise
            on    ((
                    -- Greater flexibility on amount accommodates tips for whole foods grocery store pickups coordinated by Amazon
                    lower(substr(plaid.name, 1, 6))  = 'amazon'
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
                    and plaid.category = splitwise.category -- Assumes good categorization in the extract step
                  ))
            and not splitwise.is_payment
            -- This only applies to inflows from Venmo to the checking account
            and splitwise.paid_share > 0
            and plaid.name <> "Venmo"

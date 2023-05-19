Arete enables data-driven reflection by pulling and visualizing personal data from a variety of sources. So far, Arete supports:

- Fitbit
- Plaid
- Splitwise
- Strava

## Setup

### Install dependencies

1. Make sure your python version is 3.10 or greater
1. Install all python dependencies `pip install -r requirements.txt`

### Set up authorization

This step enables Arete to procure access tokens that allow it to access your data.

1. Copy `creds_template.yml` to `creds.yml`
1. Register your application with [Strava](https://developers.strava.com/docs/getting-started/), [Fitbit](https://dev.fitbit.com/build/reference/web-api/developer-guide/getting-started/), [Splitwise](https://secure.splitwise.com/oauth_clients), and [Plaid](https://dashboard.plaid.com/overview)
    - Set the "Redirect URL" and "Callback URL field to `http://local_host:8000/` for Fitbit and Splitwise
1. Fill in the `client_id` and `client_secret` fields in `creds.yml`

### Set up special authorization for Plaid

Plaid's special "Link" authorization requires a bit more manual work.
1. [Register for Plaid](https://dashboard.plaid.com/signup) and apply for a development account
1. Follow this excellent [video tutorial](https://youtu.be/sGBvKDGgPjc) to get access keys for each of the accounts you wish to include
    1. [Here's a decent written tutorial to supplement the video](https://plaid.com/docs/transactions/quickstart/#run-the-quickstart-app)


## Ideas

- Add a tutorial explaining the configuration of the `etl_config.yml`
- Integrate with a secrets manager to avoid arduous setup after crash
- Dockerize so that this can run on an arduino
- Listen for new data from sources instead of pulling on a schedule
    - [Strava](https://developers.strava.com/docs/webhookexample/)

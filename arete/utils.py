import base64
import collections.abc
import datetime
import json
import logging
from python_fitbit.gather_keys_oauth2 import OAuth2Server
import os
import requests
from stravaio import strava_oauth2
import time
import yaml

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)


# TODO - This should be a class
class CredsFile:
    def __init__(self, path):
        self.path = path

def lookup_yaml(path: str) -> dict:
    """ "
    Returns a dictionary containing the credentials relevant for a particular
    API, generally including a  client id and secret
    """
    with open(path, "r") as stream:
        return yaml.safe_load(stream)


def write_yaml(path: str, entries: dict):
    with open(path, "w") as stream:
        # Careful, this rewrites the entire yaml file
        yaml.safe_dump(entries, stream)


def deep_update(d, u):
    # https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def update_yaml(path: str, new_entry: dict):
    entries = lookup_yaml(path)
    updated_entries = deep_update(entries, new_entry)
    write_yaml(path, updated_entries)


def yaml_lookup(path: str, key: str) -> dict:
    "For backwards compatability"
    print(path + '/' + key)
    return lookup_yaml(path + '/' + key)


class Creds:
    def __init__(self, creds_path, creds_key):
        """ TODO
        - What attributes actually need to be exposed for external use?
        - If the `access_token` variable hangs around for days, it may not be fresh.
          Write a function `get_access_token` which checks for freshness before providing access to the variable.
        """
        creds = lookup_yaml(creds_path)[creds_key]
        self.creds_key = creds_key
        self.client_id = creds["client_id"]
        self.client_secret = creds["client_secret"]
        self.access_token = creds["access_token"]
        self.refresh_token = creds["refresh_token"]
        self.expires_at = creds["expires_at"]
        self.endpoint = creds["endpoint"]
        if self.access_token_expired():
            response = self.refresh_tokens()
            self.access_token = response["access_token"]
            self.refresh_token = response["refresh_token"]
            self.expires_at = time.time() + response["expires_in"]
            new_creds = {
                creds_key: {
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "expires_at": self.expires_at,
                }
            }
            update_yaml(creds_path, new_creds)

    def access_token_expired(self):
        return datetime.datetime.today().timestamp() > self.expires_at

    def refresh_tokens(self):
        logging.info("Using refresh token to procure fresh access token")
        secret = str(self.client_id) + ":" + self.client_secret
        header = {"Authorization": "Basic " + self.encode_secret(secret)}
        response = requests.post(
            url=self.endpoint,
            headers=header,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
        )
        return response.json()

    def refresh_expired_tokens_with_oauth2(self):
        # TODO - Can this be generalized? Should this be removed if I never use it?
        logging.info("Attempting to refresh access token through oauth2")
        if self.creds_key == "strava":
            output = strava_oauth2(self.client_id, self.client_secret)
        elif self.creds_key == "fitbit":
            server = OAuth2Server(
                client_id=self.client_id, client_secret=self.client_secret
            )
            server.browser_authorize()
            output = server.fitbit.client.session.token
        self.access_token = output["access_token"]
        self.refresh_token = output["refresh_token"]
        self.expires_at = output["expires_at"]

    @staticmethod
    def encode_secret(secret):
        """Standard protocol for passing bytes over a network to avoid misinterpretation of data
        b64 explanation:
        https://stackoverflow.com/questions/201479/what-is-base-64-encoding-used-for
        Fibit request documentation:
        https://dev.fitbit.com/build/reference/web-api/authorization/client-credentials/
        """
        bytes_secret = secret.encode()
        encoded_bytes_secret = base64.b64encode(bytes_secret)
        encoded_string_secret = encoded_bytes_secret.decode()
        return encoded_string_secret


class RelativeDate:
    def __init__(self, day_of_week: str, weeks_ago: int):
        self.__today = datetime.date.today()
        self.date = self._date_of_weekday(day_of_week, weeks_ago)

    def long_format(self):
        # Returns date as string like "January 4, 1989"
        return self.date.strftime(format="%B %-d, %Y")

    def short_format(self):
        # Returns date as string like "1989-01-04"
        return self.date.strftime(format="%Y-%m-%d")

    def _date_of_weekday(self, day_of_week: str, weeks_ago: int):
        days_since_last_day_of_week = (
            self._days_since_this_monday()
            + weeks_ago * 7
            - self._weekday_number(day_of_week)
        )
        return self.__today - datetime.timedelta(days=days_since_last_day_of_week)

    def _days_since_this_monday(self):
        # `weekday` method returns 0 for mon, 1 for tue, etc.
        return self.__today.weekday()

    @staticmethod
    def _weekday_number(day_of_week: str):
        return time.strptime(day_of_week, "%A").tm_wday


if __name__ == "__main__":
    # # YAML tests
    # path, key = "test.yml", {"foo": {"bar": "asdf"}}
    # write_yaml(path, key)
    # print(lookup_yaml(path))
    # update = {"foo": {"baz": "qwerty"}}
    # update_yaml(path, update)
    # print(lookup_yaml(path))
    # os.remove(path)

    # Creds tests
    # print("CREDS TESTS")
    # CREDS_KEY = "strava"
    # CREDS_PATH = "creds.yml"
    # creds = Creds(CREDS_PATH, CREDS_KEY)
    # CREDS_KEY = "fitbit"
    # CREDS_PATH = "creds.yml"
    # creds = Creds(CREDS_PATH, CREDS_KEY)

    # Date tests
    date = RelativeDate()
    print("This Monday      : ", date.this_monday)
    print("Last Monday      : ", date.last_monday)
    print("Last Monday Short: ", date.last_monday.short_date_format)
    print("Last Monday Long : ", date.last_monday.long_date_format)
    print("Last Sunday      : ", date.last_sunday)

import collections.abc
import datetime
import logging
from python_fitbit.gather_keys_oauth2 import OAuth2Server
import os
from stravaio import strava_oauth2
import time
import yaml


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)


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
    return lookup_yaml(path, key)


class Creds:
    def __init__(self, creds_path, creds_key):
        creds = lookup_yaml(creds_path)[creds_key]
        self.creds_key = creds_key
        self.client_id = creds["client_id"]
        self.client_secret = creds["client_secret"]
        self.access_token = creds["access_token"]
        self.refresh_token = creds["refresh_token"]
        self.expires_at = creds["expires_at"]
        if self.access_token_not_expired():
            self.refresh_tokens()
            new_creds = {
                creds_key: {
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "expires_at": self.expires_at,
                }
            }
            update_yaml(creds_path, new_creds)

    def access_token_not_expired(self):
        return datetime.datetime.today().timestamp() > self.expires_at

    def refresh_tokens(self):
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


# TODO create date class with different intuitive representations 
# short, long, weekday, etc
class RelativeDate:
    def __init__(self):
        self.today = datetime.date.today()
        self.this_monday = self.date_of_weekday("Monday", weeks_ago=0)
        self.this_monday_short = self.short_date(self.this_monday)
        self.last_sunday = self.date_of_weekday("Sunday", weeks_ago=1)
        self.last_sunday_long = self.long_date(self.last_sunday)
        self.last_monday = self.date_of_weekday("Monday", weeks_ago=1)
        self.last_monday_short = self.short_date(self.last_monday)
        self.last_monday_long = self.long_date(self.last_monday)

    def date_of_weekday(self, day_of_week:str, weeks_ago:int):
        days_since_last_day_of_week = self.days_since_this_monday() + weeks_ago * 7 - self.weekday_number(day_of_week)
        return self.today - datetime.timedelta(days=days_since_last_day_of_week)

    def days_since_this_monday(self):
        # `weekday` method returns 0 for mon, 1 for tue, etc.
        return self.today.weekday()

    # TODO - Should these be broken out into seperate class?
    @staticmethod
    def weekday_number(day_of_week:str):
        return time.strptime(day_of_week, "%A").tm_wday

    # TODO - This should probably be removed since it isn't used by the class
    @staticmethod
    def date_from_string(date_as_string:str):
        return datetime.datetime.strptime(date_as_string, "%Y-%m-%d").date()

    @staticmethod
    def long_date(timestamp):
        # Returns date as string like "January 4, 1989"
        return timestamp.strftime("%B %d, %Y")

    @staticmethod
    def short_date(timestamp):
        # Returns date as string like "1989-01-04"
        return timestamp.strftime("%Y-%m-%d")


if __name__ == "__main__":
    # YAML tests
    path, key = "test.yml", {"foo": {"bar": "asdf"}}
    write_yaml(path, key)
    print(lookup_yaml(path))
    update = {"foo": {"baz": "qwerty"}}
    update_yaml(path, update)
    print(lookup_yaml(path))
    os.remove(path)

    # Date tests
    relative_date = RelativeDate()
    print("This Monday      : ", relative_date.this_monday)
    print("Last Monday      : ", relative_date.last_monday)
    print("Last Monday Short: ", relative_date.last_monday_short)
    print("Last Monday Long : ", relative_date.last_monday_long)
    print("Last Sunday      : ", relative_date.last_sunday)

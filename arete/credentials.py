from werkzeug.serving import make_server
from threading import Thread
import base64
import datetime
import collections
from flask import Flask, request
import logging
from requests_oauthlib import OAuth2Session
import requests
from threading import Timer
import time
import webbrowser
import yaml


class FlaskServer:
    def __init__(self, oauth2_server, host="localhost", port=8000):
        self.oauth2_server = oauth2_server
        self.app = Flask(__name__)
        self.srv = make_server(host, port, self.app)
        self.ctx = self.app.app_context()
        self.ctx.push()
        self.setup_routes()

    def setup_routes(self):
        @self.app.route("/callback")
        def callback():
            authorization_code = request.args.get("code")
            response = self.oauth2_server.fetch_token(authorization_code)
            self.oauth2_server.save_token(response)

            shutdown_thread = Thread(target=self.srv.shutdown)
            shutdown_thread.start()
            return "Success!"

    def run(self, authorization_url):
        Timer(1, lambda: webbrowser.open(authorization_url)).start()
        self.srv.serve_forever()


class OAuth2Server:
    def __init__(
        self,
        client_id,
        client_secret,
        authorization_base_url,
        token_url,
        scope,
        redirect_uri="http://localhost:8000/callback",
    ):
        self.server = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
        self.client_id = client_id
        self.client_secret = client_secret
        self.authorization_base_url = authorization_base_url
        self.token_url = token_url
        self.flask_server = FlaskServer(self)
        self.response = None

    def fetch_token(self, authorization_code):
        return self.server.fetch_token(
            self.token_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
            code=authorization_code,
            include_client_id=True,
        )

    def save_token(self, response):
        self.response = response

    def run(self):
        authorization_url, _ = self.server.authorization_url(
            self.authorization_base_url
        )
        logging.info(f"Please go to {authorization_url} and authorize access.")
        self.flask_server.run(authorization_url)


class CredsFile:
    def __init__(self, path):
        self.path = path

    def read_file(self) -> dict:
        with open(self.path, "r") as stream:
            return yaml.safe_load(stream)

    def get(self, key) -> dict:
        return self.read_file()[key]

    def write(self, entries):
        with open(self.path, "w") as stream:
            # Careful, this rewrites the entire yaml file
            yaml.safe_dump(entries, stream)

    def update(self, new_entry):
        entries = self.read_file()
        updated_entries = self.deep_update(entries, new_entry)
        self.write(updated_entries)
        entries = self.read_file()

    def deep_update(self, d, u):
        # https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
        for k, v in u.items():
            if isinstance(v, collections.abc.Mapping):
                d[k] = self.deep_update(d.get(k, {}), v)
            else:
                d[k] = v
        return d


class AccessTokenManager:
    def __init__(self, creds_file):
        self.creds_file = creds_file
        self.creds = None

    def refresh(self, creds_key):
        self.load_creds(creds_key)
        if "access_token" not in self.creds:
            response = self.refresh_with_oauth2()
            self.update_token(response, creds_key)
        elif (
            "expires_at" in self.creds
            and self.token_expired()
            and "refresh_token" in self.creds
        ):
            response = self.refresh_with_refresh_token()
            self.update_token(response, creds_key)

    def token_expired(self):
        return datetime.datetime.today().timestamp() > self.creds["expires_at"]

    def refresh_with_oauth2(self):
        logging.info("Fetching access token using OAuth2")
        server = OAuth2Server(
            self.creds["client_id"],
            self.creds["client_secret"],
            self.creds["authorization_uri"],
            self.creds["token_uri"],
            self.creds.get("scope"),  # Splitwise doesn't API access
        )
        server.run()
        return server.response

    def refresh_with_refresh_token(self):
        logging.info("Using refresh token to fetch fresh access token")
        secret = str(self.creds["client_id"]) + ":" + self.creds["client_secret"]
        header = {"Authorization": "Basic " + self.encode_secret(secret)}
        data = {
            "client_id": self.creds["client_id"],
            "client_secret": self.creds["client_secret"],
            "refresh_token": self.creds["refresh_token"],
            "grant_type": "refresh_token",
        }
        response = requests.post(
            url=self.creds["token_uri"],
            headers=header,
            data=data,
        )
        return response.json()

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

    def update_token(self, response, creds_key):
        if "expires_in" in response:
            response["expires_at"] = time.time() + response["expires_in"]
        keys_to_update = ["access_token", "refresh_token", "expires_at"]
        new_creds = {
            creds_key: {k: response[k] for k in keys_to_update if k in response}
        }
        self.creds_file.update(new_creds)

    def load_creds(self, creds_key):
        self.creds = self.creds_file.get(creds_key)


if __name__ == "__main__":
    # Creds tests
    print("STRAVA")
    CREDS_KEY = "strava"
    CREDS_PATH = "creds.yml"
    creds_file = CredsFile(CREDS_PATH)
    creds = AccessTokenManager(creds_file)
    creds.refresh(CREDS_KEY)

    print("FITBIT")
    CREDS_KEY = "fitbit"
    CREDS_PATH = "creds.yml"
    creds = AccessTokenManager(creds_file)
    creds.refresh(CREDS_KEY)

    print("SPLITWISE")
    CREDS_KEY = "splitwise"
    CREDS_PATH = "creds.yml"
    creds = AccessTokenManager(creds_file)
    creds.refresh(CREDS_KEY)

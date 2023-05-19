import collections.abc
import dateparser
import inflection
import logging
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)


def snakecase_format(string):
    return inflection.underscore(string)


def title_format(string):
    return inflection.titleize(string)


def convert_string_to_date(string):
    return dateparser.parse(string).date()


def convert_vector_to_date(vector):
    return vector.map(convert_string_to_date)
    # return pd.to_datetime(vector).dt.date.astype("datetime64[ns]")


def date_dimension_table(start, end):
    df = pd.DataFrame({"date": pd.date_range(start, end, periods=None, freq="d")})
    df["week"] = date_period_vector(df["date"], "W")
    df["month"] = date_period_vector(df["date"], "M")
    df["quarter"] = date_period_vector(df["date"], "Q")
    df["date"] = convert_vector_to_date(df["date"].astype(str))
    return df


def lookup_yaml(path: str) -> dict:
    """Returns a dictionary containing the credentials relevant for a particular API, generally including a  client id and secret"""
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


if __name__ == "__main__":
    pass
    # # YAML tests
    # path, key = "test.yml", {"foo": {"bar": "asdf"}}
    # write_yaml(path, key)
    # print(lookup_yaml(path))
    # update = {"foo": {"baz": "qwerty"}}
    # update_yaml(path, update)
    # print(lookup_yaml(path))
    # os.remove(path)

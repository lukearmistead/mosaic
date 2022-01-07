import collections.abc
import os
import yaml


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


if __name__ == "__main__":
    # Tests
    path, key = "test.yml", {"foo": {"bar": "asdf"}}
    write_yaml(path, key)
    print(lookup_yaml(path))
    update = {"foo": {"baz": "qwerty"}}
    update_yaml(path, update)
    print(lookup_yaml(path))
    os.remove(path)

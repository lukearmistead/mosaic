import yaml


def yaml_lookup(path, key) -> dict:
    """"
    Returns a dictionary containing the credentials relevant for a particular
    API, generally including a  client id and secret
    """
    with open(path, 'r') as stream:
        creds = yaml.safe_load(stream)[key]
    return creds

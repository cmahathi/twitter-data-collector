import json
import os
KEYS_JSON = 'keys.json'
SETTINGS_JSON = 'settings.json'


def load_keys():
    path = os.path.join(os.getcwd(), KEYS_JSON)
    with open(path) as f:
        return json.load(f)


def load_settings():
    path = os.path.join(os.getcwd(), SETTINGS_JSON)
    with open(path) as f:
        return json.load(f)
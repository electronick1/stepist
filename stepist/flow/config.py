import json
import pickle
from redis import Redis
from .utils import AttrDict


config = AttrDict({
    'redis': Redis(),
    'pickler': pickle
})


def setup_config(**kwargs):
    global config

    config.update(kwargs)


def get_config(key):
    global config

    return config[key]

import ujson
from .utils import AttrDict


config = AttrDict({
    'redis_kwargs': dict(host='localhost', port=6379, decode_responses=True),
    'redis_stats_kwargs': {},
    'pickler': ujson,
    'celery_app': None,
    'rq_app': None,
    'wait_timeout': 5,
})


def setup_config(**kwargs):
    global config

    config.update(kwargs)


def get_config(key):
    global config

    return config[key]

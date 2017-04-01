from .step import step, reducer_step
from .workers import worker_engine, simple_multiprocessing
from .config import setup_config


def setup(**kwargs):
    setup_config(**kwargs)


def run(*args, **kwargs):
    return worker_engine().run(*args, **kwargs)


def just_do_it(workers_count, *args, _warning=True,  **kwargs):
    if _warning:
        print("You are using python multiprocessing for workers,"
              "do NOT do it in production\n")
    simple_multiprocessing(workers_count, *args, **kwargs)

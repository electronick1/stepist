from . import simple_queue, celery_driver, rq_driver
from .jobs import register_job


JOBS = {}



_worker_engine = simple_queue


def setup_worker_engine(engine):
    global _worker_engine
    _worker_engine = engine


def worker_engine():
    global _worker_engine
    return _worker_engine


def simple_multiprocessing(workers_count, *args, **kwargs):
    from multiprocessing import Process

    for i in range(workers_count):
        Process(target=worker_engine().run, args=args, kwargs=kwargs).start()

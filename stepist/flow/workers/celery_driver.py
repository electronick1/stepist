from .jobs import JOBS

from ..config import setup_config, config

_celery_tasks = None


def setup(celery_app, redis=config.redis, pickler=config.pickler, wait_timeout=5):
    global _celery_tasks

    setup_config(celery_app=celery_app,
                 wait_timeout=wait_timeout,
                 redis=redis,
                 pickler=pickler)

    _celery_tasks = {}

    for job, job_handler in JOBS.items():
        _celery_tasks[job] = config.celery_app.task(name=job)(job_wrapper(job_handler.execute_step))


def add_job(key, data, call_config, result_reader=None):
    global _celery_tasks

    if result_reader is None:
        result_reader = ResultReader([])

    celery_result = _celery_tasks[key].apply_async(kwargs=dict(data=data,
                                                               config=call_config))
    result_reader.add_result(celery_result)

    return result_reader


def job_wrapper(job_handler):
    from ..step import CallConfig

    def _wrapp(config, *args, **kwargs):
        return job_handler(config=CallConfig.from_json(config),
                           *args, **kwargs)

    return _wrapp


def run(*args, **kwargs):
    from celery.bin import worker

    worker = worker.worker(app=config.celery_app)
    worker.run(*args, **kwargs)


# HELPERS

def has_jobs():
    global _celery_tasks

    for task in _celery_tasks:
        r = config.celery_app.result.AsyncResult(task.id)
        if r.status == "PENDING" or r.status == "STARTED":
            return True

    return False


class ResultReader(object):

    def __init__(self, celery_result):
        self.celery_result = celery_result

    def add_result(self, r):
        self.celery_result.append(r)

    def read(self):
        from celery.result import allow_join_result

        for row_result in self.celery_result:
            with allow_join_result():
                yield row_result.get(timeout=config.wait_timeout)

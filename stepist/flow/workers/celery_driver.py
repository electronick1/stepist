from .jobs import JOBS


_celery_tasks = None
_wait_timeout = None
_celery_app = None


def setup(celery_app, wait_timeout=5):
    global _wait_timeout
    global _celery_tasks
    global _celery_app

    _wait_timeout = wait_timeout
    _celery_tasks = {}
    _celery_app = celery_app

    for job, job_handler in JOBS.items():
        _celery_tasks[job] = celery_app.task(name=job)(job_wrapper(job_handler.execute_step))


def add_job(key, data_rows, last_step):
    global _celery_tasks
    global _celery_app
    global _wait_timeout

    result = []
    for row in data_rows:
        result.append(_celery_tasks[key].apply_async(kwargs=dict(data=row,
                                                                 last_step=last_step),
                                                     ))
    return ResultReader(result)


def job_wrapper(job_handler):

    def _wrapp(*args, **kwargs):
        return job_handler(*args, **kwargs)

    return _wrapp


def run(*args, **kwargs):
    from celery.bin import worker

    global _celery_app

    worker = worker.worker(app=_celery_app)
    worker.run(*args, **kwargs)


# HELPERS

def has_jobs():
    global _celery_tasks
    global _celery_app

    for task in _celery_tasks:
        r = _celery_app.result.AsyncResult(task.id)
        if r.status == "PENDING" or r.status == "STARTED":
            return True

    return False


class ResultReader(object):

    def __init__(self, celery_result):
        self.celery_result = celery_result

    def read(self):
        global _wait_timeout
        from celery.result import allow_join_result

        for row_result in self.celery_result:
            with allow_join_result():
                yield row_result.get(timeout=_wait_timeout)

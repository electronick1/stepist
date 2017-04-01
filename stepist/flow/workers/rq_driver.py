from .jobs import JOBS
from rq.decorators import job as rq_job
from functools import wraps

_rq_tasks = None
_wait_timeout = None
_rq_queue = None


def setup(rq_queue, wait_timeout=5):
    global _wait_timeout
    global _rq_tasks
    global _rq_queue

    _wait_timeout = wait_timeout
    _rq_tasks = {}
    _rq_queue = rq_queue

    for job, job_handler in JOBS.items():
        _rq_tasks[job] = rq_job(rq_queue)(job_wrapper)


def add_job(key, data_rows, last_step):
    global _rq_tasks
    global _wait_timeout

    result = []
    for row in data_rows:
        result.append(_rq_tasks[key].delay(data=row,
                                           last_step=last_step,
                                           job_key=key,
                                           ))
    return ResultReader(result)


def job_wrapper(job_key, data, last_step):
   return JOBS[job_key].execute_step(data=data,
                                     last_step=last_step)


def run(*args, **kwargs):
    from rq import Connection, Worker

    with Connection():
        w = Worker(*args, **kwargs)
        w.work()


# HELPERS

def has_jobs():
    global _rq_tasks

    for task in _rq_tasks:
        if task.is_queued or task.is_started:
            return True

    return False


class ResultReader(object):

    def __init__(self, rq_jobs):
        self.rq_jobs = rq_jobs

    def read(self):
        global _wait_timeout
        jobs_on_process = self.rq_jobs

        while jobs_on_process:
            _jobs_on_process = []

            for job in reversed(jobs_on_process):
                if job.is_failed:
                    raise RuntimeError("Job %s failed" % job)

                if job.is_finished:
                    yield job.return_value
                else:
                    _jobs_on_process.append(job)

            jobs_on_process = _jobs_on_process



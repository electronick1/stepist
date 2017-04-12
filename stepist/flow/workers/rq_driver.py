from .jobs import JOBS
from functools import wraps

from ..config import setup_config, config

_rq_tasks = None


def setup(rq_app, redis=config.redis, pickler=config.pickler, wait_timeout=5):
    from rq.decorators import job as rq_job

    global _rq_tasks

    setup_config(rq_app=rq_app,
                 redis=redis,
                 pickler=pickler,
                 wait_timeout=wait_timeout)

    _rq_tasks = {}

    for job, job_handler in JOBS.items():
        _rq_tasks[job] = rq_job(rq_app)(job_wrapper)


def add_job(key, data, call_config, result_reader=None):
    global _rq_tasks

    if result_reader is None:
        result_reader = ResultReader([])

    rq_job = _rq_tasks[key].delay(data=data,
                                  call_config=call_config,
                                  job_key=key)
    result_reader.add_job(rq_job)
    return result_reader


def job_wrapper(job_key, data, call_config):
    from ..step import CallConfig

    return JOBS[job_key].execute_step(data=data,
                                      config=CallConfig.from_json(call_config))


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

    def add_job(self, job):
        self.rq_jobs.append(job)

    def read(self):
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



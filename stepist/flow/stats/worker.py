from stepist.flow.dbs import get_redis_stats

from . import utils


WORKER_CONNECTION_NAME = "STATS::process::||{steps}"
JOB_ADDED = "STATS::job_added::{job_key}"


def starts(steps):
    steps_str = utils.concat_steps_to_str(steps)
    get_redis_stats().client_setname(WORKER_CONNECTION_NAME.format(steps=steps_str))


def job_added(job_key, data):
    get_redis_stats().incr(JOB_ADDED.format(job_key=job_key))


def jobs_scheduled(*steps):
    from stepist.flow.workers import jobs_count

    return jobs_count(*steps)

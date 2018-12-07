import sys
from stepist.flow import stats
from stepist.flow.session import get_steps_to_listen

from .worker_engine import worker_engine, setup_worker_engine
from .adapters import simple_queue

from . import utils

setup_worker_engine(simple_queue.SimpleQueueAdapter())


def add_job(step, data, **kwargs):
    worker_engine().add_job(step=step,
                            data=data,
                            **kwargs)

    # stats.worker.job_added(step.step_key(),
    #                        data)


def jobs_count(*steps):
    return worker_engine().jobs_count(*steps)


def flush_queue(step):
    worker_engine().flush_queue(step)


def process(*steps, **kwargs):
    steps = utils.validate_steps(steps)
    #stats.worker.starts(steps)
    worker_engine().process(*steps, **kwargs)


def simple_multiprocessing(workers_count, *args, **kwargs):
    from multiprocessing import Process

    steps = get_steps_to_listen().values()

    process_list = []
    for i in range(workers_count):
        p = Process(target=process, args=steps, kwargs=kwargs)
        p.start()
        process_list.append(p)

    return process_list



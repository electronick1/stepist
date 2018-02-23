from stepist.flow import stats

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


def process(*steps):

    steps = utils.validate_steps(steps)
    stats.worker.starts(steps)

    worker_engine().process(*steps)


def simple_multiprocessing(workers_count, *args, **kwargs):
    from multiprocessing import Process

    for i in range(workers_count):
        Process(target=worker_engine().run, args=args, kwargs=kwargs).start()



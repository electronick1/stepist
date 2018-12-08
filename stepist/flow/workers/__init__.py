from .adapters import simple_queue
from . import utils


def process(app, *steps, **kwargs):
    steps = utils.validate_steps(steps)
    app.worker_engine.process(*steps, **kwargs)


def simple_multiprocessing(app, workers_count, steps, *args, **kwargs):
    from multiprocessing import Process

    process_list = []
    for i in range(workers_count):
        p = Process(target=process, args=[app, *steps], kwargs=kwargs)
        p.start()
        process_list.append(p)

    return process_list



import random
import traceback
from .adapters import simple_queue
from . import utils


class QueueProcessor:
    def __init__(self, app, steps, worker_engine=None):
        self.app = app
        self.steps = steps
        self.worker_engine = worker_engine or app.worker_engine

    def process_job(self,):
        return self.worker_engine.process_job()


class BoostProcessor:
    def __init__(self, app, steps, booster=None):
        self.app = app
        self.steps = steps
        self.booster = booster or app.worker_booster

    def process_job(self,):
        return self.booster.process_job()


def processor_loop(*processors, die_when_empty=False, die_on_error=True):
    while True:
        at_least_one_has_job = False
        for processor in processors:
            try:
                has_job = processor.process_job()
            except Exception:
                if die_on_error:
                    raise
                else:
                    traceback.print_exc()

            if not has_job:
                #TODO: decrease chance to be executed next
                pass
            else:
                #TODO: back chances

            at_least_one_has_job = at_least_one_has_job or has_job

        if die_when_empty and not at_least_one_has_job:
            print("Queue is empty")
            exit()






def simple_multiprocessing(app, workers_count, steps, *args, **kwargs):
    from multiprocessing import Process

    process_list = []
    for i in range(workers_count):
        p = Process(target=process, args=[app, *steps], kwargs=kwargs)
        p.start()
        process_list.append(p)

    return process_list



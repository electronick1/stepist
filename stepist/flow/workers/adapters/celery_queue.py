import celery
from celery.bin import worker

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class CeleryAdapter(BaseWorkerEngine):

    def __init__(self, **celery_options):
        self.tasks = dict()
        self.app = celery.Celery(**celery_options)

    def add_job(self, step, data, result_reader=None, **kwargs):
        task = self.tasks.get(step.step_key())
        if task is None:
            raise RuntimeError("task not found")

        if result_reader:
            return task.apply_async(kwargs=data, countdown=3)

        task.delay(**data)

    def process(self, *steps, die_when_empty=False):
        steps_keys = [step.step_key() for step in steps]

        self.app.start(argv=['celery',
                             'worker',
                             '-l',
                             'info',
                             '-Q',
                             ','.join(steps_keys)])

    def flush_queue(self, step):
        self.app.control.purge()

    def jobs_count(self, *steps):
        # TODO:
        return 0

    def register_worker(self, step):
        self.app.conf.task_routes[step.step_key()] = dict(queue=step.step_key())
        self.tasks[step.step_key] = self.app.task(step, name=step.step_key())


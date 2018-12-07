import celery
from celery.bin import worker

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class CeleryAdapter(BaseWorkerEngine):

    def __init__(self, app, celery_app=None, **celery_options):
        self.tasks = dict()
        self.app = app
        self.celery_app = celery_app

        if celery_app is None:
            self.celery_app = celery.Celery(**celery_options)

    def add_job(self, step, data, result_reader=None, **kwargs):
        self.register_worker(step)
        task = self.tasks.get(step.step_key(), None)
        if task is None:
            raise RuntimeError("task not found")

        if result_reader:
            return task.apply_async(kwargs=data, countdown=3)
        task.apply_async(kwargs=data.get_dict())

    def process(self, *steps, die_when_empty=False):
        steps_keys = [step.step_key() for step in steps]

        self.celery_app.start(argv=['celery',
                                     'worker',
                                     '-l',
                                     'info',
                                     '-Q',
                                     ','.join(steps_keys)])

    def flush_queue(self, step):
        self.celery_app.control.purge()

    def jobs_count(self, *steps):
        return self.celery_app.control.inspect()

    def register_worker(self, step):
        if step.step_key() in self.tasks:
            return

        self.celery_app.conf.task_routes = \
            {step.step_key(): dict(queue=step.step_key())}

        self.tasks[step.step_key()] = \
            self.celery_app.task(name=step.step_key(),
                                 typing=False)(step.receive_job)


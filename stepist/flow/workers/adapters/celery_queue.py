import celery
from kombu import Exchange, Queue

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

        result = self.celery_app.send_task(step.step_key(),
                                           kwargs=data.get_dict(),
                                           queue=step.step_key())
        if result_reader:
            result_reader.set(result)
            result_reader.read = result.collect

    def process(self, *steps, die_when_empty=False):
        steps_keys = [step.step_key() for step in steps]

        for step in steps:
            self.register_worker(step)

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

        Queue(name=step.step_key(),
              exchange=Exchange('stepist'),
              routing_key='stepist.%s' % step.step_key()),

        if self.celery_app.conf.task_routes is None:
            self.celery_app.conf.task_routes = dict()

        self.celery_app.conf.task_routes[step.step_key()] = dict(queue=step.step_key())

        self.tasks[step.step_key()] = \
            self.celery_app.task(name=step.step_key(),
                                 typing=False)(step.receive_job)


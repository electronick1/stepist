from stepist.flow.libs.simple_queue import SimpleQueue

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class SimpleQueueAdapter(BaseWorkerEngine):
    def __init__(self, app, redis_connection):
        self.app = app
        self.redis_connection = redis_connection

        self.queue = SimpleQueue(self.app.config.pickler,
                                 self.redis_connection)

    def add_job(self, step, data, **kwargs):
        self.queue.add_job(step.step_key(), data)

    def process(self, *steps, die_when_empty=False):
        self.queue.process({step.step_key(): step for step in steps},
                           die_when_empty=die_when_empty)

    def flush_queue(self, step):
        self.queue.redis_db.delete(step.step_key())

    def jobs_count(self, *steps):
        sum_by_steps = 0
        for step in steps:
            q_key = self.queue.redis_queue_key(step.step_key())
            sum_by_steps += self.queue.redis_db.llen(q_key)

        return sum_by_steps

    def register_worker(self, handler):
        pass

from stepist.flow.libs import simple_queue as queue

from stepist.flow.workers.worker_engine import BaseWorkerEngine


class SimpleQueueAdapter(BaseWorkerEngine):

    def add_job(self, step, data, result_reader=None, **kwargs):
        queue.add_job(step.step_key(), data, result_reader=None)

    def process(self, *steps):
        queue.process({step.step_key(): step for step in steps})

    def flush_queue(self, step):
        for r_db in queue.redis_dbs:
            r_db.delete(step.step_key())

    def jobs_count(self, *steps):
        sum_by_steps = 0
        for step in steps:
            for r_db in queue.redis_dbs:
                sum_by_steps += r_db.llen(queue.redis_queue_key(step.step_key()))

        return sum_by_steps

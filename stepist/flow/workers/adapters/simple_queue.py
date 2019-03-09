from stepist.flow.libs.simple_queue import SimpleQueue

from stepist.flow.workers.worker_engine import BaseWorkerEngine

from stepist.flow.workers.adapters import utils


class SimpleQueueAdapter(BaseWorkerEngine):
    def __init__(self, app, redis_connection):
        self.app = app
        self.redis_connection = redis_connection

        self.queue = SimpleQueue(self.app.config.pickler,
                                 self.redis_connection)

    def add_job(self, step, data, **kwargs):
        self.queue.add_job(step.step_key(), data.get_dict())

    def add_jobs(self, step, jobs_data, **kwargs):
        jobs_data_dict = [data.get_dict() for data in jobs_data]
        self.queue.add_jobs(step.step_key(), jobs_data_dict)

    def receive_job(self, step):
        return self.queue.reserve_job(step.step_key())

    def process(self, *steps, die_when_empty=False, die_on_error=True):
        self.queue.process({step.step_key(): step for step in steps},
                           die_when_empty=die_when_empty,
                           die_on_error=die_on_error,
                           verbose=self.app.verbose)

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

    def monitor_steps(self, step_keys, monitoring_for_sec):
        push = dict()
        pop = dict()

        pool = self.redis_connection.connection_pool
        monitor = utils.RedisMonitor(pool)
        commands = monitor.monitor(monitoring_for_sec)

        for command in commands:
            command = command.lower()

            for step_key in step_keys:
                key = self.queue.redis_queue_key(step_key).lower()
                if key in command and 'lpush' in command:
                    push[step_key] = push.get(step_key, 0) + 1
                if key in command and 'lpop' in command:
                    pop[step_key] = pop.get(step_key, 0) + 1

        return push, pop




import ujson
from stepist.flow.libs.simple_queue import SimpleQueue

from stepist.flow.workers.worker_engine import BaseWorkerEngine

from stepist.flow.workers.adapters import utils


class SimpleQueueAdapter(BaseWorkerEngine):
    def __init__(self, redis_connection, pickler=ujson, verbose=True):
        self.redis_connection = redis_connection
        self.verbose = verbose
        self.queue = SimpleQueue(pickler,
                                 self.redis_connection)

    def add_job(self, step, data, **kwargs):
        self.queue.add_job(self.get_queue_name(step), data.get_dict())

    def add_jobs(self, step, jobs_data, **kwargs):
        jobs_data_dict = [data.get_dict() for data in jobs_data]
        self.queue.add_jobs(self.get_queue_name(step), jobs_data_dict)

    def receive_job(self, step):
        return self.queue.reserve_job(self.get_queue_name(step))

    def process(self, *steps, die_when_empty=False, die_on_error=True):
        self.queue.process({self.get_queue_name(step): step for step in steps},
                           die_when_empty=die_when_empty,
                           die_on_error=die_on_error,
                           verbose=self.verbose)

    def flush_queue(self, step):
        queue_name = self.get_queue_name(step)
        self.queue.flush_jobs(queue_name)

    def jobs_count(self, *steps):
        sum_by_steps = 0
        for step in steps:
            q_key = step.get_queue_name()
            sum_by_steps += self.queue.redis_db.llen(q_key)

        return sum_by_steps

    def register_worker(self, handler):
        pass

    def monitor_steps(self, steps, monitoring_for_sec):
        push = dict()
        pop = dict()

        pool = self.redis_connection.connection_pool
        monitor = utils.RedisMonitor(pool)
        commands = monitor.monitor(monitoring_for_sec)

        for command in commands:
            command = command.lower()

            for step in steps:
                key = step.get_queue_name()
                step_key = step.step_key()
                if key in command and 'lpush' in command:
                    push[step_key] = push.get(step_key, 0) + 1
                if key in command and 'lpop' in command:
                    pop[step_key] = pop.get(step_key, 0) + 1

        return push, pop

    @staticmethod
    def get_queue_name(step):
        return "stepist::%s" % step.step_key()




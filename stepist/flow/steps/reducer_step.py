

class ReducerStep(object):
    def __init__(self, handler):
        self.handler = handler

    @property
    def __name__(self):
        return self.handler.__name__

    def __call__(self):
        """
        Execute step with default, configuration values
        """
        self.handler(self.get_workers_result())

    def add_data(self, data):
        config.redis.lpush(self.step_key(),
                           config.pickler.dumps(data))

    def get_workers_result(self):
        while worker_engine().jobs_count() or config.redis.llen(self.step_key()):
            yield config.pickler.loads(config.redis.blpop(self.step_key())[1])

    def step_key(self):
        return "reducer_step::%s" % self.__name__

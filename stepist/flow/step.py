from . import utils, workers, signals

from .workers import worker_engine

from .global_objects import STEPS
from .config import config


def step(next_step, as_worker=False, wait_result=False):
    def _wrapper(handler):
        step = Step(handler,
                    next_step,
                    as_worker=as_worker,
                    wait_result=wait_result)

        if step.step_key() in STEPS:
            raise(RuntimeError("duplicate step key"))
        STEPS[step.step_key()] = step

        return step

    return _wrapper


def reducer_step():
    def _wrapper(handler):
        step = ReducerStep(handler)

        if step.step_key() in STEPS:
            raise(RuntimeError("duplicate step key"))
        STEPS[step.step_key()] = step

        return step

    return _wrapper


def factory_step(next_step, as_worker=False, wait_result=False):
    def _wrapper(handler):
        step = FactoryStep(handler,
                           next_step,
                           as_worker=as_worker,
                           wait_result=wait_result)

        STEPS[step.step_key()] = step

        return step

    return _wrapper


class Step(object):

    def __init__(self, handler, next_step, as_worker, wait_result):
        self.handler = handler
        self.next_step = next_step
        self.as_worker = as_worker
        self.wait_result = wait_result

        self._last_step = None

        if self.as_worker:
            self.register_step_as_worker()

    @property
    def __name__(self):
        return self.handler.__name__

    def __call__(self, **kwargs):
        """
        Execute step with default, configuration values
        """
        return self.execute_step(data=kwargs,
                                 last_step=None,
                                 reducer_step=None)

    def execute_step(self, data, last_step, reducer_step):
        """
        Execute step with custom configuration
        :param data: next step data
        :param last_step: Step object or step_key value
        :return: Flow result
        """

        result_data = None

        try:
            signals.before_step.send(data=data,
                                     handler=self.handler,
                                     next_step=self.next_step)

            result_data = self.handler(**data)

        except utils.StopFlowFlag:
            return self.step_result(result_data,
                                    reducer_step)

        finally:
            signals.after_step.send(data=data,
                                    result_data=result_data,
                                    handler=self.handler,
                                    next_step=self.next_step)

        if self.is_last_step(last_step):
            signals.flow_finished.send(data=data,
                                       result_data=result_data,
                                       handler=self.handler,
                                       next_step=self.next_step)

            return self.step_result(result_data,
                                    reducer_step)

        return self.step_result(self.init_next_step(result_data,
                                                    last_step=last_step,
                                                    reducer_step=reducer_step),
                                reducer_step)

    def step_result(self, result_data, reducer_step):
        if reducer_step:
            reducer_step()
        else:
            return result_data

    def init_next_step(self, data, last_step=None, next_step=None, reducer_step=None):
        if next_step is None:
            next_step = self.next_step

        if isinstance(next_step, ReducerStep):
            next_step.add_data(data)
            return None

        if self.next_step.as_worker:
            reader = worker_engine().add_job(next_step.step_key(),
                                             data_rows=[data],
                                             last_step=self.last_step_key(last_step))
            if self.next_step.wait_result:
                return list(reader.read())[0]

            return None
        else:
            return next_step.execute_step(data=data,
                                          last_step=last_step)

    def is_last_step(self, last_step):
        if self.next_step is None:
            return True

        if not last_step:
            return False

        if self.next_step:
            return last_step == self.next_step.step_key() or last_step == self.next_step
        else:
            return True

    def step_key(self):
        return "%s" % self.handler.__name__

    def register_step_as_worker(self):
        workers.register_job(self.step_key(),
                             self)

    @property
    def config(self):
        return StepExecutionConfig(self)

    @staticmethod
    def last_step_key(last_step):
        if last_step is None:
            return None

        if isinstance(last_step, str):
            return last_step
        else:
            last_step.step_key()


class FactoryStep(Step):

    def init_next_step(self, data, last_step=None, next_step=None, reducer_step=None):

        # check if data iterable, else throw TypeError
        iter(data)

        if next_step is None:
            next_step = self.next_step

        if isinstance(next_step, ReducerStep):
            for row in data:
                next_step.add_data(row)
            return None

        if self.next_step.as_worker:
            reader = worker_engine().add_job(next_step.step_key(),
                                             data_rows=data,
                                             last_step=self.last_step_key(last_step))
            if self.next_step.wait_result:
                return reader.read()

            return None
        else:
            for row in data:
                yield next_step.execute_step(data=row,
                                             last_step=last_step)


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
        while worker_engine().has_jobs() or config.redis.llen(self.step_key()):
            yield config.pickler.loads(config.redis.blpop(self.step_key())[1])

    def step_key(self):
        return "reducer_step::%s" % self.__name__


class StepExecutionConfig(object):

    def __init__(self, step):
        self.step = step

    def __call__(self, last_step=None, reducer_step=None):
        return StepExecutionHelper(self.step,
                                   last_step=last_step,
                                   reducer_step=reducer_step)


class StepExecutionHelper(object):
    def __init__(self, step, **call_kwargs):
        self.step = step
        self.call_kwargs = call_kwargs

    def execute(self, **data):
        return self.step.execute_step(data=data,
                                      **self.call_kwargs)

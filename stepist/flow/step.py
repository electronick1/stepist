import inspect

from . import utils, workers, signals

from .workers import worker_engine

from .global_objects import STEPS
from .config import config


def step(next_step, as_worker=False, wait_result=False, next_flow=None):
    def _wrapper(handler):
        step = Step(handler,
                    next_step,
                    as_worker=as_worker,
                    wait_result=wait_result,
                    next_flow=next_flow)

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


def factory_step(next_step, as_worker=False):
    def _wrapper(handler):
        step = Step(handler,
                    next_step,
                    as_worker=as_worker,
                    wait_result=False,
                    next_flow=None)

        step.set_factory(FactoryStep(step))

        STEPS[step.step_key()] = step

        return step

    return _wrapper


class CallConfig(object):
    def __init__(self, last_step, reducer_step):
        self.last_step = last_step
        self.reducer_step = reducer_step

    def json(self):
        last_step = self.last_step
        reducer_step = self.reducer_step

        if last_step and not isinstance(last_step, str):
            last_step = last_step.step_key()

        if reducer_step and not isinstance(reducer_step, str):
            reducer_step = reducer_step.step_key()

        return {
            'last_step': last_step,
            'reducer_step': reducer_step,
        }

    @classmethod
    def from_json(cls, json_data):
        return cls(**json_data)


class Hub(object):
    def __init__(self, *steps):
        self.steps = steps


class FactoryStep(object):
    def __init__(self, step):
        self.step = step

        self._config = None
        self.result_reader = None

    def set_config(self, config):
        self._config = config

    def get_config(self):
        return self._config

    def add_data_iter(self, data_iter):
        for data in data_iter:
            self.add_item(data)

    def add_item(self, data):
        if self.step.as_worker:
            self.result_reader = worker_engine().add_job(self.step.step_key(),
                                                         data=data,
                                                         call_config=self.get_config().json(),
                                                         result_reader=self.result_reader)
        else:
            self.step.execute_step(data=data,
                                   config=self.get_config())

    def result(self):
        return self.result_reader.read()


class Step(object):

    def __init__(self, handler, next_step, as_worker, wait_result, next_flow):
        self.handler = handler
        self.next_step = next_step
        self.as_worker = as_worker
        self.wait_result = wait_result
        self.next_flow = next_flow

        self.factory = None

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
                                 config=CallConfig(None, None))

    def execute_step(self, data, config):
        """
        Execute step with custom configuration
        :param data: next step data
        :param last_step: Step object or step_key value
        :return: Flow result
        """

        result_data = None

        if 'next_flow' in data:
            raise RuntimeError("You can't use 'next_flow' var in data")
        if 'self_step' in data:
            raise RuntimeError("You can't use 'self_step' var in data")

        reducer_step = config.reducer_step
        config.reducer_step = None

        try:
            signals.before_step.send(data=data,
                                     handler=self.handler,
                                     next_step=self.next_step)

            if self.next_flow:
                if self.next_flow.factory:
                    factory = self.next_flow.factory
                    factory.set_config(config)
                    data['next_flow'] = factory
                else:
                    data['next_flow'] = self.next_step.config(config=config)

            result_data = self.handler(**data)

        except utils.StopFlowFlag:
            return result_data

        finally:
            signals.after_step.send(data=data,
                                    result_data=result_data,
                                    handler=self.handler,
                                    next_step=self.next_step)

        if self.is_last_step(config.last_step):
            signals.flow_finished.send(data=data,
                                       result_data=result_data,
                                       handler=self.handler,
                                       next_step=self.next_step)

            return result_data

        if self.next_step and isinstance(self.next_step, Hub):
            for next_step_item in self.next_step.steps:
                self.init_next_step(result_data,
                                    config=config,
                                    next_step=next_step_item)
            return None

        else:
            next_step_data = self.init_next_step(result_data,
                                                 config=config)

        if reducer_step:
            return reducer_step()
        else:
            return next_step_data

    def init_next_step(self, data, config, next_step=None):
        if next_step is None:
            next_step = self.next_step

        if isinstance(next_step, ReducerStep):
            next_step.add_data(data)
            return None

        if next_step.factory:
            next_step.factory.set_config(config)
            next_step.factory.add_data_iter(data)
            return None

        if next_step.as_worker:
            reader = worker_engine().add_job(next_step.step_key(),
                                             data=data,
                                             call_config=config.json())
            if next_step.wait_result:
                return list(reader.read())[0]

            return None
        else:
            return next_step.execute_step(data=data,
                                          config=config)

    def set_factory(self, factory):
        self.factory = factory

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

    def __call__(self, last_step=None,
                       reducer_step=None,
                       config=None):

        if config is None:
            config = CallConfig(last_step, reducer_step)

        return StepExecutionHelper(self.step,
                                   config=config)


class StepExecutionHelper(object):
    def __init__(self, step, config):
        self.step = step
        self.config = config

    def execute(self, **data):
        return self.step.execute_step(data=data,
                                      config=self.config)

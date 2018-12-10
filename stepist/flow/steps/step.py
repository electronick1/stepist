import types
import time
import ujson
from stepist.flow import utils, session

from .next_step import call_next_step


class StepData(object):

    flow_data = None
    meta_data = None

    def __init__(self, flow_data, meta_data=None):
        self.flow_data = flow_data
        self.meta_data = meta_data

    def get_dict(self):
        return {
            'flow_data': self.flow_data,
            'meta_data': self.meta_data
        }

    def __json__(self):
        return ujson.dumps(self.get_dict())


class Step(object):
    """
    Step object.
    """

    # handler function which handle data
    handler = None

    # next step object which getting current handler result
    next_step = None

    # True, if we need to run current handler in distribute way (using queues)
    as_worker = None

    # True, if we need to wait result from current handler
    # (used in previous step)
    wait_result = None

    # Factor object for iterator handling
    factory = None

    def __init__(self, app, handler, next_step, as_worker, wait_result,
                 unique_id=None):
        self.app = app
        self.handler = handler
        self.next_step = next_step
        self.as_worker = as_worker
        self.wait_result = wait_result
        self.unique_id = unique_id

        self.factory = None

    @property
    def __name__(self):
        return self.unique_id or self.handler.__name__

    def __call__(self, **kwargs):
        """
        """
        try:
            result_data = self.execute_step(**kwargs)
        except utils.StopFlowFlag:
            return None

        if self.is_last_step():
            return result_data

        # if isinstance(result_data, types.GeneratorType):
        #     for row_data in result_data:
        #         call_next_step(row_data, next_step=self.next_step)
        #     return None

        return call_next_step(result_data,
                              next_step=self.next_step)

    def execute_step(self, **data):
        """
        :param data: next step data
        :param last_step: Step object or step_key value
        :return: Flow result
        """

        # if 'self_step' in data:
        #     raise RuntimeError("You can't use 'self_step' var in data")
        handler_data = utils.validate_handler_data(self.handler, data)
        result_data = self.handler(**handler_data)
        session.set_flow_data(result_data)

        return result_data

    def add_job(self, data, **kwargs):
        step_data = StepData(flow_data=data,
                             meta_data=session.get_meta_data())

        return self.app.worker_engine.add_job(step=self,
                                              data=step_data,
                                              **kwargs)

    def receive_job(self, **data):
        if "flow_data" not in data:
            raise RuntimeError("flow_data not found in job payload")

        with session.change_flow_ctx(data.get('meta_data', {}), data['flow_data']):
            return self(**session.get_flow_data())

    def set_factory(self, factory):
        self.factory = factory

    def is_last_step(self):
        if self.next_step is None:
            return True

        return False

    def step_key(self):
        if isinstance(self.handler, types.FunctionType):
            key = self.unique_id or self.handler.__name__
        else:
            key = self.unique_id or self.handler.__name__()

        return "%s" % key




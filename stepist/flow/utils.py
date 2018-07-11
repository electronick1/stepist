import inspect

__all__ = ['StopFlowFlag']


class StopFlowFlag(Exception):

    def __init__(self, reason=''):
        self.reason = reason
        super(StopFlowFlag, self).__init__()


class AttrDict(dict):

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError('%s not found' % name)

    def __setattr__(self, name, value):
        self[name] = value

    @property
    def __members__(self):
        return self.keys()


def validate_handler_data(handler, data):
    spec = inspect.getfullargspec(handler)

    args = spec.args
    if spec.varkw:
        return data

    handler_data = {k:v for k,v in data.items() if k in args}

    return handler_data

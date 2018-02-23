
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

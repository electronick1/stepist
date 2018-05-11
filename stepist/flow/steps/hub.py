

class Hub(object):
    """
    Allow to push data in multiple steps
    """
    def __init__(self, *steps):
        self.steps = list(steps)

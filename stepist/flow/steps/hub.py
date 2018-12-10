import uuid
from stepist.flow.session import update_meta_data


class Hub(object):
    """
    Allow to push data in multiple steps
    """
    def __init__(self, *steps):
        self.steps = list(steps)

    def update_meta(self):
        hub_job_id = "%s:%s" % (uuid.uuid4(), len(self.steps))
        update_meta_data(hub_job_id=hub_job_id)



class BaseWorkerEngine(object):

    def add_job(self, step, data, result_reader, **kwargs):
        raise NotImplemented()

    def jobs_count(self, *steps):
        raise NotImplemented()

    def flush_queue(self, step):
        raise NotImplemented()

    def process(self, *steps):
        raise NotImplemented()

    def register_worker(self, handler):
        raise NotImplemented()

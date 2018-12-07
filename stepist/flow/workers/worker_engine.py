
_worker_engine = None


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


def setup_worker_engine(engine):
    global _worker_engine
    _worker_engine = engine
    return engine


def worker_engine():
    global _worker_engine
    return _worker_engine

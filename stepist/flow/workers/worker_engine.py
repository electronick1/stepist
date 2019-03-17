
NOT_IMPLEMENTED_DESC = "Not supported yet ..."


class BaseWorkerEngine(object):

    def add_job(self, step, data, result_reader, **kwargs):
        """
        Add data to queue/streaming service. 
        """
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

    def add_jobs(self, step, data_iter, result_reader):
        """
        Add batch of data to queue/streaming service in one transaction
        """
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

    def receive_job(self, step):
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

    def jobs_count(self, *steps):
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

    def flush_queue(self, step):
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

    def process(self, *steps):
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

    def register_worker(self, handler):
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

    def monitor_steps(self, step_keys, monitoring_for_sec):
        raise NotImplemented(NOT_IMPLEMENTED_DESC)

import redis
import uuid


HANDLERS = {}


class SimpleQueue:

    def __init__(self, pickler, redis_db):
        self.pickler = pickler
        self.redis_db = redis_db

    def process(self, jobs, wait_time_for_job=1, die_when_empty=False):
        keys = list(jobs.keys())
        while True:
            key, data = self.reserve_jobs(keys, wait_time_for_job)

            if data is None:
                if die_when_empty:
                    exit()
                continue

            handler = jobs[key]

            try:
                handler.receive_job(**data)
            except Exception:
                self.add_job(key, data)
                raise

    def add_job(self, job_key, data):
        data = {'data': data}

        self.redis_db.lpush(self.redis_queue_key(job_key),
                            self.pickler.dumps(data))

    def reserve_jobs(self, job_keys, wait_timeout):

        try:
            job_data = self.redis_db.blpop(map(self.redis_queue_key, job_keys),
                                           timeout=wait_timeout)

        except redis.exceptions.TimeoutError:
            return None, None

        if not job_data:
            return None, None

        key = job_data[0].decode("utf-8").split("step_flow::job::")[1]
        job_data = self.pickler.loads(job_data[1].decode("utf-8"))

        return key, job_data['data']

    def reserve_job(self, job_key):

        try:
            job_data = self.redis_db.lpop(self.redis_queue_key(job_key))
        except redis.exceptions.TimeoutError:
            return None

        if job_data is None:
            return None

        job_data = self.pickler.loads(job_data)
        return job_data['data']

    def flush_jobs(self, job_key):
        self.redis_db.delete(job_key)

    # -- HELPERS --

    def redis_queue_key(self, job_key):
        return "step_flow::job::%s" % job_key



